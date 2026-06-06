//! Reactive data structures (B53 / D54 / D60), starting with `reactive_list`.
//!
//! These are per-language product surfaces (D6/D24), not conformance scenarios.
//! The substrate pull behavior is reused through `NodeOpts::pull_id`; no protocol
//! tier/message semantics live here.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::rc::Rc;

use crate::graph::{Graph, GraphNodeOpts};
use crate::node::{Node, NodeOpts};
use crate::operators::{init_node, Operator};
use crate::protocol::{LockId, Message};

type Disposer = Box<dyn FnOnce()>;
type DisposerSlots = Rc<RefCell<Vec<Option<Disposer>>>>;

#[derive(Debug, Clone, PartialEq)]
pub enum ListChange<T> {
    Append { value: T },
    AppendMany { values: Vec<T> },
    Insert { index: usize, value: T },
    InsertMany { index: usize, values: Vec<T> },
    Pop { index: usize, value: T },
    TrimHead { n: usize },
    Clear { count: usize },
}

#[derive(Clone, Default)]
pub struct ReactiveListOptions {
    pub name: Option<String>,
    pub graph: Option<Graph>,
    pub max_size: Option<usize>,
}

impl fmt::Debug for ReactiveListOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReactiveListOptions")
            .field("name", &self.name)
            .field("graph", &self.graph.as_ref().map(|g| g.name()))
            .field("max_size", &self.max_size)
            .finish()
    }
}

impl ReactiveListOptions {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }

    pub fn graph(mut self, graph: Graph) -> Self {
        self.graph = Some(graph);
        self
    }

    pub fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = Some(max_size);
        self
    }
}

#[derive(Clone)]
struct ListBackend<T> {
    buf: Rc<RefCell<Vec<T>>>,
    version: Rc<Cell<u64>>,
}

impl<T: Clone> ListBackend<T> {
    fn new(initial: Vec<T>, max_size: Option<usize>) -> Self {
        let mut initial = initial;
        trim_head_overflow(&mut initial, max_size);
        Self {
            buf: Rc::new(RefCell::new(initial)),
            version: Rc::new(Cell::new(0)),
        }
    }

    fn version(&self) -> u64 {
        self.version.get()
    }

    fn bump(&self) {
        self.version.set(self.version.get().wrapping_add(1));
    }

    fn snapshot(&self) -> Vec<T> {
        self.buf.borrow().clone()
    }

    fn instance_token(&self) -> usize {
        Rc::as_ptr(&self.buf) as usize
    }

    fn len(&self) -> usize {
        self.buf.borrow().len()
    }

    fn at(&self, index: isize) -> Option<T> {
        let buf = self.buf.borrow();
        let i = normalize_read_index(index, buf.len())?;
        buf.get(i).cloned()
    }

    fn append(&self, value: T) {
        self.buf.borrow_mut().push(value);
        self.bump();
    }

    fn append_many(&self, values: &[T]) {
        if values.is_empty() {
            return;
        }
        self.buf.borrow_mut().extend_from_slice(values);
        self.bump();
    }

    fn insert(&self, index: usize, value: T) {
        let mut buf = self.buf.borrow_mut();
        assert!(
            index <= buf.len(),
            "insert: index {index} out of range [0, {}]",
            buf.len()
        );
        buf.insert(index, value);
        self.bump();
    }

    fn insert_many(&self, index: usize, values: &[T]) {
        let mut buf = self.buf.borrow_mut();
        assert!(
            index <= buf.len(),
            "insert_many: index {index} out of range [0, {}]",
            buf.len()
        );
        if values.is_empty() {
            return;
        }
        buf.splice(index..index, values.iter().cloned());
        self.bump();
    }

    fn pop(&self, index: Option<isize>) -> (usize, T) {
        let mut buf = self.buf.borrow_mut();
        assert!(!buf.is_empty(), "pop from empty list");
        let raw = index.unwrap_or(-1);
        let i = normalize_read_index(raw, buf.len())
            .unwrap_or_else(|| panic!("pop: index {raw} out of range"));
        let value = buf.remove(i);
        self.bump();
        (i, value)
    }

    fn clear(&self) -> usize {
        let mut buf = self.buf.borrow_mut();
        let n = buf.len();
        if n == 0 {
            return 0;
        }
        buf.clear();
        self.bump();
        n
    }

    fn enforce_max_size(&self, max_size: Option<usize>) -> usize {
        let mut buf = self.buf.borrow_mut();
        let removed = trim_head_overflow(&mut buf, max_size);
        if removed > 0 {
            self.bump();
        }
        removed
    }
}

#[derive(Clone)]
pub struct ReactiveList<T> {
    pub delta: Node<ListChange<T>>,
    pub snapshot: Node<Vec<T>>,
    pub pull_id: LockId,
    backend: ListBackend<T>,
    max_size: Option<usize>,
    graph: Option<Graph>,
    id_prefix: String,
    bind_seq: Rc<Cell<usize>>,
    disposers: DisposerSlots,
}

impl<T: Clone + 'static> ReactiveList<T> {
    pub fn new(initial: Vec<T>, options: ReactiveListOptions) -> Self {
        if matches!(options.max_size, Some(0)) {
            panic!("reactive_list: max_size must be a positive integer");
        }
        let backend = ListBackend::new(initial, options.max_size);
        let token = backend.instance_token();
        let id_prefix = options
            .name
            .clone()
            .unwrap_or_else(|| format!("reactiveList@{token:x}"));
        let pull_id = LockId::new(format!(
            "{id_prefix}.snapshot@{:x}",
            backend.instance_token()
        ));
        let delta = make_delta_node::<T>(options.graph.as_ref(), &id_prefix);
        let snapshot = make_snapshot_node(
            options.graph.as_ref(),
            &id_prefix,
            &delta,
            &backend,
            pull_id.clone(),
        );
        Self {
            delta,
            snapshot,
            pull_id,
            backend,
            max_size: options.max_size,
            graph: options.graph,
            id_prefix,
            bind_seq: Rc::new(Cell::new(0)),
            disposers: Rc::new(RefCell::new(Vec::new())),
        }
    }

    pub fn len(&self) -> usize {
        self.backend.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn at(&self, index: isize) -> Option<T> {
        self.backend.at(index)
    }

    pub fn to_vec(&self) -> Vec<T> {
        self.backend.snapshot()
    }

    pub fn append(&self, value: T) {
        self.backend.append(value.clone());
        self.emit(ListChange::Append { value });
        self.enforce_capacity();
    }

    pub fn append_many(&self, values: Vec<T>) {
        if values.is_empty() {
            return;
        }
        self.backend.append_many(&values);
        self.emit(ListChange::AppendMany { values });
        self.enforce_capacity();
    }

    pub fn insert(&self, index: usize, value: T) {
        self.backend.insert(index, value.clone());
        self.emit(ListChange::Insert { index, value });
        self.enforce_capacity();
    }

    pub fn insert_many(&self, index: usize, values: Vec<T>) {
        self.backend.insert_many(index, &values);
        if values.is_empty() {
            return;
        }
        self.emit(ListChange::InsertMany { index, values });
        self.enforce_capacity();
    }

    pub fn pop(&self, index: Option<isize>) -> T {
        let (index, value) = self.backend.pop(index);
        self.emit(ListChange::Pop {
            index,
            value: value.clone(),
        });
        value
    }

    pub fn clear(&self) {
        let count = self.backend.clear();
        if count > 0 {
            self.emit(ListChange::Clear { count });
        }
    }

    pub fn append_from(&self, src: &Node<T>) -> Disposer {
        let graph = self.graph.as_ref().unwrap_or_else(|| {
            panic!("reactive_list.append_from requires options.graph so the input fold is describe-visible (D61)")
        });
        let bind_idx = self.bind_seq.get();
        self.bind_seq.set(bind_idx + 1);
        let backend = self.backend.clone();
        let delta = self.delta.clone();
        let max_size = self.max_size;
        let op = Operator::with_opts(
            "reactiveList.bindSource",
            NodeOpts {
                partial: true,
                ..NodeOpts::default()
            },
            move |ctx| {
                for value in ctx.batch::<T>(0) {
                    let value = (*value).clone();
                    backend.append(value.clone());
                    delta.down(vec![Message::Data(Rc::new(ListChange::Append { value }))]);
                    emit_trimmed(&backend, &delta, max_size);
                }
            },
        );
        let mut opts = GraphNodeOpts::named(format!("{}.bind#{bind_idx}", self.id_prefix));
        opts.meta
            .insert("kind".to_owned(), "collection_bind_source".to_owned());
        opts.meta
            .insert("collection".to_owned(), "reactiveList".to_owned());
        let src_core = src.erased();
        let folder = graph.init_node::<ListChange<T>>(op, vec![src_core.clone()], opts);
        let unsub = folder.subscribe(|_| {});
        let slot = {
            let mut disposers = self.disposers.borrow_mut();
            let slot = disposers.len();
            disposers.push(Some(unsub));
            slot
        };
        let disposers = self.disposers.clone();
        let folder_for_dispose = folder.clone();
        Box::new(move || {
            folder_for_dispose.unsubscribe_dep(src_core, |_| {});
            if let Some(disposer) = disposers.borrow_mut().get_mut(slot).and_then(Option::take) {
                disposer();
            }
        })
    }

    pub fn dispose(&self) {
        for disposer in self.disposers.borrow_mut().iter_mut() {
            if let Some(disposer) = disposer.take() {
                disposer();
            }
        }
    }

    fn emit(&self, change: ListChange<T>) {
        self.delta.down(vec![Message::Data(Rc::new(change))]);
    }

    fn enforce_capacity(&self) {
        emit_trimmed(&self.backend, &self.delta, self.max_size);
    }
}

pub fn reactive_list<T: Clone + 'static>(
    initial: Vec<T>,
    options: ReactiveListOptions,
) -> ReactiveList<T> {
    ReactiveList::new(initial, options)
}

fn make_delta_node<T: Clone + 'static>(
    graph: Option<&Graph>,
    id_prefix: &str,
) -> Node<ListChange<T>> {
    match graph {
        Some(graph) => graph.empty_source(
            "reactiveList.delta",
            GraphNodeOpts::named(node_name(id_prefix, "delta")),
        ),
        None => Node::state_empty(),
    }
}

fn make_snapshot_node<T: Clone + 'static>(
    graph: Option<&Graph>,
    id_prefix: &str,
    delta: &Node<ListChange<T>>,
    backend: &ListBackend<T>,
    pull_id: LockId,
) -> Node<Vec<T>> {
    let backend = backend.clone();
    let op = Operator::with_opts(
        "reactiveList.snapshot",
        NodeOpts {
            partial: true,
            pull_id: Some(pull_id),
            ..NodeOpts::default()
        },
        move |ctx| {
            let version = backend.version();
            let last = ctx.state_get::<u64>().map(|v| *v);
            if last == Some(version) {
                return;
            }
            ctx.state_set(version);
            ctx.emit(backend.snapshot());
        },
    );
    match graph {
        Some(graph) => graph.init_node(
            op,
            vec![delta.erased()],
            GraphNodeOpts::named(node_name(id_prefix, "snapshot")),
        ),
        None => init_node(op, vec![delta.erased()], NodeOpts::default()),
    }
}

fn node_name(prefix: &str, suffix: &str) -> String {
    format!("{prefix}.{suffix}")
}

fn emit_trimmed<T: Clone + 'static>(
    backend: &ListBackend<T>,
    delta: &Node<ListChange<T>>,
    max_size: Option<usize>,
) {
    let n = backend.enforce_max_size(max_size);
    if n > 0 {
        delta.down(vec![Message::Data(Rc::new(ListChange::<T>::TrimHead {
            n,
        }))]);
    }
}

fn normalize_read_index(index: isize, len: usize) -> Option<usize> {
    let len = isize::try_from(len).ok()?;
    let i = if index >= 0 { index } else { len + index };
    (i >= 0 && i < len).then_some(i as usize)
}

fn trim_head_overflow<T>(buf: &mut Vec<T>, max_size: Option<usize>) -> usize {
    let Some(max_size) = max_size else {
        return 0;
    };
    assert!(max_size > 0, "max_size must be a positive integer");
    if buf.len() <= max_size {
        return 0;
    }
    let removed = buf.len() - max_size;
    buf.drain(0..removed);
    removed
}
