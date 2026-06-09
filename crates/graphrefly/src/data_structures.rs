//! Reactive data structures (B53 / D54 / D60), starting with `reactive_list`.
//!
//! These are per-language product surfaces (D6/D24), not conformance scenarios.
//! The substrate pull behavior is reused through `NodeOpts::pull_id`; no protocol
//! tier/message semantics live here.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::graph::{Graph, GraphNodeOpts, TopologyGroupOptions};
use crate::node::{Node, NodeOpts};
use crate::operators::{init_node, Operator};
use crate::protocol::{LockId, Message};

type Disposer = Box<dyn FnOnce()>;
type DisposerSlots = Rc<RefCell<Vec<Option<Disposer>>>>;
type ViewDisposeAction = Rc<dyn Fn()>;
type ViewDisposeHook = Box<dyn FnOnce()>;
type PageView<T> = ReactiveView<LogChange<T>, Vec<T>>;
type PageMemo<T> = Rc<RefCell<Vec<(String, PageView<T>)>>>;
type IndexRangeView<K, S, V> = ReactiveView<IndexChange<K, S, V>, Vec<V>>;
type IndexRangeMemo<K, S, V> = Rc<RefCell<Vec<((K, K), IndexRangeView<K, S, V>)>>>;
type MapSelectView<K, V> = ReactiveView<MapChange<K, V>, BTreeMap<K, V>>;
type MapSelectPredicate<K, V> = Rc<dyn Fn(&V, &K) -> bool>;
type MapSelectMemo<K, V> = Rc<RefCell<Vec<(MapSelectPredicate<K, V>, MapSelectView<K, V>)>>>;

static VIEW_PULL_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
struct ViewFactories {
    group: &'static str,
    delta: &'static str,
    snapshot: &'static str,
}

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

#[derive(Debug, Clone, PartialEq)]
pub enum LogChange<T> {
    Append { value: T },
    AppendMany { values: Vec<T> },
    TrimHead { n: usize },
    Clear { count: usize },
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexChange<K, S, V> {
    Upsert { primary: K, secondary: S, value: V },
    Delete { primary: K },
    Clear { count: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapChange<K, V> {
    Set { key: K, value: V },
    Delete { key: K, previous: V },
    Clear { count: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRow<K, S, V> {
    pub primary: K,
    pub secondary: S,
    pub value: V,
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

#[derive(Clone, Default)]
pub struct ReactiveLogOptions {
    pub name: Option<String>,
    pub graph: Option<Graph>,
    pub max_size: Option<usize>,
}

#[derive(Clone, Default)]
pub struct ReactiveIndexOptions {
    pub name: Option<String>,
    pub graph: Option<Graph>,
}

#[derive(Clone, Default)]
pub struct ReactiveMapOptions {
    pub name: Option<String>,
    pub graph: Option<Graph>,
}

impl fmt::Debug for ReactiveIndexOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReactiveIndexOptions")
            .field("name", &self.name)
            .field("graph", &self.graph.as_ref().map(|g| g.name()))
            .finish()
    }
}

impl fmt::Debug for ReactiveMapOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReactiveMapOptions")
            .field("name", &self.name)
            .field("graph", &self.graph.as_ref().map(|g| g.name()))
            .finish()
    }
}

impl ReactiveIndexOptions {
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
}

impl ReactiveMapOptions {
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
}

impl fmt::Debug for ReactiveLogOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReactiveLogOptions")
            .field("name", &self.name)
            .field("graph", &self.graph.as_ref().map(|g| g.name()))
            .field("max_size", &self.max_size)
            .finish()
    }
}

impl ReactiveLogOptions {
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
struct LogBackend<T> {
    buf: Rc<RefCell<Vec<T>>>,
    version: Rc<Cell<u64>>,
    max_size: Option<usize>,
}

#[derive(Clone)]
struct IndexBackend<K, S, V> {
    rows: Rc<RefCell<BTreeMap<K, (S, V)>>>,
    version: Rc<Cell<u64>>,
}

#[derive(Clone)]
struct MapBackend<K, V> {
    rows: Rc<RefCell<BTreeMap<K, V>>>,
    version: Rc<Cell<u64>>,
}

impl<K: Clone + Ord, S: Clone + Ord, V: Clone> IndexBackend<K, S, V> {
    fn new(initial: Vec<IndexRow<K, S, V>>) -> Self {
        let rows = initial
            .into_iter()
            .map(|row| (row.primary, (row.secondary, row.value)))
            .collect();
        Self {
            rows: Rc::new(RefCell::new(rows)),
            version: Rc::new(Cell::new(0)),
        }
    }

    fn version(&self) -> u64 {
        self.version.get()
    }

    fn bump(&self) {
        self.version.set(self.version.get().wrapping_add(1));
    }

    fn instance_token(&self) -> usize {
        Rc::as_ptr(&self.rows) as usize
    }

    fn len(&self) -> usize {
        self.rows.borrow().len()
    }

    fn has(&self, primary: &K) -> bool {
        self.rows.borrow().contains_key(primary)
    }

    fn get(&self, primary: &K) -> Option<V> {
        self.rows
            .borrow()
            .get(primary)
            .map(|(_, value)| value.clone())
    }

    fn snapshot(&self) -> Vec<IndexRow<K, S, V>> {
        let mut rows = self
            .rows
            .borrow()
            .iter()
            .map(|(primary, (secondary, value))| IndexRow {
                primary: primary.clone(),
                secondary: secondary.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            a.secondary
                .cmp(&b.secondary)
                .then_with(|| a.primary.cmp(&b.primary))
        });
        rows
    }

    fn range_by_primary(&self, start: &K, end: &K) -> Vec<V> {
        if start >= end {
            return Vec::new();
        }
        self.rows
            .borrow()
            .range(start.clone()..end.clone())
            .map(|(_, (_, value))| value.clone())
            .collect()
    }

    fn upsert(&self, primary: K, secondary: S, value: V) {
        self.rows.borrow_mut().insert(primary, (secondary, value));
        self.bump();
    }

    fn delete(&self, primary: &K) -> bool {
        let removed = self.rows.borrow_mut().remove(primary).is_some();
        if removed {
            self.bump();
        }
        removed
    }

    fn clear(&self) -> usize {
        let mut rows = self.rows.borrow_mut();
        let count = rows.len();
        if count == 0 {
            return 0;
        }
        rows.clear();
        self.bump();
        count
    }
}

impl<K: Clone + Ord, V: Clone> MapBackend<K, V> {
    fn new(initial: Vec<(K, V)>) -> Self {
        Self {
            rows: Rc::new(RefCell::new(initial.into_iter().collect())),
            version: Rc::new(Cell::new(0)),
        }
    }

    fn version(&self) -> u64 {
        self.version.get()
    }

    fn bump(&self) {
        self.version.set(self.version.get().wrapping_add(1));
    }

    fn instance_token(&self) -> usize {
        Rc::as_ptr(&self.rows) as usize
    }

    fn len(&self) -> usize {
        self.rows.borrow().len()
    }

    fn has(&self, key: &K) -> bool {
        self.rows.borrow().contains_key(key)
    }

    fn get(&self, key: &K) -> Option<V> {
        self.rows.borrow().get(key).cloned()
    }

    fn snapshot(&self) -> BTreeMap<K, V> {
        self.rows.borrow().clone()
    }

    fn selected_snapshot(&self, predicate: &MapSelectPredicate<K, V>) -> BTreeMap<K, V> {
        self.rows
            .borrow()
            .iter()
            .filter(|(key, value)| predicate(value, key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    fn set(&self, key: K, value: V) {
        self.rows.borrow_mut().insert(key, value);
        self.bump();
    }

    fn delete(&self, key: &K) -> Option<V> {
        let previous = self.rows.borrow_mut().remove(key);
        if previous.is_some() {
            self.bump();
        }
        previous
    }

    fn clear(&self) -> usize {
        let mut rows = self.rows.borrow_mut();
        let count = rows.len();
        if count == 0 {
            return 0;
        }
        rows.clear();
        self.bump();
        count
    }
}

impl<T: Clone> LogBackend<T> {
    fn new(initial: Vec<T>, max_size: Option<usize>) -> Self {
        let mut initial = initial;
        trim_head_overflow(&mut initial, max_size);
        Self {
            buf: Rc::new(RefCell::new(initial)),
            version: Rc::new(Cell::new(0)),
            max_size,
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

    fn append(&self, value: T) -> usize {
        self.buf.borrow_mut().push(value);
        self.bump();
        self.enforce_max_size()
    }

    fn append_many(&self, values: &[T]) -> usize {
        if values.is_empty() {
            return 0;
        }
        self.buf.borrow_mut().extend_from_slice(values);
        self.bump();
        self.enforce_max_size()
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

    fn trim_head(&self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        let mut buf = self.buf.borrow_mut();
        let removed = n.min(buf.len());
        if removed == 0 {
            return 0;
        }
        buf.drain(0..removed);
        self.bump();
        removed
    }

    fn enforce_max_size(&self) -> usize {
        let mut buf = self.buf.borrow_mut();
        let removed = trim_head_overflow(&mut buf, self.max_size);
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

#[derive(Clone)]
pub struct ReactiveLog<T> {
    pub delta: Node<LogChange<T>>,
    pub snapshot: Node<Vec<T>>,
    pub pull_id: LockId,
    backend: LogBackend<T>,
    graph: Option<Graph>,
    id_prefix: String,
    bind_seq: Rc<Cell<usize>>,
    page_seq: Rc<Cell<usize>>,
    page_memo: PageMemo<T>,
    disposers: DisposerSlots,
}

pub struct ReactiveView<C, S> {
    pub delta: Node<C>,
    pub snapshot: Node<S>,
    pub pull_id: LockId,
    disposed: Rc<Cell<bool>>,
    dispose_action: ViewDisposeAction,
}

#[derive(Clone)]
pub struct ReactiveIndex<K, S, V> {
    pub delta: Node<IndexChange<K, S, V>>,
    pub snapshot: Node<Vec<IndexRow<K, S, V>>>,
    pub pull_id: LockId,
    backend: IndexBackend<K, S, V>,
    graph: Option<Graph>,
    id_prefix: String,
    range_seq: Rc<Cell<usize>>,
    range_memo: IndexRangeMemo<K, S, V>,
}

#[derive(Clone)]
pub struct ReactiveMap<K, V> {
    pub delta: Node<MapChange<K, V>>,
    pub snapshot: Node<BTreeMap<K, V>>,
    pub pull_id: LockId,
    backend: MapBackend<K, V>,
    graph: Option<Graph>,
    id_prefix: String,
    select_seq: Rc<Cell<usize>>,
    select_memo: MapSelectMemo<K, V>,
}

impl<K, S, V> ReactiveIndex<K, S, V>
where
    K: Clone + Ord + fmt::Debug + 'static,
    S: Clone + Ord + 'static,
    V: Clone + 'static,
{
    pub fn new(initial: Vec<IndexRow<K, S, V>>, options: ReactiveIndexOptions) -> Self {
        let backend = IndexBackend::new(initial);
        let token = backend.instance_token();
        let id_prefix = options
            .name
            .clone()
            .unwrap_or_else(|| format!("reactiveIndex@{token:x}"));
        let pull_id = LockId::new(format!(
            "{id_prefix}.snapshot@{:x}",
            backend.instance_token()
        ));
        let delta = make_index_delta_node::<K, S, V>(options.graph.as_ref(), &id_prefix);
        let snapshot = make_index_snapshot_node(
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
            graph: options.graph,
            id_prefix,
            range_seq: Rc::new(Cell::new(0)),
            range_memo: Rc::new(RefCell::new(Vec::new())),
        }
    }

    pub fn len(&self) -> usize {
        self.backend.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn has(&self, primary: &K) -> bool {
        self.backend.has(primary)
    }

    pub fn get(&self, primary: &K) -> Option<V> {
        self.backend.get(primary)
    }

    pub fn to_vec(&self) -> Vec<IndexRow<K, S, V>> {
        self.backend.snapshot()
    }

    pub fn range_by_primary(&self, start: &K, end: &K) -> Vec<V> {
        self.backend.range_by_primary(start, end)
    }

    pub fn upsert(&self, primary: K, secondary: S, value: V) {
        self.backend
            .upsert(primary.clone(), secondary.clone(), value.clone());
        self.delta
            .down(vec![Message::Data(Rc::new(IndexChange::Upsert {
                primary,
                secondary,
                value,
            }))]);
    }

    pub fn delete(&self, primary: &K) {
        if self.backend.delete(primary) {
            self.delta
                .down(vec![Message::Data(Rc::new(IndexChange::Delete {
                    primary: primary.clone(),
                }
                    as IndexChange<K, S, V>))]);
        }
    }

    pub fn clear(&self) {
        let count = self.backend.clear();
        if count > 0 {
            self.delta.down(vec![Message::Data(Rc::new(
                IndexChange::Clear { count } as IndexChange<K, S, V>
            ))]);
        }
    }

    pub fn range(&self, start: K, end: K) -> ReactiveView<IndexChange<K, S, V>, Vec<V>> {
        let key = (start.clone(), end.clone());
        if let Some((_, view)) = self
            .range_memo
            .borrow()
            .iter()
            .find(|(existing, _)| existing == &key)
        {
            return view.clone();
        }
        let name = self.graph.as_ref().map(|_| {
            let next = self.range_seq.get();
            self.range_seq.set(next + 1);
            format!("{}.range#{next}", self.id_prefix)
        });
        let backend = self.backend.clone();
        let materialize = move || backend.range_by_primary(&start, &end);
        let memo = self.range_memo.clone();
        let dispose_key = key.clone();
        let view = light_reactive_view::<IndexChange<K, S, V>, Vec<V>, _>(
            &self.delta,
            self.graph.as_ref(),
            ViewFactories {
                group: "reactiveIndex.range",
                delta: "reactiveIndex.range.delta",
                snapshot: "reactiveIndex.range.snapshot",
            },
            name,
            materialize,
            Some(Box::new(move || {
                memo.borrow_mut()
                    .retain(|(existing, _)| existing != &dispose_key);
            })),
        );
        self.range_memo.borrow_mut().push((key, view.clone()));
        view
    }

    pub fn dispose(&self) {
        let views = self
            .range_memo
            .borrow()
            .iter()
            .map(|(_, view)| view.clone())
            .collect::<Vec<_>>();
        for view in views {
            view.dispose();
        }
        self.range_memo.borrow_mut().clear();
    }
}

impl<K, V> ReactiveMap<K, V>
where
    K: Clone + Ord + fmt::Debug + 'static,
    V: Clone + 'static,
{
    pub fn new(initial: Vec<(K, V)>, options: ReactiveMapOptions) -> Self {
        let backend = MapBackend::new(initial);
        let token = backend.instance_token();
        let id_prefix = options
            .name
            .clone()
            .unwrap_or_else(|| format!("reactiveMap@{token:x}"));
        let pull_id = LockId::new(format!(
            "{id_prefix}.snapshot@{:x}",
            backend.instance_token()
        ));
        let delta = make_map_delta_node::<K, V>(options.graph.as_ref(), &id_prefix);
        let snapshot = make_map_snapshot_node(
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
            graph: options.graph,
            id_prefix,
            select_seq: Rc::new(Cell::new(0)),
            select_memo: Rc::new(RefCell::new(Vec::new())),
        }
    }

    pub fn len(&self) -> usize {
        self.backend.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn has(&self, key: &K) -> bool {
        self.backend.has(key)
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.backend.get(key)
    }

    pub fn to_map(&self) -> BTreeMap<K, V> {
        self.backend.snapshot()
    }

    pub fn set(&self, key: K, value: V) {
        self.backend.set(key.clone(), value.clone());
        self.delta
            .down(vec![Message::Data(Rc::new(MapChange::Set { key, value }))]);
    }

    pub fn set_many(&self, entries: Vec<(K, V)>) {
        for (key, value) in entries {
            self.set(key, value);
        }
    }

    pub fn delete(&self, key: &K) {
        if let Some(previous) = self.backend.delete(key) {
            self.delta
                .down(vec![Message::Data(Rc::new(MapChange::Delete {
                    key: key.clone(),
                    previous,
                }
                    as MapChange<K, V>))]);
        }
    }

    pub fn delete_many(&self, keys: Vec<K>) {
        for key in keys {
            self.delete(&key);
        }
    }

    pub fn clear(&self) {
        let count = self.backend.clear();
        if count > 0 {
            self.delta.down(vec![Message::Data(Rc::new(
                MapChange::Clear { count } as MapChange<K, V>
            ))]);
        }
    }

    pub fn select<F>(&self, predicate: F) -> ReactiveView<MapChange<K, V>, BTreeMap<K, V>>
    where
        F: Fn(&V, &K) -> bool + 'static,
    {
        let predicate: MapSelectPredicate<K, V> = Rc::new(predicate);
        self.select_by(predicate)
    }

    pub fn select_by(
        &self,
        predicate: MapSelectPredicate<K, V>,
    ) -> ReactiveView<MapChange<K, V>, BTreeMap<K, V>> {
        if let Some((_, view)) = self
            .select_memo
            .borrow()
            .iter()
            .find(|(existing, _)| Rc::ptr_eq(existing, &predicate))
        {
            return view.clone();
        }
        let name = self.graph.as_ref().map(|_| {
            let next = self.select_seq.get();
            self.select_seq.set(next + 1);
            format!("{}.select#{next}", self.id_prefix)
        });
        let backend = self.backend.clone();
        let materialize_predicate = predicate.clone();
        let materialize = move || backend.selected_snapshot(&materialize_predicate);
        let memo = self.select_memo.clone();
        let dispose_predicate = predicate.clone();
        let view = light_reactive_view::<MapChange<K, V>, BTreeMap<K, V>, _>(
            &self.delta,
            self.graph.as_ref(),
            ViewFactories {
                group: "reactiveMap.select",
                delta: "reactiveMap.select.delta",
                snapshot: "reactiveMap.select.snapshot",
            },
            name,
            materialize,
            Some(Box::new(move || {
                memo.borrow_mut()
                    .retain(|(existing, _)| !Rc::ptr_eq(existing, &dispose_predicate));
            })),
        );
        self.select_memo
            .borrow_mut()
            .push((predicate, view.clone()));
        view
    }

    pub fn dispose(&self) {
        let views = self
            .select_memo
            .borrow()
            .iter()
            .map(|(_, view)| view.clone())
            .collect::<Vec<_>>();
        for view in views {
            view.dispose();
        }
        self.select_memo.borrow_mut().clear();
    }
}

impl<C, S> Clone for ReactiveView<C, S> {
    fn clone(&self) -> Self {
        Self {
            delta: self.delta.clone(),
            snapshot: self.snapshot.clone(),
            pull_id: self.pull_id.clone(),
            disposed: self.disposed.clone(),
            dispose_action: self.dispose_action.clone(),
        }
    }
}

impl<C, S> ReactiveView<C, S> {
    pub fn dispose(&self) {
        if self.disposed.get() {
            return;
        }
        (self.dispose_action)();
        self.disposed.set(true);
    }
}

impl<T: Clone + 'static> ReactiveLog<T> {
    pub fn new(initial: Vec<T>, options: ReactiveLogOptions) -> Self {
        if matches!(options.max_size, Some(0)) {
            panic!("reactive_log: max_size must be a positive integer");
        }
        let backend = LogBackend::new(initial, options.max_size);
        let token = backend.instance_token();
        let id_prefix = options
            .name
            .clone()
            .unwrap_or_else(|| format!("reactiveLog@{token:x}"));
        let pull_id = LockId::new(format!(
            "{id_prefix}.snapshot@{:x}",
            backend.instance_token()
        ));
        let delta = make_log_delta_node::<T>(options.graph.as_ref(), &id_prefix);
        let snapshot = make_log_snapshot_node(
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
            graph: options.graph,
            id_prefix,
            bind_seq: Rc::new(Cell::new(0)),
            page_seq: Rc::new(Cell::new(0)),
            page_memo: Rc::new(RefCell::new(Vec::new())),
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
        let trimmed = self.backend.append(value.clone());
        self.emit(LogChange::Append { value });
        self.emit_trimmed(trimmed);
    }

    pub fn append_many(&self, values: Vec<T>) {
        if values.is_empty() {
            return;
        }
        let trimmed = self.backend.append_many(&values);
        self.emit(LogChange::AppendMany { values });
        self.emit_trimmed(trimmed);
    }

    pub fn clear(&self) {
        let count = self.backend.clear();
        if count > 0 {
            self.emit(LogChange::Clear { count });
        }
    }

    pub fn trim_head(&self, n: usize) {
        let removed = self.backend.trim_head(n);
        self.emit_trimmed(removed);
    }

    pub fn tail(&self, n: usize) -> Node<Vec<T>> {
        let backend = self.backend.clone();
        Node::derived_opts(
            vec![self.delta.erased()],
            NodeOpts {
                partial: true,
                factory: Some("reactiveLog.tail".to_owned()),
                ..NodeOpts::default()
            },
            move |ctx| {
                let all = backend.snapshot();
                let start = all.len().saturating_sub(n);
                ctx.emit(all[start..].to_vec());
            },
        )
    }

    pub fn slice(&self, start: usize, stop: Option<usize>) -> Node<Vec<T>> {
        let backend = self.backend.clone();
        Node::derived_opts(
            vec![self.delta.erased()],
            NodeOpts {
                partial: true,
                factory: Some("reactiveLog.slice".to_owned()),
                ..NodeOpts::default()
            },
            move |ctx| {
                let all = backend.snapshot();
                let end = stop.unwrap_or(all.len()).min(all.len());
                let start = start.min(end);
                ctx.emit(all[start..end].to_vec());
            },
        )
    }

    pub fn page(&self, offset: usize, limit: usize) -> ReactiveView<LogChange<T>, Vec<T>> {
        let key = format!("{offset}:{limit}");
        if let Some((_, view)) = self
            .page_memo
            .borrow()
            .iter()
            .find(|(existing, _)| existing == &key)
        {
            return view.clone();
        }
        let name = self.graph.as_ref().map(|_| {
            let next = self.page_seq.get();
            self.page_seq.set(next + 1);
            format!("{}.page#{next}", self.id_prefix)
        });
        let backend = self.backend.clone();
        let materialize = move || {
            let all = backend.snapshot();
            let start = offset.min(all.len());
            let end = offset.saturating_add(limit).min(all.len());
            all[start..end].to_vec()
        };
        let memo = self.page_memo.clone();
        let dispose_key = key.clone();
        let view = light_reactive_view::<LogChange<T>, Vec<T>, _>(
            &self.delta,
            self.graph.as_ref(),
            ViewFactories {
                group: "reactiveLog.page",
                delta: "reactiveLog.page.delta",
                snapshot: "reactiveLog.page.snapshot",
            },
            name,
            materialize,
            Some(Box::new(move || {
                memo.borrow_mut()
                    .retain(|(existing, _)| existing != &dispose_key);
            })),
        );
        self.page_memo.borrow_mut().push((key, view.clone()));
        view
    }

    pub fn scan<A: Clone + 'static, F: Fn(A, &T) -> A + 'static>(
        &self,
        initial: A,
        step: F,
    ) -> Node<A> {
        let backend = self.backend.clone();
        Node::derived_opts(
            vec![self.delta.erased()],
            NodeOpts {
                partial: true,
                factory: Some("reactiveLog.scan".to_owned()),
                ..NodeOpts::default()
            },
            move |ctx| {
                let changes = ctx.batch::<LogChange<T>>(0);
                let appended = changes
                    .iter()
                    .map(|change| match change.as_ref() {
                        LogChange::Append { .. } => 1,
                        LogChange::AppendMany { values } => values.len(),
                        LogChange::TrimHead { .. } | LogChange::Clear { .. } => 0,
                    })
                    .sum::<usize>();
                let reset_change = changes.iter().any(|change| {
                    matches!(
                        change.as_ref(),
                        LogChange::TrimHead { .. } | LogChange::Clear { .. }
                    )
                });
                let previous = ctx.state_get::<LogScanState<A>>();
                let mut state = previous
                    .as_ref()
                    .map(|s| LogScanState {
                        acc: s.acc.clone(),
                        processed: s.processed,
                    })
                    .unwrap_or_else(|| LogScanState {
                        acc: initial.clone(),
                        processed: 0,
                    });
                let all = backend.snapshot();
                if reset_change || all.len() < state.processed {
                    state.acc = initial.clone();
                    state.processed = 0;
                }
                if appended > 0 && all.len() < state.processed.saturating_add(appended) {
                    ctx.state_set(state);
                    return;
                }
                for value in all.iter().skip(state.processed) {
                    state.acc = step(state.acc, value);
                }
                state.processed = all.len();
                let out = state.acc.clone();
                ctx.state_set(state);
                ctx.emit(out);
            },
        )
    }

    pub fn attach(&self, src: &Node<T>) -> Disposer {
        let graph = self.graph.as_ref().unwrap_or_else(|| {
            panic!("reactive_log.attach requires options.graph so the input fold is describe-visible (D61)")
        });
        let bind_idx = self.bind_seq.get();
        self.bind_seq.set(bind_idx + 1);
        let backend = self.backend.clone();
        let delta = self.delta.clone();
        let op = Operator::with_opts(
            "reactiveLog.bindSource",
            NodeOpts {
                partial: true,
                ..NodeOpts::default()
            },
            move |ctx| {
                for value in ctx.batch::<T>(0) {
                    let value = (*value).clone();
                    let trimmed = backend.append(value.clone());
                    delta.down(vec![Message::Data(Rc::new(LogChange::Append { value }))]);
                    emit_log_trimmed(&delta, trimmed);
                }
            },
        );
        let mut opts = GraphNodeOpts::named(format!("{}.bind#{bind_idx}", self.id_prefix));
        opts.meta
            .insert("kind".to_owned(), "collection_bind_source".to_owned());
        opts.meta
            .insert("collection".to_owned(), "reactiveLog".to_owned());
        let src_core = src.erased();
        let folder = graph.init_node::<LogChange<T>>(op, vec![src_core.clone()], opts);
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
        let views = self
            .page_memo
            .borrow()
            .iter()
            .map(|(_, view)| view.clone())
            .collect::<Vec<_>>();
        for view in views {
            view.dispose();
        }
        self.page_memo.borrow_mut().clear();
        for disposer in self.disposers.borrow_mut().iter_mut() {
            if let Some(disposer) = disposer.take() {
                disposer();
            }
        }
    }

    fn emit(&self, change: LogChange<T>) {
        self.delta.down(vec![Message::Data(Rc::new(change))]);
    }

    fn emit_trimmed(&self, n: usize) {
        emit_log_trimmed(&self.delta, n);
    }
}

struct LogScanState<A> {
    acc: A,
    processed: usize,
}

pub fn reactive_list<T: Clone + 'static>(
    initial: Vec<T>,
    options: ReactiveListOptions,
) -> ReactiveList<T> {
    ReactiveList::new(initial, options)
}

pub fn reactive_log<T: Clone + 'static>(
    initial: Vec<T>,
    options: ReactiveLogOptions,
) -> ReactiveLog<T> {
    ReactiveLog::new(initial, options)
}

pub fn reactive_index<K, S, V>(
    initial: Vec<IndexRow<K, S, V>>,
    options: ReactiveIndexOptions,
) -> ReactiveIndex<K, S, V>
where
    K: Clone + Ord + fmt::Debug + 'static,
    S: Clone + Ord + 'static,
    V: Clone + 'static,
{
    ReactiveIndex::new(initial, options)
}

pub fn reactive_map<K, V>(initial: Vec<(K, V)>, options: ReactiveMapOptions) -> ReactiveMap<K, V>
where
    K: Clone + Ord + fmt::Debug + 'static,
    V: Clone + 'static,
{
    ReactiveMap::new(initial, options)
}

pub fn merge_reactive_logs<T: Clone + 'static>(logs: Vec<ReactiveLog<T>>) -> Node<LogChange<T>> {
    let deps = logs
        .iter()
        .map(|log| log.delta.erased())
        .collect::<Vec<_>>();
    Node::derived_opts(
        deps,
        NodeOpts {
            partial: true,
            factory: Some("mergeReactiveLogs".to_owned()),
            ..NodeOpts::default()
        },
        move |ctx| {
            for i in 0..logs.len() {
                for change in ctx.batch::<LogChange<T>>(i) {
                    ctx.emit((*change).clone());
                }
            }
        },
    )
}

pub fn scan_log<T: Clone + 'static, A: Clone + 'static, F: Fn(A, &T) -> A + 'static>(
    log: &ReactiveLog<T>,
    initial: A,
    step: F,
) -> Node<A> {
    log.scan(initial, step)
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

fn make_log_delta_node<T: Clone + 'static>(
    graph: Option<&Graph>,
    id_prefix: &str,
) -> Node<LogChange<T>> {
    match graph {
        Some(graph) => graph.empty_source(
            "reactiveLog.delta",
            GraphNodeOpts::named(node_name(id_prefix, "delta")),
        ),
        None => Node::state_empty(),
    }
}

fn make_index_delta_node<K: Clone + Ord + 'static, S: Clone + Ord + 'static, V: Clone + 'static>(
    graph: Option<&Graph>,
    id_prefix: &str,
) -> Node<IndexChange<K, S, V>> {
    match graph {
        Some(graph) => graph.empty_source(
            "reactiveIndex.delta",
            GraphNodeOpts::named(node_name(id_prefix, "delta")),
        ),
        None => Node::state_empty(),
    }
}

fn make_map_delta_node<K: Clone + Ord + 'static, V: Clone + 'static>(
    graph: Option<&Graph>,
    id_prefix: &str,
) -> Node<MapChange<K, V>> {
    match graph {
        Some(graph) => graph.empty_source(
            "reactiveMap.delta",
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

fn make_log_snapshot_node<T: Clone + 'static>(
    graph: Option<&Graph>,
    id_prefix: &str,
    delta: &Node<LogChange<T>>,
    backend: &LogBackend<T>,
    pull_id: LockId,
) -> Node<Vec<T>> {
    let backend = backend.clone();
    let op = Operator::with_opts(
        "reactiveLog.snapshot",
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

fn make_index_snapshot_node<
    K: Clone + Ord + 'static,
    S: Clone + Ord + 'static,
    V: Clone + 'static,
>(
    graph: Option<&Graph>,
    id_prefix: &str,
    delta: &Node<IndexChange<K, S, V>>,
    backend: &IndexBackend<K, S, V>,
    pull_id: LockId,
) -> Node<Vec<IndexRow<K, S, V>>> {
    let backend = backend.clone();
    let op = Operator::with_opts(
        "reactiveIndex.snapshot",
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

fn make_map_snapshot_node<K: Clone + Ord + 'static, V: Clone + 'static>(
    graph: Option<&Graph>,
    id_prefix: &str,
    delta: &Node<MapChange<K, V>>,
    backend: &MapBackend<K, V>,
    pull_id: LockId,
) -> Node<BTreeMap<K, V>> {
    let backend = backend.clone();
    let op = Operator::with_opts(
        "reactiveMap.snapshot",
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

fn light_reactive_view<C: Clone + 'static, S: Clone + 'static, F: Fn() -> S + 'static>(
    parent_delta: &Node<C>,
    graph: Option<&Graph>,
    factories: ViewFactories,
    name: Option<String>,
    materialize_snapshot: F,
    on_dispose: Option<ViewDisposeHook>,
) -> ReactiveView<C, S> {
    let group = graph.map(|graph| {
        graph.topology_group_opts(TopologyGroupOptions::named(
            name.clone().unwrap_or_else(|| factories.group.to_owned()),
        ))
    });
    let delta_op = Operator::with_opts(
        factories.delta,
        NodeOpts {
            partial: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            for change in ctx.batch::<C>(0) {
                ctx.emit((*change).clone());
            }
        },
    );
    let delta = match group.as_ref() {
        Some(group) => {
            let mut opts =
                graph_node_opts_with_optional_name(name.as_ref().map(|n| format!("{n}.delta")));
            opts.meta
                .insert("kind".to_owned(), "collection_view_delta".to_owned());
            opts.meta
                .insert("factory".to_owned(), factories.group.to_owned());
            group.init_node(delta_op, vec![parent_delta.erased()], opts)
        }
        None => init_node(delta_op, vec![parent_delta.erased()], NodeOpts::default()),
    };
    let pull_id = LockId::new(match &name {
        Some(name) => format!("{name}.snapshot"),
        None => format!(
            "{}.snapshot#{}",
            factories.group,
            VIEW_PULL_SEQ.fetch_add(1, Ordering::Relaxed)
        ),
    });
    let snapshot_op = Operator::with_opts(
        factories.snapshot,
        NodeOpts {
            partial: true,
            pull_id: Some(pull_id.clone()),
            ..NodeOpts::default()
        },
        move |ctx| {
            ctx.emit(materialize_snapshot());
        },
    );
    let snapshot = match group.as_ref() {
        Some(group) => {
            let mut opts =
                graph_node_opts_with_optional_name(name.as_ref().map(|n| format!("{n}.snapshot")));
            opts.meta
                .insert("kind".to_owned(), "collection_view_snapshot".to_owned());
            opts.meta
                .insert("factory".to_owned(), factories.group.to_owned());
            group.init_node(snapshot_op, vec![delta.erased()], opts)
        }
        None => init_node(snapshot_op, vec![delta.erased()], NodeOpts::default()),
    };
    let delta_core = delta.erased();
    let snapshot_core = snapshot.erased();
    let group_for_dispose = group.clone();
    let on_dispose = Rc::new(RefCell::new(on_dispose));
    let dispose_action = Rc::new(move || {
        if let Some(group) = group_for_dispose.as_ref() {
            group.release_with_reason(factories.group);
        } else {
            assert!(
                snapshot_core.release_runtime_for_graph() && delta_core.release_runtime_for_graph(),
                "reactive view: cannot release runtime; view is not quiescent (D124)"
            );
        }
        if let Some(on_dispose) = on_dispose.borrow_mut().take() {
            on_dispose();
        }
    });
    ReactiveView {
        delta,
        snapshot,
        pull_id,
        disposed: Rc::new(Cell::new(false)),
        dispose_action,
    }
}

fn graph_node_opts_with_optional_name(name: Option<String>) -> GraphNodeOpts {
    match name {
        Some(name) => GraphNodeOpts::named(name),
        None => GraphNodeOpts::default(),
    }
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

fn emit_log_trimmed<T: Clone + 'static>(delta: &Node<LogChange<T>>, n: usize) {
    if n > 0 {
        delta.down(vec![Message::Data(Rc::new(LogChange::<T>::TrimHead { n }))]);
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
