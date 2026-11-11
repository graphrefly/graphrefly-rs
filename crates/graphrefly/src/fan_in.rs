//! An input that host-attached sources feed (graphrefly-rs:D8).
//!
//! A bundle whose input takes values from sources the host attaches and detaches at
//! runtime keeps that input a computed node with no declared dependency. The node
//! rewires itself to the attached sources for its lifetime (ctx.rewire,
//! R-request-order@2): its setup keeps its ctx, an attach or detach rewires through
//! that kept ctx, and each run checks that its dependencies are the attached sources.
//! A closing lifetime puts the declared (empty) list back; the next lifetime's first
//! run rewires again. There is no host-level rewire. Kept sources keep their records,
//! so an attach delivers only the added source's snapshot.

use std::cell::RefCell;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;

use crate::ctx::{Ctx, DeferredCtx};
use crate::node::{Core, NodeOpts};

/// The attached sources of one input and the input's kept ctx.
#[derive(Default)]
pub(crate) struct FanInSources {
    sources: RefCell<Vec<Core>>,
    kept: Rc<RefCell<Option<DeferredCtx>>>,
}

impl FanInSources {
    pub(crate) fn new() -> Rc<Self> {
        Rc::new(Self::default())
    }

    /// The input node's options: its setup keeps its ctx for the lifetime, and its
    /// cleanup lets the kept ctx go.
    pub(crate) fn node_opts(&self, base: NodeOpts) -> NodeOpts {
        let kept = self.kept.clone();
        base.with_setup(move |ctx| {
            *kept.borrow_mut() = Some(ctx.defer());
            let kept = kept.clone();
            crate::lifetime::cleanup(move |_| {
                let released = kept.borrow_mut().take();
                drop(released);
            })
        })
    }

    /// The input node's function: rewires to the attached sources when they differ
    /// from its dependencies, and forwards each source's values in order.
    pub(crate) fn body<T: Clone + 'static>(self: &Rc<Self>) -> impl Fn(&Ctx) + 'static {
        let me = self.clone();
        move |ctx| {
            let wanted = me.sources.borrow().clone();
            let current = ctx.dep_len() == wanted.len()
                && wanted.iter().enumerate().all(|(i, dep)| ctx.dep_is(i, dep));
            if !current {
                ctx.rewire(wanted);
            }
            for i in 0..ctx.dep_len() {
                for value in ctx.batch::<T>(i) {
                    ctx.emit((*value).clone());
                }
            }
        }
    }

    /// Attaches `source`; a source already attached is left as it is.
    ///
    /// # Panics
    ///
    /// Panics, attaching nothing, when the source would make the input depend on
    /// itself or belongs to another graph.
    pub(crate) fn attach(&self, source: Core) {
        if self.sources.borrow().iter().any(|s| s.ptr_eq(&source)) {
            return;
        }
        self.sources.borrow_mut().push(source.clone());
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| self.rewire())) {
            self.sources.borrow_mut().retain(|s| !s.ptr_eq(&source));
            resume_unwind(panic);
        }
    }

    /// Detaches `source`; a no-op if it is not attached.
    pub(crate) fn detach(&self, source: &Core) {
        let before = self.sources.borrow().len();
        self.sources.borrow_mut().retain(|s| !s.ptr_eq(source));
        if self.sources.borrow().len() != before {
            self.rewire();
        }
    }

    fn rewire(&self) {
        let sources = self.sources.borrow().clone();
        if let Some(kept) = self.kept.borrow().as_ref() {
            kept.rewire(sources);
        }
    }
}
