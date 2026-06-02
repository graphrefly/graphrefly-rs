//! Sync source factories (D43/D40).
//!
//! Async/timer sources stay for a later Rust product slice; this first cut gives
//! the operator catalog a graph-visible depless source base.

use std::rc::Rc;

use crate::operators::Operator;
use crate::protocol::{AnyValue, Message};

/// of: emit one value and COMPLETE on activation.
pub fn of<T: Clone + 'static>(value: T) -> Operator<T> {
    Operator::new("of", move |ctx| {
        let out: AnyValue = Rc::new(value.clone());
        ctx.down(vec![Message::Data(out), Message::Complete]);
    })
}

/// from_iter: emit every item in order, then COMPLETE, on activation.
pub fn from_iter<T: Clone + 'static>(items: impl IntoIterator<Item = T>) -> Operator<T> {
    let values: Vec<T> = items.into_iter().collect();
    Operator::new("fromIter", move |ctx| {
        for value in &values {
            let out: AnyValue = Rc::new(value.clone());
            ctx.down(vec![Message::Data(out)]);
        }
        ctx.down(vec![Message::Complete]);
    })
}
