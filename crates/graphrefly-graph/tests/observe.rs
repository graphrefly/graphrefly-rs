//! `Graph::observe()` / `Graph::observe_all()` default sink-style tap
//! (canonical spec §3.6.2 default mode).

mod common;

use std::sync::{Arc, Mutex};

use common::binding;
use graphrefly_core::{HandleId, Message, Sink};
use graphrefly_graph::{Graph, GraphOps};

fn recording_sink() -> (Arc<Mutex<Vec<Message>>>, Sink) {
    let log: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
    let log_for_sink = log.clone();
    let sink: Sink = Arc::new(move |msgs: &[Message]| {
        log_for_sink.lock().unwrap().extend_from_slice(msgs);
    });
    (log, sink)
}

#[test]
fn observe_subscribe_taps_named_node() {
    let g = Graph::new("system", binding());
    let s = g.state("a", Some(HandleId::new(1))).unwrap();
    let (log, sink) = recording_sink();
    let _sub = g.observe("a").subscribe(sink);
    g.emit(s, HandleId::new(2));
    let log = log.lock().unwrap();
    // Handshake delivers [Start, Data(1)]; wave delivers [Dirty, Data(2)].
    assert!(log.iter().any(|m| matches!(m, Message::Start)));
    assert!(log
        .iter()
        .any(|m| matches!(m, Message::Data(h) if *h == HandleId::new(2))));
}

#[test]
fn observe_up_pause_pauses_target_node() {
    let g = Graph::new("system", binding());
    g.state("a", Some(HandleId::new(1))).unwrap();
    let observer = g.observe("a");
    let lock = g.alloc_lock_id();
    observer.pause(lock).unwrap();
    assert!(g.core().is_paused(observer.node_id()));
    observer.resume(lock).unwrap();
    assert!(!g.core().is_paused(observer.node_id()));
}

#[test]
fn observe_up_invalidate_clears_cache() {
    let g = Graph::new("system", binding());
    let s = g.state("a", Some(HandleId::new(7))).unwrap();
    g.observe("a").invalidate();
    assert_eq!(g.cache_of(s), graphrefly_core::NO_HANDLE);
}

#[test]
#[should_panic(expected = "Graph::node: no node at path `nope`")]
fn observe_unknown_path_panics() {
    let g = Graph::new("system", binding());
    let _ = g.observe("nope");
}

#[test]
fn observe_all_multicasts_to_every_named_node() {
    let g = Graph::new("system", binding());
    let a = g.state("a", Some(HandleId::new(1))).unwrap();
    let b = g.state("b", Some(HandleId::new(2))).unwrap();

    let log: Arc<Mutex<Vec<(String, Message)>>> = Arc::new(Mutex::new(Vec::new()));
    let log_for_sink = log.clone();
    let mut all = g.observe_all();
    let count = all.subscribe(move |path: &str, msgs: &[Message]| {
        let mut log = log_for_sink.lock().unwrap();
        for m in msgs {
            log.push((path.to_owned(), *m));
        }
    });
    assert_eq!(count, 2);

    g.emit(a, HandleId::new(10));
    g.emit(b, HandleId::new(20));
    let log = log.lock().unwrap();
    // Each emit produces Dirty + Data; with handshake on both we expect
    // Start + Data(initial) per node + 2 emit waves' Dirty + Data.
    assert!(log
        .iter()
        .any(|(p, m)| p == "a" && matches!(m, Message::Data(h) if *h == HandleId::new(10))));
    assert!(log
        .iter()
        .any(|(p, m)| p == "b" && matches!(m, Message::Data(h) if *h == HandleId::new(20))));
}

#[test]
fn observe_all_late_added_node_not_auto_subscribed_in_v1() {
    let g = Graph::new("system", binding());
    g.state("a", Some(HandleId::new(1))).unwrap();
    let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let log_for_sink = log.clone();
    let mut all = g.observe_all();
    all.subscribe(move |path: &str, _msgs: &[Message]| {
        log_for_sink.lock().unwrap().push(path.to_owned());
    });
    // Add a node AFTER subscribe — not picked up.
    let b = g.state("b", Some(HandleId::new(2))).unwrap();
    g.emit(b, HandleId::new(99));
    let log = log.lock().unwrap();
    assert!(!log.iter().any(|p| p == "b"));
    // 'a' however is observed (handshake delivers Start + Data(1)).
    assert!(log.iter().any(|p| p == "a"));
}

#[test]
fn dropping_observe_all_unsubscribes_all_sinks() {
    let g = Graph::new("system", binding());
    let s = g.state("a", Some(HandleId::new(1))).unwrap();
    let log: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let log_for_sink = log.clone();
        let mut all = g.observe_all();
        all.subscribe(move |_path, msgs| {
            log_for_sink.lock().unwrap().extend_from_slice(msgs);
        });
    } // `all` dropped → all subscriptions detached.
    g.emit(s, HandleId::new(99));
    let log = log.lock().unwrap();
    // We saw the handshake's [Start, Data(1)] before the drop, then the
    // emit happens with no live sinks — its Dirty + Data must NOT
    // appear.
    assert!(!log
        .iter()
        .any(|m| matches!(m, Message::Data(h) if *h == HandleId::new(99))));
}
