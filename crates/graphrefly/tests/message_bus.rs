//! The message bus's public surface, in process. These replace fixtures that each
//! wrote a temporary crate and ran `cargo run` on it (review §4.2): what they checked
//! was that this code compiles, which the test binary itself now checks, and the
//! old `dynamicHub` alias's absence is a compile_fail doctest on `message_bus`.

use graphrefly::{graph, message_bus, MessageBusOptions, MessageBusSubscriptionOptions};

#[test]
fn message_bus_surface_is_public() {
    let g = graph();
    let bus = message_bus::<String>(&g, MessageBusOptions::default().with_topics(["orders"]));
    let _ = (&bus.commands, &bus.messages, &bus.status, &bus.issues);
    let catalog = bus.catalog();
    let dead = bus.dead_letter();
    let topic = bus.topic("orders");
    let _ = (
        &catalog.snapshot_request,
        &dead.snapshot_request,
        &topic.snapshot_request,
    );
}

#[test]
fn message_bus_subscription_cursor_and_commands_are_public() {
    let g = graph();
    let bus = message_bus::<String>(&g, MessageBusOptions::default().with_topics(["orders"]));
    let sub = bus.subscription(MessageBusSubscriptionOptions::new("orders", "worker-a"));
    let _ = (
        &sub.available,
        &sub.cursor,
        &sub.status,
        &sub.issues,
        &sub.available_request,
    );
    let _ = sub.ack(1, None);
    let _ = sub.seek(2, None);
    let _ = sub.close(None);
}

#[test]
fn message_bus_rejection_and_retention_paths_run() {
    let g = graph();
    let bus = message_bus::<String>(&g, MessageBusOptions::default().with_topics(["orders"]));
    let _keep = bus.status.subscribe(|_| {});
    let _ = bus.publish("orders", "ok".to_owned(), None, None, None);
    let _ = bus.publish("missing", "x".to_owned(), None, None, None);
    let sub = bus.subscription(MessageBusSubscriptionOptions::new("orders", "worker-a"));
    let _ = sub.ack(1, None);
    let _ = sub.seek(1, None);
    let _ = sub.close(None);
    assert!(bus.status.cache().is_some(), "the bus reports its status");
}
