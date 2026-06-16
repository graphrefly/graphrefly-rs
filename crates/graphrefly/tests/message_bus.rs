use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn repo_crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after unix epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "graphrefly-rs-message-bus-{label}-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).expect("temp dir can be created");
    dir
}

fn cargo_bin() -> String {
    std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned())
}

fn write_fixture(dir: &Path, source: &str) {
    fs::create_dir_all(dir.join("src")).expect("fixture src dir");
    fs::write(
        dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "message-bus-gate"
version = "0.0.0"
edition = "2021"

[dependencies]
graphrefly = {{ package = "graphrefly-rs", path = "{}" }}
"#,
            repo_crate_root().display()
        ),
    )
    .expect("fixture manifest");
    fs::write(dir.join("src/main.rs"), source).expect("fixture source");
}

fn run_fixture(label: &str, source: &str) -> std::process::Output {
    let dir = temp_dir(label);
    write_fixture(&dir, source);

    Command::new(cargo_bin())
        .arg("run")
        .arg("--quiet")
        .arg("--manifest-path")
        .arg(dir.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", dir.join("target"))
        .output()
        .expect("cargo fixture can run")
}

fn assert_fixture_compiles(label: &str, source: &str) {
    let output = run_fixture(label, source);
    assert!(
        output.status.success(),
        "fixture {label} failed to compile/run\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_fixture_fails_with(label: &str, source: &str, needle: &str) {
    let output = run_fixture(label, source);
    assert!(
        !output.status.success(),
        "fixture {label} unexpectedly compiled and ran"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(needle),
        "fixture {label} failed, but stderr did not contain `{needle}`\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&output.stdout),
        stderr
    );
}

#[test]
fn dynamichub_camelcase_alias_is_not_public() {
    assert_fixture_fails_with(
        "dynamichub-alias",
        r#"
use graphrefly::dynamicHub;

fn main() {}
"#,
        "dynamicHub",
    );
}

#[test]
fn message_bus_v0_public_surface_compiles_when_implemented() {
    assert_fixture_compiles(
        "message-bus-surface",
        r#"
use graphrefly::{graph, message_bus, MessageBusOptions};

fn main() {
    let g = graph();
    let bus = message_bus::<String>(&g, MessageBusOptions::default().with_topics(["orders"]));

    let _ = &bus.commands;
    let _ = &bus.messages;
    let _ = &bus.status;
    let _ = &bus.issues;

    let catalog = bus.catalog();
    let dead = bus.dead_letter();
    let topic = bus.topic("orders");

    let _ = catalog.snapshot.pull_id().expect("catalog pull id");
    let _ = dead.snapshot.pull_id().expect("dead-letter pull id");
    let _ = topic.snapshot.pull_id().expect("topic pull id");
}
"#,
    );
}

#[test]
fn message_bus_subscription_cursor_and_command_facts_compile_when_implemented() {
    assert_fixture_compiles(
        "message-bus-subscription",
        r#"
use graphrefly::{graph, message_bus, MessageBusOptions, MessageBusSubscriptionOptions};

fn main() {
    let g = graph();
    let bus = message_bus::<String>(&g, MessageBusOptions::default().with_topics(["orders"]));

    let sub = bus.subscription(MessageBusSubscriptionOptions::new("orders", "worker-a"));

    let _ = &sub.available;
    let _ = &sub.cursor;
    let _ = &sub.status;
    let _ = &sub.issues;

    let _ = sub.available.pull_id().expect("available pull id");

    let _ = sub.ack(1, None);
    let _ = sub.seek(2, None);
    let _ = sub.close(None);
}
"#,
    );
}

#[test]
fn message_bus_rejection_and_retention_policy_facts_compile_when_implemented() {
    assert_fixture_compiles(
        "message-bus-policies",
        r#"
use graphrefly::{graph, message_bus, MessageBusOptions, MessageBusSubscriptionOptions};

fn main() {
    let g = graph();
    let bus = message_bus::<String>(&g, MessageBusOptions::default().with_topics(["orders"]));

    let _ = bus.publish("orders", "ok".to_owned(), None, None, None);
    let _ = bus.publish("missing", "x".to_owned(), None, None, None);

    let catalog = bus.catalog();
    let dead = bus.dead_letter();

    let _ = catalog.snapshot.pull_id().expect("catalog pull id");
    let _ = dead.snapshot.pull_id().expect("dead-letter pull id");

    let sub = bus.subscription(MessageBusSubscriptionOptions::new("orders", "worker-a"));
    let _ = sub.ack(1, None);
    let _ = sub.seek(1, None);
    let _ = sub.close(None);

    bus.commands.down(vec![]);
}
"#,
    );
}
