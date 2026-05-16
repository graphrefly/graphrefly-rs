#!/bin/zsh
set -u
cd /Users/davidchenallio/src/graphrefly-rs
BIN_GLOB='per_subgraph_parallelism-[0-9a-f]*'
log(){ print -r -- "[$(date +%H:%M:%S)] $*"; }

# 1) Wait for the machine to quiesce. Requires the 1-min load to be
#    < 3.0 on TWO consecutive checks (avoids a transient dip triggering
#    measurement). Cap ~60 min so there's no rush freeing the machine.
log "WAITING for you to free the machine (close Claude.app/Chrome/Cursor or let it idle); auto-runs the clean A/B when 1-min load < 3.0 twice in a row."
ok=0
for i in {1..360}; do
  l1=$(uptime | sed -E 's/.*load averages?: *([0-9.]+).*/\1/')
  if awk "BEGIN{exit !($l1 < 3.0)}"; then
    ok=$((ok+1))
    log "load1=$l1 (<3.0, streak $ok/2)"
    [ "$ok" -ge 2 ] && { log "quiesced — starting clean A/B"; break; }
  else
    ok=0
    log "load1=$l1 (waiting for <3.0)"
  fi
  sleep 10
done

find_bin(){ ls -t target/release/deps/ 2>/dev/null | grep -E "^per_subgraph_parallelism-[0-9a-f]+$" | head -1; }

# 2) Prototype side (working tree currently has the prototype).
log "=== building PROTOTYPE release benches ==="
RUSTFLAGS="-D warnings" cargo build --release -p graphrefly-core --benches >/tmp/proto_ab_build1.log 2>&1
log "build1 exit=$? ; running prototype bench (save-baseline proto2)"
PB=$(find_bin); log "proto bin=$PB"
./target/release/deps/$PB --bench --save-baseline proto2 >/tmp/proto_ab_proto.log 2>&1
log "proto bench exit=$?"

# 3) Stash prototype -> baseline.
git stash push -m "in_tick proto AB2" -- crates/graphrefly-core/src/batch.rs crates/graphrefly-core/src/node.rs >/tmp/proto_ab_stash.log 2>&1
log "stash exit=$? (prototype removed; tree at committed baseline)"

# 4) Baseline side.
log "=== building BASELINE release benches ==="
RUSTFLAGS="-D warnings" cargo build --release -p graphrefly-core --benches >/tmp/proto_ab_build2.log 2>&1
log "build2 exit=$? ; running baseline bench (--baseline proto2)"
BB=$(find_bin); log "base bin=$BB"
./target/release/deps/$BB --bench --baseline proto2 >/tmp/proto_ab_base.log 2>&1
log "base bench exit=$?"

# 5) Restore prototype no matter what.
git stash pop >/tmp/proto_ab_pop.log 2>&1
log "stash pop exit=$? (prototype restored)"
git status --short | grep -E 'batch.rs|node.rs' && log "PROTOTYPE PRESENT" || log "WARN: prototype files not modified after pop"
log "=== DONE ==="
