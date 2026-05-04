//! Centralised timestamp utilities.
//!
//! Mirrors `~/src/graphrefly-ts/src/core/clock.ts` and the Python
//! `core/clock.py`. Convention: nanoseconds, `_ns` suffix, two clock domains
//! (monotonic for ordering, wall-clock for attribution) per CLAUDE.md
//! "Time utility rule."
//!
//! # Precision
//!
//! - [`monotonic_ns`] — `std::time::Instant` resolves to true nanoseconds on
//!   most modern platforms. Captured against a process-static origin (lazy via
//!   `OnceLock`) so values fit in `u64` for ~584 years of process uptime.
//! - [`wall_clock_ns`] — `SystemTime::now() - UNIX_EPOCH` truncated to `u64`
//!   nanoseconds; valid until ~2554. Beyond that, returns `u64::MAX`. Pre-epoch
//!   system clocks (rare misconfiguration) return 0.
//!
//! Mixing the two clocks is a bug — durations belong in monotonic; user-visible
//! timestamps belong in wall-clock. The Core dispatcher uses monotonic
//! exclusively.

use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Process-static monotonic origin, captured lazily on first call. All
/// monotonic timestamps are deltas from this origin so the result fits in
/// `u64` ns.
static MONOTONIC_ORIGIN: OnceLock<Instant> = OnceLock::new();

/// Monotonic nanosecond timestamp.
///
/// Use for: event ordering, durations, version counters, timeline events.
/// Never use for: user-visible timestamps (wall-clock attribution), since
/// monotonic time has no relation to civil time.
///
/// Saturates at `u64::MAX` after ~584 years of process uptime — a non-issue
/// in practice, and avoids unwrap on the `u128 -> u64` narrowing.
#[must_use]
pub fn monotonic_ns() -> u64 {
    let origin = MONOTONIC_ORIGIN.get_or_init(Instant::now);
    let elapsed = origin.elapsed().as_nanos();
    u64::try_from(elapsed).unwrap_or(u64::MAX)
}

/// Wall-clock nanosecond timestamp (Unix epoch).
///
/// Use for: user-visible timestamps, mutation attribution, cron emission.
/// Never use for: ordering or durations within the dispatcher (use
/// [`monotonic_ns`] — wall-clock can jump backwards on NTP correction).
///
/// Returns 0 for pre-epoch system clocks (misconfiguration). Saturates at
/// `u64::MAX` past ~2554 — same reasoning as [`monotonic_ns`].
#[must_use]
pub fn wall_clock_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn monotonic_ns_is_nondecreasing() {
        let a = monotonic_ns();
        let b = monotonic_ns();
        assert!(b >= a, "monotonic clock went backwards: {a} -> {b}");
    }

    #[test]
    fn monotonic_ns_advances() {
        let a = monotonic_ns();
        sleep(Duration::from_millis(2));
        let b = monotonic_ns();
        // 2ms = 2_000_000ns; allow generous slack for low-resolution clocks.
        assert!(
            b - a >= 1_000_000,
            "monotonic clock advanced too little: {a} -> {b}"
        );
    }

    #[test]
    fn wall_clock_ns_is_in_recent_epoch() {
        // Sanity: wall clock is past 2020 (1.577e18 ns) and before 2554 (saturate).
        let t = wall_clock_ns();
        assert!(t > 1_577_000_000_000_000_000, "wall clock too low: {t}");
        assert!(t < u64::MAX, "wall clock saturated: {t}");
    }

    #[test]
    fn monotonic_and_wall_clock_are_independent() {
        // Different clocks; values should differ in any sane environment.
        // The point of having two functions is that they can't be conflated.
        let m = monotonic_ns();
        let w = wall_clock_ns();
        assert_ne!(m, w);
    }
}
