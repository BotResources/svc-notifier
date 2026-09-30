// Shared fixtures of the notifier 1.1 oracle. Each tests/*.rs is a separate
// binary, so a helper looks dead from every binary that skips it.
#![allow(dead_code, unused_imports)]

mod outage;
mod stack;
mod storage;
mod stream;
mod wire;

pub use outage::PausedPostgres;
pub use stack::{ServiceInstance, TestContext, TestStack};
pub use storage::{NotificationRecord, deliver_rich_to, deliver_to};
pub use stream::{Delta, Session, instant};
pub use wire::*;

use std::time::Duration;

// How long a scenario waits for a fact it expects (a row, a delta, a recovery).
pub const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);
// How long a frame of the stream may take to arrive on a healthy session.
pub const SSE_TIMEOUT: Duration = Duration::from_secs(5);
// The quiet window that turns "nothing arrived yet" into "nothing arrives":
// an absence is only asserted after the fact it must not follow has committed.
pub const QUIET_WINDOW: Duration = Duration::from_secs(3);
