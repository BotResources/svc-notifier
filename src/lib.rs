//! svc-notifier on br-service-engine: one slice, `notifications`, and the kernel
//! (principal, faults). The engine owns the process, the transport, the intake
//! consumer, the realtime fan-out and the delivery ledger.
pub mod db;
pub mod kernel;
pub mod register;
pub mod slices;
