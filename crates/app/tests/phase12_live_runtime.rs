mod common;
#[path = "phase12_live_runtime/control.rs"]
mod control;

use binary_alpha_app::{broker, live};

#[test]
#[ignore = "requires an explicitly supplied isolated PostgreSQL configuration"]
fn postgres_control() {
    control::postgres_control();
}
#[path = "common/research.rs"]
mod fixture_config;
#[path = "phase12_live_runtime/resilience.rs"]
mod resilience;
#[path = "phase12_live_runtime/runtime.rs"]
mod runtime;
#[path = "phase12_live_runtime/support.rs"]
mod support;
