mod common;
#[path = "phase12_live_runtime/control.rs"]
mod control;

use binary_alpha_app::{broker, live};

#[test]
#[ignore = "requires an explicitly supplied isolated PostgreSQL configuration"]
fn postgres_control() {
    control::postgres_control();
}
