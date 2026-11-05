//! `camus` admin binary.
//!
//! Phase 0 has nothing to administer beyond the broker's HTTP routes.
//! Later phases add `truncate-to-sealed` and membership changes here.
//! This binary does not report success for a command it cannot run.

fn main() {
    eprintln!("camus 0.1.0");
    eprintln!("no admin command is implemented. Phase 0 is the broker process, camus-broker.");
    std::process::exit(2);
}
