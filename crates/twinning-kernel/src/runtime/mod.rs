pub mod run_child;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execution {
    pub exit_code: u8,
    pub stdout: String,
}
