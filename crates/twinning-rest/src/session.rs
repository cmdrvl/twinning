//! Per-request session id synthesis for the stateless REST adapter.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Clone, Default)]
pub struct RestSessionIds {
    next_id: Arc<AtomicU64>,
}

impl RestSessionIds {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next_session_id(&self) -> String {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("rest-req-{id}")
    }
}

#[cfg(test)]
mod tests {
    use super::RestSessionIds;

    #[test]
    fn session_ids_are_monotonic_and_rest_scoped() {
        let ids = RestSessionIds::new();

        assert_eq!(ids.next_session_id(), "rest-req-1");
        assert_eq!(ids.next_session_id(), "rest-req-2");
    }
}
