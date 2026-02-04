use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct CancellationContext {
    token: CancellationToken,
    reason: Arc<Mutex<Option<String>>>,
}

impl CancellationContext {
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            reason: Arc::new(Mutex::new(None)),
        }
    }

    pub fn cancel_with_reason(&self, reason: &str) {
        let mut reason_guard = self.reason.lock().unwrap();
        *reason_guard = Some(reason.to_string());
        self.token.cancel();
    }

    pub fn token(&self) -> CancellationToken { self.token.clone() }

    pub fn get_reason(&self) -> Option<String> {
        let reason_guard = self.reason.lock().unwrap();
        reason_guard.clone()
    }
}
