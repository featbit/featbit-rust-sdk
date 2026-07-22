use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum SyncStatus {
    Starting = 0,
    Ready = 1,
    Stale = 2,
    Closed = 3,
}

#[derive(Debug)]
pub(crate) struct StatusTracker {
    status: AtomicU8,
    initialized: AtomicBool,
    wait_lock: Mutex<()>,
    wait_condition: Condvar,
}

impl StatusTracker {
    pub(crate) fn new(status: SyncStatus, initialized: bool) -> Self {
        Self {
            status: AtomicU8::new(status as u8),
            initialized: AtomicBool::new(initialized),
            wait_lock: Mutex::new(()),
            wait_condition: Condvar::new(),
        }
    }

    pub(crate) fn status(&self) -> SyncStatus {
        match self.status.load(Ordering::Acquire) {
            1 => SyncStatus::Ready,
            2 => SyncStatus::Stale,
            3 => SyncStatus::Closed,
            _ => SyncStatus::Starting,
        }
    }

    pub(crate) fn initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    pub(crate) fn set(&self, status: SyncStatus) {
        let _guard = self.wait_lock.lock();
        if status == SyncStatus::Ready {
            self.initialized.store(true, Ordering::Release);
        }
        let previous = self.status.swap(status as u8, Ordering::AcqRel);
        if previous != status as u8 || status == SyncStatus::Ready {
            self.wait_condition.notify_all();
        }
    }

    pub(crate) fn wait_until_initialized(&self, timeout: Duration) -> bool {
        if self.initialized() {
            return true;
        }

        let now = Instant::now();
        let deadline = now.checked_add(timeout).unwrap_or(now);
        let mut guard = self.wait_lock.lock();
        while !self.initialized() && self.status() != SyncStatus::Closed {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            if remaining.is_zero()
                || self
                    .wait_condition
                    .wait_for(&mut guard, remaining)
                    .timed_out()
            {
                break;
            }
        }
        self.initialized()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use super::{StatusTracker, SyncStatus};

    #[test]
    fn waiters_observe_ready_and_terminal_transitions() {
        let ready = Arc::new(StatusTracker::new(SyncStatus::Starting, false));
        let ready_setter = Arc::clone(&ready);
        let setter = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            ready_setter.set(SyncStatus::Ready);
        });
        assert!(ready.wait_until_initialized(Duration::from_secs(1)));
        setter.join().expect("status setter should finish");
        assert!(ready.initialized());

        let closed = StatusTracker::new(SyncStatus::Starting, false);
        closed.set(SyncStatus::Closed);
        assert!(!closed.wait_until_initialized(Duration::from_secs(1)));
    }
}
