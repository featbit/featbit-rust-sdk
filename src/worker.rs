use std::io;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkerWait {
    Completed,
    Panicked,
    TimedOut,
}

#[derive(Debug)]
pub(crate) struct WorkerThread {
    name: &'static str,
    completion: Arc<CompletionState>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl WorkerThread {
    pub(crate) fn spawn<F>(name: &'static str, run: F) -> io::Result<Self>
    where
        F: FnOnce() + Send + 'static,
    {
        let completion = Arc::new(CompletionState::default());
        let worker_completion = Arc::clone(&completion);
        let handle = thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                let _completion = CompletionSignal(worker_completion);
                run();
            })?;

        Ok(Self {
            name,
            completion,
            handle: Mutex::new(Some(handle)),
        })
    }

    pub(crate) fn wait(&self, timeout: Duration) -> WorkerWait {
        {
            let mut completed = self.completion.completed.lock();
            if !*completed {
                let timed_out = self
                    .completion
                    .condition
                    .wait_for(&mut completed, timeout)
                    .timed_out();
                if timed_out && !*completed {
                    return WorkerWait::TimedOut;
                }
            }
        }

        let Some(handle) = self.handle.lock().take() else {
            return WorkerWait::Completed;
        };
        if handle.join().is_ok() {
            WorkerWait::Completed
        } else {
            WorkerWait::Panicked
        }
    }
}

impl Drop for WorkerThread {
    fn drop(&mut self) {
        let Some(handle) = self.handle.get_mut().take() else {
            return;
        };
        if handle.is_finished() {
            if handle.join().is_err() {
                log::warn!("FeatBit {} stopped after a worker panic", self.name);
            }
        } else {
            log::warn!(
                "FeatBit {} was still stopping when its final handle was dropped",
                self.name
            );
        }
    }
}

#[derive(Debug, Default)]
struct CompletionState {
    completed: Mutex<bool>,
    condition: Condvar,
}

struct CompletionSignal(Arc<CompletionState>);

impl Drop for CompletionSignal {
    fn drop(&mut self) {
        let mut completed = self.0.completed.lock();
        *completed = true;
        self.0.condition.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_is_reported_for_normal_exit_and_panic() {
        let normal =
            WorkerThread::spawn("test-normal-worker", || {}).expect("normal worker should start");
        assert_eq!(normal.wait(Duration::from_secs(1)), WorkerWait::Completed);

        let panicking = WorkerThread::spawn("test-panicking-worker", || panic!("test panic"))
            .expect("panicking worker should start");
        assert_eq!(panicking.wait(Duration::from_secs(1)), WorkerWait::Panicked);
    }
}
