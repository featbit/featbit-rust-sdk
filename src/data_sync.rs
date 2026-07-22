mod connection;
mod protocol;
mod status;

#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::options::FbOptions;
use crate::store::SnapshotStore;
use crate::worker::{WorkerThread, WorkerWait};

pub(crate) use status::{StatusTracker, SyncStatus};

#[cfg(test)]
use protocol::{apply_message, connection_token, encode_number, streaming_url, ApplyResult};

#[derive(Debug)]
pub(crate) struct WebSocketDataSynchronizer {
    stop: watch::Sender<bool>,
    worker: WorkerThread,
    status: Arc<StatusTracker>,
    close_timeout: Duration,
    closed: AtomicBool,
}

impl WebSocketDataSynchronizer {
    pub(crate) fn start(
        options: FbOptions,
        store: Arc<SnapshotStore>,
        status: Arc<StatusTracker>,
    ) -> Option<Self> {
        let (stop, stop_receiver) = watch::channel(false);
        let worker_status = Arc::clone(&status);
        let close_timeout = options.close_timeout;
        let worker = WorkerThread::spawn("data synchronizer", move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            match runtime {
                Ok(runtime) => runtime.block_on(connection::run_sync_loop(
                    options,
                    store,
                    Arc::clone(&worker_status),
                    stop_receiver,
                )),
                Err(error) => {
                    log::error!("failed to create FeatBit WebSocket runtime: {error}");
                    worker_status.set(SyncStatus::Closed);
                }
            }
        });

        match worker {
            Ok(worker) => Some(Self {
                stop,
                worker,
                status,
                close_timeout,
                closed: AtomicBool::new(false),
            }),
            Err(error) => {
                log::error!("failed to start FeatBit WebSocket worker: {error}");
                status.set(SyncStatus::Closed);
                None
            }
        }
    }

    pub(crate) fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            let _ignored = self.worker.wait(Duration::ZERO);
            return;
        }
        let _ignored = self.stop.send(true);
        match self.worker.wait(self.close_timeout) {
            WorkerWait::Completed => {}
            WorkerWait::Panicked => {
                log::warn!("FeatBit WebSocket worker stopped after a panic");
            }
            WorkerWait::TimedOut => {
                log::warn!("FeatBit WebSocket worker did not close within the configured timeout");
            }
        }
        self.status.set(SyncStatus::Closed);
    }
}

impl Drop for WebSocketDataSynchronizer {
    fn drop(&mut self) {
        self.close();
    }
}
