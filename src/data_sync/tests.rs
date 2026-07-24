mod protocol;
mod reconnect;
mod support;
mod synchronization;

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::bounded;
use tokio::sync::watch;

use super::{StatusTracker, SyncStatus, SyncWorkerStatusOnExit, WebSocketDataSynchronizer};
use crate::worker::{WorkerThread, WorkerWait};

#[test]
fn additional_close_waits_for_an_in_progress_sync_worker() {
    let (release_sender, release_receiver) = bounded(1);
    let worker = WorkerThread::spawn("test-sync-close-worker", move || {
        let _released = release_receiver.recv();
    })
    .expect("test worker should start");
    let (stop, _stop_receiver) = watch::channel(false);
    let synchronizer = Arc::new(WebSocketDataSynchronizer {
        stop,
        worker,
        status: Arc::new(StatusTracker::new(SyncStatus::Ready, true)),
        close_timeout: Duration::from_secs(1),
        closed: std::sync::atomic::AtomicBool::new(true),
        close_complete: std::sync::atomic::AtomicBool::new(false),
    });
    let (entered_sender, entered_receiver) = bounded(1);
    let (completed_sender, completed_receiver) = bounded(1);
    let closing = Arc::clone(&synchronizer);
    let closer = thread::spawn(move || {
        entered_sender
            .send(())
            .expect("test should observe close entry");
        closing.close();
        completed_sender
            .send(())
            .expect("test should observe close completion");
    });

    entered_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("close thread should start");
    assert!(
        completed_receiver
            .recv_timeout(Duration::from_millis(50))
            .is_err(),
        "a concurrent close must not return while the shared worker is still running"
    );
    release_sender
        .send(())
        .expect("test worker should be released");
    completed_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("close should finish after the worker");
    closer.join().expect("close thread should not panic");
    assert_eq!(
        synchronizer.worker.wait(Duration::ZERO),
        WorkerWait::Completed
    );
}

#[test]
fn abnormal_sync_worker_exit_closes_the_public_status() {
    let status = Arc::new(StatusTracker::new(SyncStatus::Ready, true));
    let worker_status = Arc::clone(&status);
    let worker = WorkerThread::spawn("test-panicking-sync-worker", move || {
        let _status_on_exit = SyncWorkerStatusOnExit(worker_status);
        panic!("simulated sync worker failure");
    })
    .expect("test worker should start");

    assert_eq!(worker.wait(Duration::from_secs(1)), WorkerWait::Panicked);
    assert_eq!(status.status(), SyncStatus::Closed);
}
