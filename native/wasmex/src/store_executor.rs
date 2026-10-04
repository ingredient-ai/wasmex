use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use tokio::sync::mpsc;
use wasmtime::{Engine, Store, UpdateDeadline};

const DEFAULT_QUEUE_CAPACITY: usize = 1024;

static LIVE_STORES: AtomicUsize = AtomicUsize::new(0);

/// Number of native Stores owned by an executor task that have not been dropped yet.
/// A Store outlives its last `StoreExecutor` handle: the executor task finishes the
/// commands already queued and drops the Store afterwards, so this lags handle drops.
pub fn live_store_count() -> usize {
    LIVE_STORES.load(Ordering::Acquire)
}

/// Counts one Store from executor creation until the executor task has dropped it.
/// Decrementing in `Drop` keeps the count honest when a command panics, because the
/// task unwinds its Store before this guard (declared earlier, dropped later).
struct LiveStore(&'static AtomicUsize);

impl LiveStore {
    fn track(counter: &'static AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self(counter)
    }
}

impl Drop for LiveStore {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

type StoreFuture<T> = Pin<Box<dyn Future<Output = Store<T>> + Send + 'static>>;
type StoreCommand<T> = Box<dyn FnOnce(Store<T>) -> StoreFuture<T> + Send + 'static>;

pub trait InterruptState {
    fn interrupt_requested(&self) -> &AtomicBool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    Busy,
    Closed,
}

/// Serializes access to a Wasmtime Store without blocking an executor thread.
///
/// A Store is owned by one long-lived Tokio task. NIF calls enqueue owned
/// commands and return to the BEAM; the executor runs one command at a time,
/// preserving Wasmtime's single-owner requirement.
pub struct StoreExecutor<T: 'static> {
    sender: mpsc::Sender<StoreCommand<T>>,
    engine: Engine,
    cancelled: Arc<AtomicBool>,
}

impl<T: 'static> Clone for StoreExecutor<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            engine: self.engine.clone(),
            cancelled: self.cancelled.clone(),
        }
    }
}

impl<T: InterruptState + Send + 'static> StoreExecutor<T> {
    pub(crate) fn new_async(mut store: Store<T>, epoch_ticker: crate::engine::EpochTicker) -> Self {
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_for_callback = cancelled.clone();
        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(move |store| {
            if cancelled_for_callback.load(Ordering::Acquire)
                || store.data().interrupt_requested().load(Ordering::Acquire)
            {
                Ok(UpdateDeadline::Interrupt)
            } else {
                Ok(UpdateDeadline::Yield(1))
            }
        });
        Self::with_capacity(
            store,
            DEFAULT_QUEUE_CAPACITY,
            Some(epoch_ticker),
            cancelled,
            &LIVE_STORES,
        )
    }
}

impl<T: Send + 'static> StoreExecutor<T> {
    fn with_capacity(
        store: Store<T>,
        capacity: usize,
        epoch_ticker: Option<crate::engine::EpochTicker>,
        cancelled: Arc<AtomicBool>,
        live_stores: &'static AtomicUsize,
    ) -> Self {
        let engine = store.engine().clone();
        let (sender, mut receiver) = mpsc::channel::<StoreCommand<T>>(capacity);
        let live_store = LiveStore::track(live_stores);

        crate::engine::TOKIO_RUNTIME.spawn(async move {
            let live_store = live_store;
            let _epoch_ticker = epoch_ticker;
            let mut store = store;
            while let Some(command) = receiver.recv().await {
                store = command(store).await;
            }
            drop(store);
            drop(live_store);
        });

        Self {
            sender,
            engine,
            cancelled,
        }
    }

    /// Interrupts the store's running WebAssembly at the next epoch tick and every call
    /// after it. Unlike a call deadline, the flag is never cleared: a cancelled store
    /// only traps. It is set directly, not through the command queue, so it reaches a
    /// call that is already running.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub fn submit<F, Fut>(&self, command: F) -> Result<(), SubmitError>
    where
        F: FnOnce(Store<T>) -> Fut + Send + 'static,
        Fut: Future<Output = Store<T>> + Send + 'static,
    {
        self.sender
            .try_send(Box::new(move |store| Box::pin(command(store))))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => SubmitError::Busy,
                mpsc::error::TrySendError::Closed(_) => SubmitError::Closed,
            })
    }
}

pub async fn with_deadline<F>(
    interrupt_requested: Arc<AtomicBool>,
    deadline: Option<tokio::time::Instant>,
    future: F,
) -> Option<F::Output>
where
    F: Future,
{
    let Some(deadline) = deadline else {
        return Some(future.await);
    };
    interrupt_requested.store(false, Ordering::Release);
    if deadline <= tokio::time::Instant::now() {
        return None;
    }

    tokio::pin!(future);
    tokio::select! {
        biased;
        _ = tokio::time::sleep_until(deadline) => {
            interrupt_requested.store(true, Ordering::Release);
            let _ = future.await;
            interrupt_requested.store(false, Ordering::Release);
            None
        }
        output = &mut future => Some(output),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };

    use wasmtime::{Engine, Store};

    use super::{StoreExecutor, SubmitError, LIVE_STORES};

    // Private to the test so executors from parallel tests cannot move it.
    static COUNTED_STORES: AtomicUsize = AtomicUsize::new(0);

    /// Store data that reports the live count at the moment the Store drops it.
    struct ReportsCountOnDrop(std::sync::mpsc::Sender<usize>);

    impl Drop for ReportsCountOnDrop {
        fn drop(&mut self) {
            let _ = self.0.send(COUNTED_STORES.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn counts_the_store_until_the_executor_task_drops_it() {
        let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel::<()>();
        let executor = StoreExecutor::with_capacity(
            Store::new(&Engine::default(), ReportsCountOnDrop(dropped_tx)),
            1,
            None,
            Arc::default(),
            &COUNTED_STORES,
        );
        assert_eq!(COUNTED_STORES.load(Ordering::SeqCst), 1);

        executor
            .submit(move |store| async move {
                let _ = finish_rx.await;
                store
            })
            .unwrap();
        drop(executor);

        // The running command still owns the Store, so dropping the last handle is not enough.
        assert!(dropped_rx.try_recv().is_err());
        assert_eq!(COUNTED_STORES.load(Ordering::SeqCst), 1);

        finish_tx.send(()).unwrap();
        let count_while_dropping = dropped_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            count_while_dropping, 1,
            "decremented before the Store was dropped"
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        while COUNTED_STORES.load(Ordering::SeqCst) != 0 {
            assert!(Instant::now() < deadline, "live count never returned to 0");
            std::thread::yield_now();
        }
    }

    #[test]
    fn executes_commands_in_submission_order() {
        let executor = StoreExecutor::with_capacity(
            Store::new(&Engine::default(), 0usize),
            2,
            None,
            Arc::default(),
            &LIVE_STORES,
        );
        let observed = Arc::new(AtomicUsize::new(0));

        for expected in 0..2 {
            let observed = observed.clone();
            executor
                .submit(move |mut store| async move {
                    assert_eq!(*store.data(), expected);
                    *store.data_mut() += 1;
                    observed.fetch_add(1, Ordering::SeqCst);
                    store
                })
                .unwrap();
        }

        while observed.load(Ordering::SeqCst) != 2 {
            std::thread::yield_now();
        }
    }

    #[test]
    fn reports_backpressure() {
        let executor = StoreExecutor::with_capacity(
            Store::new(&Engine::default(), ()),
            1,
            None,
            Arc::default(),
            &LIVE_STORES,
        );
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let (finish_queued_tx, finish_queued_rx) = tokio::sync::oneshot::channel();

        executor
            .submit(move |store| async move {
                started_tx.send(()).unwrap();
                let _ = finish_rx.await;
                store
            })
            .unwrap();
        started_rx.recv().unwrap();

        executor
            .submit(move |store| async move {
                let _ = finish_queued_rx.await;
                store
            })
            .unwrap();

        assert_eq!(
            executor.submit(|store| async move { store }),
            Err(SubmitError::Busy)
        );
        finish_tx.send(()).unwrap();
        finish_queued_tx.send(()).unwrap();
    }
}
