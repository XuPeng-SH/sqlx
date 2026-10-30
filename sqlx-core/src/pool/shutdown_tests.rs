use super::*;
use crate::any::{
    Any, AnyArguments, AnyConnectOptions, AnyConnection, AnyConnectionBackend, AnyQueryResult,
    AnyRow, AnyStatement, AnyTypeInfo,
};
use crate::pool::Pool;
use crate::sql_str::SqlStr;
use either::Either;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use std::str::FromStr;

/// A backend that constructs but never executes anything. The pool's `release`/`pop_idle`
/// paths only move the opaque connection around — they never call any of these methods.
#[derive(Debug)]
struct StubBackend {
    control: Arc<Control>,
}

#[derive(Debug, Default)]
struct Control {
    blocked: std::sync::atomic::AtomicBool,
    ping_error: std::sync::atomic::AtomicBool,
    entered: AtomicUsize,
    destroyed: AtomicUsize,
    resume: tokio::sync::Notify,
}

impl Drop for StubBackend {
    fn drop(&mut self) {
        self.control.destroyed.fetch_add(1, Ordering::SeqCst);
    }
}

impl AnyConnectionBackend for StubBackend {
    fn name(&self) -> &str {
        "stub"
    }
    fn close(self: Box<Self>) -> BoxFuture<'static, crate::Result<()>> {
        Box::pin(async move {
            drop(self);
            Ok(())
        })
    }
    fn close_hard(self: Box<Self>) -> BoxFuture<'static, crate::Result<()>> {
        Box::pin(async move {
            drop(self);
            Ok(())
        })
    }
    fn ping(&mut self) -> BoxFuture<'_, crate::Result<()>> {
        Box::pin(async move {
            let notified = self.control.resume.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.control.entered.fetch_add(1, Ordering::SeqCst);
            if self.control.blocked.load(Ordering::SeqCst) {
                notified.await;
            }
            if self.control.ping_error.load(Ordering::SeqCst) {
                Err(crate::Error::Protocol("test ping failed".into()))
            } else {
                Ok(())
            }
        })
    }
    fn begin(&mut self, _statement: Option<SqlStr>) -> BoxFuture<'_, crate::Result<()>> {
        unimplemented!()
    }
    fn commit(&mut self) -> BoxFuture<'_, crate::Result<()>> {
        unimplemented!()
    }
    fn rollback(&mut self) -> BoxFuture<'_, crate::Result<()>> {
        unimplemented!()
    }
    fn start_rollback(&mut self) {
        unimplemented!()
    }
    fn shrink_buffers(&mut self) {
        unimplemented!()
    }
    fn flush(&mut self) -> BoxFuture<'_, crate::Result<()>> {
        unimplemented!()
    }
    fn should_flush(&self) -> bool {
        unimplemented!()
    }
    fn fetch_many(
        &mut self,
        _query: SqlStr,
        _persistent: bool,
        _arguments: Option<AnyArguments>,
    ) -> BoxStream<'_, crate::Result<Either<AnyQueryResult, AnyRow>>> {
        unimplemented!()
    }
    fn fetch_optional(
        &mut self,
        _query: SqlStr,
        _persistent: bool,
        _arguments: Option<AnyArguments>,
    ) -> BoxFuture<'_, crate::Result<Option<AnyRow>>> {
        unimplemented!()
    }
    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        _sql: SqlStr,
        _parameters: &[AnyTypeInfo],
    ) -> BoxFuture<'c, crate::Result<AnyStatement>> {
        unimplemented!()
    }
    #[cfg(feature = "offline")]
    fn describe(
        &mut self,
        _sql: SqlStr,
    ) -> BoxFuture<'_, crate::Result<crate::describe::Describe<Any>>> {
        unimplemented!()
    }
}

use std::time::Duration;
fn pool(max: u32, control: &Arc<Control>) -> Pool<Any> {
    pool_with_min(max, 0, control)
}
fn pool_with_min(max: u32, min: u32, control: &Arc<Control>) -> Pool<Any> {
    let options = AnyConnectOptions::from_str("stub::memory:").unwrap();
    let pool = PoolOptions::new()
        .max_connections(max)
        .min_connections(min)
        .idle_timeout(None)
        .max_lifetime(None)
        .test_before_acquire(false)
        .connect_lazy_with(options);
    for _ in 0..max {
        pool.0.size.fetch_add(1, Ordering::AcqRel);
        let permit = pool.0.semaphore.try_acquire(1).unwrap();
        let guard = DecrementSizeGuard::from_permit(Arc::clone(&pool.0), permit);
        let connection = AnyConnection {
            backend: Box::new(StubBackend {
                control: control.clone(),
            }),
        };
        pool.0.release(Floating::new_live(connection, guard));
    }
    pool
}
async fn wait_for(value: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while value.load(Ordering::SeqCst) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("state transition completed");
}
async fn close(pool: &Pool<Any>) {
    tokio::time::timeout(Duration::from_secs(2), pool.close())
        .await
        .expect("pool close completed");
    assert_eq!((pool.size(), pool.num_idle()), (0, 0));
}
#[tokio::test]
async fn close_cancels_pending_return_ping_and_releases_connection() {
    let control = Arc::new(Control::default());
    let pool = pool(1, &control);
    let connection = pool.acquire().await.unwrap();
    control.blocked.store(true, Ordering::SeqCst);
    drop(connection);
    wait_for(&control.entered, 1).await;
    assert_eq!((pool.size(), pool.num_idle()), (1, 0));
    close(&pool).await;
    assert_eq!(control.destroyed.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn healthy_returns_reuse_connection_and_error_returns_discard_it() {
    let control = Arc::new(Control::default());
    let pool = pool(1, &control);
    for expected in 1..=2 {
        let mut connection = pool.acquire().await.unwrap();
        connection.return_to_pool().await;
        assert_eq!(control.entered.load(Ordering::SeqCst), expected);
        assert_eq!((pool.size(), pool.num_idle()), (1, 1));
        assert_eq!(control.destroyed.load(Ordering::SeqCst), 0);
    }
    control.ping_error.store(true, Ordering::SeqCst);
    let mut connection = pool.acquire().await.unwrap();
    connection.return_to_pool().await;
    assert_eq!(pool.size(), 0);
    assert_eq!(control.destroyed.load(Ordering::SeqCst), 1);
    close(&pool).await;
}
#[tokio::test]
async fn close_preserves_checked_out_connection_ownership() {
    let control = Arc::new(Control::default());
    let pool = pool(1, &control);
    let connection = pool.acquire().await.unwrap();
    let close = pool.close();
    tokio::pin!(close);
    assert!(futures_util::future::poll_immediate(&mut close)
        .await
        .is_none());
    assert_eq!(control.destroyed.load(Ordering::SeqCst), 0);
    drop(connection);
    tokio::time::timeout(Duration::from_secs(2), close)
        .await
        .unwrap();
    assert_eq!(control.entered.load(Ordering::SeqCst), 0);
    assert_eq!(control.destroyed.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn thousand_waiters_and_capped_pending_returns_close_together() {
    const LIMIT: u32 = 8;
    let control = Arc::new(Control::default());
    let pool = pool(LIMIT, &control);
    let mut held = Vec::new();
    for _ in 0..LIMIT {
        held.push(pool.acquire().await.unwrap());
    }
    let started = Arc::new(AtomicUsize::new(0));
    let mut waiters = Vec::new();
    for _ in 0..1000 {
        let pool = pool.clone();
        let started = started.clone();
        waiters.push(tokio::spawn(async move {
            started.fetch_add(1, Ordering::SeqCst);
            assert!(matches!(
                pool.acquire().await,
                Err(crate::Error::PoolClosed)
            ));
        }));
    }
    wait_for(&started, 1000).await;
    control.blocked.store(true, Ordering::SeqCst);
    drop(held);
    wait_for(&control.entered, LIMIT as usize).await;
    assert_eq!((pool.size(), pool.num_idle()), (LIMIT, 0));
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(pool.close(), pool.close());
        for waiter in waiters {
            waiter.await.unwrap();
        }
    })
    .await
    .expect("all waiting borrowers and concurrent close callers complete");
    assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    assert_eq!(control.destroyed.load(Ordering::SeqCst), LIMIT as usize);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ping_success_racing_close_preserves_exactly_once_cleanup() {
    let control = Arc::new(Control::default());
    let pool = pool(8, &control);
    let mut held = Vec::new();
    for _ in 0..8 {
        held.push(pool.acquire().await.unwrap());
    }
    control.blocked.store(true, Ordering::SeqCst);
    drop(held);
    wait_for(&control.entered, 8).await;
    tokio::join!(
        async {
            control.blocked.store(false, Ordering::SeqCst);
            control.resume.notify_waiters();
        },
        async {
            tokio::time::timeout(Duration::from_secs(2), pool.close())
                .await
                .unwrap();
        }
    );
    assert!(pool.is_closed());
    assert_eq!(pool.size(), 0);
    // Baseline SQLx publishes num_idle after the permit, so a healthy
    // release racing close can leave that legacy counter stale.
    // Physical destruction, not that counter, proves this race's cleanup.
    assert_eq!(control.destroyed.load(Ordering::SeqCst), 8);
}
#[tokio::test]
async fn close_events_observe_close_before_and_after_registration() {
    for already_closed in [false, true] {
        let control = Arc::new(Control::default());
        let pool = pool(1, &control);
        if already_closed {
            close(&pool).await;
        }
        let first = pool.close_event();
        let second = pool.close_event();
        if !already_closed {
            close(&pool).await;
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            first.await;
            second.await;
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn close_event_registration_cannot_lose_concurrent_close() {
    let control = Arc::new(Control::default());
    let pool = pool(1, &control);
    let inner = Arc::clone(&pool.0);
    // Force close between the two operations whose order is critical.
    CLOSE_EVENT_REGISTRATION_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || inner.mark_closed()));
    });
    tokio::time::timeout(Duration::from_secs(2), pool.close_event())
        .await
        .expect("close notification must not be lost during registration");
    close(&pool).await;
    assert_eq!(control.destroyed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shutdown_cancellation_does_not_replenish_minimum_connections() {
    let control = Arc::new(Control::default());
    let pool = pool_with_min(2, 2, &control);
    let first = pool.acquire().await.unwrap();
    let second = pool.acquire().await.unwrap();
    control.blocked.store(true, Ordering::SeqCst);
    drop((first, second));
    wait_for(&control.entered, 2).await;
    close(&pool).await;
    assert_eq!(control.destroyed.load(Ordering::SeqCst), 2);
}
