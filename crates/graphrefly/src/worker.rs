//! Graph-helper-first worker compute (D137/D138).
//!
//! Worker work never receives `Ctx`, `Node`, `Rc<RefCell<...>>`, live topology,
//! or erased graph values. The graph-thread kickoff prepares one owned `Send`
//! input from normal ctx dep reads, then submits the owned compute to the
//! dispatcher-owned worker backend. Completion is awaited on the graph-local
//! async driver and emitted through `DeferredCtx` as a fresh later wave.

use std::cell::Cell;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

use crate::ctx::Ctx;
use crate::dispatcher::{PoolKind, WorkerSubmitError};
use crate::graph::{Graph, GraphNodeOpts};
use crate::node::{Core, Node, NodeOpts};
use crate::operators::Operator;
use crate::protocol::Message;

/// Create an async-pool derived node whose CPU-heavy part runs on Tokio's
/// blocking worker pool.
///
/// `prepare` runs synchronously on the graph thread and must return an owned
/// worker input. `compute` runs off-thread and may only close over `Send + Sync`
/// state. The result or error comes back as a brand-new graph wave via
/// `DeferredCtx`, preserving F-SYNC-CORE and D22.
pub fn worker_derived<I, R, E, P, C>(
    graph: &Graph,
    deps: Vec<Core>,
    prepare: P,
    compute: C,
    mut opts: GraphNodeOpts,
) -> Node<R>
where
    I: Send + 'static,
    R: Send + 'static,
    E: fmt::Display + Send + 'static,
    P: Fn(&Ctx) -> Option<I> + 'static,
    C: Fn(I) -> Result<R, E> + Send + Sync + 'static,
{
    opts.node.pool = PoolKind::Async;
    let compute = Arc::new(compute);
    let latest_invocation = Rc::new(Cell::new(0u64));
    let op = Operator::with_opts(
        "workerDerived",
        NodeOpts {
            // A dep COMPLETE must not seal the worker before an in-flight owned
            // compute result returns through DeferredCtx.
            complete_when_deps_complete: false,
            pool: PoolKind::Async,
            ..NodeOpts::default()
        },
        move |ctx| {
            let latest_invocation = latest_invocation.clone();
            let invocation = latest_invocation
                .get()
                .checked_add(1)
                .expect("worker_derived invocation generation overflow");
            latest_invocation.set(invocation);
            let Some(input) = prepare(ctx) else {
                ctx.down(vec![Message::Resolved]);
                return;
            };
            let Some(driver) = ctx.local_async_driver() else {
                ctx.down(vec![Message::Error(
                    "worker_derived: missing local async driver".into(),
                )]);
                return;
            };
            let out = ctx.defer();
            let compute = compute.clone();
            let job = match ctx.dispatcher().submit_worker(input, compute) {
                Ok(job) => job,
                Err(WorkerSubmitError::MissingBackend) => {
                    ctx.down(vec![Message::Error(
                        "worker_derived: missing worker backend".into(),
                    )]);
                    return;
                }
                Err(WorkerSubmitError::MissingRuntime) => {
                    ctx.down(vec![Message::Error(
                        "worker_derived: missing Tokio runtime".into(),
                    )]);
                    return;
                }
            };
            let cancel = driver.spawn_local(Box::pin(async move {
                let joined = job.spawn().await;
                if latest_invocation.get() != invocation {
                    return;
                }
                match joined {
                    Ok(Ok(value)) => out.emit(value),
                    Ok(Err(error)) => out.down(vec![Message::Error(error.into())]),
                    Err(error) => out.down(vec![Message::Error(
                        format!("worker_derived: worker task failed: {error}").into(),
                    )]),
                }
            }));
            ctx.on_deactivation(cancel);
        },
    );
    graph.init_node(op, deps, opts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::ThreadId;
    use std::time::Duration;

    use crate::async_driver::{DriverCancel, LocalAsyncDriver, TokioLocalDriver};
    use crate::dispatcher::Dispatcher;
    use crate::environment::EnvironmentDrivers;
    use crate::graph::{graph_opts, GraphOptions};
    use crate::node::Status;

    fn run_tokio_local<F>(future: F) -> F::Output
    where
        F: std::future::Future,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio current-thread runtime");
        let local = tokio::task::LocalSet::new();
        local.block_on(&runtime, future)
    }

    async fn wait_until(label: &str, mut done: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !done() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"));
    }

    #[test]
    fn worker_derived_runs_owned_compute_off_graph_thread_and_emits_later() {
        run_tokio_local(async {
            let graph_thread = std::thread::current().id();
            let worker_thread = Arc::new(Mutex::new(None::<ThreadId>));
            let worker_thread_for_compute = worker_thread.clone();
            let g = graph_opts(GraphOptions {
                environment: EnvironmentDrivers::new().with_local_async(Rc::new(TokioLocalDriver)),
                ..GraphOptions::default()
            });
            let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
            let doubled = worker_derived(
                &g,
                vec![source.erased()],
                |ctx| ctx.data::<i32>(0).map(|v| *v),
                move |value| {
                    *worker_thread_for_compute
                        .lock()
                        .expect("worker thread lock") = Some(std::thread::current().id());
                    Ok::<_, String>(value * 2)
                },
                GraphNodeOpts::named("worker"),
            );
            let _sub = doubled.subscribe(|_| {});

            source.set(21);

            assert_eq!(doubled.cache(), None);
            wait_until("worker result", || doubled.cache() == Some(42)).await;
            assert_ne!(
                worker_thread
                    .lock()
                    .expect("worker thread lock")
                    .expect("worker ran"),
                graph_thread
            );

            let snap = g.describe();
            assert!(snap
                .edges
                .iter()
                .any(|edge| edge.from == "source" && edge.to == "worker"));
            assert!(snap.nodes.iter().any(|node| node.id == "worker"
                && node.factory == "workerDerived"
                && node.status == Status::Settled));
        });
    }

    struct NeverDriver;

    impl LocalAsyncDriver for NeverDriver {
        fn sleep(&self, _duration: Duration, _callback: Box<dyn FnOnce()>) -> DriverCancel {
            panic!("NeverDriver.sleep should not be called")
        }

        fn interval(&self, _period: Duration, _callback: Rc<dyn Fn()>) -> DriverCancel {
            panic!("NeverDriver.interval should not be called")
        }

        fn spawn_local(&self, _fut: Pin<Box<dyn Future<Output = ()> + 'static>>) -> DriverCancel {
            panic!("NeverDriver.spawn_local should not be called")
        }
    }

    struct PanickingSpawnDriver;

    impl LocalAsyncDriver for PanickingSpawnDriver {
        fn sleep(&self, _duration: Duration, _callback: Box<dyn FnOnce()>) -> DriverCancel {
            panic!("PanickingSpawnDriver.sleep should not be called")
        }

        fn interval(&self, _period: Duration, _callback: Rc<dyn Fn()>) -> DriverCancel {
            panic!("PanickingSpawnDriver.interval should not be called")
        }

        fn spawn_local(&self, _fut: Pin<Box<dyn Future<Output = ()> + 'static>>) -> DriverCancel {
            panic!("local waiter unavailable")
        }
    }

    #[test]
    fn worker_derived_routes_compute_error_as_later_error_wave() {
        run_tokio_local(async {
            let g = graph_opts(GraphOptions {
                environment: EnvironmentDrivers::new().with_local_async(Rc::new(TokioLocalDriver)),
                ..GraphOptions::default()
            });
            let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
            let worker = worker_derived(
                &g,
                vec![source.erased()],
                |ctx| ctx.data::<i32>(0).map(|v| *v),
                |_value| Err::<i32, _>("worker failed"),
                GraphNodeOpts::named("worker"),
            );
            let errors = Rc::new(RefCell::new(Vec::new()));
            let errors_sink = errors.clone();
            let _sub = worker.subscribe(move |msg| {
                if let Message::Error(error) = msg {
                    errors_sink.borrow_mut().push(error.to_string());
                }
            });

            source.set(1);

            wait_until("worker error", || worker.status() == Status::Errored).await;
            assert_eq!(&*errors.borrow(), &["worker failed".to_owned()]);
        });
    }

    #[test]
    fn worker_derived_routes_worker_panic_as_later_error_wave() {
        run_tokio_local(async {
            let g = graph_opts(GraphOptions {
                environment: EnvironmentDrivers::new().with_local_async(Rc::new(TokioLocalDriver)),
                ..GraphOptions::default()
            });
            let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
            let worker = worker_derived(
                &g,
                vec![source.erased()],
                |ctx| ctx.data::<i32>(0).map(|v| *v),
                |_value| -> Result<i32, String> { panic!("worker boom") },
                GraphNodeOpts::named("worker"),
            );
            let errors = Rc::new(RefCell::new(Vec::new()));
            let errors_sink = errors.clone();
            let _sub = worker.subscribe(move |msg| {
                if let Message::Error(error) = msg {
                    errors_sink.borrow_mut().push(error.to_string());
                }
            });

            source.set(1);

            wait_until("worker panic error", || worker.status() == Status::Errored).await;
            assert_eq!(errors.borrow().len(), 1);
            assert!(errors.borrow()[0].contains("worker_derived: worker task failed"));
        });
    }

    #[test]
    fn worker_derived_drops_superseded_worker_results() {
        run_tokio_local(async {
            let g = graph_opts(GraphOptions {
                environment: EnvironmentDrivers::new().with_local_async(Rc::new(TokioLocalDriver)),
                ..GraphOptions::default()
            });
            let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
            let worker = worker_derived(
                &g,
                vec![source.erased()],
                |ctx| ctx.data::<i32>(0).map(|v| *v),
                |value| {
                    if value == 1 {
                        std::thread::sleep(Duration::from_millis(75));
                    }
                    Ok::<_, String>(value)
                },
                GraphNodeOpts::named("worker"),
            );
            let _sub = worker.subscribe(|_| {});

            source.set(1);
            source.set(2);

            wait_until("latest worker result", || worker.cache() == Some(2)).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            assert_eq!(worker.cache(), Some(2));
        });
    }

    #[test]
    fn worker_derived_no_submit_invocation_fences_prior_worker_result() {
        run_tokio_local(async {
            let g = graph_opts(GraphOptions {
                environment: EnvironmentDrivers::new().with_local_async(Rc::new(TokioLocalDriver)),
                ..GraphOptions::default()
            });
            let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
            let worker = worker_derived(
                &g,
                vec![source.erased()],
                |ctx| {
                    let value = *ctx.data::<i32>(0)?;
                    (value != 0).then_some(value)
                },
                |value| {
                    std::thread::sleep(Duration::from_millis(75));
                    Ok::<_, String>(value)
                },
                GraphNodeOpts::named("worker"),
            );
            let _sub = worker.subscribe(|_| {});

            source.set(1);
            source.set(0);

            tokio::time::sleep(Duration::from_millis(120)).await;
            assert_eq!(worker.cache(), None);
            assert_eq!(worker.status(), Status::Sentinel);
        });
    }

    #[test]
    fn worker_derived_dep_complete_does_not_seal_pending_worker_result() {
        run_tokio_local(async {
            let g = graph_opts(GraphOptions {
                environment: EnvironmentDrivers::new().with_local_async(Rc::new(TokioLocalDriver)),
                ..GraphOptions::default()
            });
            let source = g.producer_opts::<i32, _>(|_ctx| {}, GraphNodeOpts::named("source"));
            let worker = worker_derived(
                &g,
                vec![source.erased()],
                |ctx| ctx.data::<i32>(0).map(|v| *v),
                |value| {
                    std::thread::sleep(Duration::from_millis(25));
                    Ok::<_, String>(value * 3)
                },
                GraphNodeOpts::named("worker"),
            );
            let _sub = worker.subscribe(|_| {});

            source.down(vec![Message::Data(Rc::new(7i32)), Message::Complete]);

            wait_until("worker result after dep complete", || {
                worker.cache() == Some(21) && worker.status() == Status::Settled
            })
            .await;
        });
    }

    #[test]
    fn worker_derived_missing_local_async_driver_errors_on_activation() {
        let g = graph_opts(GraphOptions::default());
        let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
        let worker = worker_derived(
            &g,
            vec![source.erased()],
            |ctx| ctx.data::<i32>(0).map(|v| *v),
            Ok::<_, String>,
            GraphNodeOpts::named("worker"),
        );
        let errors = Rc::new(RefCell::new(Vec::new()));
        let errors_sink = errors.clone();
        let _sub = worker.subscribe(move |msg| {
            if let Message::Error(error) = msg {
                errors_sink.borrow_mut().push(error.to_string());
            }
        });

        source.set(1);

        assert_eq!(
            &*errors.borrow(),
            &["worker_derived: missing local async driver".to_owned()]
        );
    }

    #[test]
    fn worker_derived_missing_worker_backend_errors_before_spawning_local_task() {
        let dispatcher = Dispatcher::new();
        dispatcher.set_worker_backend_for_test(false);
        let g = graph_opts(GraphOptions {
            dispatcher: Some(dispatcher),
            environment: EnvironmentDrivers::new().with_local_async(Rc::new(NeverDriver)),
            ..GraphOptions::default()
        });
        let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
        let worker = worker_derived(
            &g,
            vec![source.erased()],
            |ctx| ctx.data::<i32>(0).map(|v| *v),
            Ok::<_, String>,
            GraphNodeOpts::named("worker"),
        );
        let errors = Rc::new(RefCell::new(Vec::new()));
        let errors_sink = errors.clone();
        let _sub = worker.subscribe(move |msg| {
            if let Message::Error(error) = msg {
                errors_sink.borrow_mut().push(error.to_string());
            }
        });

        source.set(1);

        assert_eq!(
            &*errors.borrow(),
            &["worker_derived: missing worker backend".to_owned()]
        );
    }

    #[test]
    fn worker_derived_does_not_start_worker_before_local_waiter_is_scheduled() {
        run_tokio_local(async {
            let compute_runs = Arc::new(AtomicUsize::new(0));
            let compute_runs_for_worker = compute_runs.clone();
            let g = graph_opts(GraphOptions {
                environment: EnvironmentDrivers::new()
                    .with_local_async(Rc::new(PanickingSpawnDriver)),
                ..GraphOptions::default()
            });
            let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
            let worker = worker_derived(
                &g,
                vec![source.erased()],
                |ctx| ctx.data::<i32>(0).map(|v| *v),
                move |value| {
                    compute_runs_for_worker.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, String>(value)
                },
                GraphNodeOpts::named("worker"),
            );
            let _sub = worker.subscribe(|_| {});

            source.set(1);
            tokio::task::yield_now().await;

            assert_eq!(compute_runs.load(Ordering::SeqCst), 0);
        });
    }

    #[test]
    fn worker_derived_missing_tokio_runtime_errors_before_spawning_local_task() {
        let g = graph_opts(GraphOptions {
            environment: EnvironmentDrivers::new().with_local_async(Rc::new(NeverDriver)),
            ..GraphOptions::default()
        });
        let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
        let worker = worker_derived(
            &g,
            vec![source.erased()],
            |ctx| ctx.data::<i32>(0).map(|v| *v),
            Ok::<_, String>,
            GraphNodeOpts::named("worker"),
        );
        let errors = Rc::new(RefCell::new(Vec::new()));
        let errors_sink = errors.clone();
        let _sub = worker.subscribe(move |msg| {
            if let Message::Error(error) = msg {
                errors_sink.borrow_mut().push(error.to_string());
            }
        });

        source.set(1);

        assert_eq!(
            &*errors.borrow(),
            &["worker_derived: missing Tokio runtime".to_owned()]
        );
    }
}
