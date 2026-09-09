//! Bounded scoped-thread fan-out over independent items, preserving input order.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Applies `worker` to every item using at most one thread per available core.
pub fn map<T: Sync, R: Send>(items: &[T], worker: impl Fn(&T) -> R + Sync) -> Vec<R> {
    let threads = std::thread::available_parallelism()
        .map_or(1, |count| count.get())
        .min(items.len().max(1));
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<Option<R>>> = Mutex::new((0..items.len()).map(|_| None).collect());
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(item) = items.get(index) else { break };
                    let result = worker(item);
                    results.lock().expect("no worker panicked")[index] = Some(result);
                }
            });
        }
    });
    results
        .into_inner()
        .expect("no worker panicked")
        .into_iter()
        .map(|result| result.expect("every item was processed"))
        .collect()
}
