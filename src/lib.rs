//! This crate provides a stable, safe and scoped threadpool.
//!
//! It can be used to execute a number of short-lived jobs in parallel
//! without the need to respawn the underlying threads.
//!
//! Jobs are runnable by borrowing the pool for a given scope, during which
//! an arbitrary number of them can be executed. These jobs can access data of
//! any lifetime outside of the pools scope, which allows working on
//! non-`'static` references in parallel.
//!
//! For safety reasons, a panic inside a worker thread will not be isolated,
//! but rather propagate to the outside of the pool.
//!
//! # Examples:
//!
//! ```rust
//! extern crate scoped_threadpool;
//! use scoped_threadpool::Pool;
//!
//! fn main() {
//!     // Create a threadpool holding 4 threads
//!     let pool = Pool::new(4);
//!
//!     let mut vec = vec![0, 1, 2, 3, 4, 5, 6, 7];
//!
//!     // Use the threads as scoped threads that can
//!     // reference anything outside this closure
//!     pool.scoped(|scope| {
//!         // Create references to each element in the vector ...
//!         for e in &mut vec {
//!             // ... and add 1 to it in a separate thread
//!             scope.execute(move || {
//!                 *e += 1;
//!             });
//!         }
//!     });
//!
//!     assert_eq!(vec, vec![1, 2, 3, 4, 5, 6, 7, 8]);
//! }
//! ```

#![warn(missing_docs)]

extern crate crossbeam;

use std::borrow::{Borrow, BorrowMut};
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex, Condvar};
use std::thread::{self};
use std::usize;

use crossbeam::sync::MsQueue;

struct Semaphore {
    lock: Mutex<(usize, bool)>,
    cvar: Condvar,
}

impl Semaphore {
    fn new(count: usize) -> Semaphore {
        Semaphore {
            lock: Mutex::new((count, false)),
            cvar: Condvar::new(),
        }
    }

    fn panicked(&self) -> bool {
        self.lock.lock().unwrap().1
    }

    fn acquire(&self, count: usize) -> bool {
        let mut v = self.lock.lock().unwrap();
        while v.0 < count {
            v = self.cvar.wait(v).unwrap();
        }
        v.0 -= count;
        v.1 |= thread::panicking();
        v.1
    }

    fn acquire_all(&self) -> (usize, bool) {
        let mut v = self.lock.lock().unwrap();
        v.1 |= thread::panicking();
        (mem::replace(&mut v.0, 0), v.1)
    }

    fn release(&self) {
        let mut v = self.lock.lock().unwrap();
        v.0 += 1;
        v.1 |= thread::panicking();
        self.cvar.notify_one();
    }
}

//    d88b  .d88b.  d8888b.
//    `8P' .8P  Y8. 88  `8D
//     88  88    88 88oooY'
//     88  88    88 88~~~b.
// db. 88  `8b  d8' 88   8D
// Y8888P   `Y88P'  Y8888P'

/// TODO: Convert to std::boxed::FnBox. Blocked by rust-lang/rust#28796
trait FnBox<A, R> {
    fn call_box(self: Box<Self>, a: A) -> R;
}

impl<F: FnOnce()> FnBox<(), ()> for F {
    fn call_box(self: Box<F>, _: ()) {
        (*self)()
    }
}

struct SemaphoreGuard<'pool>(&'pool Semaphore);

impl <'pool> Drop for SemaphoreGuard<'pool> {
    fn drop(&mut self) {
        self.0.release()
    }
}

type Thunk<'a> = Box<FnBox<(), ()> + Send + 'a>;

enum Message {
    Job(Thunk<'static>),
    ScopedJob {
        job: Thunk<'static>,
        semaphore: &'static Semaphore,
    },
    Remove,
    Dropping,
}

// .d8888. d88888b d8b   db d888888b d888888b d8b   db d88888b db
// 88'  YP 88'     888o  88 `~~88~~'   `88'   888o  88 88'     88
// `8bo.   88ooooo 88V8o 88    88       88    88V8o 88 88ooooo 88
//   `Y8b. 88~~~~~ 88 V8o88    88       88    88 V8o88 88~~~~~ 88
// db   8D 88.     88  V888    88      .88.   88  V888 88.     88booo.
// `8888Y' Y88888P VP   V8P    YP    Y888888P VP   V8P Y88888P Y88888P

struct Sentinel {
    pool: Arc<PoolShared>,
    respawn: bool,
}

impl Sentinel {
    fn new(pool: Arc<PoolShared>) {
        let builder = thread::Builder::new();
        let builder = if let Some(stack_size) = pool.stack_size {
            builder.stack_size(stack_size)
        } else { builder };

        builder.spawn(move || {
            // Will spawn a new thread on panic unless it is cancelled.
            let sentinel = Sentinel { pool: pool, respawn: true };

            loop {
                match sentinel.pool.jobs.pop() {
                    // Execute the job and send the response
                    Message::Job(job) => job.call_box(()),
                    // Execute the job and send the response
                    Message::ScopedJob{job, semaphore} => {
                        // If the scope is panicked, don't execute jobs from it
                        if !semaphore.panicked() {
                            let _guard = SemaphoreGuard(semaphore);
                            job.call_box(());
                        }
                    },
                    // A message to kill a single thread
                    Message::Remove => {
                        sentinel.die();
                        return
                    },
                    // A message the pool is being dropped.
                    Message::Dropping => {
                        sentinel.pool.threads.release();
                        sentinel.die();
                        return
                    },
                }
            }
        }).unwrap();
    }

    // Destroy this sentinel and let the thread die.
    fn die(mut self) {
        self.respawn = false;
    }
}

impl Drop for Sentinel {
    fn drop(&mut self) {
        if self.respawn { Sentinel::new(self.pool.clone()) }
    }
}

// d8888b. db    db d888888b db      d8888b. d88888b d8888b.
// 88  `8D 88    88   `88'   88      88  `8D 88'     88  `8D
// 88oooY' 88    88    88    88      88   88 88ooooo 88oobY'
// 88~~~b. 88    88    88    88      88   88 88~~~~~ 88`8b
// 88   8D 88b  d88   .88.   88booo. 88  .8D 88.     88 `88.
// Y8888P' ~Y8888P' Y888888P Y88888P Y8888D' Y88888P 88   YD

/// Pool configuration. Provides detailed control over the properties and behavior of new threads.
pub struct Builder {
    // The size of the stack for the spawned thread
    stack_size: Option<usize>,
}

impl Builder {
    /// Generates the base configuration for constructing a threadpool, from which configuration
    /// methods can be chained.
    pub fn new() -> Builder {
        Builder {
            stack_size: None,
        }
    }

    /// Sets the size of the stack for the threadpool threads.
    pub fn stack_size(mut self, size: usize) -> Self {
        self.stack_size = Some(size);
        self
    }

    /// Construct a threadpool with the given number of threads.
    pub fn build(self, threads: usize) -> Pool {
        let Builder { stack_size } = self;

        let shared = Arc::new(PoolShared{
            threads: Semaphore::new(0),
            stack_size: stack_size,
            jobs: MsQueue::new(),
        });

        // Spawn n threads, put them in waiting mode
        for _ in 0..threads { Sentinel::new(shared.clone()) }

        Pool{
            shared: shared,
            count: AtomicUsize::new(threads),
        }
    }
}



// d8888b.  .d88b.   .d88b.  db
// 88  `8D .8P  Y8. .8P  Y8. 88
// 88oodD' 88    88 88    88 88
// 88~~~   88    88 88    88 88
// 88      `8b  d8' `8b  d8' 88booo.
// 88       `Y88P'   `Y88P'  Y88888P

struct PoolShared {
    threads: Semaphore,
    stack_size: Option<usize>,
    jobs: MsQueue<Message>,
}

/// A threadpool that acts as a handle to a number of threads spawned at construction.
// `Drop` will not block.
pub struct Pool {
    shared: Arc<PoolShared>,
    count: AtomicUsize,
}

impl Pool {
    /// Construct a threadpool with the given number of threads.
    pub fn new(threads: usize) -> Self { Builder::new().build(threads) }

    /// Adds threads to the pool, returning the previous number of threads. The number of threads
    /// will not overflow.
    pub fn add_threads(&self, val: usize) -> usize {
        let mut current = self.count.load(Relaxed);
        if val == 0 { return current; }

        loop {
            let old = self.count.compare_and_swap(current, current.saturating_add(val), Relaxed);
            if current == old { break }
            current = old;
        }

        // Spawn n threads, put them in waiting mode
        for _ in 0..(current.saturating_add(val) - current) {
            Sentinel::new(self.shared.clone())
        }

        current
    }

    /// Removes threads from the pool, returning the previous number of threads. The number of
    /// threads will not underflow.
    pub fn sub_threads(&self, val: usize) -> usize {
        let mut current = self.count.load(Relaxed);
        if val == 0 { return current; }

        loop {
            let old = self.count.compare_and_swap(current, current.saturating_sub(val), Relaxed);
            if current == old { break }
            current = old;
        }

        // Send kill messages to threads
        for _ in 0..(current - current.saturating_sub(val)) {
            self.shared.jobs.push(Message::Remove);
        }

        current
    }

    /// Sets the number of threads in the pool to the value requested, adding and removing threads
    /// as needed. Returns the old number of threads
    pub fn set_threads(&self, new: usize) -> usize {
        let old = self.count.swap(new, Relaxed);

        if new > old {
            // Spawn n threads, put them in waiting mode
            for _ in 0..(new-old) {
                Sentinel::new(self.shared.clone())
            }
        } else {
            // Send kill messages to threads
            for _ in 0..(old-new) {
                self.shared.jobs.push(Message::Remove);
            }
        }

        old
    }

    /// Sets the number of threads if the current value is the same as the `current` value.
    ///
    /// The return value is always the previous value. If it is equal to current, then the number
    /// of threads was updated.
    pub fn compare_and_swap_threads(&self, current: usize, new: usize) -> usize {
        let old = self.count.compare_and_swap(current, new, Relaxed);

        // If current is equal, perform changes
        if current == old {
            if new > old {
                // Spawn n threads, put them in waiting mode
                for _ in 0..(new-old) {
                    Sentinel::new(self.shared.clone())
                }
            } else {
                // Send kill messages to threads
                for _ in 0..(old-new) {
                    self.shared.jobs.push(Message::Remove);
                }
            }
        }

        old
    }

    /// Gets the number of threads. The number of threads may change immediately after this call
    /// is made.
    pub fn threads(&self) -> usize {
        self.count.load(Relaxed)
    }

    /// Submits a static job for execution on the threadpool.
    ///
    /// The body of the closure will be send to one of the internal threads, and this method itself
    /// will not wait for its completion.
    pub fn execute<F: 'static+Send+FnOnce()>(&self, f: F) {
        self.shared.jobs.push(Message::Job(Box::new(f)));
    }

    /// Borrows the pool and allows executing jobs on other threads during that scope via the
    /// argument of the closure.
    ///
    /// This method will block until the closure and all its jobs have run to completion.
    pub fn scoped<'pool, R, F: FnOnce(&mut Scope<'pool>) -> R>(&'pool self, f: F) -> R {
        let mut scope = Scope::new(&self.shared.jobs);
        f(&mut scope)
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        // Kill all the threads, but don't bother blocking
        self.set_threads(0);
    }
}

//  .d8b.  d8b   db  .o88b. db   db  .d88b.  d8888b. d88888b d8888b.
// d8' `8b 888o  88 d8P  Y8 88   88 .8P  Y8. 88  `8D 88'     88  `8D
// 88ooo88 88V8o 88 8P      88ooo88 88    88 88oobY' 88ooooo 88   88
// 88~~~88 88 V8o88 8b      88~~~88 88    88 88`8b   88~~~~~ 88   88
// 88   88 88  V888 Y8b  d8 88   88 `8b  d8' 88 `88. 88.     88  .8D
// YP   YP VP   V8P  `Y88P' YP   YP  `Y88P'  88   YD Y88888P Y8888D'

/// An anchored thread pool that will block on `Drop` until all threads are cleaned up.
pub struct Anchored {
    pool: Pool,
    _marker: PhantomData<* const ()>, // TODO: Convert to !Send. Blocked by rust-lang/rust#13231
}

// TODO: Convert to !Send. Blocked by rust-lang/rust#13231
unsafe impl Sync for Anchored {}

impl Anchored {
    /// Construct a threadpool with the given number of threads.
    pub fn new(n: usize) -> Self { Anchored{ pool: Pool::new(n), _marker: PhantomData } }
}

impl Deref for Anchored {
    type Target = Pool;
    fn deref(&self) -> &Self::Target { &self.pool }
}

impl DerefMut for Anchored {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.pool }
}

impl AsRef<Pool> for Anchored {
    fn as_ref(&self) -> &Pool { &self.pool }
}

impl AsMut<Pool> for Anchored {
    fn as_mut(&mut self) -> &mut Pool { &mut self.pool }
}

impl Borrow<Pool> for Anchored {
    fn borrow(&self) -> &Pool { &self.pool }
}

impl BorrowMut<Pool> for Anchored {
    fn borrow_mut(&mut self) -> &mut Pool { &mut self.pool }
}

impl Drop for Anchored {
    fn drop(&mut self) {
        // See how many threads we have
        let n = self.pool.count.swap(0, Relaxed);

        // Send drop messages to threads
        for _ in 0..n {
            self.pool.shared.jobs.push(Message::Dropping);
        }

        if !thread::panicking() {
            self.pool.shared.threads.acquire(n);
        }
    }
}

// .d8888.  .o88b.  .d88b.  d8888b. d88888b
// 88'  YP d8P  Y8 .8P  Y8. 88  `8D 88'
// `8bo.   8P      88    88 88oodD' 88ooooo
//   `Y8b. 8b      88    88 88~~~   88~~~~~
// db   8D Y8b  d8 `8b  d8' 88      88.
// `8888Y'  `Y88P'  `Y88P'  88      Y88888P

/// Handle to the scope.
pub struct Scope<'pool> {
    jobs: &'pool MsQueue<Message>,
    job_count: usize,
    semaphore: Semaphore,
}

impl <'pool> Scope<'pool> {
    fn new(jobs: &'pool MsQueue<Message>) -> Self {
        Scope{
            jobs: jobs,
            job_count: 0,
            semaphore: Semaphore::new(0),
        }
    }

    /// Submit a job for execution on the threadpool.
    ///
    /// The body of the closure will be send to one of the internal threads, and this method
    /// itself will not wait for its completion.
    pub fn execute<'scope, F: 'scope+Send+FnOnce()>(&'scope mut self, f: F)  {
        // Check the current state and count any completed jobs
        let (finished, panicked) = self.semaphore.acquire_all();
        self.job_count -= finished;

        // Block job submission if a panic occurs. Also panic if needed.
        if panicked {
            if thread::panicking() {
                return;
            } else {
                panic!("Scoped thread panicked");
            }
        }

        // Block job overflow
        if self.job_count == usize::MAX {
            panic!("Job overflow, max jobs is {}", usize::MAX)
        }
        self.job_count += 1;

        let f = Box::new(f);
        self.jobs.push(Message::ScopedJob{
            job: unsafe {
                mem::transmute::<Thunk<'scope>, Thunk<'static>>(f)
            },
            semaphore: unsafe {
                mem::transmute::<&'scope Semaphore, &'static Semaphore>(&self.semaphore)
            },
        });
    }

    /// Blocks until all submitted jobs have run to completion.
    pub fn wait_all(&mut self) {
        // Wait for job completion.
        let panicked = self.semaphore.acquire(mem::replace(&mut self.job_count, 0));

        // Panic if needed
        if panicked {
            if thread::panicking() {
                return;
            } else {
                panic!("Scoped thread panicked");
            }
        }
    }
}

impl <'pool> Drop for Scope<'pool> {
    fn drop(&mut self) {
        self.wait_all();
    }
}

// d888888b d88888b .d8888. d888888b
// `~~88~~' 88'     88'  YP `~~88~~'
//    88    88ooooo `8bo.      88
//    88    88~~~~~   `Y8b.    88
//    88    88.     db   8D    88
//    YP    Y88888P `8888Y'    YP

#[cfg(test)]
mod tests {
    use super::{Pool, Anchored};
    use std::borrow::Borrow;
    use std::thread;
    use std::time::Duration;
    use std::sync::mpsc::{channel, sync_channel};

    #[test] fn smoketest_pool() { smoketest(Pool::new(4)) }
    #[test] fn smoketest_anchored() { smoketest(Anchored::new(4)) }

    fn smoketest<P: Borrow<Pool>>(pool: P) {
        let pool = pool.borrow();

        for i in 1..7 {
            let mut vec = vec![0, 1, 2, 3, 4];
            pool.scoped(|s| {
                for e in vec.iter_mut() {
                    s.execute(move || {
                        *e += i;
                    });
                }
            });

            let mut vec2 = vec![0, 1, 2, 3, 4];
            for e in vec2.iter_mut() {
                *e += i;
            }

            assert_eq!(vec, vec2);
        }
    }

    #[test] fn add_pool() { add(Pool::new(0)) }
    #[test] fn add_anchored() { add(Anchored::new(0)) }

    fn add<P: Borrow<Pool>>(pool: P) {
        let pool = pool.borrow();
        pool.add_threads(1);
        pool.scoped(|scoped| {
            let pool = &pool;
            scoped.execute(move || {
                pool.sub_threads(1);
            });
        });
    }

    #[test] fn remove_pool() { remove(Pool::new(2)) }
    #[test] fn remove_anchored() { remove(Anchored::new(2)) }

    fn remove<P: Borrow<Pool>>(pool: P) {
        let pool = pool.borrow();
        pool.sub_threads(1);
        pool.scoped(|scoped| {
            scoped.execute(move || {
            });
        });
    }

    #[test] fn remove_underflow_pool() { remove_underflow(Pool::new(0)) }
    #[test] fn remove_underflow_anchored() { remove_underflow(Anchored::new(0)) }

    fn remove_underflow<P: Borrow<Pool>>(pool: P) {
        let pool = pool.borrow();
        pool.sub_threads(1);
        assert_eq!(pool.threads(), 0);
    }


    #[test] fn double_borrow_pool() { double_borrow(Pool::new(2)) }
    #[test] fn double_borrow_anchored() { double_borrow(Anchored::new(2)) }

    fn double_borrow<P: Borrow<Pool>>(pool: P) {
        let pool = pool.borrow();
        pool.scoped(|s| {
            let pool = &pool;
            s.execute(move || {
                pool.scoped(|s2| {
                    let (tx, rx) = sync_channel(0);
                    s2.execute(move || {
                        tx.send(1).unwrap();
                    });
                    rx.recv().unwrap();
                });
            });
        });
    }

    #[test] fn thread_panic_pool() { thread_panic(Pool::new(2)) }
    #[test] fn thread_panic_anchored() { thread_panic(Anchored::new(2)) }

    fn thread_panic<P: Borrow<Pool>>(pool: P) {
        let pool = pool.borrow();
        let (tx, rx) = channel();
        pool.execute(move || {
            let _tx = tx;
            if false { let _ = _tx.send(()); }
            panic!();
        });
        rx.recv().unwrap_err();
    }

    #[should_panic]
    #[test] fn scoped_thread_panic_pool() { scoped_thread_panic(Pool::new(4)) }
    #[should_panic]
    #[test] fn scoped_thread_panic_anchored() { scoped_thread_panic(Anchored::new(4)) }

    fn scoped_thread_panic<P: Borrow<Pool>>(pool: P) {
        let pool = pool.borrow();
        pool.scoped(|scoped| {
            scoped.execute(move || {
                panic!()
            });
        });
    }

    #[test]
    #[should_panic]
    fn scoped_panic_pool() { scope_panic(Pool::new(4)) }
    #[test]
    #[should_panic]
    fn scoped_panic_anchored() { scope_panic(Anchored::new(4)) }

    fn scope_panic<P: Borrow<Pool>>(pool: P) {
        let pool = pool.borrow();
        pool.scoped(|_scoped| {
            panic!()
        });
    }

    #[test]
    #[should_panic]
    fn panic_pool() { pool_panic(Pool::new(4)) }
    #[test]
    #[should_panic]
    fn panic_anchored() { pool_panic(Anchored::new(4)) }

    fn pool_panic<P: Borrow<Pool>>(pool: P) {
        let _pool = pool.borrow();
        panic!()
    }

    #[test] fn wait_all_pool() { wait_all(Pool::new(4)) }
    #[test] fn wait_all_anchored() { wait_all(Anchored::new(4)) }

    fn wait_all<P: Borrow<Pool>>(pool: P) {
        let pool = pool.borrow();

        let (tx_, rx) = channel();

        pool.scoped(|scoped| {
            let tx = tx_.clone();
            scoped.execute(move || {
                thread::sleep(Duration::from_millis(1000));
                tx.send(2).unwrap();
            });

            let tx = tx_.clone();
            scoped.execute(move || {
                tx.send(1).unwrap();
            });

            scoped.wait_all();

            let tx = tx_.clone();
            scoped.execute(move || {
                tx.send(3).unwrap();
            });
        });

        assert_eq!(rx.iter().take(3).collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test] fn simple_pool() { simple(Pool::new(4)) }
    #[test] fn simple_anchored() { simple(Anchored::new(4)) }

    fn simple<P: Borrow<Pool>>(pool: P) {
        let pool = pool.borrow();
        pool.scoped(|scoped| {
            scoped.execute(move || {
            });
        });
    }
}
