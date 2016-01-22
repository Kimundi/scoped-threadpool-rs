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
//!     let manager = pool.manager();
//!
//!     let mut vec = vec![0, 1, 2, 3, 4, 5, 6, 7];
//!
//!     // Use the threads as scoped threads that can
//!     // reference anything outside this closure
//!     manager.scoped(|scope| {
//!         // Create references to each element in the vector ...
//!         for e in &mut vec {
//!             // ... and add 1 to it in a separate thread
//!             scope.submit(move || {
//!                 *e += 1;
//!             });
//!         }
//!     });
//!
//!     assert_eq!(vec, vec![1, 2, 3, 4, 5, 6, 7, 8]);
//! }
//! ```

#![warn(missing_docs)]

use std::thread::{self, JoinHandle};
use std::mem;
use std::borrow::Borrow;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::atomic::{AtomicIsize,Ordering};
use std::sync::{Arc, Mutex, RwLock};

//    d88b  .d88b.  d8888b.
//    `8P' .8P  Y8. 88  `8D
//     88  88    88 88oooY'
//     88  88    88 88~~~b.
// db. 88  `8b  d8' 88   8D
// Y8888P   `Y88P'  Y8888P'

type JobResult = Result<(), JoinHandle<()>>;

struct JobGuard {
    value: JobResult,
    result_tx: Sender<JobResult>,
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        self.result_tx.send( mem::replace(&mut self.value, Ok(())) ).unwrap();
    }
}

/// TODO: Convert to std::boxed::FnBox. Blocked by rust-lang/rust#28796
trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

type Thunk<'a> = Box<FnBox + Send + 'a>;

struct Message {
    job: Thunk<'static>,
    result_tx: Sender<JobResult>,
}

// .d8888. d88888b d8b   db d888888b d888888b d8b   db d88888b db
// 88'  YP 88'     888o  88 `~~88~~'   `88'   888o  88 88'     88
// `8bo.   88ooooo 88V8o 88    88       88    88V8o 88 88ooooo 88
//   `Y8b. 88~~~~~ 88 V8o88    88       88    88 V8o88 88~~~~~ 88
// db   8D 88.     88  V888    88      .88.   88  V888 88.     88booo.
// `8888Y' Y88888P VP   V8P    YP    Y888888P VP   V8P Y88888P Y88888P

struct Sentinel<'a> {
    job_rx: Arc<Mutex<Receiver<Option<Message>>>>,
    done: &'a Arc<RwLock<()>>,
    respawn: bool,
}

impl <'a> Sentinel<'a> {
    fn new(job_rx: Arc<Mutex<Receiver<Option<Message>>>>, done: Arc<RwLock<()>>) {
        // Create a channel the send the JoinHandle to the thread
        let (tx, rx) = channel();

        // Create the thread
        let thread = thread::spawn(move || {
            // Get the JoinHandle
            let mut thread = rx.recv().unwrap();
            mem::drop(rx);

            // Will spawn a new thread on panic unless it is cancelled.
            let sentinel = Sentinel {
                job_rx: job_rx,
                done: &done,
                respawn: true,
            };

            let _guard = done.read().unwrap();

            loop {
                let message = {
                    // Only lock jobs for the time it takes
                    // to get a job, not run it.
                    let lock = sentinel.job_rx.lock().unwrap();
                    lock.recv()
                };

                thread = match message {
                    /// Execute the job and send the response
                    Ok(Some(Message{job, result_tx})) => {
                        let mut job_guard = JobGuard {
                            result_tx: result_tx,
                            value: Err(thread),
                        };

                        job.call_box();

                        mem::replace(&mut job_guard.value, Ok(())).unwrap_err()
                    }
                    // A message to kill a single thread or the pool was dropped.
                    Ok(None) | Err(_) => {
                        sentinel.die();
                        return
                    }
                }
            }

        });

        // Send the JoinHandle to the thread
        tx.send(thread).unwrap();
    }

    // Destroy this sentinel and let the thread die.
    fn die(mut self) {
        self.respawn = false;
    }
}

impl <'a> Drop for Sentinel<'a> {
    fn drop(&mut self) {
        if self.respawn {
            Sentinel::new(self.job_rx.clone(), self.done.clone())
        }
    }
}

// d8888b.  .d88b.   .d88b.  db
// 88  `8D .8P  Y8. .8P  Y8. 88
// 88oodD' 88    88 88    88 88
// 88~~~   88    88 88    88 88
// 88      `8b  d8' `8b  d8' 88booo.
// 88       `Y88P'   `Y88P'  Y88888P

struct ThreadDone(Arc<RwLock<()>>);

impl Drop for ThreadDone {
    fn drop(&mut self) {
        let _guard = self.0.write().unwrap();
    }
}

/// A threadpool that acts as a handle to a number of threads spawned at construction.
pub struct Pool {
    job_tx: Mutex<Sender<Option<Message>>>,
    job_rx: Arc<Mutex<Receiver<Option<Message>>>>,
    count: AtomicIsize,
    thread_done: ThreadDone,
}

fn adjust_helper(n: isize, job_rx: &Arc<Mutex<Receiver<Option<Message>>>>, thread_done: &Arc<RwLock<()>>, job_tx: &Sender<Option<Message>>) {
    // spawn n threads, put them in waiting mode
    for _ in 0..n {
        Sentinel::new(job_rx.clone(), thread_done.clone())
    }
    // Send kill mesages to threads
    for _ in n..0 {
        job_tx.send(None).unwrap();
    }
}

impl Pool {
    /// Construct a threadpool with the given number of threads.
    pub fn new(n: isize) -> Pool {

        let thread_done = Arc::new(RwLock::new(()));
        let (job_tx, job_rx) = channel();
        let job_rx = Arc::new(Mutex::new(job_rx));
        adjust_helper(n, &job_rx, &thread_done, &job_tx);

        let p = Pool{
            job_tx: Mutex::new(job_tx),
            job_rx: job_rx,
            count: AtomicIsize::new(n),
            thread_done: ThreadDone(thread_done),
        };
        p
    }

    /// Create a manager for the pool. Pools can have multiple managers at the same time.
    ///
    /// Creating a manager from a pool requires acquiring a lock, so cloning an existing manager
    /// for a pool is usually cheaper.
    pub fn manager<'pool>(&'pool self) -> Manager<&'pool Pool> {
        Manager{
            jobs: 0,
            job_tx: self.job_tx.lock().unwrap().clone(),
            results: channel(),
            pool: self,
        }
    }

    /// Gets the number of threads
    pub fn threads(&self) -> isize {
        self.count.load(Ordering::Relaxed)
    }
}


// .88b  d88.  .d8b.  d8b   db  .d8b.   d888b  d88888b d8888b.
// 88'YbdP`88 d8' `8b 888o  88 d8' `8b 88' Y8b 88'     88  `8D
// 88  88  88 88ooo88 88V8o 88 88ooo88 88      88ooooo 88oobY'
// 88  88  88 88~~~88 88 V8o88 88~~~88 88  ooo 88~~~~~ 88`8b
// 88  88  88 88   88 88  V888 88   88 88. ~8~ 88.     88 `88.
// YP  YP  YP YP   YP VP   V8P YP   YP  Y888P  Y88888P 88   YD

/// A manager for controlling a threadpool.
pub struct Manager<P: Borrow<Pool>> {
    jobs: usize,
    job_tx: Sender<Option<Message>>,
    results: (Sender<JobResult>, Receiver<JobResult>),
    pool: P,
}

impl <P: Borrow<Pool>> Manager<P> {
    /// Create a new Manager
    pub fn new(pool: P) -> Self {
        let job_tx = pool.borrow().job_tx.lock().unwrap().clone();
        Manager{
            jobs: 0,
            job_tx: job_tx,
            results: channel(),
            pool: pool,
        }
    }

    /// Gets the pool
    pub fn pool(&self) -> &Pool { self.pool.borrow() }

    /// Submits a static job for execution on the threadpool.
    ///
    /// The body of the closure will be send to one of the
    /// internal threads, and this method itself will not wait
    /// for its completion.
    pub fn submit<F: FnOnce() + Send + 'static>(&mut self, f: F) {
        let f = Box::new(f);
        let f = Message{ job: f, result_tx: self.results.0.clone() };

        self.job_tx.send(Some(f)).unwrap();
    }

    /// Borrows the manger and allows executing jobs on other
    /// threads during that scope via the argument of the closure.
    ///
    /// This method will block until the closure and all its jobs have
    /// run to completion.
    pub fn scoped<'manager, R, F: FnOnce(&mut Scope<'manager>) -> R>(&'manager self, f: F) -> R {
        let mut scope = Scope {
            job_tx: &self.job_tx,
            results: channel(),
            jobs: 0,
        };
        f(&mut scope)
    }

    /// Adjusts the number of threads in the pool as requested. If n is positive, threads will be
    /// added to the pool. If n is negative, threads will be removed from the pool. This call is
    /// non-blocking.
    pub fn adjust_threads(&self, n: isize) {
        self.pool.borrow().count.fetch_add(n, Ordering::Relaxed);
        adjust_helper(n, &self.pool.borrow().job_rx, &self.pool.borrow().thread_done.0, &self.job_tx)
    }

    /// Sets the number of threads in the pool to the value requested, adding and removing threads
    /// as needed. This call is non-blocking.
    pub fn set_threads(&self, n: isize) {
        let old = self.pool.borrow().count.swap(n, Ordering::Relaxed);
        adjust_helper(n - old, &self.pool.borrow().job_rx, &self.pool.borrow().thread_done.0, &self.job_tx)
    }

    /// Gets the number of threads
    pub fn threads(&self) -> isize {
        self.pool.borrow().count.load(Ordering::Relaxed)
    }

    /// Blocks until all submitted jobs have run to completion.
    /// If the child thread panics, `Err` is returned with the parameter given to `panic`.
    pub fn join_all(&mut self) -> thread::Result<()> {
        while self.jobs > 0 {
            self.jobs -= 1;
            if let Err(thread) = self.results.1.recv().unwrap() {
                return thread.join()
            }
        }
        Ok(())
    }
}

impl <P: Clone+Borrow<Pool>> Clone for Manager<P> {
    fn clone(&self) -> Self {
        Manager{
            jobs: 0,
            job_tx: self.job_tx.clone(),
            results: channel(),
            pool: self.pool.clone(),
        }
    }
}

impl <P: Borrow<Pool>> Drop for Manager<P> {
    fn drop(&mut self) {
        if !thread::panicking() {
            if let Err(e) = self.join_all() {
                panic!(e)
            }
        }
    }
}

// .d8888.  .o88b.  .d88b.  d8888b. d88888b
// 88'  YP d8P  Y8 .8P  Y8. 88  `8D 88'
// `8bo.   8P      88    88 88oodD' 88ooooo
//   `Y8b. 8b      88    88 88~~~   88~~~~~
// db   8D Y8b  d8 `8b  d8' 88      88.
// `8888Y'  `Y88P'  `Y88P'  88      Y88888P

/// Handle to the scope during which the manager is borrowed.
pub struct Scope<'manager> {
    job_tx: &'manager Sender<Option<Message>>,
    results: (Sender<JobResult>, Receiver<JobResult>),
    jobs: usize,
}

impl <'manager> Scope<'manager> {
    /// Submit a job for execution on the threadpool.
    ///
    /// The body of the closure will be send to one of the
    /// internal threads, and this method itself will not wait
    /// for its completion.
    pub fn submit<'scope, F: FnOnce() + Send + 'scope>(&'scope mut self, f: F) {
        let f = Box::new(f);
        let f = unsafe { mem::transmute::<Thunk<'scope>, Thunk<'static>>(f) };
        let f = Message{ job: f, result_tx: self.results.0.clone() };

        self.job_tx.send(Some(f)).unwrap();
        self.jobs += 1;
    }

    /// Blocks until all submitted jobs have run to completion.
    /// If the child thread panics, `Err` is returned with the parameter given to `panic`.
    pub fn join_all(&mut self) -> thread::Result<()> {
        while self.jobs > 0 {
            self.jobs -= 1;
            if let Err(thread) = self.results.1.recv().unwrap() {
                return thread.join()
            }
        }
        Ok(())
    }
}

impl <'manager> Drop for Scope<'manager> {
    fn drop(&mut self) {
        if !thread::panicking() {
            if let Err(e) = self.join_all() {
                panic!(e)
            }
        }
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
    use super::{Pool, Manager};
    use std::thread;
    use std::time::Duration;
    use std::sync::mpsc::{channel, sync_channel};

    #[test]
    fn smoketest() {
        let pool = Pool::new(4);
        let manager = pool.manager();

        for i in 1..7 {
            let mut vec = vec![0, 1, 2, 3, 4];
            manager.scoped(|s| {
                for e in vec.iter_mut() {
                    s.submit(move || {
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


    #[test]
    fn managed() {
        let pool = Pool::new(4);
        let manager = Manager::new(pool);

        for i in 1..7 {
            let mut vec = vec![0, 1, 2, 3, 4];
            manager.scoped(|s| {
                for e in vec.iter_mut() {
                    s.submit(move || {
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


    #[test]
    fn add() {
        let pool = Pool::new(0);
        let manager = pool.manager();
        manager.adjust_threads(1);
        manager.scoped(|scoped| {
            let manager = manager.clone();
            scoped.submit(move || {
                manager.adjust_threads(1);
            });
        });
    }

    #[test]
    fn remove() {
        let pool = Pool::new(2);
        let manager = pool.manager();
        manager.adjust_threads(-1);
        manager.scoped(|scoped| {
            scoped.submit(move || {
            });
        });
    }

    #[test]
    fn double_borrow() {
        let pool = Pool::new(2);
        let manager = pool.manager();
        manager.scoped(|s| {
            let manager = pool.manager();
            s.submit(move || {
                manager.scoped(|s2| {
                    let (tx, rx) = sync_channel(0);
                    s2.submit(move || {
                        tx.send(1).unwrap();
                    });
                    rx.recv().unwrap();
                });
            });
        });
    }

    #[test]
    #[should_panic]
    fn thread_panic() {
        let pool = Pool::new(4);
        let manager = pool.manager();
        manager.scoped(|scoped| {
            scoped.submit(move || {
                panic!()
            });
        });
    }

    #[test]
    #[should_panic]
    fn scope_panic() {
        let pool = Pool::new(4);
        let manager = pool.manager();
        manager.scoped(|_scoped| {
            panic!()
        });
    }

    #[test]
    #[should_panic]
    fn pool_panic() {
        let _pool = Pool::new(4);
        panic!()
    }

    #[test]
    fn join_all() {
        let pool = Pool::new(4);
        let manager = pool.manager();

        let (tx_, rx) = channel();

        manager.scoped(|scoped| {
            let tx = tx_.clone();
            scoped.submit(move || {
                thread::sleep(Duration::from_millis(1000));
                tx.send(2).unwrap();
            });

            let tx = tx_.clone();
            scoped.submit(move || {
                tx.send(1).unwrap();
            });

            scoped.join_all().unwrap();

            let tx = tx_.clone();
            scoped.submit(move || {
                tx.send(3).unwrap();
            });
        });

        assert_eq!(rx.iter().take(3).collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn simple() {
        let pool = Pool::new(4);
        let manager = pool.manager();
        manager.scoped(|scoped| {
            scoped.submit(move || {
            });
        });
    }
}
