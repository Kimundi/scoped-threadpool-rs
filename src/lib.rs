#![feature(test)]

#[macro_use]
#[cfg(test)]
extern crate lazy_static;

use std::thread::{self, JoinHandle};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::{Arc, Mutex, Barrier};
use std::cell::RefCell;
use std::marker::PhantomData;

enum Message {
    NewJob(Thunk<'static>),
    Join,
}

trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

type Thunk<'a> = Box<FnBox + Send + 'a>;

impl Drop for PoolCache {
    fn drop(&mut self) {
        ::std::mem::replace(&mut self.job_sender, None);
    }
}

pub struct PoolCache {
    threads: Vec<JoinHandle<()>>,
    job_sender: Option<Sender<Message>>,
    join_barrier: Arc<(Mutex<Receiver<Message>>, Barrier)>,
}

impl PoolCache {
    pub fn new(n: u32) -> PoolCache {
        assert!(n >= 1);

        let (job_sender, job_receiver) = channel();
        let job_receiver = Arc::new((Mutex::new(job_receiver),
                                     Barrier::new(n as usize + 1)));

        let mut threads = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let job_receiver = job_receiver.clone();
            threads.push(thread::spawn(move || {
                loop {
                    let message = {
                        // Only lock jobs for the time it takes
                        // to get a job, not run it.
                        let lock = job_receiver.0.lock().unwrap();
                        lock.recv()
                    };

                    match message {
                        Ok(Message::NewJob(job)) => {
                            job.call_box();
                        }
                        Ok(Message::Join) => {
                            job_receiver.1.wait();
                        }
                        // The pool was dropped.
                        Err(..) => break
                    }
                }
            }));
        }

        PoolCache {
            threads: threads,
            job_sender: Some(job_sender),
            join_barrier: job_receiver,
        }

        // spawn n threads, put them in waiting mode
    }

    pub fn scope<'pool, 'scope, F, R>(&'pool mut self, f: F) -> R
        where F: FnOnce(&Scope<'pool, 'scope>) -> R
    {
        let mut scope = Scope {
            pool: self,
            dtors: RefCell::new(None),
            _marker: PhantomData,
        };
        let ret = f(&scope);
        scope.drop_all();
        ret
    }

    pub fn thread_count(&self) -> u32 {
        self.threads.len() as u32
    }
}

/////////////////////////////////////////////////////////////////////////////

pub struct Scope<'pool, 'scope> {
    pool: &'pool mut PoolCache,
    dtors: RefCell<Option<DtorChain<'scope>>>,
    _marker: PhantomData<&'scope mut ()>,
}

struct DtorChain<'a> {
    dtor: Box<FnBox + 'a>,
    next: Option<Box<DtorChain<'a>>>
}

impl<'pool, 'scope> Scope<'pool, 'scope> {
    // This method is carefully written in a transactional style, so
    // that it can be called directly and, if any dtor panics, can be
    // resumed in the unwinding this causes. By initially running the
    // method outside of any destructor, we avoid any leakage problems
    // due to #14875.
    fn drop_all(&mut self) {
        loop {
            // use a separate scope to ensure that the RefCell borrow
            // is relinquished before running `dtor`
            let dtor = {
                let mut dtors = self.dtors.borrow_mut();
                if let Some(mut node) = dtors.take() {
                    *dtors = node.next.take().map(|b| *b);
                    node.dtor
                } else {
                    break
                }
            };
            dtor.call_box()
        }

        for _ in 0..self.pool.threads.len() {
            self.pool.job_sender.as_ref().unwrap().send(Message::Join).unwrap();
        }

        self.pool.join_barrier.1.wait();
    }

    fn defer<F>(&self, f: F) where F: FnOnce() + 'scope {
        let mut dtors = self.dtors.borrow_mut();
        *dtors = Some(DtorChain {
            dtor: Box::new(f),
            next: dtors.take().map(Box::new)
        });
    }

    pub fn execute<F>(&self, f: F)
        where F: FnOnce() + Send + 'scope
    {
        let b = unsafe {
            ::std::mem::transmute::<Thunk<'scope>, Thunk<'static>>(Box::new(f))
        };
        self.pool.job_sender.as_ref().unwrap().send(Message::NewJob(b)).unwrap();
        self.defer(move || {

        });
    }
}

impl<'pool, 'scope> Drop for Scope<'pool, 'scope> {
    fn drop(&mut self) {
        self.drop_all()
    }
}

/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    extern crate test;

    use self::test::{Bencher, black_box};
    use super::PoolCache;
    use std::sync::Mutex;
    use std::thread;

    #[test]
    fn smoketest() {
        let mut pool = PoolCache::new(4);

        for i in 1..7 {
            let mut vec = vec![0, 1, 2, 3, 4];
            pool.scope(|s| {
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

    #[test]
    fn test_bench() {
        threads_interleaved_n(&mut POOL_8.lock().unwrap());
        threads_chunked_n(&mut POOL_8.lock().unwrap());
    }

    const MS_SLEEP_PER_OP: u32 = 1;

    lazy_static! {
        static ref POOL_1: Mutex<PoolCache> = Mutex::new(PoolCache::new(1));
        static ref POOL_2: Mutex<PoolCache> = Mutex::new(PoolCache::new(2));
        static ref POOL_4: Mutex<PoolCache> = Mutex::new(PoolCache::new(4));
        static ref POOL_8: Mutex<PoolCache> = Mutex::new(PoolCache::new(8));
    }

    fn threads_interleaved_n(pool: &mut PoolCache)  {
        let size = 1024; // 1kiB

        let mut data = vec![0u8; size];
        pool.scope(|s| {
            for e in data.iter_mut() {
                s.execute(move || {
                    *e += 1;
                    //thread::sleep_ms(MS_SLEEP_PER_OP);
                });
            }
        });
        assert_eq!(data, vec![1u8; size]);
    }

    #[bench]
    fn threads_interleaved_1(b: &mut Bencher) {
        b.iter(|| threads_interleaved_n(&mut POOL_1.lock().unwrap()))
    }

    #[bench]
    fn threads_interleaved_2(b: &mut Bencher) {
        b.iter(|| threads_interleaved_n(&mut POOL_2.lock().unwrap()))
    }

    #[bench]
    fn threads_interleaved_4(b: &mut Bencher) {
        b.iter(|| threads_interleaved_n(&mut POOL_4.lock().unwrap()))
    }

    #[bench]
    fn threads_interleaved_8(b: &mut Bencher) {
        b.iter(|| threads_interleaved_n(&mut POOL_8.lock().unwrap()))
    }

    fn threads_chunked_n(pool: &mut PoolCache) {
        let size = 1024 * 1024 * 1; // 1MiB

        let n = pool.thread_count();
        let mut data = vec![0u8; size];
        pool.scope(|s| {
            let l = (data.len() - 1) / n as usize + 1;
            for es in data.chunks_mut(l) {
                s.execute(move || {
                    for e in es {
                        *e += 1;
                    }
                    thread::sleep_ms(MS_SLEEP_PER_OP);
                });
            }
        });
        assert_eq!(data, vec![1u8; size]);
    }

    #[bench]
    fn threads_chunked_1(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_1.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_2(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_2.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_4(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_4.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_8(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_8.lock().unwrap()))
    }
}
