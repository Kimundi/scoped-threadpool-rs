#![cfg_attr(all(feature="nightly", test), feature(test))]
#![cfg_attr(feature="nightly", feature(const_fn))]

#[macro_use]
#[cfg(test)]
extern crate lazy_static;

use std::thread::{self, JoinHandle};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::{Arc, Mutex, Barrier};
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

        // spawn n threads, put them in waiting mode
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
    }

    pub fn scope<'pool, 'scope, F, R>(&'pool mut self, f: F) -> R
        where F: FnOnce(&Scope<'pool, 'scope>) -> R
    {
        let scope = Scope {
            pool: self,
            _marker: PhantomData,
        };
        f(&scope)
    }

    pub fn thread_count(&self) -> u32 {
        self.threads.len() as u32
    }
}

/////////////////////////////////////////////////////////////////////////////

pub struct Scope<'pool, 'scope> {
    pool: &'pool mut PoolCache,
    _marker: PhantomData<&'scope mut ()>,
}

impl<'pool, 'scope> Scope<'pool, 'scope> {
    pub fn execute<F>(&self, f: F)
        where F: FnOnce() + Send + 'scope
    {
        let b = unsafe {
            ::std::mem::transmute::<Thunk<'scope>, Thunk<'static>>(Box::new(f))
        };
        self.pool.job_sender.as_ref().unwrap().send(Message::NewJob(b)).unwrap();
    }
}

impl<'pool, 'scope> Drop for Scope<'pool, 'scope> {
    fn drop(&mut self) {
        for _ in 0..self.pool.threads.len() {
            self.pool.job_sender.as_ref().unwrap().send(Message::Join).unwrap();
        }

        self.pool.join_barrier.1.wait();
    }
}

/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::PoolCache;

    #[test]
    fn example() {
        // Create a threadpool holding 4 threads
        let mut pool = PoolCache::new(4);

        let mut vec = vec![0, 1, 2, 3, 4, 5, 6, 7];

        // Use the threads as scoped threads that can
        // reference anything outside this closure
        pool.scope(|scoped| {

            // Create references to each element in the vector ...
            for e in &mut vec {
                // ... and add 1 to it in a seperate thread
                scoped.execute(move || {
                    *e += 1;
                });
            }
        });

        assert_eq!(vec, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

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
    #[should_panic]
    fn panicking() {
        let mut pool = PoolCache::new(4);
        pool.scope(|scoped| {
            scoped.execute(move || {
                panic!()
            });
        });
    }
}

#[cfg(all(test, feature="nightly"))]
mod benches {
    extern crate test;

    use self::test::{Bencher, black_box};
    use super::PoolCache;
    use std::sync::Mutex;

    // const MS_SLEEP_PER_OP: u32 = 1;

    lazy_static! {
        static ref POOL_1: Mutex<PoolCache> = Mutex::new(PoolCache::new(1));
        static ref POOL_2: Mutex<PoolCache> = Mutex::new(PoolCache::new(2));
        static ref POOL_3: Mutex<PoolCache> = Mutex::new(PoolCache::new(3));
        static ref POOL_4: Mutex<PoolCache> = Mutex::new(PoolCache::new(4));
        static ref POOL_5: Mutex<PoolCache> = Mutex::new(PoolCache::new(5));
        static ref POOL_8: Mutex<PoolCache> = Mutex::new(PoolCache::new(8));
    }

    fn fib(n: u64) -> u64 {
        let mut prev_prev: u64 = 1;
        let mut prev = 1;
        let mut current = 1;
        for _ in 2..(n+1) {
            current = prev_prev.wrapping_add(prev);
            prev_prev = prev;
            prev = current;
        }
        current
    }

    fn threads_interleaved_n(pool: &mut PoolCache)  {
        let size = 1024; // 1kiB

        let mut data = vec![1u8; size];
        pool.scope(|s| {
            for e in data.iter_mut() {
                s.execute(move || {
                    *e += fib(black_box(1000 * (*e as u64))) as u8;
                    //thread::sleep_ms(MS_SLEEP_PER_OP);
                });
            }
        });
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
        let size = 1024 * 1024 * 100 / 4; // 100MiB

        let n = pool.thread_count();
        let mut data = vec![0u32; size];
        pool.scope(|s| {
            let l = (data.len() - 1) / n as usize + 1;
            for es in data.chunks_mut(l) {
                s.execute(move || {
                    if es.len() > 1 {
                        es[0] = 1;
                        es[1] = 1;
                        for i in 2..es.len() {
                            // Fibonnaci gets big fast,
                            // so just wrap around all the time
                            es[i] = es[i-1].wrapping_add(es[i-2]);
                        }
                    }
                    //thread::sleep_ms(MS_SLEEP_PER_OP);
                });
            }
        });
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
    fn threads_chunked_3(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_3.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_4(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_4.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_5(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_5.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_8(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_8.lock().unwrap()))
    }
}
