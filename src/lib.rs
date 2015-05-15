use std::thread::{self, JoinHandle};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::{Arc, Mutex, Barrier};
use std::cell::{RefCell, Cell};
//use std::marker::PhantomData;

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
        println!("Ending {} threads (outer)...", self.threads.len());
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
        for ti in 0..n {
            let job_receiver = job_receiver.clone();
            println!("Creating thread {}...", ti);
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
                            println!("  Joining thread {}...", ti);
                            job_receiver.1.wait();
                            println!("  Joined thread {}", ti);
                        }
                        // The pool was dropped.
                        Err(..) => break
                    }
                }
                println!("Ending thread {} (inner)", ti);
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
        println!(" Scope start");
        let mut scope = Scope { pool: self, dtors: RefCell::new(None), exec_counter: Cell::new(0) };
        let ret = f(&scope);
        scope.drop_all();
        println!(" Scope end");
        ret
    }
}

/////////////////////////////////////////////////////////////////////////////

pub struct Scope<'pool, 'scope> {
    pool: &'pool mut PoolCache,
    dtors: RefCell<Option<DtorChain<'scope>>>,
    exec_counter: Cell<u32>,
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
        println!("  Joining master...");
        self.pool.join_barrier.1.wait();
        println!("  Joined master");
    }

    fn defer<F>(&self, f: F) where F: FnOnce() + 'scope {
        let mut dtors = self.dtors.borrow_mut();
        *dtors = Some(DtorChain {
            dtor: Box::new(f),
            next: dtors.take().map(Box::new)
        });
    }

    pub fn execute<F>(&'scope self, f: F)
        where F: FnOnce() + Send + 'scope
    {
        let b = unsafe {
            ::std::mem::transmute::<Thunk<'scope>, Thunk<'static>>(Box::new(f))
        };
        let id = self.exec_counter.get();
        println!("  exec {}: Sending thread job...", id);
        self.pool.job_sender.as_ref().unwrap().send(Message::NewJob(b)).unwrap();
        println!("  exec {}: Sent thread job", id);
        self.defer(move || {
            println!("  exec {}: Sending thread join...", id);
            self.pool.job_sender.as_ref().unwrap().send(Message::Join).unwrap();
            println!("  exec {}: Sent thread join", id);
        });
        self.exec_counter.set(self.exec_counter.get() + 1);
    }
}

impl<'pool, 'scope> Drop for Scope<'pool, 'scope> {
    fn drop(&mut self) {
        self.drop_all()
    }
}

#[test]
fn smoketest() {
    let mut pool = PoolCache::new(4);

    for i in 0..6 {
        let mut vec = vec![1,2];
        pool.scope(|s| {
            for e in &mut vec {
                s.execute(|| {
                    thread::sleep_ms(1000);
                    *e += i
                });
            }
        });
    }
}
