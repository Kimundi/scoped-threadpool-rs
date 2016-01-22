#![feature(test)]
#![feature(const_fn)]

extern crate test;
#[macro_use]
extern crate lazy_static;
extern crate scoped_threadpool;

use self::test::{Bencher, black_box};
use scoped_threadpool::Pool;
use std::sync::Mutex;

// const MS_SLEEP_PER_OP: u32 = 1;

lazy_static! {
    static ref POOL_1: Mutex<Pool> = Mutex::new(Pool::new(1));
    static ref POOL_2: Mutex<Pool> = Mutex::new(Pool::new(2));
    static ref POOL_3: Mutex<Pool> = Mutex::new(Pool::new(3));
    static ref POOL_4: Mutex<Pool> = Mutex::new(Pool::new(4));
    static ref POOL_5: Mutex<Pool> = Mutex::new(Pool::new(5));
    static ref POOL_8: Mutex<Pool> = Mutex::new(Pool::new(8));
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

fn threads_interleaved_n(pool: &Pool)  {
    let size = 1024; // 1kiB

    let mut data = vec![1u8; size];
    let manager = pool.manager();
    manager.scoped(|s| {
        for e in data.iter_mut() {
            s.submit(move || {
                *e += fib(black_box(1000 * (*e as u64))) as u8;
                for i in 0..10000 { black_box(i); }
                //thread::sleep_ms(MS_SLEEP_PER_OP);
            });
        }
    });
}

#[bench]
fn threads_interleaved_1(b: &mut Bencher) {
    b.iter(|| threads_interleaved_n(&POOL_1.lock().unwrap()))
}

#[bench]
fn threads_interleaved_2(b: &mut Bencher) {
    b.iter(|| threads_interleaved_n(&POOL_2.lock().unwrap()))
}

#[bench]
fn threads_interleaved_4(b: &mut Bencher) {
    b.iter(|| threads_interleaved_n(&POOL_4.lock().unwrap()))
}

#[bench]
fn threads_interleaved_8(b: &mut Bencher) {
   b.iter(|| threads_interleaved_n(&mut POOL_8.lock().unwrap()))
}

fn threads_chunked_n(pool: &Pool) {
   // Set this to 1GB and 40 to get good but slooow results
   let size = 1024 * 1024 * 10 / 4; // 10MiB
   let bb_repeat = 50;

   let mut data = vec![0u32; size];
   let manager = pool.manager();
   let n = manager.threads();
   manager.scoped(|s| {
       let l = (data.len() - 1) / n as usize + 1;
       for es in data.chunks_mut(l) {
           s.submit(move || {
               if es.len() > 1 {
                   es[0] = 1;
                   es[1] = 1;
                   for i in 2..es.len() {
                       // Fibonnaci gets big fast,
                       // so just wrap around all the time
                       es[i] = black_box(es[i-1].wrapping_add(es[i-2]));
                       for i in 0..bb_repeat { black_box(i); }
                   }
               }
               //thread::sleep_ms(MS_SLEEP_PER_OP);
           });
       }
   });
}

#[bench]
fn threads_chunked_1(b: &mut Bencher) {
    b.iter(|| threads_chunked_n(&POOL_1.lock().unwrap()))
}

#[bench]
fn threads_chunked_2(b: &mut Bencher) {
    b.iter(|| threads_chunked_n(&POOL_2.lock().unwrap()))
}

#[bench]
fn threads_chunked_3(b: &mut Bencher) {
    b.iter(|| threads_chunked_n(&POOL_3.lock().unwrap()))
}

#[bench]
fn threads_chunked_4(b: &mut Bencher) {
    b.iter(|| threads_chunked_n(&POOL_4.lock().unwrap()))
}

#[bench]
fn threads_chunked_5(b: &mut Bencher) {
    b.iter(|| threads_chunked_n(&POOL_5.lock().unwrap()))
}

#[bench]
fn threads_chunked_8(b: &mut Bencher) {
    b.iter(|| threads_chunked_n(&POOL_8.lock().unwrap()))
}
