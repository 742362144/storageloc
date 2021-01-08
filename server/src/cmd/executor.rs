use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::collections::VecDeque;
use crate::cmd::invoke::InvokeResult;
use crate::cmd::Invoke;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};


pub struct Executor {
    rx: Receiver<Invoke>
}

impl Executor {
    pub fn new(rec: Receiver<Invoke> ) -> Executor {
        Executor {
            rx: rec
        }
    }

    pub fn run(&self) {
        loop {
            for received in self.rx {
                println!("{}", received.key());
            }
        }
    }
}