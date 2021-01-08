/* Copyright (c) 2018 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

use std::ops::{Generator, GeneratorState};

use std::cell::{Cell, RefCell};
use std::panic::*;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;

use super::cycles;

//use log::info;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};

use spin::RwLock;
use crate::{Connection, Db, Frame};
use libloading::os::unix::{Library, Symbol};
use bytes::Bytes;
use log::info;


/// This enum represents the different states a task can be in.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq)]
pub enum TaskState {
    /// A task is in this state when it has just been created, but has not
    /// had a chance to execute on the CPU yet.
    INITIALIZED = 0x01,

    /// A task is in this state when it is currently running on the CPU.
    RUNNING = 0x02,

    /// A task is in this state when it has got a chance to run on the CPU at
    /// least once, but has yeilded to the scheduler, and is currently not
    /// executing on the CPU.
    YIELDED = 0x03,

    /// A task is in this state when it has finished executing completely, and
    /// it's results are ready.
    COMPLETED = 0x04,

    /// A task is in this state when it has been stopped without completion, after
    /// setting this state, the pushback mechanism will run.
    STOPPED = 0x5,

    /// A task is in this state when it has been suspended due to IO. On the client side
    /// the task can wait for the native operation responses.
    WAITING = 0x6,
}

/// This enum represents the priority of a task in the system. A smaller value
/// indicates a task with a higher priority.
#[repr(u8)]
#[derive(Clone, PartialEq)]
pub enum TaskPriority {
    /// The priority of a dispatch task. Highest in the system, because this
    /// task is responsible for all network processing.
    DISPATCH = 0x01,

    /// The priority of a task corresponding to an RPC request.
    REQUEST = 0x02,
}

/// This trait consists of methods that will allow a type to be run as a task
/// on Sandstorm's scheduler.
pub trait Task {
    /// When called, this method should "run" the task.
    ///
    /// # Return
    ///
    /// A tuple whose first member consists of the current state of the task
    /// (`TaskState`), and whose second member consists of the amount of time
    /// in cycles the task continuously ran for during this call to run().
    fn run(&mut self) -> (TaskState, u64);

    /// When called, this method should return the current state of the task.
    ///
    /// # Return
    ///
    /// The current state of the task (`TaskState`).
    fn state(&self) -> TaskState;

    /// When called, this method should return the total time for which the task
    /// has run since it was created.
    ///
    /// # Return
    ///
    /// The total time for which the task has run in cycles.
    fn time(&self) -> u64;

    /// When called, this method should return the total time for which the task
    /// has spent in db operations since it was created.
    ///
    /// # Return
    ///
    /// The total time for which the task has spent in db operations in cycles.
    fn db_time(&self) -> u64;

    /// When called, this method should return the priority of the task.
    ///
    /// # Return
    ///
    /// The priority of the task.
    fn priority(&self) -> TaskPriority;

    /// When called, this method will change the task state to `state` and will return.
    ///
    /// # Arguments
    ///
    /// * `state`: The state, which will be assigned to the task.
    fn set_state(&mut self, state: TaskState);

    /// This method returns the unique task identifier.
    ///
    /// # Return
    ///
    /// Task id to identifiy the task uniquely.
    fn get_id(&self) -> u64;
}

pub struct Container {
    // The current state of the task. Required to determine if the task
    // has completed execution.
    state: TaskState,

    // The priority of the task. Required to determine when the task should
    // be run next, if it has not completed already.
    priority: TaskPriority,

    // The total amount of time in cycles the task has run for. Required to
    // determine when the task should be run next, and for accounting purposes.
    time: u64,

    // The total amount of time in cycles the task has spend inside the database.
    // Required to determine the credit for each run of an extension.
    db_time: u64,

    // An execution context for the task that implements the DB trait. Required
    // for the task to interact with the database.
    db: Cell<Option<Rc<Context>>>,

    // The actual generator/coroutine containing the extension's code to be
    // executed inside the database.
    gen: Option<Pin<Box<dyn Generator<Yield = u64, Return = u64>>>>,
}

pub struct Context {
    // The credit which the extension has earned by making the db calls.
    db_credit: RefCell<u64>,
}

impl Context {

    pub fn new(
    ) -> Context {
        Context {
            db_credit: RefCell::new(0),
        }
    }

    /// This method returns the value of the credit which an extension has accumulated over time.
    /// The extension credit is increased whenever it makes a DB function call; like get(),
    /// multiget(), put(), etc. For each DB call the credit is time spent in the called function
    /// plus some extra credit which the datastore need to waste in RPC handling.
    ///
    /// # Return
    ///
    /// The current value of the credit for the extension.
    pub fn db_credit(&self) -> u64 {
        self.db_credit.borrow().clone()
    }
}


// Implementation of methods on Container.
impl Container {
    /// Creates a new container holding an untrusted extension that can be
    /// scheduled by the database.
    ///
    /// # Arguments
    ///
    /// * `prio`:    The priority of the container/task. Required by the
    ///              scheduler.
    /// * `context`: The execution context for the extension. Allows the
    ///              extension to interact with the database.
    /// * `ext`:     A handle to the extension that will be run inside this
    ///              container.
    ///
    /// # Return
    ///
    /// A container that when scheduled, runs the extension.
    pub fn new(
        prio: TaskPriority,
        context: Rc<Context>,
        gen: Pin<Box<Generator<Yield = u64, Return = u64>>>,
    ) -> Container {
        // The generator is initialized to a dummy. The first call to run() will
        // retrieve the actual generator from the extension.
        Container {
            state: TaskState::INITIALIZED,
            priority: prio,
            time: 0,
            db_time: 0,
            db: Cell::new(Some(context)),
            gen: Some(gen),
        }
    }
}


impl Task for Container {
    /// Refer to the Task trait for Documentation.
    fn run(&mut self) -> (TaskState, u64) {
        let start = cycles::_rdtsc();

        // Resume the task if need be. The task needs to be run/resumed only
        // if it is in the INITIALIZED or YIELDED state. Nothing needs to be
        // done if it has already completed, or was aborted.
        if self.state == TaskState::INITIALIZED || self.state == TaskState::YIELDED {
            self.state = TaskState::RUNNING;

            // Catch any panics thrown from within the extension.
            let res = catch_unwind(AssertUnwindSafe(|| match self.gen.as_mut() {
                Some(gen) => match gen.as_mut().resume(()) {
                    GeneratorState::Yielded(_) => {
                        if let Some(db) = self.db.get_mut() {
                            self.db_time = db.db_credit();
                        }
                        self.state = TaskState::YIELDED;
                    }

                    GeneratorState::Complete(_) => {
                        if let Some(db) = self.db.get_mut() {
                            self.db_time = db.db_credit();
                        }
                        self.state = TaskState::COMPLETED;
                    }
                },

                None => {
                    panic!("No generator available for extension execution");
                }
            }));

            // If there was a panic thrown, then mark the container as COMPLETED so that it
            // does not get run again.
            if let Err(_) = res {
                self.state = TaskState::COMPLETED;
                if thread::panicking() {
                    // Wait for 100 millisecond so that the thread is moved to the GHETTO core.
                    let start = cycles::_rdtsc();
                    while cycles::_rdtsc() - start < cycles::cycles_per_second() / 10 {}
                }
            }
        }

        // Calculate the amount of time the task executed for in cycles.
        let exec = cycles::_rdtsc() - start;

        // Update the total execution time of the task.
        self.time += exec;

        // Return the state and the amount of time the task executed for.
        return (self.state, exec);
    }

    /// Refer to the Task trait for Documentation.
    fn state(&self) -> TaskState {
        self.state.clone()
    }

    /// Refer to the Task trait for Documentation.
    fn time(&self) -> u64 {
        self.time.clone()
    }

    /// Refer to the Task trait for Documentation.
    fn db_time(&self) -> u64 {
        self.db_time.clone()
    }

    /// Refer to the Task trait for Documentation.
    fn priority(&self) -> TaskPriority {
        self.priority.clone()
    }

    /// Refer to the `Task` trait for Documentation.
    fn set_state(&mut self, state: TaskState) {
        self.state = state;
    }


    /// Refer to the `Task` trait for Documentation.
    fn get_id(&self) -> u64 {
        0
    }
}


/// Interval in microsecond which each task can use as credit to perform CPU work.
/// Under load shedding, the task which used more than this credit will be pushed-back.
const CREDIT_LIMIT_US: f64 = 0.5f64;
/// The maximum number of tasks the dispatcher can take in one go.
const MAX_RX_PACKETS: usize = 32;

const MAX_CONNECTIONS: usize = 128;

/// A simple round robin scheduler for Tasks in Sandstorm.
pub struct RoundRobin {
    // The time-stamp at which the scheduler last ran. Required to identify whether there is an
    // uncooperative task running on the scheduler.
    latest: AtomicUsize,

    // Atomic flag indicating whether there is a malicious/long running procedure on this
    // scheduler. If true, the scheduler must return down to Netbricks on the next call to poll().
    compromised: AtomicBool,

    // Identifier of the thread this scheduler is running on. Required for pre-emption.
    thread: AtomicUsize,

    // Identifier of the core this scheduler is running on. Required for pre-emption.
    core: AtomicIsize,

    // Run-queue of tasks waiting to execute. Tasks on this queue have either yielded, or have been
    // recently enqueued and never run before.
    waiting: RwLock<VecDeque<Box<Task>>>,

    // task_completed is incremented after the completion of each task. Reset to zero
    // after every 1M tasks.
    task_completed: RefCell<u64>,

    db: Rc<Db>,
}

// Implementation of methods on RoundRobin.
impl RoundRobin {
    /// Creates and returns a round-robin scheduler that can run tasks implementing the `Task`
    /// trait.
    ///
    /// # Arguments
    ///
    /// * `thread`: Identifier of the thread this scheduler will run on.
    /// * `core`:   Identifier of the core this scheduler will run on.
    pub fn new(thread: u64, core: i32, db: Rc<Db>) -> RoundRobin {
        RoundRobin {
            latest: AtomicUsize::new(cycles::_rdtsc() as usize),
            compromised: AtomicBool::new(false),
            thread: AtomicUsize::new(thread as usize),
            core: AtomicIsize::new(core as isize),
            waiting: RwLock::new(VecDeque::new()),
//            responses: RwLock::new(Vec::new()),
            task_completed: RefCell::new(0),
            db
        }
    }

    /// Enqueues a task onto the scheduler. The task is enqueued at the end of the schedulers
    /// queue.
    ///
    /// # Arguments
    ///
    /// * `task`: The task to be added to the scheduler. Must implement the `Task` trait.
    #[inline]
    pub fn enqueue(&self, task: Box<Task>) {
        self.waiting.write().push_back(task);
    }

    /// Enqueues multiple tasks onto the scheduler.
    ///
    /// # Arguments
    ///
    /// * `tasks`: A deque of tasks to be added to the scheduler. These tasks will be run in the
    ///            order that they are provided in, and must implement the `Task` trait.
    #[inline]
    pub fn enqueue_many(&self, mut tasks: VecDeque<Box<Task>>) {
        self.waiting.write().append(&mut tasks);
    }

    /// Dequeues all waiting tasks from the scheduler.
    ///
    /// # Return
    ///
    /// A deque of all waiting tasks in the scheduler. This tasks might be in various stages of
    /// execution. Some might have run for a while and yielded, and some might have never run
    /// before. If there are no tasks waiting to run, then an empty vector is returned.
    #[inline]
    pub fn dequeue_all(&self) -> VecDeque<Box<Task>> {
        let mut tasks = self.waiting.write();
        return tasks.drain(..).collect();
    }


    /// Returns the time-stamp at which the latest scheduling decision was made.
    #[inline]
    pub fn latest(&self) -> u64 {
        self.latest.load(Ordering::Relaxed) as u64
    }

    /// Sets the compromised flag on the scheduler.
    #[inline]
    pub fn compromised(&self) {
        self.compromised.store(true, Ordering::Relaxed);
    }

    /// Returns the identifier of the thread this scheduler was configured to run on.
    #[inline]
    pub fn thread(&self) -> u64 {
        self.thread.load(Ordering::Relaxed) as u64
    }

    /// Returns the identifier of the core this scheduler was configured to run on.
    #[inline]
    pub fn core(&self) -> i32 {
        self.core.load(Ordering::Relaxed) as i32
    }

    /// Picks up a task from the waiting queue, and runs it until it either yields or completes.
    pub fn poll(&self) {
        let mut total_time: u64 = 0;
        let mut db_time: u64 = 0;
        let credit = (CREDIT_LIMIT_US / 1000000f64) * (cycles::cycles_per_second() as f64);

        // 如果两个dispatcher调用相隔20个us，则触发Pushback。
        // XXX: Trigger Pushback if the two dispatcher invocation is 20 us apart.
        let time_trigger: u64 = 2000 * credit as u64;
        let mut previous: u64 = 0;
        loop {
            // Set the time-stamp of the latest scheduling decision.
            let current = cycles::_rdtsc();
            self.latest.store(current as usize, Ordering::Relaxed);

            // If the compromised flag was set, then return.
            if self.compromised.load(Ordering::Relaxed) {
                return;
            }

            // If there are tasks to run, then pick one from the head of the queue, and run it until it
            // either completes or yields back.
            let task = self.waiting.write().pop_front();

            if let Some(mut task) = task {
                let mut is_dispatcher: bool = false;
                let mut queue_length: usize = 0;
                let mut difference: u64 = 0;

                // 是调度任务，读取队列长度
                match task.priority() {
                    TaskPriority::DISPATCH => {
                        is_dispatcher = true;
                        queue_length = self.waiting.read().len();

                        // The time difference include the dispatcher time to account the native
                        // operations.
                        difference = current - previous;
                        previous = current;
                    }

                    _ => {}
                }

                // task.run() 运行任务
                if task.run().0 == TaskState::COMPLETED {
                    // The task finished execution, check for request and response packets. If they
                    // exist, then free the request packet, and enqueue the response packet.
                    // 任务完成后，检查请求和响应包。如果存在，则释放请求包，并将响应包入队。
//                    if let Some((req, res)) = unsafe { task.tear() } {
//                        req.free_packet();
////                        self.responses
////                            .write()
////                            .push(?);
//                    }
                    if cfg!(feature = "execution") {
                        total_time += task.time();
                        db_time += task.db_time();
                        let mut count = self.task_completed.borrow_mut();
                        *count += 1;
                        // 每隔1000000打印一次信息
                        let every = 1000000;
                        if *count >= every {
//                            info!("Total {}, DB {}", total_time / (*count), db_time / (*count));
                            *count = 0;
                            total_time = 0;
                            db_time = 0;
                        }
                    }
                } else {
                    // 任务没有完成。要么将其添加回等待列表以便再次运行，要么运行推回机制。
                    // 只有在调度程序任务执行之后才会启动回推。
                    // 触发回推:任务没有完成。要么将其添加回等待列表以便再次运行，要么运行推回机制。
                    // 只有在调度程序任务执行之后才会启动回推。触发回推
                    //
                    // The task did not complete execution. EITHER add it back to the waiting list so that it
                    // gets to run again OR run the pushback mechanism. The pushback starts only after that
                    // dispatcher task execution. Trigger pushback:-
                    //
                    // 如果队列中有MAX_RX_PACKETS /4 yeilded任务，或者两个dispatcher调用相隔2000 us，并且当前dispatcher调用接收到MAX_RX_PACKETS /4个新任务。
                    //
                    // if there are MAX_RX_PACKETS /4 yeilded tasks in the queue, OR
                    // if two dispatcher invocations are 2000 us apart, AND
                    // if the current dispatcher invocation received MAX_RX_PACKETS /4 new tasks.
                    if cfg!(feature = "pushback")
                        && is_dispatcher == true
                        && (queue_length >= MAX_RX_PACKETS / 8 || difference > time_trigger)
                        && ((self.waiting.read().len() - queue_length) > 0)
                    {
                        for _i in 0..queue_length {
                            let mut yeilded_task = self.waiting.write().pop_front().unwrap();

                            // 为每个任务计算排名/信用，以推回一些排名/信用超过阈值的任务。
                            // Compute Ranking/Credit on the go for each task to pushback
                            // some of the tasks whose rank/credit is more than the threshold.
                            if (yeilded_task.state() == TaskState::YIELDED)
                                && ((yeilded_task.time() - yeilded_task.db_time()) > credit as u64)
                            {
                                yeilded_task.set_state(TaskState::STOPPED);
//                                if let Some((req, res)) = unsafe { yeilded_task.tear() } {
//                                    req.free_packet();
////                                    self.responses
////                                        .write()
////                                        .push(?);
//                                }
                            } else {
                                self.waiting.write().push_front(yeilded_task);
                            }
                        }
                    }
                    self.waiting.write().push_back(task);
                }
            }
        }
    }

    pub fn run(&self) {
        loop {
            let mut cmd = self.db.deque();

            let val = cmd.value.clone();
            // Set the value in the shared database state.
            // db.set(self.key, self.value, self.expire);

            // type Proc = unsafe extern "C" fn(Rc<Db>) -> Pin<Box<Generator<Yield=u64, Return=InvokeResult>>>;
            type Proc = unsafe extern "C" fn(Rc<&Db>) -> Pin<Box<Generator<Yield=u64, Return=u64>>>;
            // type Proc = unsafe extern "C" fn(Rc<Db>) -> Pin<Box<Generator<Yield=u64, Return=u64>>>;
            let library_path = String::from("/home/coder/IdeaProjects/storageloc/ext/add/target/debug/libadd.so");
            println!("Loading add() from {}", library_path);

            let lib = Library::new(library_path).unwrap();

            unsafe {
                let func: Symbol<Proc> = lib.get(b"init").unwrap();
                let mut generator = func(Rc::new(self.db));

                // println!("1");
                // Pin::new(&mut generator).resume(());
                // println!("3");
                // let Some(GeneratorState<res1, res2>) = Pin::new(&mut generator).resume(());
                // println!("5");

                // db.set(String::from("c"), Bytes::from("dadada"), None);
                match generator.as_mut().resume(()) {
                    GeneratorState::Yielded(1) => println!("Yielded"),
                    _ => panic!("unexpected return from resume"),
                }
                match generator.as_mut().resume(()) {
                    GeneratorState::Complete(1111) => println!("Completed"),
                    _ => panic!("unexpected return from resume"),
                }
                // println!("1 + 2 = {}", answer);
            }

            // let value = String::from_utf8(val.to_vec()).expect("Found invalid UTF-8");
            // println!("1 + 2 = {}", value);


            if let Some(value) = self.db.get("c") {
                // let s = String::from_utf8(value.to_vec()).expect("Found invalid UTF-8");
                let v = String::from_utf8(value.to_vec()).unwrap();
                println!("1 + 2 = {}", v);
                self.db.set(String::from("c"), Bytes::from(v.clone()), None);
            };

            // Create a success response and write it to `dst`.
            // let response = Frame::Simple("OK".to_string());
            // debug!(?response);
            // dst.write_frame(&response).await?;

            // Ok(())
        }
    }
}


#[test]
fn test() {
    let mut generator = || {
        println!("2");
        yield;
        println!("4");
    };

    println!("1");
    Pin::new(&mut generator).resume(());
    println!("3");
    Pin::new(&mut generator).resume(());
    println!("5");
}




