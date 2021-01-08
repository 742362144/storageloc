#![feature(
generators,
generator_trait
)]

use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};

use std::pin::Pin;
use std::ops::{Generator, GeneratorState};
use libloading::Symbol;
use libloading::Library;
use std::rc::Rc;

/// Set `key` to hold the string `value`.
///
/// If `key` already holds a value, it is overwritten, regardless of its type.
/// Any previous time to live associated with the key is discarded on successful
/// SET operation.
///
/// # Options
///
/// Currently, the following options are supported:
///
/// * EX `seconds` -- Set the specified expire time, in seconds.
/// * PX `milliseconds` -- Set the specified expire time, in milliseconds.
#[derive(Debug)]
pub struct Invoke {
    /// the lookup key
    key: String,

    /// the value to be stored
    pub(crate) value: Bytes,

    /// When to expire the key
    expire: Option<Duration>,
}

pub struct InvokeResult {
    /// the lookup key
    pub key: String,

    /// the value to be stored
    pub value: Bytes,
}

impl Invoke {
    /// Create a new `Set` command which sets `key` to `value`.
    ///
    /// If `expire` is `Some`, the value should expire after the specified
    /// duration.
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Invoke {
        Invoke {
            key: key.to_string(),
            value,
            expire,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the value
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// Get the expire
    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }

    /// Parse a `Set` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `SET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Set` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing at least 3 entries.
    ///
    /// ```text
    /// SET key value [EX seconds|PX milliseconds]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Invoke> {
        use ParseError::EndOfStream;

        // Read the key to set. This is a required field
        let key = parse.next_string()?;

        // Read the value to set. This is a required field.
        let value = parse.next_bytes()?;

        // The expiration is optional. If nothing else follows, then it is
        // `None`.
        let mut expire = None;

        // Attempt to parse another string.
        match parse.next_string() {
            Ok(s) if s == "EX" => {
                // An expiration is specified in seconds. The next value is an
                // integer.
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s == "PX" => {
                // An expiration is specified in milliseconds. The next value is
                // an integer.
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            // Currently, mini-redis does not support any of the other SET
            // options. An error here results in the connection being
            // terminated. Other connections will continue to operate normally.
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            // The `EndOfStream` error indicates there is no further data to
            // parse. In this case, it is a normal run time situation and
            // indicates there are no specified `SET` options.
            Err(EndOfStream) => {}
            // All other errors are bubbled up, resulting in the connection
            // being terminated.
            Err(err) => return Err(err.into()),
        }

        Ok(Invoke { key, value, expire })
    }

    /// Apply the `Set` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {

        db.enque(Invoke{key:self.key.clone(), value: self.value.clone(), expire: None});

        let val = self.value.clone();
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
            let mut generator = func(Rc::new(db));

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



        if let Some(value) = db.get("c"){
            // let s = String::from_utf8(value.to_vec()).expect("Found invalid UTF-8");
            let v = String::from_utf8(value.to_vec()).unwrap();
            println!("1 + 2 = {}", v);
            db.set(String::from("c"), Bytes::from(v.clone()), None);
        };

        // Create a success response and write it to `dst`.
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Set` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("invoke".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);
        frame
    }
}
