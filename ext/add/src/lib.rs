#![forbid(unsafe_code)]
#![feature(generators)]
#![feature(generator_trait)]

extern crate storage;

use std::rc::Rc;
use std::ops::Generator;
use std::pin::Pin;

use bytes::{Bytes, BytesMut, BufMut, Buf};

use storage::db::Db;
use storage::cmd::invoke::InvokeResult;

#[no_mangle]
#[allow(unreachable_code)]
#[allow(unused_assignments)]
pub fn init(db: Rc<&Db>) -> Pin<Box<Generator<Yield=u64, Return=u64> + '_>> {
    Box::pin(move || {
        let i:u64 = 1;
        yield i;
        // yield 1;
        println!("{}", "success");
        let mut c = String::from("c");
        let ms = "bbbbbb";
        let mut b = Bytes::from(ms);
        let mut expire = None;
        db.set(c, b, expire);
        // InvokeResult {
        //     key: c,
        //     value: b,
        // }
        1111
    })
}
// pub fn init(db: Rc<Db>) -> Pin<Box<Generator<Yield=u64, Return=&'static str>>> {
//     Box::pin(move || {
//         yield 1;
//         // yield 1;
//         println!("{}", "success");
//         let c = String::from("c");
//         let b = Bytes::from("bbbbbb");
//         db.set(c, b, None);
//         "foo"
//     })
// }



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
