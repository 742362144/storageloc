#![forbid(unsafe_code)]
#![feature(generators)]
#![feature(generator_trait)]

extern crate storageloc;

use std::rc::Rc;
use std::ops::Generator;
use std::pin::Pin;

use storageloc::db::Db;

#[no_mangle]
#[allow(unreachable_code)]
#[allow(unused_assignments)]
pub fn init(db: Rc<&Db>) -> Pin<Box<Generator<Yield=u64, Return=&str>>> {
    Box::pin(move || {
        yield 1;
        "foo"
    })
}



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
