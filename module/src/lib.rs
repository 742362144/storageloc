//! A minimal (i.e. very incomplete) implementation of a Redis server and
//! client.
//!
//! The purpose of this project is to provide a larger example of an
//! asynchronous Rust project built with Tokio. Do not attempt to run this in
//! production... seriously.
//!
//! # Layout
//!
//! The library is structured such that it can be used with guides. There are
//! modules that are public that probably would not be public in a "real" redis
//! client library.
//!
//! The major components are:
//!
//! * `server`: Redis server implementation. Includes a single `run` function
//!   that takes a `TcpListener` and starts accepting redis client connections.
//!
//! * `client`: an asynchronous Redis client implementation. Demonstrates how to
//!   build clients with Tokio.
//!
//! * `cmd`: implementations of the supported Redis commands.
//!
//! * `frame`: represents a single Redis protocol frame. A frame is used as an
//!   intermediate representation between a "command" and the byte
//!   representation.

//! This crate is useful in writing a new client and handling pushback
//! extension on the client side.

pub mod module;

