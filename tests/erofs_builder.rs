//! The tests of the vendored erofs-builder, which snouty compiles in as its
//! `erofs_builder` module. See vendor/erofs-builder/VENDORED.md.
#![allow(clippy::all, clippy::pedantic)]

#[path = "../vendor/erofs-builder/tests/reader_round_trip.rs"]
#[rustfmt::skip]
mod reader_round_trip;
#[path = "../vendor/erofs-builder/tests/round_trip.rs"]
#[rustfmt::skip]
mod round_trip;
