// Copyright 2020 Tyler Neely, Alex Regueiro
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::all)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// `deref_nullptr` is a new lint in Rust 1.53, so to retain compatibility with earlier Rust
// versions we allow unknown lints.  This should go away when the minimum supported Rust version is
// 1.53 or higher.
#![allow(unknown_lints)]
// Work around bindgen bug https://github.com/rust-lang/rust-bindgen/issues/1651 and allow the
// deref of a null pointer.  It doesn't actually do this but it generates the code as part of tests
// or something.
#![allow(deref_nullptr)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(feature = "bzip2")]
#[no_mangle]
pub fn bz_internal_error(errcode: libc::c_int) {
    panic!("bz internal error: {}", errcode);
}
