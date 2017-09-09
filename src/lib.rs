#![doc(html_logo_url = "https://www.rust-lang.org/logos/rust-logo-128x128-blk-v2.png",
       html_favicon_url = "https://www.rust-lang.org/favicon.ico", html_root_url = ".")]

//! The [`HashMap`][hash-map] and [`BTreeMap`][btree-map] in the standard library
//! offer very good performance when it comes to inserting and getting stuff,
//! but they're memory killers. If the "stuff" gets large - say, a trillion
//! (10<sup>12</sup>) of them, then we're gonna be in trouble, as we'll then
//! be needing gigs of RAM to hold the data.
//!
//! Moreover, once the program quits, all the *hard-earned* stuff gets deallocated,
//! and we'd have to re-insert them allover again. [`HashFile`][hash-file] deals
//! with this specific problem. It makes use of a `BTreeMap` for storing the keys
//! and values. So, until it reaches the defined capacity, it offers the same
//! performance as that of the btree-map. However, once (and whenever) it reaches
//! the capacity, it *flushes* the stuff to a file (the necessary parameters can be
//! defined in its methods).
//!
//! Hence, at any given moment, the upper limit for the memory eaten by this thing
//! is set by its [capacity][capacity]. This gives us good control over the space-time
//! trade-off. But, the flushing will take O(2<sup>n</sup>) time, depending on the
//! processor and I/O speed, as it does things on the fly with the help of iterators.
//!
//! After the [final manual flush][finish], the file can be stored, moved around, and
//! since it makes use of binary search, values can be obtained in O(log-n) time
//! whenever they're required (depending on the seeking speed of the drive). For
//! example, an average seek takes around 0.3 ms, and a file containing a trillion
//! values demands about 40 seeks (in the worse case), which translates to 12 ms.
//!
//! This kind of "search and seek" is [already being used][wiki] by databases. But,
//! the system is simply an unnecessary complication if you just want a table with
//! a *zillion* rows and only two columns (a key and a value).
//!
//! [*See the `HashFile` type for more info.*][hash-file]
//!
//! [hash-map]: https://doc.rust-lang.org/std/collections/struct.HashMap.html
//! [btree-map]: https://doc.rust-lang.org/std/collections/struct.BTreeMap.html
//! [finish]: struct.HashFile.html#method.finish
//! [capacity]: struct.HashFile.html#method.set_capacity
//! [hash-file]: struct.HashFile.html
//! [wiki]: https://en.wikipedia.org/wiki/B-tree#B-tree_usage_in_databases

extern crate siphasher;

pub const SEP: char = '\0';

mod helpers;
mod hash_file;

pub use hash_file::HashFile;
