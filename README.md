## rust-catalog

A "file-backed" map, which inserts keys and values into a file in O(n) time, and gets the values in O(log-n) time using binary search and file seeking. For now, it only supports (hashable) keys and values that implement the `Display` and `FromStr` traits (i.e., those which can be converted to string and parsed back from string). This will change to serialization in the near future.

See the [module documentation](https://wafflespeanut.github.io/rust-catalog/catalog/) for more information.

### Usage

Note that this is still **experimental**, and so use it at your own risk!

Add the following to your `Cargo.toml`...

``` toml
catalog = "0.1.0"
```

Have a look at the [detailed example](https://wafflespeanut.github.io/rust-catalog/catalog/struct.HashFile.html#examples) for the precise usage.

### Checklist
 - [x] basic documentation and examples
 - [x] maintain the values in a separate data file (which eliminates the sparse file problem)
 - [ ] serialize the values, so that all (serializable) types can be supported
 - [ ] maintain an auxiliary index for speeding up the search
 - [ ] merge the auxiliary index, data file and the main file (during `finish`)
 - [ ] add more methods required for maps (for now, it only supports `insert` and `get`)
 - [ ] documentation of methods
 - [ ] memory-mapped file?
 - [ ] maintain a separate thread for file-writing, so that we don't block on insertion
