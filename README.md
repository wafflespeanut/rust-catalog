## rust-catalog

A "file-backed" map, which inserts keys and values into a file in O(n) time, and gets the values in O(log-n) time using binary search and file seeking. For now, it only supports insertion and getting of (hashable) keys and values that implement the `Display` and `FromStr` traits (i.e., those which can be converted to string and parsed back from string).

See the [module documentation](https://docs.rs/catalog/) for more information.

### Usage

Note that this was an **experiment**, and so use it at your own risk!

Add the following to your `Cargo.toml`...

``` toml
catalog = "0.1.2"
```

Have a look at the [detailed example](https://docs.rs/catalog/^0.1/catalog/struct.HashFile.html#examples) for the precise usage.
