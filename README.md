## rust-catalog

A "file-backed" map, which inserts keys and values into a file in O(n) time, and gets the values in O(log-n) time using binary search and file seeking. For now, it only supports keys and values that support the `Display` and `FromStr` traits (i.e.,) those which can be converted to string and parsed back from string. This will change to serialization in the near future.

### Notes

While memory-backed maps are good for fast insertion and getting of data, they're memory-killers. You need gigs of RAM to store, say, a billion keys and values. Moreover, once the data gets deallocated, we have to build the map allover again.

This, on the other hand, takes ages for building (depending on the data, how often it's flushed, IO speed, etc.). But, once it's in place, getting the value is on the order of **milliseconds!**

Note that this is still **experimental**, and so use it at your own risk!

### Checklist
 - [ ] documentation and examples
 - [ ] keep track of overwritten values and return them as iterator during `get`
 - [ ] serialize the values, so that all (serializable) types can be supported
 - [ ] more methods required for maps
