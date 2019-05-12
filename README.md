# Concurrent-Lock-free-Hash-Tries
The Java implementation of Concurrent Lock-free Hash Tries (Cache-tries) presented by [Aleksandar Prokopec](http://aleksandar-prokopec.com/resources/docs/p137-prokopec.pdf). In this variant of Hash tries, an auxiliary data structure(cache) is used to allow the basic operations to run in expected O(1) time. The data structure is implemented generically as CacheTries<K, V>. For more information (e.g. proof of runtime, performance evaluation), please refer to the original paper mentioned above. 
