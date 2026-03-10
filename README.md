# `tantivy-cache`

An implementation of `tantivy`'s `Directory` that uses `deadpool_redis` to cache
some data.

For now this only caches the footer that `ManagedDirectory` reads at the end of
files, populating the cache on misses without any invalidation.
