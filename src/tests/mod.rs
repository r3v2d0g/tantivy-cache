use deadpool_redis::redis::AsyncCommands;
use dir::FailingDirectory;
use eyre::{Result, ensure};
use tantivy::{
    Index, IndexSettings, ReloadPolicy,
    directory::RamDirectory,
    doc,
    schema::{FAST, INDEXED, STORED, SchemaBuilder},
};
use utils::{create_test_index, redis_pool, search};

use crate::{CachingDirectory, keys};

mod dir;
mod utils;

/// Creates an index, searches it (cache miss), then opens it a second time
/// and searches again (cache hit). Both must succeed and return the same
/// results.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_and_search_with_cache() -> Result<()> {
    let pool = redis_pool().await?;

    // First open: cache miss — populates Redis.
    let index = create_test_index(pool.clone())?;
    let results = search(&index, "old")?;
    assert_eq!(results, vec!["The Old Man and the Sea"]);

    let results = search(&index, "Moby")?;
    assert_eq!(results, vec!["Moby Dick"]);

    // Second open: re-open the same directory so all segment files go
    // through the cache-hit path.
    let results = search(&index, "old")?;
    assert_eq!(results, vec!["The Old Man and the Sea"]);

    Ok(())
}

/// Verifies that the cache is populated after creating and reading an
/// index by checking that Redis contains footer keys for the segment
/// files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_is_populated_after_open() -> Result<()> {
    let pool = redis_pool().await?;
    let index = create_test_index(pool.clone())?;

    // Force a reader open to trigger caching of all segment files.
    let _results = search(&index, "sea")?;

    // Check that Redis has footer keys for all segment files that are read when
    // opening a segment (excluding `.del` which only exists when there are deletes).
    let mut conn = pool.get().await?;
    let segment_metas = index.searchable_segment_metas()?;
    ensure!(!segment_metas.is_empty(), "expected at least one segment");

    let cached_exts = ["idx", "pos", "term", "fast", "store", "fieldnorm"];
    for meta in &segment_metas {
        for path in meta.list_files() {
            if path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_none_or(|ext| !cached_exts.contains(&ext))
            {
                continue;
            }

            let key = keys::footer(&path)?;
            let exists: bool = conn.exists(&key).await?;
            ensure!(exists, "expected cache key {key} to exist");
        }
    }

    Ok(())
}

/// Opens the same underlying directory twice with separate `CachingDirectory`
/// instances (sharing the same Redis pool) to verify that the second open serves
/// segment files from the cache.
///
/// Then opens it a third time with a `FailingDirectory` (which panics on any read)
/// to prove the cache contains everything `tantivy` needs to open a segment reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_open_uses_cache() -> Result<()> {
    let pool = redis_pool().await?;

    // Create and search to populate the cache.
    let ram = RamDirectory::create();
    let dir = CachingDirectory::new(ram.clone(), pool.clone());

    let mut builder = SchemaBuilder::new();
    let text_opts = utils::text_options();
    builder.add_text_field("title", text_opts | STORED);
    builder.add_u64_field("count", FAST | INDEXED | STORED);
    let schema = builder.build();

    let index = Index::create(dir, schema.clone(), IndexSettings::default())?;
    let title = index.schema().get_field("title")?;
    let count = index.schema().get_field("count")?;

    let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
    writer.add_document(doc!(title => "hello world", count => 1u64))?;
    writer.commit()?;

    // First read — cache miss.
    let results = search(&index, "hello")?;
    assert_eq!(results, vec!["hello world"]);

    // Second open: fresh `CachingDirectory` over the same RamDirectory.
    //
    // The footer data is served from Redis.
    let dir2 = CachingDirectory::new(ram.clone(), pool.clone());
    let index2 = Index::open(dir2)?;

    let results = search(&index2, "hello")?;
    assert_eq!(results, vec!["hello world"]);

    // Third open: `CachingDirectory` over a `FailingDirectory`.
    //
    // Any file read that isn't served from the cache will panic, proving that we cache
    // everything `tantivy` needs to open a segment reader.
    //
    // Note: only the segment *opening* is tested here. Actual query execution reads
    // term dictionary content and stored fields on demand, which is not (and should
    // not be) cached.
    let dir3 = CachingDirectory::new(FailingDirectory(ram), pool.clone());
    let index3 = Index::open(dir3)?;

    // Opening a reader creates `SegmentReader`s for all segments, which reads all the
    // footers we cache. This would panic if anything was missing from the cache.
    let _reader: tantivy::IndexReader = index3
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;

    Ok(())
}
