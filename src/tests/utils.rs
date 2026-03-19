use deadpool_redis::{Pool, Runtime};
use eyre::{OptionExt, Result};
use tantivy::{
    Index, IndexSettings, ReloadPolicy, TantivyDocument,
    collector::TopDocs,
    directory::RamDirectory,
    doc,
    query::QueryParser,
    schema::{
        FAST, INDEXED, IndexRecordOption, STORED, SchemaBuilder, TextFieldIndexing, TextOptions,
        Value,
    },
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis};
use tokio::sync::OnceCell;

use crate::CachingDirectory;

/// A shared Redis container, started once and reused across all tests.
/// Stores the connection URL and keeps the container alive.
static REDIS: OnceCell<(String, testcontainers::ContainerAsync<Redis>)> = OnceCell::const_new();

pub async fn redis_pool() -> Result<Pool> {
    let (url, _container) = REDIS
        .get_or_try_init(|| async {
            let container = Redis::default().start().await?;
            let host = container.get_host().await?;
            let port = container.get_host_port_ipv4(REDIS_PORT).await?;
            let url = format!("redis://{host}:{port}");

            eyre::Ok((url, container))
        })
        .await?;

    let cfg = deadpool_redis::Config::from_url(url);
    let pool = cfg.create_pool(Some(Runtime::Tokio1))?;

    Ok(pool)
}

/// Returns the `TextOptions` used by the test schema (full-text with
/// positions).
pub fn text_options() -> TextOptions {
    TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("default")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    )
}

/// Creates a tantivy index backed by `CachingDirectory<RamDirectory>`,
/// writes some documents with text and fast fields to exercise all segment
/// component types, and commits.
pub fn create_test_index(pool: Pool) -> Result<Index> {
    let ram = RamDirectory::create();
    let dir = CachingDirectory::new(ram, pool);

    let mut builder = SchemaBuilder::new();
    let text_opts = text_options();
    builder.add_text_field("title", text_opts.clone() | STORED);
    builder.add_text_field("body", text_opts);
    builder.add_u64_field("count", FAST | INDEXED | STORED);
    let schema = builder.build();

    let index = Index::create(dir, schema, IndexSettings::default())?;

    let title = index.schema().get_field("title")?;
    let body = index.schema().get_field("body")?;
    let count = index.schema().get_field("count")?;

    let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
    writer.add_document(doc!(
        title => "The Old Man and the Sea",
        body => "He was an old man who fished alone in a skiff",
        count => 42u64,
    ))?;

    writer.add_document(doc!(
        title => "Of Mice and Men",
        body => "A few miles south of Soledad the Salinas River",
        count => 15u64,
    ))?;

    writer.add_document(doc!(
        title => "Moby Dick",
        body => "Call me Ishmael some years ago never mind how long",
        count => 99u64,
    ))?;

    writer.commit()?;

    Ok(index)
}

/// Opens a reader on the given index, executes a search for `query`, and
/// returns the titles of the matching documents.
pub fn search(index: &Index, query: &str) -> Result<Vec<String>> {
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();

    let title = index.schema().get_field("title")?;
    let parser = QueryParser::for_index(index, vec![title]);
    let query = parser.parse_query(query)?;
    let top = searcher.search(&query, &TopDocs::with_limit(10))?;

    top.into_iter()
        .map(|(_score, addr)| {
            let doc: TantivyDocument = searcher.doc(addr)?;
            let title = index.schema().get_field("title")?;
            let title = doc.get_first(title).ok_or_eyre("missing title field")?;
            let title = title.as_str().ok_or_eyre("title is not a string")?;

            Ok(title.into())
        })
        .collect()
}
