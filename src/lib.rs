use std::{
    io,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use block_on_place::HandleExt;
use deadpool_redis::{Connection, Pool, redis::AsyncCommands};
use eyre::{OptionExt, Result};
use tantivy::{
    Directory, HasLen,
    directory::{
        DirectoryLock, FileHandle, FileSlice, Lock, WatchCallback, WatchHandle, WritePtr,
        error::{DeleteError, LockError, OpenReadError, OpenWriteError},
    },
};
use tantivy_common::OwnedBytes;
use tokio::runtime::Handle;

use self::{error::WrapIoErrorExt, footer::Footer};

mod error;
mod footer;
mod keys;

#[cfg(test)]
mod tests;

/// A [`Directory`] implementation wrapping another one and caching some data using
/// [`deadpool_redis`].
#[derive(Clone, Debug)]
pub struct CachingDirectory<D> {
    rt: Handle,
    redis: Pool,
    dir: D,
}

#[derive(Debug)]
struct CachingHandle {
    file: FileSlice,
    footer: Footer,

    rt: Handle,
    redis: Pool,
    path: PathBuf,
}

impl<D> CachingDirectory<D> {
    /// Creates a new [`CachingDirectory`] from the given directory and pool.
    ///
    /// The provided [`Directory`] must return a [`FileHandle`] that implements
    /// [`read_bytes_async()`][1] without panicking.
    ///
    /// This must be called from within a `tokio` context.
    ///
    /// [1]: FileHandle::read_bytes_async()
    pub fn new(dir: D, redis: Pool) -> Self {
        let rt = Handle::current();

        Self { rt, redis, dir }
    }

    async fn open(&self, path: impl Into<PathBuf>, file: FileSlice) -> Result<CachingHandle> {
        let path = path.into();

        if let Some(offset) = offset(&path, &self.redis).await {
            let footer = Footer::with_offset(offset);

            let rt = self.rt.clone();
            let redis = self.redis.clone();

            return Ok(CachingHandle {
                file,
                footer,
                rt,
                redis,
                path,
            });
        }

        let footer = Footer::read(&path, &file).await?;
        update(&path, &footer, &self.redis).await;

        let rt = self.rt.clone();
        let redis = self.redis.clone();

        Ok(CachingHandle {
            file,
            footer,
            rt,
            redis,
            path,
        })
    }
}

impl CachingHandle {
    /// Fetches the file's footer.
    ///
    /// If the footer has already been fetched, this simply returns it. If it is stored
    /// in Redis, it fetches it from there. Otherwsie, it reads it and stores it in
    /// Redis.
    async fn footer(&self) -> Option<OwnedBytes> {
        let fetch = async {
            if let Some(data) = footer(&self.path, &self.redis).await {
                return Ok(OwnedBytes::new(data));
            }

            let footer = Footer::read(&self.path, &self.file).await?;
            update(&self.path, &footer, &self.redis).await;

            footer.data().ok_or_eyre("missing data")
        };

        match self.footer.get_or_fetch(fetch).await {
            Ok(footer) => Some(footer),
            Err(error) => {
                tracing::warn!("Failed to fetch footer: {error:?}");

                None
            }
        }
    }
}

impl<D: Clone + Directory> Directory for CachingDirectory<D> {
    #[inline]
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file = self.dir.open_read(path)?;
        let handle = self
            .rt
            .block_on_place(self.open(path, file))
            .map_err(OpenReadError::wrapper(path))?;

        Ok(Arc::new(handle))
    }

    #[inline]
    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        // TODO(MLB): remove from cache
        self.dir.delete(path)
    }

    #[inline]
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        // TODO(MLB): check the cache
        self.dir.exists(path)
    }

    #[inline]
    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        self.get_file_handle(path).map(FileSlice::new)
    }

    #[inline]
    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        // TODO(MLB): wrap and insert into the cache
        self.dir.open_write(path)
    }

    #[inline]
    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        // TODO(MLB): read from cache
        self.dir.atomic_read(path)
    }

    #[inline]
    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        // TODO(MLB): write to cache
        self.dir.atomic_write(path, data)
    }

    #[inline]
    fn sync_directory(&self) -> io::Result<()> {
        self.dir.sync_directory()
    }

    #[inline]
    fn watch(&self, cb: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.dir.watch(cb)
    }

    #[inline]
    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        self.dir.acquire_lock(lock)
    }
}

#[async_trait]
impl FileHandle for CachingHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.end <= self.footer.offset {
            return self.file.read_bytes_slice(range);
        }

        let Some(footer) = self.rt.block_on_place(self.footer()) else {
            return self.file.read_bytes_slice(range);
        };

        if range.start >= self.footer.offset {
            let start = range.start - self.footer.offset;
            let end = range.end - self.footer.offset;
            let slice = footer.slice(start..end);

            return Ok(slice);
        }

        let start = range.start;
        let end = self.footer.offset;
        let data = self.file.read_bytes_slice(start..end)?;

        let start = 0;
        let end = range.end - self.footer.offset;
        let footer = footer.slice(start..end);

        let mut combined = Vec::with_capacity(data.len() + footer.len());
        combined.extend_from_slice(data.as_ref());
        combined.extend_from_slice(footer.as_ref());

        Ok(OwnedBytes::new(combined))
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.end <= self.footer.offset {
            return self.file.read_bytes_slice_async(range).await;
        }

        let Some(footer) = self.footer().await else {
            return self.file.read_bytes_slice_async(range).await;
        };

        if range.start >= self.footer.offset {
            let start = range.start - self.footer.offset;
            let end = range.end - self.footer.offset;
            let slice = footer.slice(start..end);

            return Ok(slice);
        }

        let start = range.start;
        let end = self.footer.offset;
        let data = self.file.read_bytes_slice_async(start..end).await?;

        let start = 0;
        let end = range.end - self.footer.offset;
        let footer = footer.slice(start..end);

        let mut combined = Vec::with_capacity(data.len() + footer.len());
        combined.extend_from_slice(data.as_ref());
        combined.extend_from_slice(footer.as_ref());

        Ok(OwnedBytes::new(combined))
    }
}

impl HasLen for CachingHandle {
    #[inline]
    fn len(&self) -> usize {
        self.file.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.file.is_empty()
    }
}

/// Gets a connection from the given Redis pool.
///
/// If an error occurs, this returns `None` and logs the error message.
async fn conn(redis: &Pool) -> Option<Connection> {
    match redis.get().await {
        Ok(conn) => Some(conn),
        Err(error) => {
            tracing::warn!("Failed to get Redis connection: {error:?}");
            None
        }
    }
}

/// Fetches the footer offset for the given path from Redis.
///
/// If an error occurs, this returns `None` and logs the error message.
async fn offset(path: &Path, redis: &Pool) -> Option<usize> {
    let key = keys::offset(&path).ok()?;
    let mut conn = conn(redis).await?;

    match conn.get(&key).await {
        Ok(offset) => offset,
        Err(error) => {
            tracing::warn!("Failed to fetch footer offset: {error:?}");
            return None;
        }
    }
}

/// Fetches the footer data for the given path from Redis.
///
/// If an error occurs, this returns `None` and logs the error message.
async fn footer(path: &Path, redis: &Pool) -> Option<Vec<u8>> {
    let key = keys::footer(&path).ok()?;
    let mut conn = conn(redis).await?;

    match conn.get(&key).await {
        Ok(offset) => offset,
        Err(error) => {
            tracing::warn!("Failed to fetch footer: {error:?}");
            return None;
        }
    }
}

/// Updates the cached data for the given footer in Redis.
///
/// If an error occurs, this returns `None` and logs the error message.
async fn update(path: &Path, footer: &Footer, redis: &Pool) {
    let Some(mut conn) = conn(redis).await else {
        return;
    };

    let Ok(key) = keys::offset(&path) else { return };
    match conn.set(&key, footer.offset).await {
        Ok(()) => (),
        Err(error) => {
            tracing::warn!("Failed to update footer offset: {error:?}");
        }
    }

    let Ok(key) = keys::footer(&path) else { return };
    let Some(data) = footer.data() else { return };
    match conn.set(&key, data.as_slice()).await {
        Ok(()) => (),
        Err(error) => {
            tracing::warn!("Failed to update footer: {error:?}");
        }
    }
}
