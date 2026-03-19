use std::{io, ops::Range, path::Path, sync::Arc};

use async_trait::async_trait;
use block_on_place::HandleExt;
use deadpool_redis::{Pool, redis::AsyncCommands};
use eyre::Result;
use tantivy::{
    Directory, HasLen,
    directory::{
        DirectoryLock, FileHandle, FileSlice, Lock, WatchCallback, WatchHandle, WritePtr,
        error::{DeleteError, LockError, OpenReadError, OpenWriteError},
    },
};
use tantivy_common::{DeserializeFrom, OwnedBytes};
use tokio::runtime::Handle;

use crate::error::WrapIoErrorExt;

mod error;
mod keys;

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
}

/// The cached tail region of a file.
///
/// For all managed files, this includes the `ManagedDirectory` footer. For
/// composite file types (`.idx`, `.pos`, `.term`), this also includes the
/// `CompositeFile` footer. For fast field files (`.fast`), this also includes
/// the columnar footer, the SSTable footer, and the SSTable dictionary index.
/// All of these are read eagerly when opening a `SegmentReader`.
#[derive(Debug)]
struct Footer {
    data: OwnedBytes,

    /// The offset of the cached region within the file.
    offset: usize,
}

/// Returns `true` if files at the given path have a `CompositeFile` footer that
/// should be cached (i.e., they are segment files that are opened with
/// `CompositeFile::open` when a `SegmentReader` is created).
fn has_composite_footer(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("idx" | "pos" | "term")
    )
}

/// Returns `true` if files at the given path have a columnar footer that
/// should be cached (i.e., they are fast field files whose SSTable dictionary
/// index is read by [`ColumnarReader::open`] when a [`SegmentReader`] is
/// created).
fn has_columnar_footer(path: &Path) -> bool {
    path.extension().and_then(|ext| ext.to_str()) == Some("fast")
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

    async fn open(&self, path: &Path, file: FileSlice) -> Result<CachingHandle> {
        let key = keys::footer(path)?;
        let mut conn = self.redis.get().await?;

        if let Some(data) = conn.get(&key).await? {
            let data: Vec<u8> = data;
            let data = OwnedBytes::new(data);
            let offset = file.len() - data.len();
            let footer = Footer { data, offset };

            // TODO(MLB): cache in-memory as well?
            return Ok(CachingHandle { file, footer });
        }

        let file_len = file.len();

        // Read the managed directory footer metadata (last 8 bytes: footer_len
        // + magic).
        let md_meta = file
            .read_bytes_slice_async((file_len - 8)..file_len)
            .await?;
        let (md_len, _): (u32, u32) = md_meta.as_ref().deserialize()?;
        let md_start = file_len - 8 - md_len as usize;

        // For composite file types (.idx, .pos, .term), also cache the
        // composite file footer which `CompositeFile::open` reads when opening
        // a `SegmentReader`.
        //
        // For fast field files (.fast), also cache the columnar footer, the
        // SSTable footer, and the SSTable dictionary index which
        // `ColumnarReader::open` and `Dictionary::open` read when opening a
        // `SegmentReader`.
        let offset = if has_composite_footer(path) {
            let cf_meta = file
                .read_bytes_slice_async((md_start - 4)..md_start)
                .await?;
            let cf_len: u32 = cf_meta.as_ref().deserialize()?;
            md_start - 4 - cf_len as usize
        } else if has_columnar_footer(path) {
            // Read the columnar footer (20 bytes) and the SSTable footer
            // (20 bytes) that precede the managed directory footer body, in
            // one read.
            let footers = file
                .read_bytes_slice_async((md_start - 40)..md_start)
                .await?;

            // Parse `sstable_len` from the columnar footer (bytes 20..28).
            let mut buf = &footers[20..28];
            let sstable_len: u64 = buf.deserialize()?;

            // Parse `index_offset` from the SSTable footer (bytes 0..8).
            let mut buf = &footers[0..8];
            let index_offset: u64 = buf.deserialize()?;

            // Cache from the start of the SSTable index.
            let sstable_start = md_start - 20 - sstable_len as usize;
            sstable_start + index_offset as usize
        } else {
            md_start
        };

        // Read and cache the entire footer region.
        let body = file.read_bytes_slice_async(offset..(file_len - 8)).await?;

        let mut cached = Vec::with_capacity(file_len - offset);
        cached.extend_from_slice(body.as_ref());
        cached.extend_from_slice(md_meta.as_ref());

        let () = conn.set(&key, &cached).await?;
        let data = OwnedBytes::new(cached);

        let footer = Footer { data, offset };

        Ok(CachingHandle { file, footer })
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

        if range.start >= self.footer.offset {
            let start = range.start - self.footer.offset;
            let end = range.end - self.footer.offset;
            let slice = self.footer.data.slice(start..end);

            return Ok(slice);
        }

        let start = range.start;
        let end = self.footer.offset;
        let data = self.file.read_bytes_slice(start..end)?;

        let start = 0;
        let end = range.end - self.footer.offset;
        let footer = self.footer.data.slice(start..end);

        let mut combined = Vec::with_capacity(data.len() + footer.len());
        combined.extend_from_slice(data.as_ref());
        combined.extend_from_slice(footer.as_ref());

        Ok(OwnedBytes::new(combined))
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.end <= self.footer.offset {
            return self.file.read_bytes_slice_async(range).await;
        }

        if range.start >= self.footer.offset {
            let start = range.start - self.footer.offset;
            let end = range.end - self.footer.offset;
            let slice = self.footer.data.slice(start..end);

            return Ok(slice);
        }

        let start = range.start;
        let end = self.footer.offset;
        let data = self.file.read_bytes_slice_async(start..end).await?;

        let start = 0;
        let end = range.end - self.footer.offset;
        let footer = self.footer.data.slice(start..end);

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
