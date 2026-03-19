use std::{io, ops::Range, path::Path, sync::Arc};

use async_trait::async_trait;
use tantivy::{
    Directory, HasLen,
    directory::{
        FileHandle, RamDirectory, WatchCallback, WatchHandle, WritePtr,
        error::{DeleteError, OpenReadError, OpenWriteError},
    },
};
use tantivy_common::OwnedBytes;

/// A [`Directory`] whose file handles panic on any read.
///
/// Used to verify that the `CachingDirectory` serves everything `tantivy` needs
/// from cache without falling through to the underlying directory.
///
/// All non-read operations (metadata, atomic reads/writes, locks, etc.)
/// are delegated to the inner directory so that `ManagedDirectory` can
/// still load `meta.json` and `.managed.json`.
#[derive(Clone, Debug)]
pub struct FailingDirectory(pub RamDirectory);

#[derive(Debug)]
struct FailingHandle {
    len: usize,
    path: std::path::PathBuf,
}

impl Directory for FailingDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        // We need the real file length so that `CachingDirectory` can compute
        // the cache offset, but any actual read must come from the cache.
        let file = self.0.open_read(path)?;

        let len = file.len();
        let path = path.into();

        Ok(Arc::new(FailingHandle { len, path }))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.0.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.0.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        self.0.open_write(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.0.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.0.atomic_write(path, data)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.0.sync_directory()
    }

    fn watch(&self, cb: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.0.watch(cb)
    }
}

#[async_trait]
impl FileHandle for FailingHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        panic!(
            "unexpected read of {:?} at {:?} (file length {})",
            self.path, range, self.len,
        );
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        panic!(
            "unexpected async read of {:?} at {:?} (file length {})",
            self.path, range, self.len,
        );
    }
}

impl HasLen for FailingHandle {
    fn len(&self) -> usize {
        self.len
    }
}
