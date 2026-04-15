use std::path::Path;

use eyre::Result;
use tantivy::{HasLen, directory::FileSlice};
use tantivy_common::{DeserializeFrom, OwnedBytes};

/// The cached tail region of a file.
///
/// For all managed files, this includes the `ManagedDirectory` footer. For
/// composite file types (`.idx`, `.pos`, `.term`, `.fieldnorm`), this also
/// includes the `CompositeFile` footer. For fast field files (`.fast`), this
/// also includes the columnar footer, the SSTable footer, and the SSTable
/// dictionary index. For store files (`.store`), this also includes the
/// `DocStoreFooter` and the skip/offset index. All of these are read eagerly
/// when opening a `SegmentReader`.
#[derive(Debug)]
pub struct Footer {
    pub data: OwnedBytes,

    /// The offset of the cached region within the file.
    pub offset: usize,
}

/// Returns `true` if files at the given path have a `CompositeFile` footer that
/// should be cached (i.e., they are segment files that are opened with
/// `CompositeFile::open` when a `SegmentReader` is created).
fn has_composite_footer(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("idx" | "pos" | "term" | "fieldnorm")
    )
}

/// Returns `true` if files at the given path have a columnar footer that
/// should be cached (i.e., they are fast field files whose SSTable dictionary
/// index is read by [`ColumnarReader::open`] when a [`SegmentReader`] is
/// created).
fn has_columnar_footer(path: &Path) -> bool {
    path.extension().and_then(|ext| ext.to_str()) == Some("fast")
}

/// Returns `true` if files at the given path have a `DocStoreFooter` whose
/// offset index should be cached (i.e., they are store files whose skip
/// index is read by [`StoreReader::open`] when a [`SegmentReader`] is
/// created).
fn has_store_footer(path: &Path) -> bool {
    path.extension().and_then(|ext| ext.to_str()) == Some("store")
}

impl Footer {
    pub async fn read(path: &Path, file: &FileSlice) -> Result<Self> {
        let file_len = file.len();

        // Read the managed directory footer metadata (last 8 bytes: footer_len
        // + magic).
        let md_meta = file
            .read_bytes_slice_async((file_len - 8)..file_len)
            .await?;
        let (md_len, _): (u32, u32) = md_meta.as_ref().deserialize()?;
        let md_start = file_len - 8 - md_len as usize;

        // For composite file types (.idx, .pos, .term, .fieldnorm), also
        // cache the composite file footer which `CompositeFile::open` reads
        // when opening a `SegmentReader`.
        //
        // For fast field files (.fast), also cache the columnar footer, the
        // SSTable footer, and the SSTable dictionary index which
        // `ColumnarReader::open` and `Dictionary::open` read when opening a
        // `SegmentReader`.
        //
        // For store files (.store), also cache the `DocStoreFooter` and the
        // offset index which `StoreReader::open` reads when opening a
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
        } else if has_store_footer(path) {
            // Read the `DocStoreFooter` (28 bytes before the managed footer
            // body) and extract the `offset` field which points to the start
            // of the offset/skip index.
            let store_footer = file
                .read_bytes_slice_async((md_start - 28)..md_start)
                .await?;

            // Parse `offset` (u64) at bytes 4..12 of the DocStoreFooter.
            let mut buf = &store_footer[4..12];
            let offset: u64 = buf.deserialize()?;

            offset as usize
        } else {
            md_start
        };

        // Read and cache the entire footer region.
        let body = file.read_bytes_slice_async(offset..(file_len - 8)).await?;

        let mut data = Vec::with_capacity(file_len - offset);
        data.extend_from_slice(body.as_ref());
        data.extend_from_slice(md_meta.as_ref());
        let data = OwnedBytes::new(data);

        Ok(Footer { data, offset })
    }
}
