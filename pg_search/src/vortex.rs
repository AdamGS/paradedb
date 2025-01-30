use std::collections::BTreeSet;
use std::future::ready;
use std::{io::Write, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::{
    path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use object_store::{GetRange, PutMode};
use pgrx::pg_sys::Oid;
use rustc_hash::FxHashMap;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use vortex::io::{VortexReadAt, VortexWrite};

use crate::postgres::storage::block::FileEntry;
use crate::postgres::storage::{linked_bytes::RangeData, LinkedBytesList};

#[derive(Debug)]
struct BlockObjectStore {
    inner: Arc<Mutex<FxHashMap<Path, FileEntry>>>,
    // I just assume that's available from somewhere, I don't fully understand the index oid lifecycle yet
    relation_oid: Oid,
}

impl std::fmt::Display for BlockObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlockObjectStore")
    }
}

#[derive(Clone, Debug)]
struct BlockReader {
    entry: FileEntry,
    oid: Oid,
}

impl VortexReadAt for BlockReader {
    fn read_byte_range(
        &self,
        pos: u64,
        len: u64,
    ) -> impl std::future::Future<Output = std::io::Result<Bytes>> + 'static {
        let oid = self.oid;
        let block = self.entry.staring_block;
        async move {
            let pos = pos as usize;
            let len = len as usize;
            let end = pos + len;
            let linked_bytes_list = LinkedBytesList::open(oid, block);
            let data = unsafe { linked_bytes_list.get_bytes_range(pos..end) };

            let data = if matches!(data, RangeData::OnePage(_, _)) {
                Bytes::copy_from_slice(&*data)
            } else if let RangeData::MultiPage(vec) = data {
                Bytes::from(vec)
            } else {
                unreachable!()
            };

            Ok(data)
        }
    }

    fn size(&self) -> impl std::future::Future<Output = std::io::Result<u64>> + 'static {
        ready(Ok(self.entry.total_bytes as u64))
    }
}

struct BlockWriter {
    entry: FileEntry,
    oid: Oid,
    linked_bytes_list: LinkedBytesList,
}

impl BlockWriter {
    fn new(relation_oid: Oid) -> Self {
        let linked_bytes_list = unsafe { LinkedBytesList::create(relation_oid) };
        let entry = FileEntry {
            staring_block: linked_bytes_list.header_blockno,
            total_bytes: 0,
        };
        Self {
            oid: relation_oid,
            linked_bytes_list,
            entry,
        }
    }
}

impl VortexWrite for BlockWriter {
    fn write_all<B: vortex::io::IoBuf>(
        &mut self,
        buffer: B,
    ) -> impl std::future::Future<Output = std::io::Result<B>> {
        async move {
            let bytes = buffer.as_slice();
            self.linked_bytes_list.write_all(buffer.as_slice())?;
            self.linked_bytes_list.flush()?;
            self.entry.total_bytes += bytes.len();

            Ok(buffer)
        }
    }

    fn flush(&mut self) -> impl std::future::Future<Output = std::io::Result<()>> {
        async { self.linked_bytes_list.flush() }
    }

    fn shutdown(&mut self) -> impl std::future::Future<Output = std::io::Result<()>> {
        ready(Ok(()))
    }
}

#[async_trait]
impl ObjectStore for BlockObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if opts.mode != PutMode::Overwrite {
            return Err(object_store::Error::NotSupported {
                source: format!("{:?} is not supported, only Overwrite", opts.mode).into(),
            });
        }

        // I think this is fine as we assume that every `put` creates a new file
        let mut linked_bytes_list = unsafe { LinkedBytesList::create(self.relation_oid) };
        for b in payload.iter() {
            linked_bytes_list
                .write_all(b)
                .map_err(|source| object_store::Error::Generic {
                    store: "BlockObjectStore",
                    source: Box::new(source),
                })?;
        }

        linked_bytes_list
            .flush()
            .map_err(|source| object_store::Error::Generic {
                store: "BlockObjectStore",
                source: Box::new(source),
            })?;

        self.inner.lock().await.insert(
            location.clone(),
            FileEntry {
                staring_block: linked_bytes_list.header_blockno,
                total_bytes: payload.content_length(),
            },
        );

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        unimplemented!()
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let entry = self.inner.lock().await.get(location).unwrap().clone();
        let linked_bytes_list = LinkedBytesList::open(self.relation_oid, entry.staring_block);

        let range = match options.range {
            Some(r) => match r {
                GetRange::Bounded(range) => range,
                GetRange::Offset(o) => o..entry.total_bytes,
                GetRange::Suffix(s) => (entry.total_bytes - s)..entry.total_bytes,
            },
            None => 0..entry.total_bytes,
        };

        // probably want to take the optimization that SegmentComponentReader::read_bytes_raw uses
        let payload = unsafe { linked_bytes_list.get_bytes_range(range.clone()) };

        // TODO: This seems like a bad way to get data out here
        let data = if matches!(payload, RangeData::OnePage(_, _)) {
            Bytes::copy_from_slice(&*payload)
        } else if let RangeData::MultiPage(vec) = payload {
            Bytes::from(vec)
        } else {
            unreachable!()
        };

        Ok(GetResult {
            payload: GetResultPayload::Stream(futures::stream::iter([Ok(data)]).boxed()),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: Default::default(),
                size: entry.total_bytes,
                e_tag: None,
                version: None,
            },
            range,
            attributes: Default::default(),
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let mut guard = self.inner.lock().await;
        let entry = guard.get(location).unwrap();
        let mut linked_bytes_list = LinkedBytesList::open(self.relation_oid, entry.staring_block);
        unsafe { linked_bytes_list.mark_deleted() };
        guard.remove(location);
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        let map = self.inner.blocking_lock().clone();
        let prefix = prefix.cloned().unwrap_or_default();

        Box::pin(futures::stream::iter(map.into_iter().filter_map(
            move |(key, v)| {
                let filter = key
                    .prefix_match(&prefix)
                    .map(|mut x| x.next().is_some())
                    .unwrap_or(false);
                filter.then(|| {
                    Ok(ObjectMeta {
                        location: key.clone(),
                        size: v.total_bytes,
                        last_modified: Default::default(),
                        e_tag: None,
                        version: None,
                    })
                })
            },
        )))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let map = self.inner.blocking_lock();

        let mut common_prefixes = BTreeSet::new();
        let mut objects = vec![];

        for (k, v) in map.iter() {
            let mut parts = match k.prefix_match(&prefix) {
                Some(parts) => parts,
                None => continue,
            };

            let common_prefix = match parts.next() {
                Some(p) => p,
                // Should only return children of the prefix
                None => continue,
            };

            if parts.next().is_some() {
                common_prefixes.insert(prefix.child(common_prefix));
            } else {
                let object = ObjectMeta {
                    location: k.clone(),
                    size: v.total_bytes,
                    last_modified: Default::default(),
                    e_tag: None,
                    version: None,
                };
                objects.push(object);
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        unimplemented!()
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        unimplemented!()
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use datafusion::{
        execution::{object_store::ObjectStoreUrl, SessionState, SessionStateBuilder},
        prelude::SessionContext,
    };
    use pgrx::{pg_sys, pg_test, Spi};
    use vortex::{
        array::PrimitiveArray,
        file::{ExecutionMode, Scan, VortexOpenOptions, VortexWriteOptions},
        sampling_compressor::ALL_ENCODINGS_CONTEXT,
        IntoArray,
    };

    use super::*;

    #[pg_test]
    fn plain_vortex_example() -> anyhow::Result<()> {
        Spi::run("CREATE TABLE t (id SERIAL, data TEXT);")?;
        Spi::run("CREATE INDEX t_idx ON t USING bm25(id, data) WITH (key_field = 'id')")?;
        let relation_oid: pg_sys::Oid =
            Spi::get_one("SELECT oid FROM pg_class WHERE relname = 't_idx' AND relkind = 'i';")
                .expect("spi should succeed")
                .unwrap();

        tokio::runtime::Builder::new_current_thread()
            .build()?
            .block_on(async move {
                let array = PrimitiveArray::from_iter(vec![1, 2, 3]).into_array();
                let writer = BlockWriter::new(relation_oid);

                let writer = VortexWriteOptions::default()
                    .write(writer, array.into_array_stream())
                    .await?;

                let reader = BlockReader {
                    oid: writer.oid,
                    entry: writer.entry,
                };

                let reader_stream = VortexOpenOptions::new(ALL_ENCODINGS_CONTEXT.clone())
                    .with_execution_mode(ExecutionMode::Inline)
                    .open(reader)
                    .await?;

                let mut stream = Box::pin(reader_stream.scan(Scan::all())?);
                let mut total_len = 0;

                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    total_len += chunk.len();
                }

                assert_eq!(total_len, 3);

                anyhow::Ok(())
            })
    }

    #[pg_test]
    fn vortex_datafusion_example() -> anyhow::Result<()> {
        // obviously just a hack to get a oid
        Spi::run("CREATE TABLE t (id SERIAL, data TEXT);")?;
        Spi::run("CREATE INDEX t_idx ON t USING bm25(id, data) WITH (key_field = 'id')")?;
        let relation_oid: pg_sys::Oid =
            Spi::get_one("SELECT oid FROM pg_class WHERE relname = 't_idx' AND relkind = 'i';")
                .expect("spi should succeed")
                .unwrap();

        tokio::runtime::Builder::new_current_thread()
            .build()?
            .block_on(async move {
                let object_store = Arc::new(BlockObjectStore {
                    inner: Default::default(),
                    relation_oid,
                });

                let url = ObjectStoreUrl::parse("file://")?;

                // Missing a piece that will be in tomorrow's (January 31st) Vortex release
                // let state = SessionStateBuilder::new().with_file_formats(file_formats)
                let session = SessionContext::new();
                _ = session.register_object_store(url.as_ref(), object_store);

                let _df = session
                    .sql(
                        "CREATE EXTERNAL TABLE tbl
                    STORED AS VORTEX
                    LOCATION '/path/to/data';",
                    )
                    .await?;

                // TODO: insert some data
                // TODO: hopefully query it back

                anyhow::Ok(())
            })
    }
}
