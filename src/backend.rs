//! Typed storage contracts and caller-owned, bounded record buffers.

use std::{
    alloc::{Allocator, System},
    collections::BTreeMap,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type Key = uuid::Uuid;
pub type Result<T> = std::result::Result<T, String>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DataModel {
    #[default]
    KeyValue,
    Documents,
    Graph,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Durability {
    #[default]
    None,
    Buffered,
    Flushed,
}

#[derive(Clone, Copy)]
pub struct RecordInput<'a> {
    pub bytes: &'a [u8],
    pub ends: &'a [usize],
    pub found: &'a [bool],
}
impl RecordInput<'_> {
    pub fn len(&self) -> usize {
        self.ends.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        if !self.found[index] {
            return None;
        }
        Some(&self.bytes[if index == 0 { 0 } else { self.ends[index - 1] }..self.ends[index]])
    }
}
pub struct RecordOutput<'a> {
    bytes: &'a mut [u8],
    ends: &'a mut [usize],
    found: &'a mut [bool],
    rows: &'a mut usize,
    used: &'a mut usize,
}
impl RecordOutput<'_> {
    pub fn clear(&mut self) {
        *self.rows = 0;
        *self.used = 0;
    }
    pub fn len(&self) -> usize {
        *self.rows
    }
    pub fn is_empty(&self) -> bool {
        *self.rows == 0
    }
    pub fn push_bytes(&mut self, len: usize) -> Result<&mut [u8]> {
        if *self.rows == self.ends.len() || len > self.bytes.len() - *self.used {
            return Err("record output capacity exceeded".into());
        }
        let start = *self.used;
        *self.used += len;
        self.ends[*self.rows] = *self.used;
        self.found[*self.rows] = true;
        *self.rows += 1;
        Ok(&mut self.bytes[start..*self.used])
    }
    pub fn push(&mut self, value: Option<&[u8]>) -> Result<()> {
        let bytes = value.unwrap_or_default();
        if *self.rows == self.ends.len() || bytes.len() > self.bytes.len() - *self.used {
            return Err("record output capacity exceeded".into());
        }
        self.bytes[*self.used..*self.used + bytes.len()].copy_from_slice(bytes);
        *self.used += bytes.len();
        self.ends[*self.rows] = *self.used;
        self.found[*self.rows] = value.is_some();
        *self.rows += 1;
        Ok(())
    }
}
pub struct RecordBatch<A: Allocator + Clone = System> {
    bytes: Vec<u8, A>,
    ends: Vec<usize, A>,
    found: Vec<bool, A>,
    rows: usize,
    used: usize,
}
impl RecordBatch<System> {
    pub fn new(rows: usize, bytes: usize) -> Self {
        Self::new_in(rows, bytes, System)
    }
}
impl<A: Allocator + Clone> RecordBatch<A> {
    pub fn new_in(rows: usize, bytes: usize, allocator: A) -> Self {
        let mut data = Vec::with_capacity_in(bytes, allocator.clone());
        data.resize(bytes, 0);
        let mut ends = Vec::with_capacity_in(rows, allocator.clone());
        ends.resize(rows, 0);
        let mut found = Vec::with_capacity_in(rows, allocator);
        found.resize(rows, false);
        Self {
            bytes: data,
            ends,
            found,
            rows: 0,
            used: 0,
        }
    }
    pub fn as_input(&self) -> RecordInput<'_> {
        RecordInput {
            bytes: &self.bytes[..self.used],
            ends: &self.ends[..self.rows],
            found: &self.found[..self.rows],
        }
    }
    pub fn as_output(&mut self) -> RecordOutput<'_> {
        self.rows = 0;
        self.used = 0;
        RecordOutput {
            bytes: &mut self.bytes,
            ends: &mut self.ends,
            found: &mut self.found,
            rows: &mut self.rows,
            used: &mut self.used,
        }
    }
}

pub struct KeysOutput<'a> {
    keys: &'a mut [Key],
    len: usize,
}
impl<'a> KeysOutput<'a> {
    pub fn new(keys: &'a mut [Key]) -> Self {
        Self { keys, len: 0 }
    }
    pub fn clear(&mut self) {
        self.len = 0;
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn push(&mut self, key: Key) -> Result<()> {
        if self.len == self.keys.len() {
            return Err("key output capacity exceeded".into());
        }
        self.keys[self.len] = key;
        self.len += 1;
        Ok(())
    }
    pub fn as_slice(&self) -> &[Key] {
        &self.keys[..self.len]
    }
}

#[derive(Clone, Copy)]
pub struct DocumentRef<'a> {
    pub score: u64,
    pub payload: &'a str,
}
#[derive(Clone, Copy, Default)]
pub struct DocumentPatch {
    pub score: u64,
}
#[derive(Clone, Copy)]
pub struct DocumentInput<'a> {
    pub scores: &'a [u64],
    pub payloads: RecordInput<'a>,
}
impl DocumentInput<'_> {
    pub fn len(&self) -> usize {
        self.payloads.len()
    }
    pub fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }
    pub fn get(&self, index: usize) -> Option<DocumentRef<'_>> {
        self.payloads.get(index).map(|payload| DocumentRef {
            score: self.scores[index],
            payload: std::str::from_utf8(payload).expect("document payloads are UTF-8"),
        })
    }
}
pub struct DocumentOutput<'a> {
    payloads: RecordOutput<'a>,
    scores: &'a mut [u64],
}
impl DocumentOutput<'_> {
    pub fn clear(&mut self) {
        self.payloads.clear();
    }
    pub fn len(&self) -> usize {
        self.payloads.len()
    }
    pub fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }
    pub fn push_payload(&mut self, score: u64, len: usize) -> Result<&mut [u8]> {
        let row = self.len();
        let payload = self.payloads.push_bytes(len)?;
        self.scores[row] = score;
        Ok(payload)
    }
    pub fn push(&mut self, value: Option<DocumentRef<'_>>) -> Result<()> {
        let row = self.len();
        self.payloads.push(value.map(|v| v.payload.as_bytes()))?;
        self.scores[row] = value.map_or(0, |v| v.score);
        Ok(())
    }
}
pub struct DocumentBatch<A: Allocator + Clone = System> {
    payloads: RecordBatch<A>,
    scores: Vec<u64, A>,
}
impl DocumentBatch<System> {
    pub fn new(rows: usize, bytes: usize) -> Self {
        Self::new_in(rows, bytes, System)
    }
}
impl<A: Allocator + Clone> DocumentBatch<A> {
    pub fn new_in(rows: usize, bytes: usize, allocator: A) -> Self {
        let mut scores = Vec::with_capacity_in(rows, allocator.clone());
        scores.resize(rows, 0);
        Self {
            payloads: RecordBatch::new_in(rows, bytes, allocator),
            scores,
        }
    }
    pub fn as_input(&self) -> DocumentInput<'_> {
        DocumentInput {
            scores: &self.scores[..self.payloads.rows],
            payloads: self.payloads.as_input(),
        }
    }
    pub fn as_output(&mut self) -> DocumentOutput<'_> {
        DocumentOutput {
            payloads: self.payloads.as_output(),
            scores: &mut self.scores,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GraphEdge {
    pub slot: u32,
    pub target: Key,
}
#[derive(Clone, Copy)]
pub struct VertexRef<'a> {
    pub version: u64,
    pub edges: &'a [GraphEdge],
}
#[derive(Clone, Copy, Default)]
pub struct GraphPatch {
    pub version: u64,
    pub neighbor: Option<Key>,
}
#[derive(Clone, Copy)]
pub struct GraphInput<'a> {
    pub versions: &'a [u64],
    pub edges: &'a [GraphEdge],
    pub ends: &'a [usize],
    pub found: &'a [bool],
}
impl GraphInput<'_> {
    pub fn len(&self) -> usize {
        self.ends.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }
    pub fn get(&self, index: usize) -> Option<VertexRef<'_>> {
        if !self.found[index] {
            return None;
        }
        Some(VertexRef {
            version: self.versions[index],
            edges: &self.edges[if index == 0 { 0 } else { self.ends[index - 1] }..self.ends[index]],
        })
    }
}
pub struct GraphOutput<'a> {
    versions: &'a mut [u64],
    edges: &'a mut [GraphEdge],
    ends: &'a mut [usize],
    found: &'a mut [bool],
    rows: &'a mut usize,
    used: &'a mut usize,
}
impl GraphOutput<'_> {
    pub fn clear(&mut self) {
        *self.rows = 0;
        *self.used = 0;
    }
    pub fn len(&self) -> usize {
        *self.rows
    }
    pub fn is_empty(&self) -> bool {
        *self.rows == 0
    }
    pub fn push(&mut self, value: Option<VertexRef<'_>>) -> Result<()> {
        let edges = value.map_or(&[][..], |v| v.edges);
        if *self.rows == self.ends.len() || edges.len() > self.edges.len() - *self.used {
            return Err("graph output capacity exceeded".into());
        }
        self.edges[*self.used..*self.used + edges.len()].copy_from_slice(edges);
        *self.used += edges.len();
        self.versions[*self.rows] = value.map_or(0, |v| v.version);
        self.ends[*self.rows] = *self.used;
        self.found[*self.rows] = value.is_some();
        *self.rows += 1;
        Ok(())
    }
}
pub struct GraphBatch<A: Allocator + Clone = System> {
    versions: Vec<u64, A>,
    edges: Vec<GraphEdge, A>,
    ends: Vec<usize, A>,
    found: Vec<bool, A>,
    rows: usize,
    used: usize,
}
impl GraphBatch<System> {
    pub fn new(rows: usize, edges: usize) -> Self {
        Self::new_in(rows, edges, System)
    }
}
impl<A: Allocator + Clone> GraphBatch<A> {
    pub fn new_in(rows: usize, edges: usize, allocator: A) -> Self {
        let mut versions = Vec::with_capacity_in(rows, allocator.clone());
        versions.resize(rows, 0);
        let mut data = Vec::with_capacity_in(edges, allocator.clone());
        data.resize(edges, GraphEdge::default());
        let mut ends = Vec::with_capacity_in(rows, allocator.clone());
        ends.resize(rows, 0);
        let mut found = Vec::with_capacity_in(rows, allocator);
        found.resize(rows, false);
        Self {
            versions,
            edges: data,
            ends,
            found,
            rows: 0,
            used: 0,
        }
    }
    pub fn as_input(&self) -> GraphInput<'_> {
        GraphInput {
            versions: &self.versions[..self.rows],
            edges: &self.edges[..self.used],
            ends: &self.ends[..self.rows],
            found: &self.found[..self.rows],
        }
    }
    pub fn as_output(&mut self) -> GraphOutput<'_> {
        self.rows = 0;
        self.used = 0;
        GraphOutput {
            versions: &mut self.versions,
            edges: &mut self.edges,
            ends: &mut self.ends,
            found: &mut self.found,
            rows: &mut self.rows,
            used: &mut self.used,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BatchMode {
    Native,
    Pipelined,
    #[default]
    PerRecord,
}
#[derive(Clone, Copy, Debug, Serialize)]
pub struct BackendCapabilities {
    pub data_models: &'static [DataModel],
    pub ordered_ranges: bool,
    pub transactions: bool,
    pub batch_read: BatchMode,
    pub batch_insert: BatchMode,
    pub batch_update: BatchMode,
    pub batch_delete: BatchMode,
    pub bulk_load: BatchMode,
}
impl Default for BackendCapabilities {
    fn default() -> Self {
        Self {
            data_models: &[DataModel::KeyValue],
            ordered_ranges: false,
            transactions: false,
            batch_read: BatchMode::PerRecord,
            batch_insert: BatchMode::PerRecord,
            batch_update: BatchMode::PerRecord,
            batch_delete: BatchMode::PerRecord,
            bulk_load: BatchMode::PerRecord,
        }
    }
}
pub trait Backend: Sync {
    fn metadata(&self) -> BTreeMap<String, Value>;
    fn capabilities(&self) -> BackendCapabilities;
    fn session(&self) -> Result<Box<dyn BackendSession + '_, System>>;
    fn flush(&self) -> Result<()>;
    fn disk_bytes(&self) -> Result<u64>;
    fn server_usage(&self) -> Result<Option<Value>> {
        Ok(None)
    }
}
pub trait TransactionSession {
    fn begin(&mut self) -> Result<()> {
        Err("transactions unsupported".into())
    }
    fn commit(&mut self) -> Result<()> {
        Err("transactions unsupported".into())
    }
    fn rollback(&mut self) -> Result<()> {
        Err("transactions unsupported".into())
    }
}
pub trait BackendSession: TransactionSession {
    fn insert(&mut self, _keys: &[Key], _values: &RecordInput<'_>) -> Result<usize> {
        Err("key-value records unsupported".into())
    }
    fn read(&mut self, _keys: &[Key], _output: &mut RecordOutput<'_>) -> Result<usize> {
        Err("key-value records unsupported".into())
    }
    fn update(&mut self, _keys: &[Key], _values: &RecordInput<'_>) -> Result<usize> {
        Err("key-value records unsupported".into())
    }
    fn delete(&mut self, _keys: &[Key]) -> Result<usize> {
        Err("key-value records unsupported".into())
    }
    fn range_read(
        &mut self,
        _start: Key,
        _limit: usize,
        _keys: &mut KeysOutput<'_>,
        _output: &mut RecordOutput<'_>,
    ) -> Result<usize> {
        Err("ordered ranges unsupported".into())
    }
    fn bulk_load(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        self.insert(keys, values)
    }
    fn documents(&mut self) -> Option<&mut dyn DocumentSession> {
        None
    }
    fn graph(&mut self) -> Option<&mut dyn GraphSession> {
        None
    }
}
pub trait DocumentSession: TransactionSession {
    fn insert(&mut self, keys: &[Key], values: &DocumentInput<'_>) -> Result<usize>;
    fn read(&mut self, keys: &[Key], output: &mut DocumentOutput<'_>) -> Result<usize>;
    fn update(&mut self, keys: &[Key], patches: &[DocumentPatch]) -> Result<usize>;
    fn delete(&mut self, keys: &[Key]) -> Result<usize>;
    fn range_read(
        &mut self,
        start: Key,
        limit: usize,
        keys: &mut KeysOutput<'_>,
        output: &mut DocumentOutput<'_>,
    ) -> Result<usize>;
    fn bulk_load(&mut self, keys: &[Key], values: &DocumentInput<'_>) -> Result<usize> {
        self.insert(keys, values)
    }
}
pub trait GraphSession: TransactionSession {
    fn insert(&mut self, keys: &[Key], values: &GraphInput<'_>) -> Result<usize>;
    fn read(&mut self, keys: &[Key], output: &mut GraphOutput<'_>) -> Result<usize>;
    fn update(&mut self, keys: &[Key], patches: &[GraphPatch]) -> Result<usize>;
    fn delete(&mut self, keys: &[Key]) -> Result<usize>;
    fn range_read(
        &mut self,
        start: Key,
        limit: usize,
        keys: &mut KeysOutput<'_>,
        output: &mut GraphOutput<'_>,
    ) -> Result<usize>;
    fn expand_neighbors(&mut self, start: Key, limit: usize, keys: &mut KeysOutput<'_>) -> Result<usize>;
    fn bulk_load(&mut self, keys: &[Key], values: &GraphInput<'_>) -> Result<usize> {
        self.insert(keys, values)
    }
}

pub fn directory_bytes(path: &std::path::Path) -> Result<u64> {
    let mut bytes = 0;
    if !path.exists() {
        return Ok(0);
    }
    for entry in std::fs::read_dir(path).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let metadata = entry.metadata().map_err(|e| e.to_string())?;
        bytes += if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(bytes)
}

#[doc(hidden)]
#[macro_export]
macro_rules! assert_backend_contract {
    ($db:expr) => {{
        use $crate::backend::{Backend, Key, KeysOutput, RecordBatch};
        let db: &dyn Backend = $db;
        let mut session = db.session().unwrap();
        let keys = [Key::from_u128(2), Key::from_u128(4), Key::from_u128(256)];
        let mut values = RecordBatch::new(3, 64);
        {
            let mut out = values.as_output();
            out.push(Some(b"first")).unwrap();
            out.push(Some(b"")).unwrap();
            out.push(Some(b"last value")).unwrap();
        }
        assert_eq!(session.bulk_load(&keys, &values.as_input()).unwrap(), 3);
        let missing = Key::from_u128(3);
        let mut output = RecordBatch::new(4, 128);
        assert_eq!(
            session
                .read(&[keys[0], missing, keys[1], keys[2]], &mut output.as_output())
                .unwrap(),
            3
        );
        assert_eq!(output.as_input().get(0), Some(b"first".as_slice()));
        assert_eq!(output.as_input().get(1), None);
        assert_eq!(output.as_input().get(2), Some(b"".as_slice()));
        let mut changes = RecordBatch::new(2, 64);
        {
            let mut out = changes.as_output();
            out.push(Some(b"replacement")).unwrap();
            out.push(Some(b"absent")).unwrap();
        }
        assert_eq!(session.update(&[keys[0], missing], &changes.as_input()).unwrap(), 1);
        let mut storage = [Key::nil(); 10];
        let mut scanned = KeysOutput::new(&mut storage);
        assert_eq!(
            session
                .range_read(missing, 10, &mut scanned, &mut output.as_output())
                .unwrap(),
            2
        );
        assert_eq!(scanned.as_slice(), &keys[1..]);
        assert_eq!(
            session
                .range_read(keys[0], 0, &mut scanned, &mut output.as_output())
                .unwrap(),
            0
        );
        assert!(scanned.is_empty());
        assert_eq!(session.delete(&[missing, keys[1]]).unwrap(), 1);
        assert_eq!(
            session
                .read(&[keys[0], keys[1], missing], &mut output.as_output())
                .unwrap(),
            1
        );
        assert_eq!(output.as_input().get(0), Some(b"replacement".as_slice()));
        if db.capabilities().transactions {
            session.begin().unwrap();
            session.insert(&[missing], &values.as_input()).unwrap();
            assert_eq!(session.read(&[missing], &mut output.as_output()).unwrap(), 1);
            session.rollback().unwrap();
            assert_eq!(session.read(&[missing], &mut output.as_output()).unwrap(), 0);
            session.begin().unwrap();
            session.insert(&[missing], &values.as_input()).unwrap();
            session.commit().unwrap();
            assert_eq!(session.read(&[missing], &mut output.as_output()).unwrap(), 1);
        }
        drop(session);
        db.flush().unwrap();
        assert!(db.disk_bytes().unwrap() > 0);
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! assert_document_contract {
    ($backend:expr) => {{
        use $crate::backend::{Backend, DocumentBatch, DocumentPatch, DocumentRef, Key};

        let backend: &dyn Backend = $backend;

        let mut session = backend.session().unwrap();
        let documents = session.documents().unwrap();
        let keys = [Key::from_u128(1), Key::from_u128(2)];
        let missing = Key::from_u128(99);
        let mut input = DocumentBatch::new(2, 16);
        {
            let mut output = input.as_output();
            output
                .push(Some(DocumentRef {
                    score: 9_007_199_254_740_993,
                    payload: "",
                }))
                .unwrap();
            output
                .push(Some(DocumentRef {
                    score: 7,
                    payload: "abcdef",
                }))
                .unwrap();
        }
        assert_eq!(documents.insert(&keys, &input.as_input()).unwrap(), 2);
        let mut output = DocumentBatch::new(3, 64);
        assert_eq!(
            documents
                .read(&[keys[0], missing, keys[1]], &mut output.as_output())
                .unwrap(),
            2
        );
        assert_eq!(output.as_input().get(0).unwrap().score, 9_007_199_254_740_993);
        assert_eq!(output.as_input().get(0).unwrap().payload, "");
        assert!(output.as_input().get(1).is_none());
        assert_eq!(
            documents
                .update(
                    &[keys[0], missing],
                    &[DocumentPatch { score: i64::MAX as u64 }, DocumentPatch { score: 5 }]
                )
                .unwrap(),
            1
        );
        assert_eq!(documents.read(&[keys[0], missing], &mut output.as_output()).unwrap(), 1);
        assert_eq!(output.as_input().get(0).unwrap().score, i64::MAX as u64);
        assert_eq!(output.as_input().get(0).unwrap().payload, "");
        assert!(output.as_input().get(1).is_none());
        assert_eq!(documents.delete(&[keys[0], missing]).unwrap(), 1);
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! assert_graph_contract {
    ($backend:expr) => {{
        use $crate::backend::{Backend, GraphBatch, GraphEdge, GraphPatch, Key, KeysOutput, VertexRef};

        let backend: &dyn Backend = $backend;

        let mut session = backend.session().unwrap();
        let graph = session.graph().unwrap();
        let keys = [
            Key::from_u128(1),
            Key::from_u128(2),
            Key::from_u128(3),
            Key::from_u128(4),
        ];
        let missing = Key::from_u128(99);
        let mut input = GraphBatch::new(4, 5);
        {
            let mut out = input.as_output();
            out.push(Some(VertexRef {
                version: 1,
                edges: &[
                    GraphEdge {
                        slot: 0,
                        target: keys[1],
                    },
                    GraphEdge {
                        slot: 2,
                        target: keys[2],
                    },
                ],
            }))
            .unwrap();
            for target in [keys[3], keys[3], keys[0]] {
                out.push(Some(VertexRef {
                    version: 1,
                    edges: &[GraphEdge { slot: 0, target }],
                }))
                .unwrap();
            }
        }
        assert_eq!(graph.insert(&keys, &input.as_input()).unwrap(), 4);
        let mut storage = [Key::nil(); 8];
        let mut neighbors = KeysOutput::new(&mut storage);
        assert_eq!(graph.expand_neighbors(keys[0], 8, &mut neighbors).unwrap(), 1);
        assert_eq!(neighbors.as_slice(), &[keys[3]]);
        let mut output = GraphBatch::new(3, 8);
        assert_eq!(
            graph
                .update(
                    &[keys[0], missing],
                    &[
                        GraphPatch {
                            version: 2,
                            neighbor: Some(keys[3])
                        },
                        GraphPatch {
                            version: 2,
                            neighbor: Some(keys[0])
                        }
                    ]
                )
                .unwrap(),
            1
        );
        assert_eq!(graph.read(&[keys[0], missing], &mut output.as_output()).unwrap(), 1);
        let input_view = output.as_input();
        let value = input_view.get(0).unwrap();
        assert_eq!(value.version, 2);
        assert_eq!(
            value.edges,
            &[
                GraphEdge {
                    slot: 0,
                    target: keys[3]
                },
                GraphEdge {
                    slot: 2,
                    target: keys[2]
                }
            ]
        );
        assert!(output.as_input().get(1).is_none());
        assert_eq!(graph.delete(&[keys[2], missing]).unwrap(), 1);
        graph.read(&[keys[0]], &mut output.as_output()).unwrap();
        assert_eq!(
            output.as_input().get(0).unwrap().edges,
            &[GraphEdge {
                slot: 0,
                target: keys[3]
            }]
        );
        assert_eq!(graph.expand_neighbors(keys[3], 8, &mut neighbors).unwrap(), 0);
        assert_eq!(
            graph
                .update(
                    &[keys[0]],
                    &[GraphPatch {
                        version: 3,
                        neighbor: None
                    }]
                )
                .unwrap(),
            1
        );
        graph.read(&[keys[0]], &mut output.as_output()).unwrap();
        let input_view = output.as_input();
        let value = input_view.get(0).unwrap();
        assert_eq!(value.version, 3);
        assert!(value.edges.is_empty());
        input
            .as_output()
            .push(Some(VertexRef { version: 4, edges: &[] }))
            .unwrap();
        assert_eq!(graph.insert(&[missing], &input.as_input()).unwrap(), 1);
        assert_eq!(graph.read(&[missing], &mut output.as_output()).unwrap(), 1);
        assert!(output.as_input().get(0).unwrap().edges.is_empty());
        assert_eq!(graph.delete(&[missing]).unwrap(), 1);
    }};
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        alloc::{GlobalAlloc, Layout},
        cell::Cell,
    };

    use super::*;

    struct TrackingAllocator;
    thread_local! {
        static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
    }
    fn allocated() {
        let _ = ALLOCATIONS.try_with(|count| {
            if let Some(value) = count.get() {
                count.set(Some(value + 1));
            }
        });
    }
    // The wrapper preserves System's allocation and layout contracts.
    unsafe impl GlobalAlloc for TrackingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            allocated();
            unsafe { System.alloc(layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            allocated();
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            allocated();
            unsafe { System.realloc(pointer, layout, size) }
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) }
        }
    }
    #[global_allocator]
    static ALLOCATOR: TrackingAllocator = TrackingAllocator;

    pub(crate) fn assert_no_global_allocations(run: impl FnOnce()) {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                ALLOCATIONS.with(|count| count.set(None));
            }
        }
        ALLOCATIONS.with(|count| count.set(Some(0)));
        let reset = Reset;
        run();
        let count = ALLOCATIONS.with(|count| count.get().unwrap());
        drop(reset);
        assert_eq!(count, 0, "unexpected global allocation in warmed operation");
    }
    #[test]
    fn bounded_outputs_preserve_missing_rows_and_reject_overflow() {
        let mut batch = RecordBatch::new_in(2, 3, System);
        {
            let mut output = batch.as_output();
            output.push(None).unwrap();
            output.push(Some(b"abc")).unwrap();
            assert!(output.push(Some(b"x")).is_err());
        }
        assert_eq!(batch.as_input().get(0), None);
        assert_eq!(batch.as_input().get(1), Some(b"abc".as_slice()));
        let mut documents = DocumentBatch::new_in(1, 2, System);
        assert!(documents
            .as_output()
            .push(Some(DocumentRef {
                score: 7,
                payload: "abc"
            }))
            .is_err());
        let mut graph = GraphBatch::new_in(1, 0, System);
        let edge = GraphEdge {
            slot: 0,
            target: Key::nil(),
        };
        assert!(graph
            .as_output()
            .push(Some(VertexRef {
                version: 1,
                edges: &[edge]
            }))
            .is_err());
    }
}
