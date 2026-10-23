//! Deterministic typed records and allocation-free payload verification.

use std::ops::RangeInclusive;

use crate::{
    backend::{
        DataModel, DocumentOutput, DocumentPatch, DocumentRef, GraphEdge, GraphOutput, GraphPatch, Key, RecordOutput,
        Result, VertexRef,
    },
    data::{derive_seed, Distribution, Keys, RandomGenerator, ValuePool},
};

pub struct RecordGenerator<'a> {
    pool: Option<ValuePool>,
    keys: &'a Keys,
    seed: u64,
    records: u64,
    degree: usize,
    max_value_size: usize,
}
impl<'a> RecordGenerator<'a> {
    pub fn new(
        data_model: DataModel,
        keys: &'a Keys,
        seed: u64,
        sizes: RangeInclusive<usize>,
        records: u64,
        degree: usize,
    ) -> Result<Self> {
        if data_model == DataModel::Graph && records == 0 {
            return Err("graph workloads require initial records".into());
        }
        if degree > u32::MAX as usize {
            return Err("graph degree exceeds slot range".into());
        }
        Ok(Self {
            keys,
            max_value_size: *sizes.end(),
            pool: if data_model == DataModel::Graph {
                None
            } else {
                Some(ValuePool::new(seed, sizes)?)
            },
            seed,
            records,
            degree: degree.min(records.saturating_sub(1) as usize),
        })
    }
    /// How this run's ordinals become keys.
    pub fn keys(&self) -> &'a Keys {
        self.keys
    }
    pub fn max_value_size(&self) -> usize {
        self.max_value_size
    }
    pub fn degree(&self) -> usize {
        self.degree
    }
    pub fn fill_value(&self, key: Key, version: u64, output: &mut RecordOutput<'_>) -> Result<()> {
        let pool = self.pool.as_ref().unwrap();
        pool.fill(key, version, output.push_bytes(pool.len(key, version))?)
    }
    pub fn fill_document(&self, key: Key, version: u64, output: &mut DocumentOutput<'_>) -> Result<()> {
        let pool = self.pool.as_ref().unwrap();
        let len = pool
            .len(key, 0)
            .checked_mul(2)
            .ok_or("document payload size overflow")?;
        pool.fill_hex(key, output.push_payload(version & i64::MAX as u64, len)?)
    }
    pub fn fill_vertex(
        &self,
        key: Key,
        version: u64,
        floor: u64,
        scratch: &mut [GraphEdge],
        output: &mut GraphOutput<'_>,
    ) -> Result<()> {
        let version = version & i64::MAX as u64;
        let count = self.edges(key, version, floor, scratch);
        output.push(Some(VertexRef {
            version,
            edges: &scratch[..count],
        }))
    }
    pub fn graph_patch(&self, key: Key, version: u64, floor: u64, scratch: &mut [GraphEdge]) -> GraphPatch {
        let version = version & i64::MAX as u64;
        let count = self.edges(key, version, floor, scratch);
        GraphPatch {
            version,
            neighbor: scratch[..count]
                .iter()
                .find(|edge| edge.slot == 0)
                .map(|edge| edge.target),
        }
    }
    pub fn verify_value(&self, key: Key, value: &[u8]) -> Result<u64> {
        self.pool.as_ref().unwrap().verify(key, value)
    }
    pub fn verify_document(&self, key: Key, value: DocumentRef<'_>) -> Result<()> {
        if value.score > i64::MAX as u64 {
            return Err("invalid document score".into());
        }
        self.pool.as_ref().unwrap().verify_hex(key, value.payload)
    }
    pub fn verify_vertex(&self, key: Key, value: VertexRef<'_>, floor: u64, scratch: &mut [GraphEdge]) -> Result<()> {
        if value.version > i64::MAX as u64 {
            return Err("invalid graph version".into());
        }
        let count = self.edges(key, value.version, floor, scratch);
        if value.edges != &scratch[..count] {
            return Err("vertex adjacency mismatch".into());
        }
        Ok(())
    }
    pub fn edges(&self, key: Key, version: u64, floor: u64, output: &mut [GraphEdge]) -> usize {
        let source = self.keys.decode(key).expect("graph keys come from this run's ordinals");
        graph_neighbors(self.seed, self.keys, source, version, self.records, self.degree, output);
        let mut count = 0;
        for index in 0..self.degree {
            if self
                .keys
                .decode(output[index].target)
                .is_some_and(|target| target >= floor)
            {
                output[count] = output[index];
                count += 1;
            }
        }
        count
    }
}
pub fn next_version(version: u64) -> u64 {
    version.wrapping_add(1) & i64::MAX as u64
}
pub fn document_patch(version: u64) -> DocumentPatch {
    DocumentPatch {
        score: version & i64::MAX as u64,
    }
}

/// The out-edges of ordinal `source`, drawn among ordinals so every key type shares one graph.
pub fn graph_neighbors(
    seed: u64,
    keys: &Keys,
    source: u64,
    version: u64,
    records: u64,
    degree: usize,
    output: &mut [GraphEdge],
) {
    let degree = degree.min(records.saturating_sub(1) as usize);
    assert!(output.len() >= degree);
    let source_seed = seed ^ source.rotate_left(17);
    // Stable slots are selected first so a rewire cannot change another edge.
    for slot in (1..degree).chain((degree > 0).then_some(0)) {
        let mut rng = RandomGenerator::new(
            derive_seed(source_seed, "graph-neighbor", slot) ^ if slot == 0 { version } else { 0 },
        );
        let used = if slot == 0 {
            &output[1..degree]
        } else {
            &output[1..slot]
        };
        let taken = |target: u64| target == source || used.iter().any(|edge| edge.target == keys.encode(target));
        let mut target = 0;
        for _ in 0..16 {
            target = rng.sample(Distribution::Zipf, 0..records).unwrap();
            if !taken(target) {
                break;
            }
        }
        while taken(target) {
            target = if target + 1 == records { 0 } else { target + 1 };
        }
        output[slot] = GraphEdge {
            slot: slot as u32,
            target: keys.encode(target),
        };
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::System;

    use super::*;
    use crate::backend::{DocumentBatch, GraphBatch, KeyType};

    #[test]
    fn document_payload_survives_score_updates_and_detects_corruption() {
        let generator =
            RecordGenerator::new(DataModel::Documents, &Keys::Generated(KeyType::U64), 42, 32..=32, 10, 3).unwrap();
        let key = Key::from_u128(3);
        let mut rows = DocumentBatch::new_in(1, 64, System);
        generator.fill_document(key, 7, &mut rows.as_output()).unwrap();
        let input = rows.as_input();
        let doc = input.get(0).unwrap();
        generator.verify_document(key, doc).unwrap();
        assert_eq!(doc.score, 7);
        let mut original = DocumentBatch::new_in(1, 64, System);
        generator.fill_document(key, 0, &mut original.as_output()).unwrap();
        assert_eq!(original.as_input().get(0).unwrap().payload, doc.payload);
        let mut damaged = doc.payload.as_bytes().to_vec();
        damaged[0] = b'f';
        assert!(generator
            .verify_document(
                key,
                DocumentRef {
                    score: 7,
                    payload: std::str::from_utf8(&damaged).unwrap()
                }
            )
            .is_err());
    }
    #[test]
    fn graph_edges_preserve_slots_seed_and_deleted_targets() {
        let generator =
            RecordGenerator::new(DataModel::Graph, &Keys::Generated(KeyType::U64), 42, 24..=24, 100, 8).unwrap();
        assert!(generator.pool.is_none());
        let key = Key::from_u128(4);
        let mut before = [GraphEdge::default(); 8];
        let mut after = before;
        graph_neighbors(42, &Keys::Generated(KeyType::U64), 4, 0, 100, 8, &mut before);
        graph_neighbors(42, &Keys::Generated(KeyType::U64), 4, 123, 100, 8, &mut after);
        assert_eq!(&before[1..], &after[1..]);
        assert_ne!(before[0], after[0]);
        graph_neighbors(42, &Keys::Generated(KeyType::U64), 4, 0, 100, 8, &mut after);
        assert_eq!(before, after);
        graph_neighbors(43, &Keys::Generated(KeyType::U64), 4, 0, 100, 8, &mut after);
        assert_ne!(before, after);
        let mut rows = GraphBatch::new_in(1, 8, System);
        generator
            .fill_vertex(key, 19, 20, &mut before, &mut rows.as_output())
            .unwrap();
        let input = rows.as_input();
        let vertex = input.get(0).unwrap();
        generator.verify_vertex(key, vertex, 20, &mut after).unwrap();
        assert!(vertex.edges.iter().all(|e| e.target.as_u128() >= 20));
        for (i, edge) in vertex.edges.iter().enumerate() {
            assert_ne!(edge.target, key);
            assert!(!vertex.edges[..i].iter().any(|e| e.target == edge.target));
        }
        let mut damaged = vertex.edges.to_vec();
        damaged[0].target = key;
        assert!(generator
            .verify_vertex(
                key,
                VertexRef {
                    version: vertex.version,
                    edges: &damaged
                },
                20,
                &mut after
            )
            .is_err());
    }
    #[test]
    fn typed_generation_and_verification_do_not_allocate() {
        use std::alloc::System;

        use crate::backend::{DocumentBatch, GraphBatch, RecordBatch};

        let values = RecordGenerator::new(
            DataModel::KeyValue,
            &Keys::Generated(KeyType::U64),
            42,
            32..=128,
            100,
            4,
        )
        .unwrap();
        let docs = RecordGenerator::new(
            DataModel::Documents,
            &Keys::Generated(KeyType::U64),
            42,
            32..=128,
            100,
            4,
        )
        .unwrap();
        let graph =
            RecordGenerator::new(DataModel::Graph, &Keys::Generated(KeyType::U64), 42, 32..=128, 100, 4).unwrap();
        let mut records = RecordBatch::new_in(1, 128, System);
        let mut documents = DocumentBatch::new_in(1, 256, System);
        let mut vertices = GraphBatch::new_in(1, 4, System);
        let mut edges = [GraphEdge {
            slot: 0,
            target: Key::nil(),
        }; 4];
        crate::backend::tests::assert_no_global_allocations(|| {
            for index in 0..100 {
                let key = Key::from_u128(index);
                values.fill_value(key, index as u64, &mut records.as_output()).unwrap();
                values.verify_value(key, records.as_input().get(0).unwrap()).unwrap();
                docs.fill_document(key, index as u64, &mut documents.as_output())
                    .unwrap();
                docs.verify_document(key, documents.as_input().get(0).unwrap()).unwrap();
                graph
                    .fill_vertex(key, index as u64, 0, &mut edges, &mut vertices.as_output())
                    .unwrap();
                graph
                    .verify_vertex(key, vertices.as_input().get(0).unwrap(), 0, &mut edges)
                    .unwrap();
            }
        });
    }
}
