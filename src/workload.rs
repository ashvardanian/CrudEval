//! Explicit workload names, operation mixes, and checked size parsing.

use std::{fmt, ops::RangeInclusive, str::FromStr};

use serde::Serialize;

use crate::{
    data::{Distribution, RandomGenerator},
    Bytes,
};

/// Smallest payload: the 16-byte key plus the 8-byte version every value starts with.
const MIN_VALUE_SIZE: Bytes = Bytes(24);

pub const DEFAULT_WORKLOADS: &str = "bulk-load,read,batch-read-256,range-read-256,full-scan,read-50-update-50,read-latest-95-insert-5,batch-insert-1000,delete-oldest";
const EXPECTED_WORKLOADS: &str = "expected comma-separated workloads: bulk-load, read, full-scan, read-50-update-50, read-95-update-5, read-latest-95-insert-5, range-read-95-insert-5, read-50-read-modify-write-50, delete-oldest, ingest, batch-read-N, range-read-N, batch-insert-N, bulk-load-N or ingest-N";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Operation {
    Insert,
    Read,
    Update,
    Delete,
    RangeRead,
    ReadModifyWrite,
    BulkLoad,
    /// The engine's native bulk path, such as sorted-file ingestion; `BulkLoad` writes through ordinary batches.
    Ingest,
    FullScan,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Read => "read",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::RangeRead => "range-read",
            Self::ReadModifyWrite => "read-modify-write",
            Self::BulkLoad => "bulk-load",
            Self::Ingest => "ingest",
            Self::FullScan => "full-scan",
        }
    }
    /// Whether the operation loads reserved key ranges, through batches or the native bulk path.
    pub fn loads(self) -> bool {
        matches!(self, Self::BulkLoad | Self::Ingest)
    }
}

#[derive(Clone, Debug)]
pub struct Workload {
    pub name: String,
    pub operations: Vec<(Operation, u32)>,
    pub batch_size: usize,
    pub distribution: Distribution,
    pub range_size: Option<RangeInclusive<usize>>,
}

impl Workload {
    pub fn choose(&self, rng: &mut RandomGenerator) -> Operation {
        let mut draw = rng.below(100) as u32;
        for &(operation, share) in &self.operations {
            if draw < share {
                return operation;
            }
            draw -= share;
        }
        unreachable!("workload shares sum to 100")
    }
}

impl fmt::Display for Workload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

impl Serialize for Workload {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.name)
    }
}

impl FromStr for Workload {
    type Err = String;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        use Operation::*;

        let name = input;
        let distribution = Distribution::Zipf;
        let mut result = Self {
            name: name.into(),
            operations: Vec::new(),
            batch_size: 1,
            distribution,
            range_size: None,
        };
        result.operations = match name {
            "bulk-load" => {
                result.batch_size = 100_000;
                vec![(BulkLoad, 100)]
            }
            "ingest" => {
                result.batch_size = 100_000;
                vec![(Ingest, 100)]
            }
            "read" => vec![(Read, 100)],
            "full-scan" => {
                result.batch_size = 256;
                vec![(FullScan, 100)]
            }
            "read-50-update-50" => vec![(Read, 50), (Update, 50)],
            "read-95-update-5" => vec![(Read, 95), (Update, 5)],
            "read-latest-95-insert-5" => {
                result.distribution = Distribution::Latest;
                vec![(Read, 95), (Insert, 5)]
            }
            "range-read-95-insert-5" => {
                result.range_size = Some(1..=100);
                vec![(RangeRead, 95), (Insert, 5)]
            }
            "read-50-read-modify-write-50" => vec![(Read, 50), (ReadModifyWrite, 50)],
            "delete-oldest" => vec![(Delete, 100)],
            _ => {
                let (prefix, count, operation) = [
                    ("batch-read-", Read),
                    ("range-read-", RangeRead),
                    ("batch-insert-", Insert),
                    ("bulk-load-", BulkLoad),
                    ("ingest-", Ingest),
                ]
                .into_iter()
                .find_map(|(prefix, operation)| name.strip_prefix(prefix).map(|n| (prefix, n, operation)))
                .ok_or(EXPECTED_WORKLOADS)?;
                result.batch_size = parse_count(count)
                    .ok()
                    .and_then(|count| usize::try_from(count).ok())
                    .ok_or(EXPECTED_WORKLOADS)?;
                result.name = format!("{prefix}{}", result.batch_size);
                vec![(operation, 100)]
            }
        };
        Ok(result)
    }
}

/// Parses a count like `10000` or `10K`: `K`, `M`, `G`, `T` are powers of 1000 in any case, and zero is rejected.
pub fn parse_count(text: &str) -> Result<u64, String> {
    let split = text.find(|c: char| !c.is_ascii_digit()).unwrap_or(text.len());
    let exponent = ["", "k", "m", "g", "t"]
        .iter()
        .position(|unit| text[split..].eq_ignore_ascii_case(unit));
    let count = exponent.and_then(|exponent| {
        text[..split]
            .parse::<u64>()
            .ok()?
            .checked_mul(1000u64.pow(exponent as u32))
    });
    count
        .filter(|&count| count != 0)
        .ok_or_else(|| "expected a positive count".into())
}

/// Parses a size like `64MB`: bare bytes, or `KB`, `MB`, `GB`, `TB` in any case; zero is rejected.
pub fn parse_size(text: &str) -> Result<Bytes, String> {
    let split = text.find(|c: char| !c.is_ascii_digit()).unwrap_or(text.len());
    let level = ["", "kb", "mb", "gb", "tb"]
        .iter()
        .position(|unit| text[split..].eq_ignore_ascii_case(unit));
    let bytes = level.and_then(|level| text[..split].parse::<u64>().ok()?.checked_mul(1u64 << (10 * level)));
    bytes
        .filter(|&bytes| bytes != 0)
        .map(Bytes)
        .ok_or_else(|| "expected a size like 4096, 64KB or 1GB".into())
}

/// Parses a payload size like `1KB` or an ascending range like `100..1KB`, each end at least `MIN_VALUE_SIZE`.
pub fn parse_value_size(input: &str) -> Result<RangeInclusive<Bytes>, String> {
    let (min, max) = input.split_once("..").unwrap_or((input, input));
    match (parse_size(min), parse_size(max)) {
        (Ok(min), Ok(max)) if MIN_VALUE_SIZE <= min && min <= max => Ok(min..=max),
        _ => Err("expected a size like 1KB or a range like 100..1KB, of at least 24 bytes".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_workloads_round_trip() {
        for name in DEFAULT_WORKLOADS.split(',').chain([
            "read-95-update-5",
            "range-read-95-insert-5",
            "read-50-read-modify-write-50",
        ]) {
            let workload: Workload = name.parse().unwrap();
            assert_eq!(workload.to_string(), name);
            assert_eq!(workload.operations.iter().map(|(_, p)| p).sum::<u32>(), 100);
        }
        assert_eq!("batch-insert-1K".parse::<Workload>().unwrap().name, "batch-insert-1000");
        for invalid in [
            "R100",
            "batch-read-0",
            "read~uniform",
            "read-latest-95-insert-5~uniform",
        ] {
            assert!(invalid.parse::<Workload>().is_err());
        }
    }
    #[test]
    fn sizes_are_checked() {
        assert_eq!(parse_count("1M").unwrap(), 1_000_000);
        assert_eq!(parse_size("64mb").unwrap(), Bytes(64 << 20));
        assert_eq!(parse_value_size("24..1KB").unwrap(), Bytes(24)..=Bytes(1024));
        for invalid in ["18446744073709551615K", "1.5K", "-1", "+1", "K", "0", "1KB"] {
            assert!(parse_count(invalid).is_err());
        }
        for invalid in ["0", "1K", "1KiB", "24B", "+1KB"] {
            assert!(parse_size(invalid).is_err());
        }
        for invalid in ["23", "1024..24", "24.."] {
            assert!(parse_value_size(invalid).is_err());
        }
    }
}
