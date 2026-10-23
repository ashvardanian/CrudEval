//! Shared benchmark runner: CLI sweeps, workload execution, and resumable data directories.

pub mod backend;
pub mod data;
#[cfg(feature = "tier2")]
pub mod docker;
pub mod measure;
pub mod output;
pub mod perf_counters;
pub mod workload;

use std::path::{Path, PathBuf};

use clap::Args;

pub use crate::backend::{
    Backend, BackendCapabilities, BackendSession, DataModel, Durability, Key, KeyType, KeyWidth, RecordBatch, Result,
};

/// Version of the report layout, bumped when a field changes meaning.
const REPORT_SCHEMA_VERSION: u32 = 2;
/// Version of the adapters' observable behaviour, recorded in each report.
const ADAPTER_REVISION: u32 = 2;
/// Version of the data-directory identity, bumped when its inputs change.
const IDENTITY_VERSION: u32 = 3;
/// Version of the `.crudeval` manifest layout.
const MANIFEST_FORMAT: u32 = 1;

/// What happens to an embedded database between two workloads.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BetweenWorkloads {
    Keep,
    Reopen,
    /// Reopen and drop the host page cache; Linux only.
    ReopenAndDropCaches,
}

/// How much of each read is checked.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Verify {
    /// Payload values and structure.
    Values,
    /// Structure only, to measure the cost of value checks separately.
    Structure,
}

/// Attempted entries per ordinary workload: a count, or a percentage of the initial records.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum EntryBudget {
    Count(u64),
    Percent(f64),
}

impl EntryBudget {
    fn entries(self, records: u64) -> u64 {
        match self {
            Self::Count(count) => count,
            Self::Percent(percent) => ((records as f64 * percent / 100.0).ceil() as u64).max(1),
        }
    }
}

impl fmt::Display for EntryBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(count) => write!(formatter, "{count}"),
            Self::Percent(percent) => write!(formatter, "{percent}%"),
        }
    }
}

impl serde::Serialize for EntryBudget {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[derive(Args, Clone, Debug, serde::Serialize)]
pub struct CommonArgs {
    /// Initial record counts; comma-separated values form a sweep.
    #[arg(long, env = "CRUDEVAL_RECORDS", value_delimiter = ',', default_value = "100K", value_parser = workload::parse_count)]
    pub records: Vec<u64>,
    /// Worker counts, 0 for all cores; comma-separated values form a sweep.
    #[arg(long, env = "CRUDEVAL_THREADS", value_delimiter = ',', default_value = "1", value_parser = parse_threads_flag)]
    pub threads: Vec<Threads>,
    /// Ordered comma-separated workload names.
    #[arg(long, env = "CRUDEVAL_WORKLOADS", value_delimiter = ',', default_value = workload::DEFAULT_WORKLOADS, value_parser = |text: &str| text.parse::<Workload>())]
    pub workloads: Vec<Workload>,
    /// Override key sampling for the selected workloads.
    #[arg(long, env = "CRUDEVAL_DISTRIBUTION", value_enum)]
    pub distribution: Option<data::Distribution>,
    /// Attempted entries per ordinary workload, as a count or percentage.
    #[arg(long, env = "CRUDEVAL_ENTRIES", default_value = "10%", value_parser = parse_entries_flag)]
    pub entries: EntryBudget,
    /// Time limit instead of an entry budget, such as 30s or 500ms.
    #[arg(long, env = "CRUDEVAL_TIME_LIMIT", value_parser = parse_duration_flag)]
    pub time_limit: Option<Duration>,
    /// Binary payload size or range, such as 1KB or 100..1KB.
    #[arg(long, env = "CRUDEVAL_VALUE_SIZE", default_value = "1KB", value_parser = workload::parse_value_size)]
    #[serde(serialize_with = "serialize_value_size")]
    pub value_size: RangeInclusive<Bytes>,
    /// Parent directory for isolated, marked benchmark databases.
    #[arg(long, env = "CRUDEVAL_DATA_DIR", default_value = "data")]
    pub data_dir: PathBuf,
    /// Directory for JSON reports.
    #[arg(long, env = "CRUDEVAL_OUTPUT", default_value = "results")]
    pub output: PathBuf,
    /// Seed for data and per-worker random generators, or `random` to draw one.
    #[arg(long, env = "CRUDEVAL_SEED", default_value = "42", value_parser = parse_seed_flag)]
    pub seed: Seed,
    /// Requested write durability; adapters report their effective settings.
    #[arg(long, env = "CRUDEVAL_DURABILITY", value_enum, default_value = "none")]
    pub durability: Durability,
    /// Storage model to exercise.
    #[arg(long, env = "CRUDEVAL_DATA_MODEL", value_enum, default_value = "key-value")]
    pub data_model: DataModel,
    /// Keys records get from their ordinals: u32, u64, uuid-v4 or uuid-v7.
    #[arg(long, env = "CRUDEVAL_KEY_TYPE", value_enum, default_value = "uuid-v7")]
    pub key_type: KeyType,
    /// Ascending, unique keys that ordinals index into: `.i32bin`, `.u64bin` or `.uuidbin` shards.
    #[arg(long, env = "CRUDEVAL_KEYS", value_name = "PATH|GLOB", conflicts_with = "key_type")]
    pub keys: Option<PathBuf>,
    /// Outgoing graph degree, capped at the initial population minus one.
    #[arg(long, env = "CRUDEVAL_DEGREE", default_value = "8", value_parser = parse_count_flag)]
    pub degree: usize,
    /// Calls per transaction; unset leaves transaction boundaries to the adapter.
    #[arg(long, env = "CRUDEVAL_CALLS_PER_TRANSACTION", value_parser = parse_count_flag)]
    pub calls_per_transaction: Option<usize>,
    /// Aggregate scheduled calls per second across workers.
    #[arg(long, env = "CRUDEVAL_CALLS_PER_SECOND", value_parser = parse_rate_flag)]
    pub calls_per_second: Option<f64>,
    /// Check payload values and structure, or structure only.
    #[arg(long, env = "CRUDEVAL_VERIFY", value_enum, default_value = "values")]
    pub verify: Verify,
    /// Record Linux worker hardware counters; requires the perf-counters feature.
    #[arg(long, env = "CRUDEVAL_PERF_COUNTERS", default_value_t = false, value_parser = clap::builder::BoolishValueParser::new())]
    pub perf_counters: bool,
    /// Keep an embedded database open between workloads, reopen it, or also drop the page cache.
    #[arg(long, env = "CRUDEVAL_BETWEEN_WORKLOADS", value_enum, default_value = "keep")]
    pub between_workloads: BetweenWorkloads,
}

impl CommonArgs {
    /// How wide engines store this run's keys: the key file's width, or the key type's.
    pub fn key_width(&self) -> Result<KeyWidth> {
        match &self.keys {
            Some(path) => KeyWidth::of_file(path),
            None => Ok(self.key_type.width()),
        }
    }
}

/// Runs every configuration; `backend_settings` are echoed after the common settings as "- Name: value".
pub fn run(
    args: CommonArgs,
    backend_options: impl serde::Serialize,
    backend_settings: &[(&str, String)],
    open: impl Fn(&CommonArgs, &Path) -> Result<Box<dyn Backend, System>>,
) -> Result<()> {
    run_benchmarks(
        args,
        serde_json::to_value(backend_options).map_err(|e| e.to_string())?,
        backend_settings,
        open,
    )
}

use std::{
    alloc::System,
    fmt,
    hash::{BuildHasher, Hasher, RandomState},
    num::{NonZeroU16, NonZeroUsize},
    ops::RangeInclusive,
    sync::{
        atomic::{AtomicBool, Ordering},
        Barrier, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    data::{derive_seed, KeySpace, RandomGenerator},
    measure::{ResourceSampler, WorkloadMeasurements},
    workload::{Operation, Workload},
};

pub mod model;

/// Parses the command line; a bad value prints `--name="value" does not parse, expected ...` and exits with 1.
pub fn parse_cli<T: clap::Parser>() -> T {
    use clap::error::{ContextKind, ContextValue, ErrorKind};

    let error = match T::try_parse() {
        Ok(cli) => return cli,
        Err(error) if !error.use_stderr() => error.exit(),
        Err(error) => error,
    };
    let context = |kind| match error.get(kind) {
        Some(ContextValue::String(text)) => text.as_str(),
        _ => "",
    };
    let expected = match (
        error.kind(),
        std::error::Error::source(&error),
        error.get(ContextKind::ValidValue),
    ) {
        (ErrorKind::ValueValidation, Some(expected), _) => expected.to_string(),
        (ErrorKind::InvalidValue, _, Some(ContextValue::Strings(values))) if !values.is_empty() => {
            format!("expected one of {}", values.join(", "))
        }
        (ErrorKind::InvalidValue, _, _) => "expected a non-empty value".into(),
        _ => {
            eprint!("{error}");
            std::process::exit(1)
        }
    };
    let flag = context(ContextKind::InvalidArg).split(' ').next().unwrap_or_default();
    eprintln!(
        "{flag}=\"{}\" does not parse, {expected}",
        context(ContextKind::InvalidValue)
    );
    std::process::exit(1)
}

/// Parses a 32-bit unsigned integer, or `random` as 32 bits from the OS entropy source.
pub fn parse_seed(text: &str) -> Option<Seed> {
    if text == "random" {
        return Some(Seed(RandomState::new().build_hasher().finish() as u32));
    }
    let digits = !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    digits.then(|| text.parse().ok().map(Seed)).flatten()
}

/// Parses a thread count like `8`, or `0` as `all_cores`.
pub fn parse_threads(text: &str, all_cores: NonZeroUsize) -> Option<Threads> {
    match text {
        "0" => Some(Threads(all_cores)),
        _ => parse_count(text).and_then(NonZeroUsize::new).map(Threads),
    }
}

/// Parses a positive whole number in ASCII digits, like `128`; zero is `None`.
pub fn parse_count(text: &str) -> Option<usize> {
    let digits = !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    digits.then(|| text.parse().ok()).flatten().filter(|&count| count != 0)
}

/// Parses a duration like `200ms` or `10s`; a bare number, a fraction or zero is `None`.
pub fn parse_duration(text: &str) -> Option<Duration> {
    match text.strip_suffix("ms") {
        Some(count) => parse_count(count).map(|count| Duration::from_millis(count as u64)),
        None => parse_count(text.strip_suffix('s')?).map(|count| Duration::from_secs(count as u64)),
    }
}

/// Spells a duration the way `parse_duration` reads it: `1s`, `1500ms`.
pub fn spell_duration(duration: Duration) -> String {
    let milliseconds = duration.as_millis();
    match milliseconds % 1000 {
        0 => format!("{}s", milliseconds / 1000),
        _ => format!("{milliseconds}ms"),
    }
}

/// Spells a size the way `parse_size` reads it: `256MB`, `1000`.
pub fn spell_size(bytes: u64) -> String {
    let (mut count, mut unit) = (bytes, "");
    for larger in ["KB", "MB", "GB", "TB"] {
        if count == 0 || count % 1024 != 0 {
            break;
        }
        count /= 1024;
        unit = larger;
    }
    format!("{count}{unit}")
}

/// A 32-bit run seed, an integer or drawn from the OS for `random`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Seed(pub u32);

impl From<Seed> for u64 {
    fn from(seed: Seed) -> u64 {
        u64::from(seed.0)
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A thread count; `0` in the variable resolves to every core when read, so it is never zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Threads(pub NonZeroUsize);

impl Threads {
    pub const ONE: Threads = Threads(NonZeroUsize::MIN);
}

impl fmt::Display for Threads {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A size in bytes, never an element count; prints the way `parse_size` reads it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub struct Bytes(pub u64);

impl fmt::Display for Bytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&spell_size(self.0))
    }
}

/// A TCP port; `0` is rejected at parse time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Port(pub NonZeroU16);

impl From<Port> for u16 {
    fn from(port: Port) -> u16 {
        port.0.get()
    }
}

impl fmt::Display for Port {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Parses a TCP port from 1 to 65535.
pub fn parse_port(text: &str) -> Result<Port> {
    parse_count(text)
        .and_then(|port| u16::try_from(port).ok())
        .and_then(NonZeroU16::new)
        .map(Port)
        .ok_or_else(|| "expected a port from 1 to 65535".into())
}

/// `parse_count` for clap.
pub fn parse_count_flag(text: &str) -> Result<usize> {
    parse_count(text).ok_or_else(|| "expected a positive count".into())
}

/// `parse_threads` for clap, with `0` resolved to every available core.
pub fn parse_threads_flag(text: &str) -> Result<Threads> {
    let all_cores = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
    parse_threads(text, all_cores).ok_or_else(|| "expected a count, 0 for all cores".into())
}

/// `parse_seed` for clap.
pub fn parse_seed_flag(text: &str) -> Result<Seed> {
    parse_seed(text).ok_or_else(|| "expected an unsigned integer or random".into())
}

/// `parse_duration` for clap.
pub fn parse_duration_flag(text: &str) -> Result<Duration> {
    parse_duration(text).ok_or_else(|| "expected a duration like 200ms or 10s".into())
}

/// Parses an unsigned decimal like `1000` or `2.5`; signs, exponents and non-finite spellings are `None`.
fn parse_decimal(text: &str) -> Option<f64> {
    let digits = text.starts_with(|c: char| c.is_ascii_digit())
        && text.bytes().all(|byte| byte.is_ascii_digit() || byte == b'.');
    digits.then(|| text.parse().ok()).flatten()
}

fn parse_rate_flag(text: &str) -> Result<f64> {
    parse_decimal(text)
        .filter(|rate| *rate > 0.0)
        .ok_or_else(|| "expected a positive rate like 1000 or 2.5".into())
}

fn parse_entries_flag(text: &str) -> Result<EntryBudget> {
    let budget = match text.strip_suffix('%') {
        Some(percent) => parse_decimal(percent)
            .filter(|percent| *percent > 0.0 && *percent <= 100.0)
            .map(EntryBudget::Percent),
        None => workload::parse_count(text).ok().map(EntryBudget::Count),
    };
    budget.ok_or_else(|| "expected a positive count like 10K or a percentage like 10%".into())
}

/// Spells a payload size or range the way `--value-size` reads it.
fn spell_value_size(sizes: &RangeInclusive<Bytes>) -> String {
    match sizes.start() == sizes.end() {
        true => sizes.start().to_string(),
        false => format!("{}..{}", sizes.start(), sizes.end()),
    }
}

fn serialize_value_size<S: serde::Serializer>(
    sizes: &RangeInclusive<Bytes>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.collect_str(&spell_value_size(sizes))
}

/// Spells a `clap::ValueEnum` value the way the command line takes it.
pub fn spell_value<T: clap::ValueEnum>(value: &T) -> String {
    value
        .to_possible_value()
        .map_or_else(String::new, |value| value.get_name().to_owned())
}

/// Joins values with commas, the way list flags read them.
fn spell_list<T: fmt::Display>(values: &[T]) -> String {
    values.iter().map(T::to_string).collect::<Vec<_>>().join(",")
}

fn drop_page_cache() -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            libc::sync();
        }
        std::fs::write("/proc/sys/vm/drop_caches", b"3\n").map_err(|e| format!("drop page cache: {e}"))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("--between-workloads reopen-and-drop-caches requires Linux".into())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct DataManifest {
    format: u32,
    identity: String,
    floor: u64,
    next: u64,
    dirty: bool,
}

impl DataManifest {
    fn read(path: &Path, identity: &str) -> Result<Self> {
        let bytes = std::fs::read(path.join(".crudeval"))
            .map_err(|e| format!("unowned benchmark directory {}: {e}", path.display()))?;
        let manifest: Self = serde_json::from_slice(&bytes)
            .map_err(|e| format!("invalid benchmark manifest {}: {e}", path.display()))?;
        if manifest.format != MANIFEST_FORMAT || manifest.identity != identity || manifest.floor > manifest.next {
            return Err(format!("benchmark manifest configuration mismatch: {}", path.display()));
        }
        Ok(manifest)
    }
    fn write(&self, path: &Path) -> Result<()> {
        let temporary = path.join(".crudeval.tmp");
        let file = std::fs::File::create(&temporary).map_err(|e| e.to_string())?;
        serde_json::to_writer(&file, self).map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        std::fs::rename(temporary, path.join(".crudeval")).map_err(|e| e.to_string())
    }
}

fn run_benchmarks(
    args: CommonArgs,
    backend_options: serde_json::Value,
    backend_settings: &[(&str, String)],
    open: impl Fn(&CommonArgs, &Path) -> Result<Box<dyn Backend, System>>,
) -> Result<()> {
    if args.perf_counters && !cfg!(all(target_os = "linux", feature = "perf-counters")) {
        return Err("--perf-counters requires Linux and the perf-counters feature".into());
    }
    let to_usize = |bytes: Bytes| usize::try_from(bytes.0).map_err(|_| "--value-size exceeds platform limit");
    let sizes = to_usize(*args.value_size.start())?..=to_usize(*args.value_size.end())?;
    let mut workloads = args.workloads.clone();
    if let Some(distribution) = args.distribution {
        for workload in &mut workloads {
            if workload.name == "read-latest-95-insert-5" && distribution != data::Distribution::Latest {
                return Err(
                    "read-latest-95-insert-5 requires --distribution latest or no distribution override".into(),
                );
            }
            workload.distribution = distribution;
        }
    }
    if workloads.iter().any(|workload| {
        workload.operations.iter().any(|(op, _)| *op == Operation::Delete)
            && workload
                .operations
                .iter()
                .any(|(op, _)| matches!(op, Operation::Insert | Operation::Update | Operation::ReadModifyWrite))
    }) {
        return Err("delete workloads cannot mix insertion or updates".into());
    }
    let loading = workloads[0].operations[0].0 == Operation::BulkLoad;
    if workloads
        .iter()
        .skip(1)
        .any(|w| w.operations[0].0 == Operation::BulkLoad)
    {
        return Err("bulk-load must be the first workload".into());
    }
    if args.between_workloads == BetweenWorkloads::ReopenAndDropCaches && !cfg!(target_os = "linux") {
        return Err("--between-workloads reopen-and-drop-caches requires Linux".into());
    }
    let unset = || "none".to_owned();
    eprintln!("- Records: {}", spell_list(&args.records));
    eprintln!("- Threads: {}", spell_list(&args.threads));
    eprintln!("- Workloads: {}", spell_list(&args.workloads));
    eprintln!(
        "- Distribution: {}",
        args.distribution.as_ref().map_or_else(unset, spell_value)
    );
    match args.time_limit {
        Some(time_limit) => eprintln!("- Time limit: {}", spell_duration(time_limit)),
        None => eprintln!("- Entries: {}", args.entries),
    }
    eprintln!("- Value size: {}", spell_value_size(&args.value_size));
    eprintln!("- Data dir: {}", args.data_dir.display());
    eprintln!("- Output: {}", args.output.display());
    eprintln!("- Seed: {}", args.seed);
    eprintln!("- Durability: {}", spell_value(&args.durability));
    eprintln!("- Data model: {}", spell_value(&args.data_model));
    eprintln!("- Degree: {}", args.degree);
    eprintln!(
        "- Calls per transaction: {}",
        args.calls_per_transaction.map_or_else(unset, |calls| calls.to_string())
    );
    eprintln!(
        "- Calls per second: {}",
        args.calls_per_second.map_or_else(unset, |calls| calls.to_string())
    );
    eprintln!("- Verify: {}", spell_value(&args.verify));
    eprintln!("- Perf counters: {}", args.perf_counters);
    eprintln!("- Between workloads: {}", spell_value(&args.between_workloads));
    for (name, value) in backend_settings {
        eprintln!("- {name}: {value}");
    }
    let keys = data::Keys::new(args.keys.as_deref(), args.key_type)?;
    let executable = std::env::current_exe().map_err(|e| e.to_string())?;
    let engine = executable.file_stem().and_then(|s| s.to_str()).unwrap_or("crudeval");
    for &records in &args.records {
        for &threads in &args.threads {
            let budget = args.entries.entries(records);
            let mut config = args.clone();
            config.records = vec![records];
            config.threads = vec![threads];
            let identity = output::config_hash(&(
                IDENTITY_VERSION,
                records,
                threads.0.get(),
                args.data_model,
                args.key_type,
                args.keys.as_deref(),
                keys.capacity(),
                args.durability,
                args.seed,
                (*sizes.start(), *sizes.end()),
                args.degree,
                &backend_options,
            ))?;
            let path = args.data_dir.join(format!("{engine}-{identity}"));
            let previous = if path.exists() {
                Some(DataManifest::read(&path, &identity)?)
            } else {
                None
            };
            let mut manifest = if loading {
                if previous.is_some() {
                    std::fs::remove_dir_all(&path).map_err(|e| e.to_string())?;
                }
                std::fs::create_dir_all(&path).map_err(|e| e.to_string())?;
                let manifest = DataManifest {
                    format: MANIFEST_FORMAT,
                    identity,
                    floor: 0,
                    next: 0,
                    dirty: true,
                };
                manifest.write(&path)?;
                manifest
            } else {
                let manifest = previous.ok_or("no existing benchmark data; start with bulk-load")?;
                if manifest.dirty {
                    return Err("previous mutation was interrupted or failed; restart with bulk-load".into());
                }
                manifest
            };
            let mut backend = open(&config, &path)?;
            if args.calls_per_transaction.is_some() && !backend.capabilities().transactions {
                return Err("this backend does not support --calls-per-transaction".into());
            }
            let mut report = output::ConfigReport {
                schema_version: REPORT_SCHEMA_VERSION,
                started_unix_seconds: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                machine: output::collect_machine_info(),
                workload: config.clone(),
                config: backend.metadata(),
                capabilities: backend.capabilities(),
                phases: Vec::new(),
            };
            report.config.insert("key_bytes".into(), 16.into());
            report.config.insert("adapter_revision".into(), ADAPTER_REVISION.into());
            report.config.insert(
                "latency_scope".into(),
                "attempted calls, including failures and rolled-back transactions".into(),
            );
            report.config.insert(
                "throughput_scope".into(),
                "successful entries published after commit".into(),
            );
            report.config.insert("backend_options".into(), backend_options.clone());
            report.config.insert("initial_key_floor".into(), manifest.floor.into());
            report.config.insert("initial_key_end".into(), manifest.next.into());
            if args.perf_counters {
                report.config.insert(
                    "hardware_counters_scope".into(),
                    "benchmark worker threads; excludes engine background threads and servers".into(),
                );
            }
            let keyspace = KeySpace::restore(manifest.floor..manifest.next)?;
            let model = model::RecordGenerator::new(
                args.data_model,
                &keys,
                args.seed.into(),
                sizes.clone(),
                records,
                args.degree,
            )?;
            eprintln!(
                "{} · {records} records · {threads} threads · {:?}",
                engine, args.data_model
            );
            eprintln!(
                "{:<36} {:>13} {:>12} {:>10}",
                "workload", "entries/s", "p99 (µs)", "missing"
            );
            for (index, workload) in workloads.iter().enumerate() {
                if index > 0 && args.between_workloads != BetweenWorkloads::Keep {
                    backend.flush()?;
                    drop(backend);
                    if args.between_workloads == BetweenWorkloads::ReopenAndDropCaches {
                        drop_page_cache()?;
                    }
                    backend = open(&config, &path)?;
                }
                let needs_range = workload
                    .operations
                    .iter()
                    .any(|(op, _)| matches!(op, Operation::RangeRead | Operation::FullScan));
                // A full scan splits ordinals among workers and walks each slice in key order, which
                // random UUIDs do not follow.
                let scans_ordinals = workload.operations.iter().any(|(op, _)| *op == Operation::FullScan)
                    && config.key_type == KeyType::UuidV4
                    && config.keys.is_none();
                let skipped = needs_range && !backend.capabilities().ordered_ranges || scans_ordinals;
                let mutating = !skipped
                    && workload.operations.iter().any(|(operation, _)| {
                        matches!(
                            operation,
                            Operation::Insert
                                | Operation::Update
                                | Operation::Delete
                                | Operation::ReadModifyWrite
                                | Operation::BulkLoad
                        )
                    });
                if mutating {
                    manifest.dirty = true;
                    manifest.write(&path)?;
                }
                let mut phase = if skipped {
                    empty_phase(workload, "skipped", Some("ordered ranges unsupported".into()))
                } else {
                    execute_phase(
                        backend.as_ref(),
                        &config,
                        workload,
                        &keyspace,
                        &model,
                        budget,
                        args.time_limit,
                    )?
                };
                let flush_start = Instant::now();
                if phase.status != "skipped" {
                    if let Err(error) = backend.flush() {
                        phase.error = Some(error);
                        phase.status = "failed".into();
                        phase.failed += 1;
                    }
                    phase.flush_seconds = flush_start.elapsed().as_secs_f64();
                }
                if mutating && phase.status == "completed" {
                    let live = keyspace.live();
                    manifest.floor = live.start;
                    manifest.next = live.end;
                    manifest.dirty = false;
                    manifest.write(&path)?;
                }
                phase.disk_bytes = backend.disk_bytes()?;
                phase.server_usage = backend.server_usage()?;
                let p99 = phase.latency.values().map(|l| l.p99_ns).max().unwrap_or(0) as f64 / 1000.0;
                eprintln!(
                    "{:<36} {:>13.0} {:>12.1} {:>10} {}",
                    workload.name, phase.throughput, p99, phase.missing, phase.status
                );
                let error = if phase.status == "failed" {
                    phase.error.clone()
                } else {
                    None
                };
                report.phases.push(phase);
                let report_path = output::write_report(&args.output, &report)?;
                if let Some(error) = error {
                    return Err(format!("{error}; report: {}", report_path.display()));
                }
            }
        }
    }
    Ok(())
}

fn empty_phase(workload: &Workload, status: &str, error: Option<String>) -> output::WorkloadReport {
    output::WorkloadReport {
        workload: workload.name.clone(),
        status: status.into(),
        elapsed_seconds: 0.0,
        flush_seconds: 0.0,
        calls: 0,
        entries: 0,
        throughput: 0.0,
        missing: 0,
        failed: 0,
        aborted: 0,
        corrupted: 0,
        processed_bytes: 0,
        latency: Default::default(),
        timeline_entries: Vec::new_in(System),
        timeline_buffer_growths: 0,
        client_usage: Default::default(),
        server_usage: None,
        hardware_counters: None,
        disk_bytes: 0,
        error,
    }
}

fn execute_phase(
    backend: &dyn Backend,
    args: &CommonArgs,
    workload: &Workload,
    keyspace: &KeySpace,
    model: &model::RecordGenerator,
    budget: u64,
    time_limit: Option<Duration>,
) -> Result<output::WorkloadReport> {
    let threads = args.threads[0].0.get();
    keyspace.workers(threads);
    let barrier = Barrier::new(threads);
    let start = OnceLock::new();
    let cancelled = AtomicBool::new(false);
    let sampler = ResourceSampler::start();
    let result = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity_in(threads, System);
        for thread in 0..threads {
            let barrier = &barrier;
            let start = &start;
            let cancelled = &cancelled;
            handles.push(scope.spawn(move || {
                let session = backend.session();
                let live = keyspace.live();
                let total = match workload.operations[0].0 {
                    Operation::BulkLoad => args.records[0],
                    Operation::FullScan => live.end - live.start,
                    _ => budget,
                };
                let target = total / threads as u64 + u64::from((thread as u64) < total % threads as u64);
                let buffers = WorkerBuffers::new(
                    args,
                    workload,
                    model,
                    target,
                    time_limit
                        .filter(|_| !matches!(workload.operations[0].0, Operation::BulkLoad | Operation::FullScan)),
                );
                if barrier.wait().is_leader() {
                    let _ = start.set(Instant::now());
                }
                barrier.wait();
                let measurements = match (session, buffers) {
                    (Ok(mut session), Ok(buffers)) => run_worker(
                        session.as_mut(),
                        args,
                        workload,
                        keyspace,
                        model,
                        budget,
                        time_limit,
                        thread,
                        *start.get().unwrap(),
                        cancelled,
                        buffers,
                    ),
                    (Err(error), _) | (_, Err(error)) => {
                        cancelled.store(true, Ordering::Relaxed);
                        WorkloadMeasurements {
                            failed: 1,
                            error: Some(error),
                            ..Default::default()
                        }
                    }
                };
                measurements
            }));
        }
        let mut total = WorkloadMeasurements::default();
        for handle in handles {
            total.merge(handle.join().map_err(|_| "benchmark worker panicked")?)?;
        }
        Ok::<_, String>(total)
    });
    let usage = sampler.stop()?;
    let measurements = result?;
    let elapsed = measurements.elapsed.as_secs_f64();
    let mut phase = empty_phase(
        workload,
        if measurements.error.is_some() {
            "failed"
        } else {
            "completed"
        },
        measurements.error.clone(),
    );
    phase.elapsed_seconds = elapsed;
    phase.calls = measurements.calls;
    phase.entries = measurements.entries;
    phase.throughput = measurements.entries as f64 / elapsed.max(f64::MIN_POSITIVE);
    phase.missing = measurements.missing;
    phase.failed = measurements.failed;
    phase.aborted = measurements.aborted;
    phase.corrupted = measurements.corrupted;
    phase.processed_bytes = measurements.processed_bytes;
    phase.latency = measurements.latencies();
    phase.hardware_counters = measurements.hardware_counters;
    phase.timeline_entries = measurements.timeline;
    phase.timeline_buffer_growths = measurements.timeline_buffer_growths;
    phase.client_usage = usage;
    Ok(phase)
}

enum WorkerSession<'a> {
    KeyValue(&'a mut dyn BackendSession),
    Documents(&'a mut dyn backend::DocumentSession),
    Graph(&'a mut dyn backend::GraphSession),
}
impl<'a> WorkerSession<'a> {
    fn select(session: &'a mut dyn BackendSession, data_model: DataModel) -> Result<Self> {
        match data_model {
            DataModel::KeyValue => Ok(Self::KeyValue(session)),
            DataModel::Documents => session
                .documents()
                .map(Self::Documents)
                .ok_or("document records unsupported".into()),
            DataModel::Graph => session
                .graph()
                .map(Self::Graph)
                .ok_or("graph records unsupported".into()),
        }
    }
    fn transaction(&mut self) -> &mut dyn backend::TransactionSession {
        match self {
            Self::KeyValue(s) => *s,
            Self::Documents(s) => *s,
            Self::Graph(s) => *s,
        }
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        match self {
            Self::KeyValue(s) => s.delete(keys),
            Self::Documents(s) => s.delete(keys),
            Self::Graph(s) => s.delete(keys),
        }
    }
}
enum WorkerRecords {
    KeyValue {
        input: RecordBatch,
        output: RecordBatch,
    },
    Documents {
        input: backend::DocumentBatch,
        output: backend::DocumentBatch,
        patches: Vec<backend::DocumentPatch, System>,
    },
    Graph {
        input: backend::GraphBatch,
        output: backend::GraphBatch,
        patches: Vec<backend::GraphPatch, System>,
        scratch: Vec<backend::GraphEdge, System>,
    },
}
impl WorkerRecords {
    fn new(data_model: DataModel, rows: usize, generator: &model::RecordGenerator) -> Result<Self> {
        Ok(match data_model {
            DataModel::KeyValue => {
                let bytes = rows
                    .checked_mul(generator.max_value_size())
                    .ok_or("record buffers exceed address space")?;
                Self::KeyValue {
                    input: RecordBatch::new(rows, bytes),
                    output: RecordBatch::new(rows, bytes),
                }
            }
            DataModel::Documents => {
                let bytes = rows
                    .checked_mul(generator.max_value_size())
                    .and_then(|n| n.checked_mul(2))
                    .ok_or("document buffers exceed address space")?;
                let mut patches = Vec::with_capacity_in(rows, System);
                patches.resize(rows, backend::DocumentPatch::default());
                Self::Documents {
                    input: backend::DocumentBatch::new(rows, bytes),
                    output: backend::DocumentBatch::new(rows, bytes),
                    patches,
                }
            }
            DataModel::Graph => {
                let edges = rows
                    .checked_mul(generator.degree())
                    .ok_or("graph buffers exceed address space")?;
                let mut patches = Vec::with_capacity_in(rows, System);
                patches.resize(rows, backend::GraphPatch::default());
                let mut scratch = Vec::with_capacity_in(generator.degree(), System);
                scratch.resize(generator.degree(), backend::GraphEdge::default());
                Self::Graph {
                    input: backend::GraphBatch::new(rows, edges),
                    output: backend::GraphBatch::new(rows, edges),
                    patches,
                    scratch,
                }
            }
        })
    }
    fn prepare(
        &mut self,
        generator: &model::RecordGenerator,
        keys: &[Key],
        operation: Operation,
        floor: u64,
        rng: &mut RandomGenerator,
    ) -> Result<()> {
        let updating = operation == Operation::Update;
        match self {
            Self::KeyValue { input, .. } => {
                let mut out = input.as_output();
                for &key in keys {
                    generator.fill_value(
                        key,
                        if operation == Operation::BulkLoad {
                            0
                        } else {
                            rng.next_u64() & i64::MAX as u64
                        },
                        &mut out,
                    )?;
                }
            }
            Self::Documents { input, patches, .. } => {
                if updating {
                    for patch in &mut patches[..keys.len()] {
                        patch.score = rng.next_u64() & i64::MAX as u64;
                    }
                } else {
                    let mut out = input.as_output();
                    for &key in keys {
                        generator.fill_document(
                            key,
                            if operation == Operation::BulkLoad {
                                0
                            } else {
                                rng.next_u64() & i64::MAX as u64
                            },
                            &mut out,
                        )?;
                    }
                }
            }
            Self::Graph {
                input,
                patches,
                scratch,
                ..
            } => {
                if updating {
                    for (index, &key) in keys.iter().enumerate() {
                        patches[index] = generator.graph_patch(key, rng.next_u64(), floor, scratch);
                    }
                } else {
                    let mut out = input.as_output();
                    for &key in keys {
                        generator.fill_vertex(
                            key,
                            if operation == Operation::BulkLoad {
                                0
                            } else {
                                rng.next_u64()
                            },
                            floor,
                            scratch,
                            &mut out,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
    fn modify(&mut self, generator: &model::RecordGenerator, keys: &[Key], floor: u64) -> Result<()> {
        match self {
            Self::KeyValue { input, output } => {
                let values = output.as_input();
                let mut out = input.as_output();
                for (index, &key) in keys.iter().enumerate() {
                    let value = values.get(index).ok_or("missing read-modify-write value")?;
                    let header = value.get(16..24).ok_or("truncated read-modify-write header")?;
                    let version = u64::from_be_bytes(header.try_into().unwrap());
                    generator.fill_value(key, model::next_version(version), &mut out)?;
                }
            }
            Self::Documents { output, patches, .. } => {
                let values = output.as_input();
                for (index, patch) in patches[..keys.len()].iter_mut().enumerate() {
                    patch.score =
                        model::next_version(values.get(index).ok_or("missing read-modify-write document")?.score);
                }
            }
            Self::Graph {
                output,
                patches,
                scratch,
                ..
            } => {
                let values = output.as_input();
                for (index, &key) in keys.iter().enumerate() {
                    let version =
                        model::next_version(values.get(index).ok_or("missing read-modify-write vertex")?.version);
                    patches[index] = generator.graph_patch(key, version, floor, scratch);
                }
            }
        }
        Ok(())
    }
    fn read(&mut self, session: &mut WorkerSession<'_>, keys: &[Key]) -> Result<usize> {
        match (self, session) {
            (Self::KeyValue { output, .. }, WorkerSession::KeyValue(s)) => s.read(keys, &mut output.as_output()),
            (Self::Documents { output, .. }, WorkerSession::Documents(s)) => s.read(keys, &mut output.as_output()),
            (Self::Graph { output, .. }, WorkerSession::Graph(s)) => s.read(keys, &mut output.as_output()),
            _ => unreachable!(),
        }
    }
    fn write(&mut self, session: &mut WorkerSession<'_>, keys: &[Key], operation: Operation) -> Result<usize> {
        match (self, session) {
            (Self::KeyValue { input, .. }, WorkerSession::KeyValue(s)) => match operation {
                Operation::BulkLoad => s.bulk_load(keys, &input.as_input()),
                Operation::Insert => s.insert(keys, &input.as_input()),
                _ => s.update(keys, &input.as_input()),
            },
            (Self::Documents { input, patches, .. }, WorkerSession::Documents(s)) => match operation {
                Operation::BulkLoad => s.bulk_load(keys, &input.as_input()),
                Operation::Insert => s.insert(keys, &input.as_input()),
                _ => s.update(keys, &patches[..keys.len()]),
            },
            (Self::Graph { input, patches, .. }, WorkerSession::Graph(s)) => match operation {
                Operation::BulkLoad => s.bulk_load(keys, &input.as_input()),
                Operation::Insert => s.insert(keys, &input.as_input()),
                _ => s.update(keys, &patches[..keys.len()]),
            },
            _ => unreachable!(),
        }
    }
    fn range_read(
        &mut self,
        session: &mut WorkerSession<'_>,
        start: Key,
        limit: usize,
        keys: &mut backend::KeysOutput<'_>,
    ) -> Result<usize> {
        match (self, session) {
            (Self::KeyValue { output, .. }, WorkerSession::KeyValue(s)) => {
                s.range_read(start, limit, keys, &mut output.as_output())
            }
            (Self::Documents { output, .. }, WorkerSession::Documents(s)) => {
                s.range_read(start, limit, keys, &mut output.as_output())
            }
            (Self::Graph { output, .. }, WorkerSession::Graph(s)) => {
                s.range_read(start, limit, keys, &mut output.as_output())
            }
            _ => unreachable!(),
        }
    }
    fn verify(
        &mut self,
        generator: &model::RecordGenerator,
        keys: &[Key],
        affected: usize,
        floor: u64,
        verify: bool,
    ) -> Result<()> {
        let (rows, found) = match self {
            Self::KeyValue { output, .. } => {
                let rows = output.as_input();
                if verify {
                    for (index, &key) in keys.iter().enumerate().take(rows.len()) {
                        if let Some(value) = rows.get(index) {
                            generator.verify_value(key, value)?;
                        }
                    }
                }
                (rows.len(), rows.found.iter().filter(|&&f| f).count())
            }
            Self::Documents { output, .. } => {
                let rows = output.as_input();
                if verify {
                    for (index, &key) in keys.iter().enumerate().take(rows.len()) {
                        if let Some(value) = rows.get(index) {
                            generator.verify_document(key, value)?;
                        }
                    }
                }
                (rows.len(), rows.payloads.found.iter().filter(|&&f| f).count())
            }
            Self::Graph { output, scratch, .. } => {
                let rows = output.as_input();
                if verify {
                    for (index, &key) in keys.iter().enumerate().take(rows.len()) {
                        if let Some(value) = rows.get(index) {
                            generator.verify_vertex(key, value, floor, scratch)?;
                        }
                    }
                }
                (rows.len(), rows.found.iter().filter(|&&f| f).count())
            }
        };
        if rows != keys.len() || found != affected {
            return Err("backend results do not match requested rows".into());
        }
        Ok(())
    }
    fn bytes(&self, operation: Operation, rows: usize) -> u64 {
        let writing = matches!(
            operation,
            Operation::Insert | Operation::BulkLoad | Operation::Update | Operation::ReadModifyWrite
        );
        let updating = matches!(operation, Operation::Update | Operation::ReadModifyWrite);
        match self {
            Self::KeyValue { input, output } => (if writing { input } else { output }).as_input().bytes.len() as u64,
            Self::Documents { input, output, .. } => {
                if updating {
                    rows as u64 * 8
                } else {
                    let batch = (if writing { input } else { output }).as_input();
                    batch.payloads.bytes.len() as u64 + batch.len() as u64 * 8
                }
            }
            Self::Graph { input, output, .. } => {
                if updating {
                    rows as u64 * 24
                } else {
                    let batch = (if writing { input } else { output }).as_input();
                    batch.edges.len() as u64 * 20 + batch.len() as u64 * 8
                }
            }
        }
    }
}
struct WorkerBuffers {
    records: WorkerRecords,
    keys: Vec<Key, System>,
    scanned: Vec<Key, System>,
    selected: Vec<u64, System>,
    measurements: WorkloadMeasurements,
}
impl WorkerBuffers {
    fn new(
        args: &CommonArgs,
        workload: &Workload,
        generator: &model::RecordGenerator,
        target: u64,
        time_limit: Option<Duration>,
    ) -> Result<Self> {
        let batch = workload
            .range_size
            .as_ref()
            .map_or(workload.batch_size, |range| *range.end())
            .max(workload.batch_size);
        let rows = if time_limit.is_some() {
            batch
        } else {
            batch.min(target.max(1) as usize)
        };
        let mut scanned = Vec::with_capacity_in(rows, System);
        scanned.resize(rows, Key::nil());
        let slots = rows
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or("batch size exceeds address space")?;
        let mut selected = Vec::with_capacity_in(slots, System);
        selected.resize(slots, u64::MAX);
        let histogram_names = workload.operations.iter().map(|(operation, _)| *operation);
        let timeline = match time_limit {
            Some(limit) => usize::try_from(limit.as_secs())
                .ok()
                .and_then(|seconds| seconds.checked_add(2))
                .ok_or("timeline exceeds address space")?,
            None => 3600,
        };
        Ok(Self {
            records: WorkerRecords::new(args.data_model, rows, generator)?,
            keys: Vec::with_capacity_in(rows, System),
            scanned,
            selected,
            measurements: WorkloadMeasurements::new(histogram_names, args.calls_per_transaction.is_some(), timeline)?,
        })
    }
    fn select(&mut self, key: u64) -> bool {
        let mask = self.selected.len() - 1;
        let mut slot = (key.wrapping_mul(0x9e3779b97f4a7c15) >> 32) as usize & mask;
        loop {
            let value = self.selected[slot];
            if value == key {
                return false;
            }
            if value == u64::MAX {
                self.selected[slot] = key;
                return true;
            }
            slot = (slot + 1) & mask;
        }
    }
}
#[derive(Default)]
struct PendingCounts {
    entries: u64,
    missing: u64,
    bytes: u64,
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    session: &mut dyn BackendSession,
    args: &CommonArgs,
    workload: &Workload,
    keyspace: &KeySpace,
    generator: &model::RecordGenerator,
    budget: u64,
    time_limit: Option<Duration>,
    thread: usize,
    start: Instant,
    cancelled: &AtomicBool,
    mut buffers: WorkerBuffers,
) -> WorkloadMeasurements {
    let threads = args.threads[0].0.get() as u64;
    let bulk_load = workload.operations[0].0 == Operation::BulkLoad;
    let full_scan = workload.operations[0].0 == Operation::FullScan;
    let mut snapshot = keyspace.live();
    let total = if bulk_load {
        args.records[0]
    } else if full_scan {
        snapshot.end - snapshot.start
    } else {
        budget
    };
    let target = total / threads + u64::from((thread as u64) < total % threads);
    let mut scan_start = snapshot.start + (total / threads) * thread as u64 + (thread as u64).min(total % threads);
    let scan_end = scan_start + target;
    let timed = time_limit.filter(|_| !bulk_load && !full_scan);
    let mut rng = RandomGenerator::new(derive_seed(args.seed.into(), &workload.name, thread));
    let mut pending = PendingCounts::default();
    let mut attempted = 0;
    let mut calls = 0u64;
    let mut transaction_calls = 0;
    let mut in_transaction = false;
    let mut performance_counters = None;
    let mut session = match WorkerSession::select(session, args.data_model) {
        Ok(s) => s,
        Err(error) => {
            buffers.measurements.failed = 1;
            buffers.measurements.error = Some(error);
            cancelled.store(true, Ordering::Relaxed);
            return buffers.measurements;
        }
    };
    let result = (|| -> Result<()> {
        if args.perf_counters {
            performance_counters = Some(perf_counters::PerfCounters::start().map_err(|e| e.to_string())?);
        }
        loop {
            if cancelled.load(Ordering::Relaxed)
                || timed.is_some_and(|limit| start.elapsed() >= limit)
                || timed.is_none() && attempted >= target
            {
                break;
            }
            let scheduled = if let Some(rate) = args.calls_per_second {
                let seconds = (calls as f64 * threads as f64 + thread as f64) / rate;
                let seconds = timed.map_or(seconds, |limit| seconds.min(limit.as_secs_f64()));
                let delay =
                    Duration::try_from_secs_f64(seconds).map_err(|_| "rate schedule exceeds supported duration")?;
                let scheduled = start
                    .checked_add(delay)
                    .ok_or("rate schedule exceeds supported duration")?;
                while let Some(wait) = scheduled.checked_duration_since(Instant::now()) {
                    if cancelled.load(Ordering::Relaxed) {
                        break;
                    }
                    std::thread::sleep(wait.min(Duration::from_millis(10)));
                }
                if cancelled.load(Ordering::Relaxed) || timed.is_some_and(|limit| start.elapsed() >= limit) {
                    break;
                }
                scheduled
            } else {
                Instant::now()
            };
            let operation = workload.choose(&mut rng);
            if calls.is_multiple_of(256) {
                snapshot = keyspace.live();
            }
            let batch = if operation == Operation::RangeRead {
                workload.range_size.as_ref().map_or(workload.batch_size, |range| {
                    *range.start() + rng.below((range.end() - range.start() + 1) as u64) as usize
                })
            } else if operation == Operation::Insert && workload.operations.len() > 1 {
                1
            } else {
                workload.batch_size
            };
            let mut count = batch as u64;
            if timed.is_none() {
                count = count.min(target - attempted);
            }
            buffers.keys.clear();
            if matches!(operation, Operation::Insert | Operation::BulkLoad) {
                let range = keyspace.reserve(thread, count)?;
                if range.end > generator.keys().capacity() {
                    return Err(format!("keys end at ordinal {}", generator.keys().capacity()));
                }
                buffers
                    .keys
                    .extend(range.map(|ordinal| generator.keys().encode(ordinal)));
            } else if operation == Operation::Delete {
                buffers.keys.extend(
                    keyspace
                        .delete_oldest(count)
                        .map(|ordinal| generator.keys().encode(ordinal)),
                );
                count = buffers.keys.len() as u64;
                if count == 0 {
                    break;
                }
            } else if full_scan {
                if scan_start >= scan_end {
                    break;
                }
                count = count.min(scan_end - scan_start);
                buffers.keys.push(generator.keys().encode(scan_start));
            } else {
                let live_count = snapshot.end - snapshot.start;
                if live_count == 0 {
                    return Err("workload requires existing records; start with bulk-load".into());
                }
                if operation == Operation::Read {
                    count = count.min(live_count);
                }
                let distinct = if operation == Operation::Read { count } else { 1 };
                if distinct == 1 {
                    buffers.keys.push(
                        generator
                            .keys()
                            .encode(rng.sample(workload.distribution, snapshot.clone()).unwrap()),
                    );
                } else if distinct == live_count {
                    buffers
                        .keys
                        .extend(snapshot.clone().map(|ordinal| generator.keys().encode(ordinal)));
                    for index in (1..buffers.keys.len()).rev() {
                        buffers.keys.swap(index, rng.below(index as u64 + 1) as usize);
                    }
                } else {
                    buffers.selected.fill(u64::MAX);
                    let wrap = |ordinal: u64| {
                        if ordinal + 1 == snapshot.end {
                            snapshot.start
                        } else {
                            ordinal + 1
                        }
                    };
                    while buffers.keys.len() < distinct as usize {
                        let mut samples =
                            std::iter::repeat_with(|| rng.sample(workload.distribution, snapshot.clone()).unwrap());
                        // Past 16 rejected draws, one more draw starts a walk to the next unselected ordinal.
                        let ordinal = match samples.by_ref().take(16).find(|&ordinal| buffers.select(ordinal)) {
                            Some(ordinal) => ordinal,
                            None => std::iter::successors(samples.next(), |&ordinal| Some(wrap(ordinal)))
                                .find(|&ordinal| buffers.select(ordinal))
                                .expect("fewer distinct ordinals requested than are live"),
                        };
                        buffers.keys.push(generator.keys().encode(ordinal));
                    }
                }
            }
            if matches!(operation, Operation::Insert | Operation::BulkLoad | Operation::Update) {
                buffers
                    .records
                    .prepare(generator, &buffers.keys, operation, snapshot.start, &mut rng)?;
            }
            if args.calls_per_transaction.is_some() && !in_transaction {
                session.transaction().begin()?;
                in_transaction = true;
            }
            let call_start = Instant::now();
            let mut storage_time = Duration::ZERO;
            let mut scan_count = 0;
            let call = (|| -> Result<usize> {
                match operation {
                    Operation::Insert | Operation::BulkLoad | Operation::Update => {
                        buffers.records.write(&mut session, &buffers.keys, operation)
                    }
                    Operation::Delete => session.delete(&buffers.keys),
                    Operation::Read => buffers.records.read(&mut session, &buffers.keys),
                    Operation::ReadModifyWrite => {
                        let before = Instant::now();
                        let result = buffers.records.read(&mut session, &buffers.keys);
                        storage_time += before.elapsed();
                        let found = result?;
                        buffers.measurements.count_corruption(buffers.records.verify(
                            generator,
                            &buffers.keys,
                            found,
                            snapshot.start,
                            args.verify == Verify::Values,
                        ))?;
                        if found != buffers.keys.len() {
                            return Ok(0);
                        }
                        buffers.records.modify(generator, &buffers.keys, snapshot.start)?;
                        let before = Instant::now();
                        let result = buffers.records.write(&mut session, &buffers.keys, Operation::Update);
                        storage_time += before.elapsed();
                        result
                    }
                    Operation::RangeRead | Operation::FullScan => {
                        let mut keys = backend::KeysOutput::new(&mut buffers.scanned);
                        let result = if operation == Operation::RangeRead {
                            if let WorkerSession::Graph(graph) = &mut session {
                                graph.expand_neighbors(buffers.keys[0], count as usize, &mut keys)
                            } else {
                                buffers
                                    .records
                                    .range_read(&mut session, buffers.keys[0], count as usize, &mut keys)
                            }
                        } else {
                            buffers
                                .records
                                .range_read(&mut session, buffers.keys[0], count as usize, &mut keys)
                        };
                        scan_count = keys.len();
                        result
                    }
                }
            })();
            let latency = if args.calls_per_second.is_some() {
                scheduled.elapsed()
            } else if operation == Operation::ReadModifyWrite {
                storage_time
            } else {
                call_start.elapsed()
            };
            buffers.measurements.record_attempt(operation, latency)?;
            let affected = call? as u64;
            if affected > count {
                return Err("backend returned more entries than requested".into());
            }
            if matches!(operation, Operation::Insert | Operation::BulkLoad) && affected != count {
                return Err("backend did not insert the complete reserved key range".into());
            }
            if operation == Operation::Read {
                buffers.measurements.count_corruption(buffers.records.verify(
                    generator,
                    &buffers.keys,
                    affected as usize,
                    snapshot.start,
                    args.verify == Verify::Values,
                ))?;
            }
            if matches!(operation, Operation::RangeRead | Operation::FullScan) {
                let keys = &buffers.scanned[..scan_count];
                if scan_count != affected as usize {
                    return Err("range key count differs from affected count".into());
                }
                if args.data_model == DataModel::Graph && operation == Operation::RangeRead {
                    for (index, key) in keys.iter().enumerate() {
                        if *key == buffers.keys[0] || keys[..index].contains(key) {
                            return Err("invalid graph expansion".into());
                        }
                    }
                } else {
                    if keys.windows(2).any(|pair| pair[0] >= pair[1])
                        || keys.first().is_some_and(|key| *key < buffers.keys[0])
                    {
                        return Err("range keys must be unique, ascending, and at least start".into());
                    }
                    if full_scan
                        && keys.iter().enumerate().any(|(index, &key)| {
                            generator
                                .keys()
                                .decode(key)
                                .is_none_or(|ordinal| ordinal != scan_start + index as u64 || ordinal >= scan_end)
                        })
                    {
                        return Err("full scan crossed its shard or skipped an existing key".into());
                    }
                    buffers.measurements.count_corruption(buffers.records.verify(
                        generator,
                        keys,
                        affected as usize,
                        snapshot.start,
                        args.verify == Verify::Values,
                    ))?;
                }
                if full_scan {
                    if affected != count {
                        return Err("full scan ended before expected record count".into());
                    }
                    scan_start = scan_start.checked_add(affected).ok_or("scan key overflow")?;
                }
            }
            let bytes = if affected == 0
                || operation == Operation::Delete
                || args.data_model == DataModel::Graph && operation == Operation::RangeRead
            {
                0
            } else {
                buffers.records.bytes(operation, affected as usize)
            };
            if args.calls_per_transaction.is_some() {
                pending.entries += affected;
                pending.missing += count - affected;
                pending.bytes += bytes;
            } else {
                buffers
                    .measurements
                    .publish(start.elapsed(), affected, count - affected, bytes);
                if matches!(operation, Operation::Insert | Operation::BulkLoad) {
                    keyspace.acknowledge(thread)?;
                }
            }
            attempted += count;
            calls += 1;
            if args.calls_per_transaction.is_some() {
                transaction_calls += 1;
                if Some(transaction_calls) == args.calls_per_transaction {
                    commit(
                        &mut session,
                        keyspace,
                        thread,
                        &mut pending,
                        &mut buffers.measurements,
                        start,
                    )?;
                    in_transaction = false;
                    transaction_calls = 0;
                }
            }
        }
        if in_transaction {
            commit(
                &mut session,
                keyspace,
                thread,
                &mut pending,
                &mut buffers.measurements,
                start,
            )?;
            in_transaction = false;
        }
        Ok(())
    })();
    if let Err(error) = result {
        cancelled.store(true, Ordering::Relaxed);
        if in_transaction {
            let _ = session.transaction().rollback();
            buffers.measurements.aborted += pending.entries;
        }
        buffers.measurements.failed += 1;
        buffers.measurements.error = Some(error);
    }
    buffers.measurements.elapsed = start.elapsed();
    if let Some(counters) = performance_counters {
        match counters.finish() {
            Ok(sample) => buffers.measurements.hardware_counters = Some(sample),
            Err(error) => {
                cancelled.store(true, Ordering::Relaxed);
                buffers.measurements.failed += 1;
                if buffers.measurements.error.is_none() {
                    buffers.measurements.error = Some(error.to_string());
                }
            }
        }
    }
    buffers.measurements
}
fn commit(
    session: &mut WorkerSession<'_>,
    keyspace: &KeySpace,
    thread: usize,
    pending: &mut PendingCounts,
    measurements: &mut WorkloadMeasurements,
    start: Instant,
) -> Result<()> {
    let before = Instant::now();
    let result = session.transaction().commit();
    measurements.record_commit(before.elapsed())?;
    result?;
    keyspace.acknowledge(thread)?;
    measurements.publish(start.elapsed(), pending.entries, pending.missing, pending.bytes);
    *pending = PendingCounts::default();
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{atomic::AtomicUsize, Arc, Mutex},
    };

    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: CommonArgs,
    }

    #[derive(Clone, Default)]
    struct Memory {
        rows: Arc<Mutex<BTreeMap<Key, Vec<u8>>>>,
        commits: Arc<AtomicUsize>,
        fail_commit: bool,
        corrupt: bool,
        ranges: bool,
    }
    struct MemorySession<'a> {
        backend: &'a Memory,
        pending: Option<BTreeMap<Key, Option<Vec<u8>>>>,
    }
    impl Backend for Memory {
        fn metadata(&self) -> BTreeMap<String, serde_json::Value> {
            BTreeMap::new()
        }
        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities {
                ordered_ranges: self.ranges,
                transactions: true,
                ..Default::default()
            }
        }
        fn session(&self) -> Result<Box<dyn BackendSession + '_, System>> {
            Ok(Box::new_in(
                MemorySession {
                    backend: self,
                    pending: None,
                },
                System,
            ))
        }
        fn flush(&self) -> Result<()> {
            Ok(())
        }
        fn disk_bytes(&self) -> Result<u64> {
            Ok(0)
        }
    }
    impl MemorySession<'_> {
        fn get(&self, key: &Key) -> Option<Vec<u8>> {
            if let Some(value) = self.pending.as_ref().and_then(|pending| pending.get(key)) {
                return value.clone();
            }
            self.backend.rows.lock().unwrap().get(key).cloned()
        }
        fn put(&mut self, key: Key, value: Option<Vec<u8>>) {
            if let Some(pending) = &mut self.pending {
                pending.insert(key, value);
            } else if let Some(value) = value {
                self.backend.rows.lock().unwrap().insert(key, value);
            } else {
                self.backend.rows.lock().unwrap().remove(&key);
            }
        }
    }
    impl BackendSession for MemorySession<'_> {
        fn insert(&mut self, keys: &[Key], values: &backend::RecordInput<'_>) -> Result<usize> {
            for (index, &key) in keys.iter().enumerate() {
                if self.get(&key).is_some() {
                    return Err("duplicate insert".into());
                }
                self.put(key, Some(values.get(index).unwrap().to_vec()));
            }
            Ok(keys.len())
        }
        fn read(&mut self, keys: &[Key], output: &mut backend::RecordOutput<'_>) -> Result<usize> {
            output.clear();
            let mut found = 0;
            for key in keys {
                let mut value = self.get(key);
                if self.backend.corrupt {
                    if let Some(value) = &mut value {
                        value[0] ^= 1;
                    }
                }
                found += usize::from(value.is_some());
                output.push(value.as_deref())?;
            }
            Ok(found)
        }
        fn update(&mut self, keys: &[Key], values: &backend::RecordInput<'_>) -> Result<usize> {
            let mut found = 0;
            for (index, &key) in keys.iter().enumerate() {
                if self.get(&key).is_some() {
                    self.put(key, Some(values.get(index).unwrap().to_vec()));
                    found += 1;
                }
            }
            Ok(found)
        }
        fn delete(&mut self, keys: &[Key]) -> Result<usize> {
            let mut found = 0;
            for &key in keys {
                if self.get(&key).is_some() {
                    self.put(key, None);
                    found += 1;
                }
            }
            Ok(found)
        }
        fn range_read(
            &mut self,
            start: Key,
            limit: usize,
            keys: &mut backend::KeysOutput<'_>,
            output: &mut backend::RecordOutput<'_>,
        ) -> Result<usize> {
            keys.clear();
            output.clear();
            for (&key, value) in self.backend.rows.lock().unwrap().range(start..).take(limit) {
                keys.push(key)?;
                output.push(Some(value))?;
            }
            Ok(keys.len())
        }
    }
    impl backend::TransactionSession for MemorySession<'_> {
        fn begin(&mut self) -> Result<()> {
            self.pending = Some(BTreeMap::new());
            Ok(())
        }
        fn commit(&mut self) -> Result<()> {
            if self.backend.fail_commit {
                return Err("commit failed".into());
            }
            std::thread::sleep(Duration::from_millis(5));
            let pending = self.pending.take().unwrap();
            for (key, value) in pending {
                self.put(key, value);
            }
            self.backend.commits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn rollback(&mut self) -> Result<()> {
            self.pending = None;
            Ok(())
        }
    }
    fn args(extra: &[&str]) -> CommonArgs {
        let mut argv = vec!["test", "--records", "7", "--value-size", "32"];
        argv.extend_from_slice(extra);
        Cli::parse_from(argv).args
    }
    fn workloads(names: &str) -> Vec<Workload> {
        names.split(',').map(|name| name.parse().unwrap()).collect()
    }
    static KEYS: data::Keys = data::Keys::Generated(KeyType::U64);
    fn model() -> model::RecordGenerator<'static> {
        model::RecordGenerator::new(DataModel::KeyValue, &KEYS, 42, 32..=32, 7, 0).unwrap()
    }
    fn phase(
        backend: &Memory,
        args: &CommonArgs,
        workload: &str,
        keys: &KeySpace,
        model: &model::RecordGenerator,
        budget: u64,
    ) -> output::WorkloadReport {
        execute_phase(backend, args, &workload.parse().unwrap(), keys, model, budget, None).unwrap()
    }
    #[test]
    fn small_budgets_preserve_insert_uniqueness_and_scan_shards() {
        let backend = Memory {
            ranges: true,
            ..Default::default()
        };
        let args = args(&["--threads", "4"]);
        let keys = KeySpace::new(0);
        let model = model();
        let loaded = phase(&backend, &args, "bulk-load", &keys, &model, 1);
        assert_eq!((loaded.entries, loaded.failed), (7, 0));
        assert_eq!(keys.live(), 0..7);
        let read = phase(&backend, &args, "batch-read-256", &keys, &model, 2);
        assert_eq!((read.entries, read.calls), (2, 2));
        let scan = phase(&backend, &args, "full-scan", &keys, &model, 1);
        assert_eq!((scan.entries, scan.missing, scan.failed), (7, 0, 0));
    }
    #[test]
    fn transactions_publish_after_commit_and_failed_commits_publish_nothing() {
        let args = args(&["--calls-per-transaction", "2"]);
        let model = model();
        for fail_commit in [false, true] {
            let backend = Memory {
                fail_commit,
                ..Default::default()
            };
            let keys = KeySpace::new(0);
            let result = phase(&backend, &args, "bulk-load-2", &keys, &model, 7);
            if fail_commit {
                assert_eq!((result.entries, result.failed, result.aborted), (0, 1, 4));
                assert_eq!(result.calls, 3);
                assert_eq!(result.latency["commit"].count, 1);
                assert_eq!(result.latency["bulk-load"].count, 2);
                assert_eq!(keys.live(), 0..0);
                assert!(backend.rows.lock().unwrap().is_empty());
            } else {
                assert_eq!(result.entries, 7);
                assert_eq!(backend.commits.load(Ordering::Relaxed), 2);
                assert_eq!(result.latency["commit"].count, 2);
                assert!(result.elapsed_seconds >= 0.010);
                assert_eq!(keys.live(), 0..7);
            }
        }
    }
    #[test]
    fn read_modify_write_increments_observed_version_and_detects_corruption() {
        let mut backend = Memory::default();
        let args = args(&[]);
        let keys = KeySpace::new(0);
        let model = model();
        phase(&backend, &args, "bulk-load", &keys, &model, 7);
        let mut workload: Workload = "read-50-read-modify-write-50".parse().unwrap();
        workload.operations = vec![(Operation::ReadModifyWrite, 100)];
        let result = execute_phase(&backend, &args, &workload, &keys, &model, 1, None).unwrap();
        assert_eq!(result.entries, 1);
        let versions: u64 = backend
            .rows
            .lock()
            .unwrap()
            .values()
            .map(|value| u64::from_be_bytes(value[16..24].try_into().unwrap()))
            .sum();
        assert_eq!(versions, 1);
        backend.corrupt = true;
        let bad = phase(&backend, &args, "read", &keys, &model, 1);
        assert_eq!((bad.failed, bad.corrupted), (1, 1));
    }
    #[test]
    fn resumes_live_key_range_and_refuses_incomplete_mutations_or_different_backend_options() {
        let temp = tempfile::tempdir().unwrap();
        let mut backend = Memory {
            ranges: true,
            ..Default::default()
        };
        let mut args = args(&[
            "--workloads",
            "bulk-load,batch-insert-1,delete-oldest",
            "--entries",
            "2",
        ]);
        args.data_dir = temp.path().join("data");
        args.output = temp.path().join("results");
        run(args.clone(), (), &[], |_, _| Ok(Box::new_in(backend.clone(), System))).unwrap();
        let first = *backend.rows.lock().unwrap().keys().next().unwrap();
        assert_eq!(args.key_type.decode(first), Some(2));
        args.workloads = workloads("full-scan,batch-insert-1");
        run(args.clone(), (), &[], |_, _| Ok(Box::new_in(backend.clone(), System))).unwrap();
        assert_eq!(backend.rows.lock().unwrap().len(), 9);
        assert!(run(args.clone(), "different-server", &[], |_, _| Ok(Box::new_in(
            backend.clone(),
            System
        )))
        .unwrap_err()
        .contains("no existing"));
        args.workloads = workloads("batch-insert-1");
        args.calls_per_transaction = Some(1);
        backend.fail_commit = true;
        assert!(run(args.clone(), (), &[], |_, _| Ok(Box::new_in(backend.clone(), System))).is_err());
        args.workloads = workloads("read");
        assert!(run(args, (), &[], |_, _| Ok(Box::new_in(backend.clone(), System)))
            .unwrap_err()
            .contains("interrupted"));
    }

    #[test]
    fn skipped_ranges_do_not_stop_the_chain_and_rate_respects_time_limit() {
        let temp = tempfile::tempdir().unwrap();
        let backend = Memory::default();
        let mut args = args(&[
            "--workloads",
            "bulk-load,range-read-256,batch-insert-1",
            "--entries",
            "1",
        ]);
        args.data_dir = temp.path().join("data");
        args.output = temp.path().join("results");
        run(args.clone(), (), &[], |_, _| Ok(Box::new_in(backend.clone(), System))).unwrap();
        assert_eq!(backend.rows.lock().unwrap().len(), 8);
        args.calls_per_second = Some(0.01);
        let keys = KeySpace::new(8);
        let started = Instant::now();
        let result = execute_phase(
            &backend,
            &args,
            &"read".parse().unwrap(),
            &keys,
            &model(),
            100,
            Some(Duration::from_millis(20)),
        )
        .unwrap();
        assert_eq!(result.calls, 1);
        assert!(result.elapsed_seconds >= 0.020);
        assert!(result.throughput <= 50.0);
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
