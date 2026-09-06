//! Dual-write and shadow-read (#458 slices 3 and 4): two backends, one Kafka
//! name, and a receipt that says what each side stored.
//!
//! A produce is written to the primary first. If that fails, the shadow is
//! not touched and the client sees the primary's code. If the primary lands
//! and the shadow does not, the client is told `KAFKA_STORAGE_ERROR` — not
//! an ack for a write that only one side has — and the receipt records the
//! partial so an operator can see which. An idempotent retry then appends
//! nothing on the primary.
//!
//! A shadow-read fetch serves the primary and compares bytes with the
//! shadow at the translated offset. A mismatch is `CORRUPT_MESSAGE`, never a
//! silent divergence. Offset translation is the receipt's: which primary
//! offset a shadow offset became, bound to a hash of the records.

use crate::bridge::{Appended, Bridge, Fetched, Sequenced};
use crate::messages::ErrorCode;
use crate::offsets::{Committed, OffsetStore};
use crate::records::RecordBatch;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Where a dual topic's reads are served from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DualRead {
    /// Serve the primary; still write both.
    Primary,
    /// Serve the shadow (the external copy still being the source of truth
    /// for reads during a migration the other way).
    Shadow,
    /// Serve the primary and compare bytes with the shadow.
    Compare,
}

/// One produce that hit both backends, or only the primary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub topic: String,
    pub kind: ReceiptKind,
    pub primary_offset: i64,
    pub shadow_offset: Option<i64>,
    pub records: i64,
    pub sha256: [u8; 32],
    /// Set on [`ReceiptKind::NativeCursor`]: the group whose cursor is native.
    pub group: Option<String>,
    /// Set on [`ReceiptKind::NativeCursor`].
    pub partition: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptKind {
    /// Both backends appended the same bytes.
    DualWrite,
    /// The primary appended and the shadow refused; the client was told.
    PrimaryOnly,
    /// A shadow-read served identical bytes.
    ShadowMatch,
    /// A shadow-read saw different bytes; the client was told.
    ShadowMismatch,
    /// After cutover, this group's cursor is native numbering and must not
    /// be translated again.
    NativeCursor,
}

impl ReceiptKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::DualWrite => "dual_write",
            Self::PrimaryOnly => "primary_only",
            Self::ShadowMatch => "shadow_match",
            Self::ShadowMismatch => "shadow_mismatch",
            Self::NativeCursor => "native_cursor",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "dual_write" => Some(Self::DualWrite),
            "primary_only" => Some(Self::PrimaryOnly),
            "shadow_match" => Some(Self::ShadowMatch),
            "shadow_mismatch" => Some(Self::ShadowMismatch),
            "native_cursor" => Some(Self::NativeCursor),
            _ => None,
        }
    }
}

/// Inclusive of the exclusive end: Kafka commits the next offset to consume.
fn receipt_covers(start: i64, records: i64, offset: i64) -> bool {
    offset >= start && offset <= start.saturating_add(records)
}

/// The log of translations and comparisons (#458 slice 4). Memory always;
/// a path, when set, appends JSONL an operator prints with `vtopctl receipt`.
pub struct ReceiptLog {
    rows: Mutex<Vec<Receipt>>,
    path: Option<PathBuf>,
}

impl ReceiptLog {
    pub fn memory() -> Arc<Self> {
        Arc::new(Self {
            rows: Mutex::new(Vec::new()),
            path: None,
        })
    }

    pub fn create(path: impl AsRef<Path>) -> std::io::Result<Arc<Self>> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        // A restart must still know which native offset a Kafka offset
        // became (#458): the file is the log, so it is loaded before any
        // new row is appended. A corrupt line is a startup refusal, not a
        // silent empty translation.
        let rows = if path.exists() {
            let text = std::fs::read_to_string(&path)?;
            load_receipts_jsonl(&text).map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{}: {error}", path.display()),
                )
            })?
        } else {
            Vec::new()
        };
        Ok(Arc::new(Self {
            rows: Mutex::new(rows),
            path: Some(path),
        }))
    }

    /// Append the row. A durable log that cannot be written is an error:
    /// DualBridge must not ack a write whose translation would vanish on
    /// restart.
    pub fn record(&self, receipt: Receipt) -> Result<(), ErrorCode> {
        if let Some(path) = &self.path {
            if let Err(error) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .and_then(|mut file| {
                    use std::io::Write;
                    writeln!(file, "{}", receipt_json(&receipt))?;
                    file.sync_all()
                })
            {
                tracing::error!(
                    path = %path.display(),
                    error = %error,
                    "kafka receipt: the JSONL file could not be appended; the write is not acknowledged"
                );
                return Err(ErrorCode::KafkaStorageError);
            }
        }
        self.rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(receipt);
        Ok(())
    }

    pub fn rows(&self) -> Vec<Receipt> {
        self.rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// The primary offset a shadow offset became, if a dual-write covered it.
    /// The exclusive end (`start + records`) is included: Kafka commits the
    /// next offset to consume, so a consumer that finished the receipt
    /// commits that cursor, not one inside the batch.
    pub fn to_primary(&self, topic: &str, shadow_offset: i64) -> Option<i64> {
        self.rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .rev()
            .find(|row| {
                row.topic == topic
                    && row.kind == ReceiptKind::DualWrite
                    && row
                        .shadow_offset
                        .is_some_and(|start| receipt_covers(start, row.records, shadow_offset))
            })
            .map(|row| {
                let delta = shadow_offset - row.shadow_offset.unwrap();
                row.primary_offset + delta
            })
    }

    /// The shadow offset a primary offset became.
    pub fn to_shadow(&self, topic: &str, primary_offset: i64) -> Option<i64> {
        self.rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .rev()
            .find(|row| {
                row.topic == topic
                    && row.kind == ReceiptKind::DualWrite
                    && receipt_covers(row.primary_offset, row.records, primary_offset)
            })
            .and_then(|row| {
                let delta = primary_offset - row.primary_offset;
                row.shadow_offset.map(|start| start + delta)
            })
    }

    /// Print the log as JSONL, the same bytes `vtopctl receipt` shows.
    pub fn to_jsonl(&self) -> String {
        self.rows()
            .into_iter()
            .map(|row| receipt_json(&row))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Load a receipt file written by the gateway: one JSON object per line.
pub fn load_receipts_jsonl(text: &str) -> Result<Vec<Receipt>, String> {
    let mut rows = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        rows.push(parse_receipt(line).map_err(|error| format!("receipt line {}: {error}", i + 1))?);
    }
    Ok(rows)
}

fn parse_receipt(line: &str) -> Result<Receipt, String> {
    let topic = json_field(line, "topic")?;
    let kind = ReceiptKind::parse(&json_field(line, "kind")?)
        .ok_or_else(|| format!("unknown kind in {line}"))?;
    let primary_offset = json_i64(line, "primary_offset")?;
    let shadow_offset = match json_field(line, "shadow_offset") {
        Ok(s) if s == "null" => None,
        Ok(s) => Some(
            s.parse::<i64>()
                .map_err(|_| format!("shadow_offset not an integer: {s}"))?,
        ),
        Err(_) => None,
    };
    let records = json_i64(line, "records")?;
    let sha = json_field(line, "sha256")?;
    let sha256 = parse_sha256(&sha)?;
    Ok(Receipt {
        topic,
        kind,
        primary_offset,
        shadow_offset,
        records,
        sha256,
        group: json_field(line, "group").ok().filter(|s| s != "null"),
        partition: json_field(line, "partition")
            .ok()
            .filter(|s| s != "null")
            .and_then(|s| s.parse().ok()),
    })
}

fn json_field(line: &str, key: &str) -> Result<String, String> {
    let needle = format!("\"{key}\":");
    let rest = line
        .split_once(&needle)
        .map(|(_, rest)| rest.trim_start())
        .ok_or_else(|| format!("missing {key}"))?;
    if let Some(inner) = rest.strip_prefix('"') {
        let mut out = String::new();
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            match c {
                '"' => return Ok(out),
                '\\' => match chars.next() {
                    Some('"') => out.push('"'),
                    Some('\\') => out.push('\\'),
                    Some('n') => out.push('\n'),
                    Some('t') => out.push('\t'),
                    Some('r') => out.push('\r'),
                    Some('b') => out.push('\u{8}'),
                    Some('f') => out.push('\u{c}'),
                    Some('u') => {
                        let hex: String = chars.by_ref().take(4).collect();
                        if hex.len() != 4 {
                            return Err(format!("truncated \\u escape for {key}"));
                        }
                        let code = u32::from_str_radix(&hex, 16)
                            .map_err(|_| format!("bad \\u escape for {key}"))?;
                        out.push(
                            char::from_u32(code)
                                .ok_or_else(|| format!("bad \\u escape for {key}"))?,
                        );
                    }
                    Some(other) => out.push(other),
                    None => break,
                },
                other => out.push(other),
            }
        }
        Err(format!("unterminated string for {key}"))
    } else {
        let end = rest
            .find([',', '}'])
            .ok_or_else(|| format!("truncated {key}"))?;
        Ok(rest[..end].trim().to_owned())
    }
}

fn json_i64(line: &str, key: &str) -> Result<i64, String> {
    json_field(line, key)?
        .parse()
        .map_err(|_| format!("{key} is not an integer"))
}

fn parse_sha256(hex: &str) -> Result<[u8; 32], String> {
    if hex.len() != 64 {
        return Err(format!("sha256 is {} hex digits, not 64", hex.len()));
    }
    let mut out = [0_u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).map_err(|_| "sha256 is not utf-8")?;
        out[i] = u8::from_str_radix(s, 16).map_err(|_| format!("bad sha256 hex: {s}"))?;
    }
    Ok(out)
}

fn receipt_json(row: &Receipt) -> String {
    let shadow = match row.shadow_offset {
        Some(offset) => offset.to_string(),
        None => "null".to_owned(),
    };
    let mut json = format!(
        "{{\"topic\":{},\"kind\":{},\"primary_offset\":{},\"shadow_offset\":{shadow},\"records\":{},\"sha256\":{}}}",
        json_escape(&row.topic),
        json_escape(row.kind.as_str()),
        row.primary_offset,
        row.records,
        json_escape(&hex_encode(&row.sha256)),
    );
    if let (Some(group), Some(partition)) = (&row.group, row.partition) {
        json.pop();
        json.push_str(&format!(
            ",\"group\":{},\"partition\":{partition}}}",
            json_escape(group)
        ));
    }
    json
}

fn json_escape(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

pub fn payload_hash(batches: &[RecordBatch]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for batch in batches {
        for record in &batch.records {
            hasher.update(record.key.as_deref().unwrap_or(&[]));
            hasher.update([0]);
            hasher.update(record.value.as_deref().unwrap_or(&[]));
            hasher.update([0]);
        }
    }
    hasher.finalize().into()
}

fn records_hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Two backends under one Kafka name.
pub struct DualBridge {
    kafka_topic: String,
    primary: Arc<dyn Bridge>,
    primary_topic: String,
    shadow: Arc<dyn Bridge>,
    shadow_topic: String,
    read: DualRead,
    receipts: Arc<ReceiptLog>,
    /// Primary-then-shadow is one produce: concurrent callers must not
    /// interleave the two appends, or a receipt would pair the wrong offsets.
    write: Mutex<()>,
}

impl DualBridge {
    pub fn new(
        kafka_topic: String,
        primary: Arc<dyn Bridge>,
        primary_topic: String,
        shadow: Arc<dyn Bridge>,
        shadow_topic: String,
        read: DualRead,
        receipts: Arc<ReceiptLog>,
    ) -> Self {
        Self {
            kafka_topic,
            primary,
            primary_topic,
            shadow,
            shadow_topic,
            read,
            receipts,
            write: Mutex::new(()),
        }
    }

    pub fn receipts(&self) -> Arc<ReceiptLog> {
        Arc::clone(&self.receipts)
    }
}

impl Bridge for DualBridge {
    fn topics(&self) -> Vec<String> {
        vec![self.kafka_topic.clone()]
    }

    fn produce(
        &self,
        topic: &str,
        batches: &[RecordBatch],
        sequenced: Option<Sequenced>,
    ) -> Result<Appended, ErrorCode> {
        if topic != self.kafka_topic {
            return Err(ErrorCode::UnknownTopicOrPartition);
        }
        let _write = self
            .write
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let hash = payload_hash(batches);
        let records: i64 = batches.iter().map(|b| b.records.len() as i64).sum();
        let primary = self
            .primary
            .produce(&self.primary_topic, batches, sequenced)?;
        match self.shadow.produce(&self.shadow_topic, batches, sequenced) {
            Ok(shadow) => {
                self.receipts.record(Receipt {
                    topic: self.kafka_topic.clone(),
                    kind: ReceiptKind::DualWrite,
                    primary_offset: primary.base_offset,
                    shadow_offset: Some(shadow.base_offset),
                    records,
                    sha256: hash,
                    group: None,
                    partition: None,
                })?;
                Ok(primary)
            }
            Err(error) => {
                tracing::error!(
                    topic,
                    primary_offset = primary.base_offset,
                    shadow = %error.as_i16(),
                    "kafka dual-write: the primary appended and the shadow refused; the client is \
                     told KAFKA_STORAGE_ERROR so it does not treat a one-sided write as durable"
                );
                self.receipts.record(Receipt {
                    topic: self.kafka_topic.clone(),
                    kind: ReceiptKind::PrimaryOnly,
                    primary_offset: primary.base_offset,
                    shadow_offset: None,
                    records,
                    sha256: hash,
                    group: None,
                    partition: None,
                })?;
                Err(ErrorCode::KafkaStorageError)
            }
        }
    }

    fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
        if topic != self.kafka_topic {
            return Err(ErrorCode::UnknownTopicOrPartition);
        }
        match self.read {
            DualRead::Primary => self.primary.fetch(&self.primary_topic, offset, max_bytes),
            // Bounds for this role are the shadow's, so the client already
            // speaks shadow numbering. Translating as if it had sent a
            // primary offset would skip or replay. Compare is the path
            // that translates: it advertises the primary and looks the
            // shadow up from the receipt.
            DualRead::Shadow => self.shadow.fetch(&self.shadow_topic, offset, max_bytes),
            DualRead::Compare => {
                let primary = self.primary.fetch(&self.primary_topic, offset, max_bytes)?;
                let shadow_offset = self
                    .receipts
                    .to_shadow(&self.kafka_topic, offset)
                    .unwrap_or(offset);
                let shadow = self
                    .shadow
                    .fetch(&self.shadow_topic, shadow_offset, max_bytes)
                    .map_err(|error| {
                        tracing::error!(
                            topic,
                            offset,
                            shadow_offset,
                            code = error.as_i16(),
                            "kafka shadow-read: the shadow could not be compared; the primary is \
                             not served alone"
                        );
                        ErrorCode::KafkaStorageError
                    })?;
                let hash = records_hash(&primary.records);
                if payload_bytes_equal(&primary.records, &shadow.records) {
                    self.receipts.record(Receipt {
                        topic: self.kafka_topic.clone(),
                        kind: ReceiptKind::ShadowMatch,
                        primary_offset: offset,
                        shadow_offset: Some(shadow_offset),
                        records: 0,
                        sha256: hash,
                        group: None,
                        partition: None,
                    })?;
                    Ok(primary)
                } else {
                    tracing::error!(
                        topic,
                        offset,
                        shadow_offset,
                        primary_bytes = primary.records.len(),
                        shadow_bytes = shadow.records.len(),
                        "kafka shadow-read: the two backends served different bytes"
                    );
                    self.receipts.record(Receipt {
                        topic: self.kafka_topic.clone(),
                        kind: ReceiptKind::ShadowMismatch,
                        primary_offset: offset,
                        shadow_offset: Some(shadow_offset),
                        records: 0,
                        sha256: hash,
                        group: None,
                        partition: None,
                    })?;
                    Err(ErrorCode::CorruptMessage)
                }
            }
        }
    }

    fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
        if topic != self.kafka_topic {
            return Err(ErrorCode::UnknownTopicOrPartition);
        }
        match self.read {
            DualRead::Shadow => self.shadow.bounds(&self.shadow_topic),
            DualRead::Primary | DualRead::Compare => self.primary.bounds(&self.primary_topic),
        }
    }
}

/// Encoded batches compared by the records they carry, not the offsets the
/// backends stamped: a dual-write of the same produce lands at different
/// base offsets, and a byte-exact compare of the Fetch payload would then
/// always mismatch. Keys, timestamps and headers are compared too — a
/// matching value with a rewritten key is still a divergence.
fn payload_bytes_equal(a: &[u8], b: &[u8]) -> bool {
    record_payloads(a) == record_payloads(b)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ComparedRecord {
    timestamp_millis: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<(String, Option<Vec<u8>>)>,
}

fn record_payloads(encoded: &[u8]) -> Vec<ComparedRecord> {
    decode_batches(encoded)
        .into_iter()
        .flat_map(|batch| batch.records)
        .map(|record| ComparedRecord {
            timestamp_millis: record.timestamp_millis,
            key: record.key,
            value: record.value,
            headers: record.headers,
        })
        .collect()
}

#[cfg(test)]
fn record_values(encoded: &[u8]) -> Vec<Vec<u8>> {
    decode_batches(encoded)
        .into_iter()
        .flat_map(|batch| batch.records)
        .map(|record| record.value.unwrap_or_default())
        .collect()
}

fn decode_batches(mut encoded: &[u8]) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    while encoded.len() >= 12 {
        let len = i32::from_be_bytes(encoded[8..12].try_into().unwrap()) as usize + 12;
        if len > encoded.len() {
            break;
        }
        match RecordBatch::decode(&encoded[..len]) {
            Ok(batch) => out.push(batch),
            Err(_) => break,
        }
        encoded = &encoded[len..];
    }
    out
}

/// After a cutover, OffsetFetch answers the native (primary) offset a
/// previously committed shadow offset became. Commits that arrive after
/// wrapping are native — Fetch already advertised native numbering — so
/// they are stored as-is and never translated again. A fetch of a cursor
/// that has not been rewritten still goes through the receipt.
pub struct CutoverStore {
    inner: Arc<dyn OffsetStore>,
    /// Topics whose committed offsets were recorded against the shadow
    /// numbering, and the receipt log that translates them.
    by_topic: Mutex<HashMap<String, Arc<ReceiptLog>>>,
    /// `(group, topic, partition)` committed or migrated after wrapping:
    /// those offsets are already native and must not run through
    /// `to_primary` again, even when they numerically overlap a historical
    /// shadow range. Reloaded from `NativeCursor` receipts on restart.
    native: Mutex<HashSet<(String, String, i32)>>,
    /// Commits and fetch-migrations take turns so a translation write-back
    /// cannot rewind a concurrent native commit.
    order: tokio::sync::Mutex<()>,
}

impl CutoverStore {
    pub fn new(inner: Arc<dyn OffsetStore>) -> Self {
        Self {
            inner,
            by_topic: Mutex::new(HashMap::new()),
            native: Mutex::new(HashSet::new()),
            order: tokio::sync::Mutex::new(()),
        }
    }

    pub fn cut_over(&self, topic: &str, receipts: Arc<ReceiptLog>) {
        self.by_topic
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(topic.to_owned(), receipts);
    }

    /// Wrap `inner` so OffsetFetch of these topics is translated. No
    /// topics is `inner` itself — a deployment that never cut over must
    /// not pay a translation that would rewrite native offsets.
    pub fn wrapping(
        inner: Arc<dyn OffsetStore>,
        topics: Vec<(String, Arc<ReceiptLog>)>,
    ) -> Arc<dyn OffsetStore> {
        if topics.is_empty() {
            return inner;
        }
        let store = Self::new(inner);
        for (topic, receipts) in topics {
            for row in receipts.rows() {
                if row.kind == ReceiptKind::NativeCursor {
                    if let (Some(group), Some(partition)) = (row.group, row.partition) {
                        store.remember_native(&group, &topic, partition);
                    }
                }
            }
            store.cut_over(&topic, receipts);
        }
        Arc::new(store)
    }

    fn translate(&self, topic: &str, offset: i64) -> i64 {
        let by_topic = self
            .by_topic
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match by_topic.get(topic) {
            Some(receipts) => receipts.to_primary(topic, offset).unwrap_or(offset),
            None => offset,
        }
    }

    fn remember_native(&self, group: &str, topic: &str, partition: i32) -> bool {
        self.native
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert((group.to_owned(), topic.to_owned(), partition))
    }

    fn mark_native(&self, group: &str, topic: &str, partition: i32) {
        if !self.remember_native(group, topic, partition) {
            return;
        }
        let log = {
            let by_topic = self
                .by_topic
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            by_topic.get(topic).map(Arc::clone)
        };
        if let Some(log) = log {
            if let Err(error) = log.record(Receipt {
                topic: topic.to_owned(),
                kind: ReceiptKind::NativeCursor,
                primary_offset: 0,
                shadow_offset: None,
                records: 0,
                sha256: [0; 32],
                group: Some(group.to_owned()),
                partition: Some(partition),
            }) {
                tracing::error!(
                    group,
                    topic,
                    partition,
                    code = error.as_i16(),
                    "kafka cutover: the native-cursor marker could not be appended; a restart may re-translate this group"
                );
            }
        }
    }

    fn is_native(&self, group: &str, topic: &str, partition: i32) -> bool {
        self.native
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(&(group.to_owned(), topic.to_owned(), partition))
    }
}

#[async_trait::async_trait]
impl OffsetStore for CutoverStore {
    async fn commit(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
        committed: Committed,
    ) -> Result<(), ErrorCode> {
        let _order = self.order.lock().await;
        self.inner
            .commit(group, topic, partition, committed)
            .await?;
        // After cutover, Fetch served native offsets, so this commit is
        // native numbering. Remember that so a later fetch does not treat
        // it as a leftover shadow cursor.
        self.mark_native(group, topic, partition);
        Ok(())
    }

    async fn fetch(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<Committed>, ErrorCode> {
        let _order = self.order.lock().await;
        let Some(mut committed) = self.inner.fetch(group, topic, partition).await? else {
            return Ok(None);
        };
        if self.is_native(group, topic, partition) {
            return Ok(Some(committed));
        }
        let translated = self.translate(topic, committed.offset);
        if translated != committed.offset {
            let migrated = Committed {
                offset: translated,
                metadata: committed.metadata.clone(),
            };
            self.inner
                .commit(group, topic, partition, migrated.clone())
                .await?;
            self.mark_native(group, topic, partition);
            return Ok(Some(migrated));
        }
        committed.offset = translated;
        Ok(Some(committed))
    }

    async fn committed(
        &self,
        group: &str,
        at_most: usize,
    ) -> Result<Vec<(String, i32, Committed)>, ErrorCode> {
        let _order = self.order.lock().await;
        let mut rows = self.inner.committed(group, at_most).await?;
        for (topic, partition, committed) in rows.iter_mut() {
            if self.is_native(group, topic, *partition) {
                continue;
            }
            let translated = self.translate(topic, committed.offset);
            if translated != committed.offset {
                let migrated = Committed {
                    offset: translated,
                    metadata: committed.metadata.clone(),
                };
                self.inner
                    .commit(group, topic, *partition, migrated.clone())
                    .await?;
                self.mark_native(group, topic, *partition);
                *committed = migrated;
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::MemoryBridge;
    use crate::offsets::{MemoryOffsetStore, OffsetStore};
    use crate::records::Record;

    fn batch(values: &[&str]) -> RecordBatch {
        RecordBatch {
            base_offset: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: values
                .iter()
                .enumerate()
                .map(|(i, v)| Record {
                    offset: i as i64,
                    timestamp_millis: 1,
                    key: None,
                    value: Some(v.as_bytes().to_vec()),
                    headers: Vec::new(),
                })
                .collect(),
        }
    }

    struct RefuseShadow(Arc<MemoryBridge>);
    impl Bridge for RefuseShadow {
        fn topics(&self) -> Vec<String> {
            self.0.topics()
        }
        fn produce(
            &self,
            _topic: &str,
            _batches: &[RecordBatch],
            _sequenced: Option<Sequenced>,
        ) -> Result<Appended, ErrorCode> {
            Err(ErrorCode::BrokerNotAvailable)
        }
        fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
            self.0.fetch(topic, offset, max_bytes)
        }
        fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
            self.0.bounds(topic)
        }
    }

    #[test]
    fn dual_write_lands_on_both_and_the_receipt_translates() {
        let primary = Arc::new(MemoryBridge::with_topics(["native"]));
        let shadow = Arc::new(MemoryBridge::with_topics(["kafka"]));
        let receipts = ReceiptLog::memory();
        let dual = DualBridge::new(
            "events".to_owned(),
            Arc::clone(&primary) as Arc<dyn Bridge>,
            "native".to_owned(),
            Arc::clone(&shadow) as Arc<dyn Bridge>,
            "kafka".to_owned(),
            DualRead::Primary,
            Arc::clone(&receipts),
        );
        let appended = dual.produce("events", &[batch(&["a", "b"])], None).unwrap();
        assert_eq!(appended.base_offset, 0);
        assert_eq!(primary.bounds("native"), Ok((0, 2)));
        assert_eq!(shadow.bounds("kafka"), Ok((0, 2)));
        assert_eq!(receipts.to_primary("events", 1), Some(1));
        assert_eq!(receipts.to_shadow("events", 0), Some(0));
        assert_eq!(
            dual.produce("nope", &[batch(&["x"])], None),
            Err(ErrorCode::UnknownTopicOrPartition)
        );
    }

    #[test]
    fn a_shadow_failure_after_a_primary_append_is_storage_error_and_primary_only() {
        let primary = Arc::new(MemoryBridge::with_topics(["native"]));
        let shadow = Arc::new(RefuseShadow(Arc::new(MemoryBridge::with_topics(["kafka"]))));
        let receipts = ReceiptLog::memory();
        let dual = DualBridge::new(
            "events".to_owned(),
            Arc::clone(&primary) as Arc<dyn Bridge>,
            "native".to_owned(),
            shadow,
            "kafka".to_owned(),
            DualRead::Primary,
            Arc::clone(&receipts),
        );
        assert_eq!(
            dual.produce("events", &[batch(&["a"])], None),
            Err(ErrorCode::KafkaStorageError)
        );
        assert_eq!(
            primary.bounds("native"),
            Ok((0, 1)),
            "primary kept the write"
        );
        let rows = receipts.rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, ReceiptKind::PrimaryOnly);
        assert_eq!(rows[0].shadow_offset, None);
    }

    #[test]
    fn shadow_read_serves_the_native_copy_and_names_a_mismatch() {
        let primary = Arc::new(MemoryBridge::with_topics(["native"]));
        let shadow = Arc::new(MemoryBridge::with_topics(["kafka"]));
        let receipts = ReceiptLog::memory();
        let dual = DualBridge::new(
            "events".to_owned(),
            Arc::clone(&primary) as Arc<dyn Bridge>,
            "native".to_owned(),
            Arc::clone(&shadow) as Arc<dyn Bridge>,
            "kafka".to_owned(),
            DualRead::Compare,
            Arc::clone(&receipts),
        );
        dual.produce("events", &[batch(&["a", "b"])], None).unwrap();
        let fetched = dual.fetch("events", 0, 1 << 20).unwrap();
        assert!(!fetched.records.is_empty());
        assert!(receipts
            .rows()
            .iter()
            .any(|row| row.kind == ReceiptKind::ShadowMatch));

        // Poison the shadow with other bytes.
        shadow.produce("kafka", &[batch(&["NO"])], None).unwrap();
        assert_eq!(
            dual.fetch("events", 2, 1 << 20),
            Err(ErrorCode::CorruptMessage)
        );
    }

    struct RefuseFetch(Arc<MemoryBridge>);
    impl Bridge for RefuseFetch {
        fn topics(&self) -> Vec<String> {
            self.0.topics()
        }
        fn produce(
            &self,
            topic: &str,
            batches: &[RecordBatch],
            sequenced: Option<Sequenced>,
        ) -> Result<Appended, ErrorCode> {
            self.0.produce(topic, batches, sequenced)
        }
        fn fetch(
            &self,
            _topic: &str,
            _offset: i64,
            _max_bytes: usize,
        ) -> Result<Fetched, ErrorCode> {
            Err(ErrorCode::BrokerNotAvailable)
        }
        fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
            self.0.bounds(topic)
        }
    }

    #[test]
    fn a_shadow_that_cannot_be_compared_is_storage_error_not_a_one_sided_read() {
        let primary = Arc::new(MemoryBridge::with_topics(["native"]));
        let shadow = Arc::new(RefuseFetch(Arc::new(MemoryBridge::with_topics(["kafka"]))));
        let dual = DualBridge::new(
            "events".to_owned(),
            Arc::clone(&primary) as Arc<dyn Bridge>,
            "native".to_owned(),
            shadow,
            "kafka".to_owned(),
            DualRead::Compare,
            ReceiptLog::memory(),
        );
        dual.produce("events", &[batch(&["a"])], None).unwrap();
        assert_eq!(
            dual.fetch("events", 0, 1 << 20),
            Err(ErrorCode::KafkaStorageError)
        );
    }

    #[tokio::test]
    async fn a_cutover_translates_a_committed_shadow_offset_to_native() {
        let primary = Arc::new(MemoryBridge::with_topics(["native"]));
        let shadow = Arc::new(MemoryBridge::with_topics(["kafka"]));
        // Give the shadow a head start so the offsets differ.
        shadow.produce("kafka", &[batch(&["pad"])], None).unwrap();
        let receipts = ReceiptLog::memory();
        let dual = DualBridge::new(
            "events".to_owned(),
            Arc::clone(&primary) as Arc<dyn Bridge>,
            "native".to_owned(),
            Arc::clone(&shadow) as Arc<dyn Bridge>,
            "kafka".to_owned(),
            DualRead::Primary,
            Arc::clone(&receipts),
        );
        dual.produce("events", &[batch(&["a", "b"])], None).unwrap();
        // Primary at 0, shadow at 1 (pad occupied 0).
        assert_eq!(receipts.to_primary("events", 1), Some(0));
        let inner = Arc::new(MemoryOffsetStore::default());
        inner
            .commit(
                "g",
                "events",
                0,
                Committed {
                    offset: 1,
                    metadata: None,
                },
            )
            .await
            .unwrap();
        let store = CutoverStore::new(inner);
        assert_eq!(
            store.fetch("g", "events", 0).await.unwrap().unwrap().offset,
            1,
            "before cutover the committed shadow offset stands"
        );
        store.cut_over("events", receipts);
        assert_eq!(
            store.fetch("g", "events", 0).await.unwrap().unwrap().offset,
            0,
            "after cutover the consumer resumes at the native offset"
        );
        assert_eq!(store.committed("g", 8).await.unwrap()[0].2.offset, 0);
        store
            .commit(
                "g",
                "events",
                0,
                Committed {
                    offset: 1,
                    metadata: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            store.fetch("g", "events", 0).await.unwrap().unwrap().offset,
            1,
            "a native commit after cutover is not translated again even if it overlaps a shadow range"
        );
    }

    #[test]
    fn jsonl_round_trips_and_rejects_a_bad_line() {
        let row = Receipt {
            topic: "events".to_owned(),
            kind: ReceiptKind::DualWrite,
            primary_offset: 4,
            shadow_offset: Some(9),
            records: 2,
            sha256: [0xab; 32],
            group: None,
            partition: None,
        };
        let line = receipt_json(&row);
        let parsed = load_receipts_jsonl(&format!("{line}\n")).unwrap();
        assert_eq!(parsed, vec![row]);
        assert!(load_receipts_jsonl("{\"topic\":\"x\"}").is_err());
        let tabbed = Receipt {
            topic: "ev\tents".to_owned(),
            kind: ReceiptKind::DualWrite,
            primary_offset: 0,
            shadow_offset: Some(1),
            records: 1,
            sha256: [0; 32],
            group: None,
            partition: None,
        };
        let line = receipt_json(&tabbed);
        assert_eq!(load_receipts_jsonl(&line).unwrap(), vec![tabbed]);
    }

    #[test]
    fn a_receipt_file_is_reloaded_so_a_cutover_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("receipts.jsonl");
        let first = ReceiptLog::create(&path).unwrap();
        first
            .record(Receipt {
                topic: "events".to_owned(),
                kind: ReceiptKind::DualWrite,
                primary_offset: 0,
                shadow_offset: Some(4),
                records: 2,
                sha256: [1; 32],
                group: None,
                partition: None,
            })
            .unwrap();
        drop(first);
        let reopened = ReceiptLog::create(&path).unwrap();
        assert_eq!(reopened.to_primary("events", 5), Some(1));
        assert_eq!(
            reopened.to_primary("events", 6),
            Some(2),
            "the exclusive end is the next offset Kafka commits"
        );
        assert_eq!(reopened.rows().len(), 1);
        let corrupt = dir.path().join("bad.jsonl");
        std::fs::write(&corrupt, "not-json\n").unwrap();
        assert!(ReceiptLog::create(&corrupt).is_err());
    }

    #[test]
    fn shadow_read_serves_the_shadow_numbering_the_bounds_advertise() {
        let primary = Arc::new(MemoryBridge::with_topics(["native"]));
        let shadow = Arc::new(MemoryBridge::with_topics(["kafka"]));
        shadow.produce("kafka", &[batch(&["pad"])], None).unwrap();
        let dual = DualBridge::new(
            "events".to_owned(),
            Arc::clone(&primary) as Arc<dyn Bridge>,
            "native".to_owned(),
            Arc::clone(&shadow) as Arc<dyn Bridge>,
            "kafka".to_owned(),
            DualRead::Shadow,
            ReceiptLog::memory(),
        );
        dual.produce("events", &[batch(&["a"])], None).unwrap();
        assert_eq!(dual.bounds("events"), Ok((0, 2)), "shadow had pad + a");
        let fetched = dual.fetch("events", 0, 1 << 20).unwrap();
        assert_eq!(
            record_values(&fetched.records)[0],
            b"pad".to_vec(),
            "offset 0 on the shadow is the pad the client would have consumed first"
        );
        let rest = dual.fetch("events", 1, 1 << 20).unwrap();
        assert_eq!(record_values(&rest.records), vec![b"a".to_vec()]);
        assert_eq!(
            dual.fetch("nope", 0, 1024),
            Err(ErrorCode::UnknownTopicOrPartition)
        );
        assert_eq!(dual.bounds("nope"), Err(ErrorCode::UnknownTopicOrPartition));
    }

    #[test]
    fn a_primary_refusal_does_not_touch_the_shadow() {
        let primary = Arc::new(MemoryBridge::with_topics(["native"]));
        let shadow = Arc::new(MemoryBridge::with_topics(["kafka"]));
        let dual = DualBridge::new(
            "events".to_owned(),
            Arc::clone(&primary) as Arc<dyn Bridge>,
            "native".to_owned(),
            Arc::clone(&shadow) as Arc<dyn Bridge>,
            "kafka".to_owned(),
            DualRead::Primary,
            ReceiptLog::memory(),
        );
        assert_eq!(
            dual.produce("nope", &[batch(&["a"])], None),
            Err(ErrorCode::UnknownTopicOrPartition)
        );
        assert_eq!(shadow.bounds("kafka"), Ok((0, 0)));
    }

    struct OnceThenLive {
        inner: Arc<MemoryBridge>,
        refused: std::sync::atomic::AtomicBool,
    }
    impl Bridge for OnceThenLive {
        fn topics(&self) -> Vec<String> {
            self.inner.topics()
        }
        fn produce(
            &self,
            topic: &str,
            batches: &[RecordBatch],
            sequenced: Option<Sequenced>,
        ) -> Result<Appended, ErrorCode> {
            if self
                .refused
                .compare_exchange(
                    false,
                    true,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_ok()
            {
                return Err(ErrorCode::BrokerNotAvailable);
            }
            self.inner.produce(topic, batches, sequenced)
        }
        fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
            self.inner.fetch(topic, offset, max_bytes)
        }
        fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
            self.inner.bounds(topic)
        }
    }

    #[test]
    fn an_idempotent_retry_completes_a_primary_only_write() {
        let primary = Arc::new(MemoryBridge::with_topics(["native"]));
        let shadow = Arc::new(OnceThenLive {
            inner: Arc::new(MemoryBridge::with_topics(["kafka"])),
            refused: std::sync::atomic::AtomicBool::new(false),
        });
        let receipts = ReceiptLog::memory();
        let dual = DualBridge::new(
            "events".to_owned(),
            Arc::clone(&primary) as Arc<dyn Bridge>,
            "native".to_owned(),
            shadow,
            "kafka".to_owned(),
            DualRead::Primary,
            Arc::clone(&receipts),
        );
        let sequenced = Sequenced {
            producer_id: 7,
            producer_epoch: 0,
            first_sequence: 0,
        };
        assert_eq!(
            dual.produce("events", &[batch(&["a"])], Some(sequenced)),
            Err(ErrorCode::KafkaStorageError)
        );
        assert_eq!(primary.bounds("native"), Ok((0, 1)));
        let retried = dual
            .produce("events", &[batch(&["a"])], Some(sequenced))
            .unwrap();
        assert_eq!(retried.base_offset, 0, "primary appended nothing on retry");
        assert_eq!(primary.bounds("native"), Ok((0, 1)));
        assert!(receipts
            .rows()
            .iter()
            .any(|row| row.kind == ReceiptKind::DualWrite));
    }

    #[test]
    fn compare_names_a_mismatch_when_only_the_key_differs() {
        let primary = Arc::new(MemoryBridge::with_topics(["native"]));
        let shadow = Arc::new(MemoryBridge::with_topics(["kafka"]));
        let dual = DualBridge::new(
            "events".to_owned(),
            Arc::clone(&primary) as Arc<dyn Bridge>,
            "native".to_owned(),
            Arc::clone(&shadow) as Arc<dyn Bridge>,
            "kafka".to_owned(),
            DualRead::Compare,
            ReceiptLog::memory(),
        );
        primary.produce("native", &[batch(&["a"])], None).unwrap();
        shadow
            .produce("kafka", &[batch_keyed("other", "a")], None)
            .unwrap();
        assert_eq!(
            dual.fetch("events", 0, 1 << 20),
            Err(ErrorCode::CorruptMessage)
        );
    }

    fn batch_keyed(key: &str, value: &str) -> RecordBatch {
        RecordBatch {
            base_offset: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                offset: 0,
                timestamp_millis: 1,
                key: Some(key.as_bytes().to_vec()),
                value: Some(value.as_bytes().to_vec()),
                headers: Vec::new(),
            }],
        }
    }

    #[test]
    fn a_durable_receipt_that_cannot_be_appended_is_storage_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("receipts.jsonl");
        let log = ReceiptLog::create(&path).unwrap();
        log.record(Receipt {
            topic: "events".to_owned(),
            kind: ReceiptKind::DualWrite,
            primary_offset: 0,
            shadow_offset: Some(0),
            records: 1,
            sha256: [0; 32],
            group: None,
            partition: None,
        })
        .unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert_eq!(
            log.record(Receipt {
                topic: "events".to_owned(),
                kind: ReceiptKind::DualWrite,
                primary_offset: 1,
                shadow_offset: Some(1),
                records: 1,
                sha256: [1; 32],
                group: None,
                partition: None,
            }),
            Err(ErrorCode::KafkaStorageError)
        );
        assert_eq!(
            log.rows().len(),
            1,
            "memory stays at the last durable row when the file refused"
        );
    }

    #[tokio::test]
    async fn a_native_cursor_marker_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("receipts.jsonl");
        let receipts = ReceiptLog::create(&path).unwrap();
        receipts
            .record(Receipt {
                topic: "events".to_owned(),
                kind: ReceiptKind::DualWrite,
                primary_offset: 0,
                shadow_offset: Some(1),
                records: 2,
                sha256: [2; 32],
                group: None,
                partition: None,
            })
            .unwrap();
        let inner = Arc::new(MemoryOffsetStore::default());
        let store = CutoverStore::wrapping(
            Arc::clone(&inner) as Arc<dyn OffsetStore>,
            vec![("events".to_owned(), Arc::clone(&receipts))],
        );
        store
            .commit(
                "g",
                "events",
                0,
                Committed {
                    offset: 1,
                    metadata: None,
                },
            )
            .await
            .unwrap();
        drop(store);
        let reopened = ReceiptLog::create(&path).unwrap();
        let again = CutoverStore::wrapping(
            inner as Arc<dyn OffsetStore>,
            vec![("events".to_owned(), reopened)],
        );
        assert_eq!(
            again.fetch("g", "events", 0).await.unwrap().unwrap().offset,
            1,
            "the native commit is not translated again after restart"
        );
    }

    #[test]
    fn wrapping_an_empty_cutover_is_the_inner_store() {
        let inner: Arc<dyn OffsetStore> = Arc::new(MemoryOffsetStore::default());
        let wrapped = CutoverStore::wrapping(Arc::clone(&inner), Vec::new());
        assert!(Arc::ptr_eq(&inner, &wrapped));
    }
}
