//! Kafka wire-compatibility gateway over the native VTOP broker (#225).
//!
//! Why a gateway and not a client library: Kafka's ecosystem is fifteen years
//! of clients, Connect plugins and ops tooling in every language, and none of
//! it can be rebuilt head-on. Speaking the protocol keeps that ecosystem and
//! keeps this engine underneath it.
//!
//! Layering, deliberately strict:
//!
//!   [`wire`]      primitives only — how a field is written, never what it means
//!   [`records`]   RecordBatch v2, the container Produce carries in and Fetch out
//!   [`messages`]  request framing, API identity, and the version gate
//!
//!   [`api`]       the five phase-1 APIs, request and response, every served version
//!   [`bridge`]    the seam: what a backend must answer, and an in-memory one
//!   [`topics`]    per-topic virtualization: which backend answers a Kafka name
//!   [`dual`]      dual-write / shadow-read, and the receipt that proves it
//!   [`remote`]    an external Kafka cluster as a Bridge, over these codecs
//!   [`gateway`]   the listener: frames in, the map behind, refusals by name
//!   [`native`]    the backend over `LocalBroker` (feature `native`, on by default)
//!
//! The codec layers landed first, and the bridge and listener after them —
//! deliberately, because everything above the codecs is a translation, and a
//! translation built on an unverified codec is a bug store. What the listener
//! serves is exactly what the engine can honestly back today; everything else
//! is a refusal with the code a client's retry policy can act on, and a
//! reason on the log. The native backend over `LocalBroker` implements
//! [`bridge::Bridge`] where brokers are wired.

pub mod api;
pub mod api_groups;
pub mod bridge;
pub mod dual;
pub mod gateway;
pub mod groups;
pub mod lease;
pub mod messages;
#[cfg(feature = "metadata")]
pub mod metadata_offsets;
#[cfg(feature = "native")]
pub mod native;
pub mod offsets;
pub mod records;
pub mod remote;
pub mod topics;
mod turnstile;
pub mod wire;

pub use bridge::{Appended, Bridge, Fetched, MemoryBridge, Sequenced};
pub use dual::{
    load_receipts_jsonl, CutoverStore, DualBridge, DualRead, Receipt, ReceiptKind, ReceiptLog,
};
pub use gateway::{coordinator_partition_index, Gateway, GatewayConfig, PartitionLeader};
pub use groups::{Coordinator, GroupConfig};
pub use lease::{LeaseState, LeaseView};
pub use messages::{ApiKey, ErrorCode, HeaderVerdict, RequestHeader};
#[cfg(feature = "native")]
pub use native::{EpochsExhausted, NativeBridge, NativeBridgeConfig};
pub use offsets::{Committed, MemoryOffsetStore, OffsetStore, OverlayOffsetStore};
pub use records::{Record, RecordBatch};
pub use remote::{RemoteBridge, RemoteConfig};
pub use topics::{TopicBinding, TopicMap};
pub use wire::{Decoder, Encoder, WireError};
