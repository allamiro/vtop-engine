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
//!   [`gateway`]   the listener: frames in, the bridge behind, refusals by name
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
pub mod bridge;
pub mod gateway;
pub mod messages;
#[cfg(feature = "native")]
pub mod native;
pub mod records;
pub mod wire;

pub use bridge::{Appended, Bridge, Fetched, MemoryBridge};
pub use gateway::{Gateway, GatewayConfig};
pub use messages::{ApiKey, ErrorCode, HeaderVerdict, RequestHeader};
#[cfg(feature = "native")]
pub use native::{EpochsExhausted, NativeBridge, NativeBridgeConfig};
pub use records::{Record, RecordBatch};
pub use wire::{Decoder, Encoder, WireError};
