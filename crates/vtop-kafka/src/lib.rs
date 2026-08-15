//! Kafka wire-compatibility gateway over the native VTOP broker (#225).
//!
//! Why a gateway and not a client library: Kafka's ecosystem is fifteen years
//! of clients, Connect plugins and ops tooling in every language, and none of
//! it can be rebuilt head-on. Speaking the protocol keeps that ecosystem and
//! keeps this engine underneath it.
//!
//! Layering, deliberately strict:
//!
//!   [`wire`]     primitives only — how a field is written, never what it means
//!   [`records`]  RecordBatch v2, the container Produce carries in and Fetch out
//!
//! Landed so far: the two layers that need no broker to be correct, and no
//! broker to be tested. The message schemas, the bridge seam and the listener
//! follow — deliberately after them, because everything above these two is a
//! translation, and a translation built on an unverified codec is a bug store.

pub mod records;
pub mod wire;

pub use records::{Record, RecordBatch};
pub use wire::{Decoder, Encoder, WireError};
