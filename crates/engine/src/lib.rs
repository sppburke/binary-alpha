//! The broker-, instrument-, strategy-, account-, and currency-neutral core of Binary Alpha.
//!
//! The engine makes no file, network, cloud, broker, command-line, or device call. Callers hand it
//! text and records; it returns validated values, canonical forms, and identities.

pub mod config;
