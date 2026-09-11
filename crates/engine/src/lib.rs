//! The broker-, instrument-, strategy-, account-, and currency-neutral core of Binary Alpha.
//!
//! The engine makes no file, network, cloud, broker, command-line, or device call. Callers hand it
//! text and records; it returns validated values, canonical forms, and identities.

pub mod config;
pub mod dataset;
pub mod features;
pub mod market;
pub mod outcomes;
pub mod stream;

/// Lowercase hexadecimal rendering of a digest or checksum.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Declares an enumeration whose configuration and manifest spelling is one exact string.
///
/// The hand-written deserializer accepts only a string; a derived enum deserializer would also
/// accept a single-key table such as `{ research = {} }`.
macro_rules! string_enum {
    ($(#[$meta:meta])* $name:ident $label:literal { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            const ALL: &[Self] = &[$(Self::$variant),+];

            /// The spelling accepted and emitted in documents.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text),+
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl std::str::FromStr for $name {
            type Err = String;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                Self::ALL
                    .iter()
                    .copied()
                    .find(|value| value.as_str() == text)
                    .ok_or_else(|| {
                        let expected = Self::ALL
                            .iter()
                            .map(|value| format!("`{}`", value.as_str()))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("unknown {} `{text}`, expected one of {expected}", $label)
                    })
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                String::deserialize(deserializer)?
                    .parse()
                    .map_err(serde::de::Error::custom)
            }
        }
    };
}

pub(crate) use string_enum;
