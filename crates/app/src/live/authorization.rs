//! Immutable authorization bound to one deployment and execution account.

use binary_alpha_engine::research::digest;
use serde::{Deserialize, Serialize};

use crate::research::publish_record;
use crate::store::{Put, Store};

const DOMAIN: &[u8] = b"binary-alpha live authorization v1\n";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authorization {
    pub schema_version: u32,
    pub deployment: String,
    pub configuration: String,
    pub bundle_sha256: String,
    pub broker: String,
    pub account: String,
    pub operator: String,
    pub reason: String,
    pub hash: String,
}

impl Authorization {
    /// Hashes every binding, excluding the hash field itself.
    pub fn content_hash(&self) -> String {
        let mut value = serde_json::to_value(self).expect("authorization serializes");
        value.as_object_mut().unwrap().remove("hash");
        digest(DOMAIN, &serde_json::to_vec(&value).unwrap())
    }

    pub fn validate(
        &self,
        deployment: &str,
        configuration: &str,
        bundle_sha256: &str,
        broker: &str,
        account: &str,
    ) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err("authorization schema_version must be 1".into());
        }
        for (name, actual, expected) in [
            ("deployment", self.deployment.as_str(), deployment),
            ("configuration", self.configuration.as_str(), configuration),
            ("bundle_sha256", self.bundle_sha256.as_str(), bundle_sha256),
            ("broker", self.broker.as_str(), broker),
            ("account", self.account.as_str(), account),
        ] {
            if actual != expected {
                return Err(format!("authorization {name} mismatch"));
            }
        }
        if self.operator.is_empty() || self.reason.is_empty() {
            return Err("authorization operator and reason must be non-empty".into());
        }
        if self.hash != self.content_hash() {
            return Err("authorization hash mismatch".into());
        }
        Ok(())
    }
}

pub fn key(deployment: &str) -> String {
    format!(
        "live/authorizations/{}.json",
        digest(DOMAIN, deployment.as_bytes())
    )
}

/// Publishes a closed local file through the common create-once artifact owner.
pub fn create(
    store: &Store,
    local: &Store,
    mut authorization: Authorization,
) -> Result<(Authorization, Put), String> {
    authorization.hash = authorization.content_hash();
    authorization.validate(
        &authorization.deployment,
        &authorization.configuration,
        &authorization.bundle_sha256,
        &authorization.broker,
        &authorization.account,
    )?;
    let key = key(&authorization.deployment);
    let bytes = serde_json::to_vec(&authorization).map_err(|error| error.to_string())?;
    let put = publish_record(local, store, &key, &bytes)?;
    Ok((authorization, put))
}

pub fn read(store: &Store, deployment: &str) -> Result<Option<Authorization>, String> {
    let key = key(deployment);
    if store.head(&key)?.is_none() {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    store.read_to(&key, None, &mut bytes)?;
    let authorization: Authorization =
        serde_json::from_slice(&bytes).map_err(|error| format!("{}: {error}", store.uri(&key)))?;
    authorization.validate(
        deployment,
        &authorization.configuration,
        &authorization.bundle_sha256,
        &authorization.broker,
        &authorization.account,
    )?;
    Ok(Some(authorization))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn authorization() -> Authorization {
        Authorization {
            schema_version: 1,
            deployment: "deployment".into(),
            configuration: "config".into(),
            bundle_sha256: "bundle".into(),
            broker: "broker".into(),
            account: "account".into(),
            operator: "operator".into(),
            reason: "approved fixture".into(),
            hash: String::new(),
        }
    }

    #[test]
    fn create_reuse_conflict_and_validate() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/live-authorization-unit");
        let _ = fs::remove_dir_all(&root);
        let local = Store::filesystem(root.join("local"));
        let store = Store::filesystem(root.join("published"));
        assert_eq!(read(&store, "deployment").unwrap(), None);
        let (value, put) = create(&store, &local, authorization()).unwrap();
        assert!(matches!(put, Put::Created(_)));
        assert_eq!(read(&store, "deployment").unwrap(), Some(value.clone()));
        assert!(matches!(
            create(&store, &local, authorization()).unwrap().1,
            Put::Reused(_)
        ));
        let mut changed = authorization();
        changed.reason = "a different reason".into();
        assert!(create(&store, &local, changed).is_err());
        assert_eq!(read(&store, "deployment").unwrap(), Some(value.clone()));
        for (index, name) in [
            "deployment",
            "configuration",
            "bundle_sha256",
            "broker",
            "account",
        ]
        .iter()
        .enumerate()
        {
            let mut bindings = ["deployment", "config", "bundle", "broker", "account"];
            bindings[index] = "different";
            assert_eq!(
                value.validate(
                    bindings[0],
                    bindings[1],
                    bindings[2],
                    bindings[3],
                    bindings[4]
                ),
                Err(format!("authorization {name} mismatch"))
            );
        }
        let mut changed = value;
        changed.reason.push('!');
        assert_eq!(
            changed.validate("deployment", "config", "bundle", "broker", "account"),
            Err("authorization hash mismatch".into())
        );
        fs::remove_dir_all(root).unwrap();
    }
}
