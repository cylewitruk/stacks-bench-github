//! Pure binary-cache values and narrow worker/backend ports.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildFingerprint {
    pub commit: String,
    pub toolchain: String,
    pub profile: String,
    pub features: String,
    pub rustflags: String,
    pub target_triple: String,
    pub recipe_version: u32,
    pub image_id: String,
    pub protocol_version: String,
}

impl BuildFingerprint {
    pub fn digest(&self) -> String {
        let json = serde_json::to_vec(self).expect("BuildFingerprint always serializes");
        hex::encode(Sha256::digest(&json))
    }

    pub fn environment(&self) -> CacheEnvironment {
        CacheEnvironment {
            profile: self.profile.clone(),
            features: self.features.clone(),
            rustflags: self.rustflags.clone(),
            target_triple: self.target_triple.clone(),
            recipe_version: self.recipe_version,
            image_id: self.image_id.clone(),
            protocol_version: self.protocol_version.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEnvironment {
    pub profile: String,
    pub features: String,
    pub rustflags: String,
    pub target_triple: String,
    pub recipe_version: u32,
    pub image_id: String,
    pub protocol_version: String,
}

impl CacheEnvironment {
    pub fn matches(&self, fp: &BuildFingerprint) -> bool {
        self.profile == fp.profile
            && self.features == fp.features
            && self.rustflags == fp.rustflags
            && self.target_triple == fp.target_triple
            && self.recipe_version == fp.recipe_version
            && self.image_id == fp.image_id
            && self.protocol_version == fp.protocol_version
    }

    pub fn fingerprint(&self, commit: String, toolchain: String) -> BuildFingerprint {
        BuildFingerprint {
            commit,
            toolchain,
            profile: self.profile.clone(),
            features: self.features.clone(),
            rustflags: self.rustflags.clone(),
            target_triple: self.target_triple.clone(),
            recipe_version: self.recipe_version,
            image_id: self.image_id.clone(),
            protocol_version: self.protocol_version.clone(),
        }
    }
}

pub struct CachedBinary {
    pub path: PathBuf,
    pub digest: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub last_used: u64,
    pub pinned: bool,
}

/// Cache operations required by an execution backend.
pub trait BinaryCacheStore: Send + Sync {
    fn get(&self, fp: &BuildFingerprint, now_unix: u64) -> Option<CachedBinary>;

    fn publish(
        &self,
        fp: &BuildFingerprint,
        src: &Path,
        now_unix: u64,
        pinned: bool,
    ) -> std::io::Result<String>;
}

/// Cache policy operations used by orchestrator-side pin management.
pub trait CacheControl: Send + Sync {
    fn evict_to_budget(&self);

    fn set_pinned_by_commit(&self, commits: &HashSet<String>, env: &CacheEnvironment);

    fn has_entry_for(&self, commit: &str, env: &CacheEnvironment) -> bool;
}
