//! Pure binary-cache values and narrow worker/backend ports.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BuildArtifact {
    #[default]
    StacksBench,
    StacksInspect,
}

impl BuildArtifact {
    pub fn executable_name(self) -> &'static str {
        match self {
            Self::StacksBench => "stacks-bench",
            Self::StacksInspect => "stacks-inspect",
        }
    }

    pub fn package_name(self) -> &'static str {
        match self {
            Self::StacksBench => "stacks-bench",
            Self::StacksInspect => "stacks-inspect",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildFingerprint {
    #[serde(default)]
    pub artifact: BuildArtifact,
    /// Canonical source repository for artifacts whose provenance must not be
    /// shared across repositories. Omitted for the legacy benchmark key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
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
        // Preserve the shipped benchmark-cache key byte-for-byte. The new
        // discriminator is serialized only for non-default artifacts, so v26
        // does not cold-invalidate every existing stacks-bench entry.
        let json = match self.artifact {
            BuildArtifact::StacksBench => serde_json::to_vec(&LegacyBenchmarkFingerprint {
                commit: &self.commit,
                toolchain: &self.toolchain,
                profile: &self.profile,
                features: &self.features,
                rustflags: &self.rustflags,
                target_triple: &self.target_triple,
                recipe_version: self.recipe_version,
                image_id: &self.image_id,
                protocol_version: &self.protocol_version,
            }),
            BuildArtifact::StacksInspect => serde_json::to_vec(self),
        }
        .expect("BuildFingerprint always serializes");
        hex::encode(Sha256::digest(&json))
    }

    pub fn environment(&self) -> CacheEnvironment {
        CacheEnvironment {
            artifact: self.artifact,
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

#[derive(Serialize)]
struct LegacyBenchmarkFingerprint<'a> {
    commit: &'a str,
    toolchain: &'a str,
    profile: &'a str,
    features: &'a str,
    rustflags: &'a str,
    target_triple: &'a str,
    recipe_version: u32,
    image_id: &'a str,
    protocol_version: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEnvironment {
    pub artifact: BuildArtifact,
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
        self.artifact == fp.artifact
            && self.profile == fp.profile
            && self.features == fp.features
            && self.rustflags == fp.rustflags
            && self.target_triple == fp.target_triple
            && self.recipe_version == fp.recipe_version
            && self.image_id == fp.image_id
            && self.protocol_version == fp.protocol_version
    }

    pub fn fingerprint(&self, commit: String, toolchain: String) -> BuildFingerprint {
        BuildFingerprint {
            artifact: self.artifact,
            repository: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn fingerprint(artifact: BuildArtifact) -> BuildFingerprint {
        BuildFingerprint {
            artifact,
            repository: (artifact == BuildArtifact::StacksInspect)
                .then(|| "stacks-network/stacks-core".into()),
            commit: "deadbeef".into(),
            toolchain: "1.97.0".into(),
            profile: "release".into(),
            features: String::new(),
            rustflags: String::new(),
            target_triple: "x86_64-unknown-linux-gnu".into(),
            recipe_version: 2,
            image_id: "golden-v1".into(),
            protocol_version: "v1".into(),
        }
    }

    #[test]
    fn executable_kind_is_a_load_bearing_cache_dimension() {
        let benchmark = fingerprint(BuildArtifact::StacksBench);
        let inspect = fingerprint(BuildArtifact::StacksInspect);
        assert_ne!(benchmark.digest(), inspect.digest());
        assert!(
            !benchmark
                .environment()
                .matches(&inspect)
        );
        assert_eq!(
            inspect
                .artifact
                .executable_name(),
            "stacks-inspect"
        );
    }

    #[test]
    fn stacks_inspect_cache_is_scoped_to_its_source_repository() {
        let first = fingerprint(BuildArtifact::StacksInspect);
        let mut second = first.clone();
        second.repository = Some("fork/stacks-core".into());
        assert_ne!(first.digest(), second.digest());
    }
}
