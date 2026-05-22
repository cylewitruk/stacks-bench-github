use std::sync::Arc;

use sbgh_core::config::Config;
use sbgh_core::db::JobStore;
use sbgh_core::github::GitHubApi;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub jobs: Arc<dyn JobStore>,
    pub gh: Arc<dyn GitHubApi>,
}
