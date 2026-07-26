pub mod api;
pub mod auth;
pub mod client;
pub mod command;
mod error;
#[cfg(feature = "testing")]
pub mod test_support;
pub mod webhook;

pub use api::{
    CheckRunConclusion, CheckRunOutput, CheckRunState, CheckRunUpdate, GitHubApi, PostedCheckRun,
    PostedComment, PullRequestAuthor, PullRequestSide, PullRequestSummary, RepoRef, RepoSummary,
    encode_ref_path,
};
pub use auth::{AppCredentials, InstallationTokenCache};
pub use client::OctocrabClient;
pub use command::{Command, parse_command};
pub use error::{GitHubError, GitHubResult};
pub use webhook::{
    CreateEvent, InstallationAccount, InstallationDetails, InstallationEvent,
    InstallationRepositoriesEvent, InstallationRepository, IssueCommentEvent, PullRequestBody,
    PullRequestBranchRef, PullRequestChanges, PullRequestEvent, PullRequestRepo, PushCommit,
    PushEvent,
};

trait IntoGitHubResult<T> {
    fn github(self) -> GitHubResult<T>;
}

impl<T, E> IntoGitHubResult<T> for std::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn github(self) -> GitHubResult<T> {
        self.map_err(|error| GitHubError::Other(anyhow::Error::new(error)))
    }
}
