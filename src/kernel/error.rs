use service_engine::error::EngineError;
use service_engine::gate::Reason;
use service_engine::inbound::{Disposition, ReactionError, sqlx_is_terminal};
use service_engine::pipeline::MutationFault;

#[derive(Debug, thiserror::Error)]
pub enum AppFault {
    #[error("the store failed")]
    Store(#[from] EngineError),
}

impl MutationFault for AppFault {
    fn reason(&self) -> Option<Reason> {
        match self {
            Self::Store(_) => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReactionFault {
    /// The request breaks a rule of the offer: it is refused as a whole and never retried.
    #[error("refused: {0}")]
    Refused(String),
    #[error("the store failed")]
    Store(#[from] EngineError),
}

impl ReactionError for ReactionFault {
    fn disposition(&self) -> Disposition {
        match self {
            Self::Refused(_) => Disposition::Terminal,
            Self::Store(EngineError::Db(db)) if sqlx_is_terminal(db) => Disposition::Terminal,
            Self::Store(_) => Disposition::Retry,
        }
    }
}

impl From<sqlx::Error> for ReactionFault {
    fn from(error: sqlx::Error) -> Self {
        Self::Store(EngineError::from(error))
    }
}
