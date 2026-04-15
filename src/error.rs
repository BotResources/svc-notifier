use async_graphql::ErrorExtensions;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("Authentication required")]
    Unauthenticated,
    #[error("{0}")]
    NotFound(String),
    #[error("Internal error")]
    Internal,
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        tracing::error!(error = %e, "database error");
        AppError::Internal
    }
}

impl ErrorExtensions for AppError {
    fn extend(&self) -> async_graphql::Error {
        async_graphql::Error::new(self.to_string()).extend_with(|_, e| match self {
            AppError::Unauthenticated => e.set("code", "UNAUTHENTICATED"),
            AppError::NotFound(_) => e.set("code", "NOT_FOUND"),
            AppError::Internal => e.set("code", "INTERNAL"),
        })
    }
}
