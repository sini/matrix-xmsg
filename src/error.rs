use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Matrix client error: {0}")]
    Matrix(String),

    #[error("xmsg client error: {0}")]
    Xmsg(String),

    #[error("Store error: {0}")]
    Store(String),

    #[error("Answer timeout after {0} seconds")]
    Timeout(u64),

    #[error("Message size {0} exceeds cap {1}")]
    SizeCapExceeded(usize, usize),

    #[error("Rate limit exceeded for user {0}")]
    RateLimitExceeded(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
