use serde_json::error::Error as SerdeError;
use std::error::Error as StdError;
use std::num::TryFromIntError;
use std::str::Utf8Error;

type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ErrorOperation {
    #[error("Operation delayed: {0}")]
    Delay(String),
    #[error("Operation failed: {0}")]
    Fail(String),
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("Database error: {0}")]
    DatabaseError(#[source] BoxedError),

    #[error("Conversion error: {0}")]
    ConversionError(#[source] BoxedError),
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Self {
        Self::ConversionError(Box::new(err))
    }
}

impl From<SerdeError> for Error {
    fn from(err: SerdeError) -> Self {
        Self::ConversionError(Box::new(err))
    }
}

impl From<TryFromIntError> for Error {
    fn from(value: TryFromIntError) -> Self {
        Self::ConversionError(Box::new(value))
    }
}
