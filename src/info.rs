use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::fmt::{self, Display};

type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

/// Information about a queued message
///
/// ```
/// use uuid::Uuid;
/// use rikka_mq::info::QueueInfo;
///
/// let info = QueueInfo::new(Uuid::new_v4(), "Data");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInfo<I, D> {
    id: I,
    data: D,
}

impl<I, T> QueueInfo<I, T> {
    pub fn new(id: I, data: T) -> Self {
        Self { id, data }
    }

    pub fn id(&self) -> &I {
        &self.id
    }

    pub fn data(&self) -> &T {
        &self.data
    }

    pub fn into_parts(self) -> (I, T) {
        (self.id, self.data)
    }
}

/// Information about a failed or delayed message
#[derive(Debug)]
pub struct ErroredInfo<I, D> {
    id: I,
    data: D,
    error: BoxedError,
}

impl<I, D> ErroredInfo<I, D> {
    pub fn new(id: I, data: D, error: BoxedError) -> Self {
        Self { id, data, error }
    }

    pub fn id(&self) -> &I {
        &self.id
    }

    pub fn data(&self) -> &D {
        &self.data
    }

    pub fn error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self.error.as_ref()
    }

    pub fn into_parts(self) -> (I, D, BoxedError) {
        (self.id, self.data, self.error)
    }
}

impl<I: Clone, D: Clone> Clone for ErroredInfo<I, D> {
    fn clone(&self) -> Self {
        Self::new(
            self.id.clone(),
            self.data.clone(),
            Box::new(StringError(self.error.to_string())),
        )
    }
}

#[derive(Debug, Clone)]
pub struct StringError(pub String);

impl Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl StdError for StringError {}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StoredErroredInfo<I, D> {
    pub id: I,
    pub data: D,
    pub error: String,
}

impl<I, D> From<ErroredInfo<I, D>> for StoredErroredInfo<I, D> {
    fn from(value: ErroredInfo<I, D>) -> Self {
        let (id, data, error) = value.into_parts();
        Self {
            id,
            data,
            error: error.to_string(),
        }
    }
}

impl<I, D> From<StoredErroredInfo<I, D>> for ErroredInfo<I, D> {
    fn from(value: StoredErroredInfo<I, D>) -> Self {
        Self::new(value.id, value.data, Box::new(StringError(value.error)))
    }
}
