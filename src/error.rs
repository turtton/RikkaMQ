use std::error::Error as StdError;

pub type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ErrorOperation {
    #[error("operation delayed: {0}")]
    Delay(#[source] BoxedError),
    #[error("operation failed: {0}")]
    Fail(#[source] BoxedError),
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("pool error: {0}")]
    Pool(#[source] BoxedError),

    #[error("backend error: {0}")]
    Backend(#[source] BoxedError),

    #[error("serialize error: {0}")]
    Serialize(#[source] BoxedError),

    #[error("deserialize error: {0}")]
    Deserialize(#[source] BoxedError),

    /// Protocol-level parse/validation failure.
    ///
    /// Carries the wire-protocol `op` (e.g. `"XREAD"`), an optional `field`
    /// pointing at the specific position in the response (e.g.
    /// `"entry.stream_id"`), and a free-form `detail` describing the expected
    /// shape and the actual value. Source-chain preservation is reserved for
    /// I/O-level errors such as [`Error::Pool`] and [`Error::Backend`].
    #[error("protocol error during {op}{}: {detail}",
        .field.map(|f| format!(" ({f})")).unwrap_or_default())]
    Protocol {
        op: &'static str,
        field: Option<&'static str>,
        detail: String,
    },

    #[error("worker join error: {0}")]
    Join(#[source] BoxedError),

    #[error("shutdown error: {0}")]
    Shutdown(#[source] BoxedError),
}

impl Error {
    pub(crate) fn protocol(op: &'static str, detail: impl Into<String>) -> Self {
        Self::Protocol {
            op,
            field: None,
            detail: detail.into(),
        }
    }

    pub(crate) fn protocol_field(
        op: &'static str,
        field: &'static str,
        detail: impl Into<String>,
    ) -> Self {
        Self::Protocol {
            op,
            field: Some(field),
            detail: detail.into(),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Deserialize(Box::new(err))
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Self::Protocol {
            op: "utf8",
            field: None,
            detail: err.to_string(),
        }
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Self {
        Self::Protocol {
            op: "int conversion",
            field: None,
            detail: err.to_string(),
        }
    }
}

#[cfg(feature = "redis")]
impl From<deadpool_redis::PoolError> for Error {
    fn from(err: deadpool_redis::PoolError) -> Self {
        Self::Pool(Box::new(err))
    }
}

#[cfg(feature = "redis")]
impl From<deadpool_redis::redis::RedisError> for Error {
    fn from(err: deadpool_redis::redis::RedisError) -> Self {
        Self::Backend(Box::new(err))
    }
}

#[cfg(feature = "redis")]
impl From<deadpool_redis::CreatePoolError> for Error {
    fn from(err: deadpool_redis::CreatePoolError) -> Self {
        Self::Pool(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_display_includes_field_when_present() {
        let err = Error::protocol_field("XREAD", "entry.stream_id", "expected bulk string, got Nil");
        assert_eq!(
            err.to_string(),
            "protocol error during XREAD (entry.stream_id): expected bulk string, got Nil",
        );
    }

    #[test]
    fn protocol_display_omits_field_when_absent() {
        let err = Error::protocol("XLEN", "expected integer, got Nil");
        assert_eq!(
            err.to_string(),
            "protocol error during XLEN: expected integer, got Nil",
        );
    }
}
