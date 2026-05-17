use crate::error::Error;
use crate::info::{ErroredInfo, QueueInfo};
use std::fmt;
use std::future::Future;

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct Cursor(pub(crate) String);

impl fmt::Debug for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Cursor(_)")
    }
}

impl Cursor {
    pub(crate) fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct ScanPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<Cursor>,
}

pub trait QueueInspector<I, T> {
    fn delayed_len(&self) -> impl Future<Output = Result<u64, Error>> + Send;
    fn failed_len(&self) -> impl Future<Output = Result<u64, Error>> + Send;
    fn get_delayed(
        &self,
        id: &I,
    ) -> impl Future<Output = Result<Option<QueueInfo<I, T>>, Error>> + Send;
    fn get_failed(
        &self,
        id: &I,
    ) -> impl Future<Output = Result<Option<ErroredInfo<I, T>>, Error>> + Send;
    fn scan_delayed(
        &self,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> impl Future<Output = Result<ScanPage<QueueInfo<I, T>>, Error>> + Send;
    fn scan_failed(
        &self,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> impl Future<Output = Result<ScanPage<ErroredInfo<I, T>>, Error>> + Send;
}

pub trait FailedRetry<I> {
    fn retry_failed(&self, id: &I) -> impl Future<Output = Result<(), Error>> + Send;
}
