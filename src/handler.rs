use crate::error::ErrorOperation;
use futures::future::BoxFuture;
use std::future::Future;
use std::sync::Arc;

pub type HandlerResult = Result<(), ErrorOperation>;
pub type HandlerFn<M, T> = Arc<dyn Fn(M, T) -> BoxFuture<'static, HandlerResult> + Send + Sync>;

pub fn into_handler<M, T, F, Fut>(f: F) -> HandlerFn<M, T>
where
    F: Fn(M, T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = HandlerResult> + Send + 'static,
{
    Arc::new(move |m, t| Box::pin(f(m, t)))
}
