use crate::error::ErrorOperation;
use std::future::Future;
use std::pin::Pin;

pub type HandlerResult = Pin<Box<dyn Future<Output = Result<(), ErrorOperation>> + Send>>;

// These traits and structs are based on axum, and codes licensed under the MIT License
// https://github.com/tokio-rs/axum/blob/b6b203b3065e4005bda01efac8429176da055ae2/axum/LICENSE

// https://github.com/tokio-rs/axum/blob/b6b203b3065e4005bda01efac8429176da055ae2/axum/src/handler/mod.rs#L134-139
pub trait Handler<M, T>: 'static + Clone + Send {
    fn call(self, module: M, data: T) -> HandlerResult;
}

// https://github.com/tokio-rs/axum/blob/b6b203b3065e4005bda01efac8429176da055ae2/axum/src/handler/mod.rs#L193
impl<Fn, Res, M, T> Handler<M, T> for Fn
where
    Fn: 'static + Clone + Send + FnOnce(M, T) -> Res,
    Res: Future<Output = Result<(), ErrorOperation>> + Send,
    M: 'static + Send,
    T: 'static + Send,
{
    fn call(self, module: M, data: T) -> HandlerResult {
        Box::pin(async move { self(module, data).await })
    }
}

// https://github.com/tokio-rs/axum/blob/b6b203b3065e4005bda01efac8429176da055ae2/axum/src/boxed.rs#L70
pub struct MakeErasedHandler<M, T, H> {
    handler: H,
    converter: fn(H, M, T) -> HandlerResult,
}

impl<M, T, H> MakeErasedHandler<M, T, H> {
    pub fn new(handler: H, converter: fn(H, M, T) -> HandlerResult) -> Self {
        Self { handler, converter }
    }
}

impl<M, T, H> Clone for MakeErasedHandler<M, T, H>
where
    H: Clone,
{
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            converter: self.converter,
        }
    }
}

// https://github.com/tokio-rs/axum/blob/b6b203b3065e4005bda01efac8429176da055ae2/axum/src/boxed.rs#L62
pub trait ErasedIntoRoute<M, T>: Send {
    fn clone_box(&self) -> Box<dyn ErasedIntoRoute<M, T>>;
    fn convert(self: Box<Self>, module: M, data: T) -> HandlerResult;
}

impl<M, T, H> ErasedIntoRoute<M, T> for MakeErasedHandler<M, T, H>
where
    H: 'static + Clone + Send,
    M: 'static,
    T: 'static,
{
    fn clone_box(&self) -> Box<dyn ErasedIntoRoute<M, T>> {
        Box::new(self.clone())
    }

    fn convert(self: Box<Self>, module: M, data: T) -> HandlerResult {
        (self.converter)(self.handler, module, data)
    }
}
