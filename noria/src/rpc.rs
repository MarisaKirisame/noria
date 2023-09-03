use serde::Serialize;
use serde::Deserialize;

pub trait RPC<A> {
    type RpcFuture<R>;

    fn rpc<Q: Serialize, R: 'static>(
        &mut self,
        path: &'static str,
        r: Q,
        err: &'static str,
    ) -> Self::RpcFuture<R>
    where
        for<'de> R: Deserialize<'de>,
        R: Send;
}


