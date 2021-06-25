use super::Dispatch;
use tokio_stream::{Stream, StreamExt};

/// Feed struct, it takes trusted stream and feed the provided dispatcher
pub struct Feed<T, D: Dispatch<E>, E>
where
    T: Stream<Item = Result<E, anyhow::Error>>,
{
    stream: T,
    dispatcher: D,
}

impl<D: Dispatch<E>, E, T: Stream<Item = Result<E, anyhow::Error>> + Unpin> Feed<T, D, E> {
    /// Create new feed source instance with provided dispatcher.
    pub fn new(stream: T, dispatcher: D) -> Self {
        Self { stream, dispatcher }
    }
    /// Run the Feed which will dispatch
    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        // Fetch the next item from the stream.
        // NOTE: we assume the Stream to be fair and collaborative
        // for example it should yield at most every N iteration to force
        // fairness and keep the system responsive
        while let Some(item) = self.stream.next().await {
            let item: E = item?;
            // dispatch item using the dispatcher
            self.dispatcher.dispatch(item)?;
        }
        Ok(())
    }
}
