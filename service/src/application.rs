use super::core::{Accountant, AccountantDispatcher, Clients, Feed, TransactionRecord};
use futures::stream::Stream;
use tokio_stream::StreamExt;

enum PaymentsEngineEvent {
    Accountant {
        #[allow(dead_code)]
        accountant_id: u16,
        result: Result<Clients, anyhow::Error>,
    },
    Feeder(Result<(), anyhow::Error>),
    ExitProgram,
}

pub struct PaymentsEngine<T> {
    writer: T,
}
impl<T> PaymentsEngine<T> {
    /// Return the writer
    pub fn into_inner(self) -> T {
        self.writer
    }
}

impl<W> std::convert::From<csv_async::AsyncSerializer<W>>
    for PaymentsEngine<csv_async::AsyncSerializer<W>>
where
    W: tokio::io::AsyncWrite + Unpin + Send + Sync,
{
    fn from(
        async_writer: csv_async::AsyncSerializer<W>,
    ) -> PaymentsEngine<csv_async::AsyncSerializer<W>> {
        Self::new(async_writer)
    }
}
impl<T> PaymentsEngine<T>
where
    T: super::core::AsyncMerge<Clients>,
{
    /// Create payments_engine instance
    pub fn new(writer: T) -> Self {
        Self { writer }
    }
    /// Process csv file,
    pub async fn from_csv<P>(self, file_path: P) -> Result<Self, anyhow::Error>
    where
        P: AsRef<std::path::Path>,
    {
        // or anyhow::ensure!(file_path.as_ref().is_file());
        if !file_path.as_ref().is_file() {
            anyhow::bail!(
                "The provided file path: {:?}, doesn't point to file",
                file_path.as_ref().as_os_str()
            )
        };
        let rdr = csv_async::AsyncReader::from_reader(tokio::fs::File::open(file_path).await?);
        let stream = rdr.into_records().map(|r| match r {
            Ok(mut r) => {
                // trim any whitespace
                r.trim();
                // try to deserialize the record as transaction_record
                r.deserialize::<TransactionRecord>(None)
                    .map_err(|e| anyhow::Error::msg(e))
            }
            Err(e) => Err(anyhow::Error::msg(e)),
        });
        self.run(stream).await
    }
    async fn run<S>(mut self, stream: S) -> Result<Self, anyhow::Error>
    where
        S: 'static + Stream<Item = Result<TransactionRecord, anyhow::Error>> + Unpin + Send,
    {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        // accountant_handles
        let mut accountant_handles = std::collections::HashMap::new();
        // for sake of example, we are going to use num_cpus as the accountant_count
        let accountant_count = num_cpus::get() as u16;
        for accountant_id in 0..accountant_count {
            let app_handle = tx.clone();
            let (handle, inbox) = Accountant::channel();
            accountant_handles.insert(accountant_id, handle);
            let accountant = Accountant::new(inbox);
            // spawn accountant
            let accountant_fut = async move {
                let app_handle = app_handle;
                let result = accountant.run::<Clients>().await;
                app_handle
                    .send(PaymentsEngineEvent::Accountant {
                        accountant_id,
                        result,
                    })
                    .ok();
            };
            tokio::spawn(accountant_fut);
        }
        // Create and spawn feed source
        let dispatcher = AccountantDispatcher::new(accountant_handles);
        let feed = Feed::new(stream, dispatcher);
        // spawn feed
        let app_handle = tx.clone();
        let feed_fut = async move {
            let app_handle = app_handle;
            let r = feed.run().await;
            app_handle.send(PaymentsEngineEvent::Feeder(r)).ok();
        };
        tokio::spawn(feed_fut);
        // spawn ctrl_c
        tokio::spawn(ctrl_c(tx.clone()));
        let mut ctrl_c_invoked_once: bool = false;
        // the total number of children (accountants + one feeder(currently))
        let mut children_count = accountant_count + 1;
        while let Some(event) = rx.recv().await {
            match event {
                PaymentsEngineEvent::Accountant { result, .. } => {
                    // Abort the program if any accountant had critical error,
                    // currently it's not being utilized by the accountant
                    let clients = result?;
                    // merge the clients from the accountant into the provided writer, and abort the program if merge had any error
                    self.writer.async_merge(clients).await?;
                    // or state.books = state.books.into_iter().chain(book).collect();
                    // check whether to shutdown/break the event loop or not
                    children_count -= 1;
                    if children_count == 0 {
                        break;
                    }
                }
                PaymentsEngineEvent::Feeder(r) => {
                    // Ensure Feeder shutdown probably
                    // otherwise we abort the program
                    r?;
                    children_count -= 1;
                    if children_count == 0 {
                        break;
                    }
                }
                PaymentsEngineEvent::ExitProgram => {
                    if ctrl_c_invoked_once {
                        log::warn!("PaymentsEngine got aborted");
                        break;
                    }
                    ctrl_c_invoked_once = true;
                    // spawn another in case
                    tokio::spawn(ctrl_c(tx.clone()));
                    // Abort the feed source
                    log::info!("PaymentsEngine is gracefully shutting down");
                }
            }
        }
        log::info!("PaymentsEngine EOL");
        Ok(self)
    }
}

/// Useful function to exit program using ctrl_c signal
#[allow(unused)]
async fn ctrl_c(handle: tokio::sync::mpsc::UnboundedSender<PaymentsEngineEvent>) {
    // await on ctrl_c
    if let Ok(_) = tokio::signal::ctrl_c().await {
        let exit_program_event = PaymentsEngineEvent::ExitProgram;
        handle.send(exit_program_event).ok();
    };
}
