use super::*;
#[derive(Clone)]
pub struct AccountantHandle {
    tx: tokio::sync::mpsc::UnboundedSender<TransactionEvent>,
}
pub struct AccountantInbox {
    rx: tokio::sync::mpsc::UnboundedReceiver<TransactionEvent>,
}
// derer impls
impl std::ops::Deref for AccountantHandle {
    type Target = tokio::sync::mpsc::UnboundedSender<TransactionEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}
impl std::ops::Deref for AccountantInbox {
    type Target = tokio::sync::mpsc::UnboundedReceiver<TransactionEvent>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}
/// Actor/Async task managing subset of clients
pub struct Accountant {
    inbox: AccountantInbox,
    book: HashMap<Client, ClientBook>,
}

impl Accountant {
    /// Create Accountant's channel
    pub fn channel() -> (AccountantHandle, AccountantInbox) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (AccountantHandle { tx }, AccountantInbox { rx })
    }
    /// Create new accountant
    pub fn new(inbox: AccountantInbox) -> Self {
        Self {
            inbox,
            book: std::collections::HashMap::new(),
        }
    }
    /// Run new accountant, and return T
    pub async fn run<T: super::AsyncFrom<HashMap<Client, ClientBook>>>(
        mut self,
    ) -> Result<T, anyhow::Error> {
        while let Some(event) = self.inbox.rx.recv().await {
            match event {
                TransactionEvent::Deposit { client, tx, amount } => {
                    let client_book = self
                        .book
                        .entry(client)
                        .or_insert_with(|| ClientBook::default());
                    // deposit the transaction into the client account
                    if let Err(e) = client_book.try_deposit(tx, amount) {
                        log::error!("Unabled to deposit client's transaction. {}", e);
                    };
                }
                TransactionEvent::Withdrawal { client, tx, amount } => {
                    let client_book = self
                        .book
                        .entry(client)
                        .or_insert_with(|| ClientBook::default());
                    // withdrawal the transaction into the client account
                    if let Err(e) = client_book.try_withdraw(tx, amount) {
                        let available = client_book.account().available();
                        log::error!(
                            "Unabled to withdraw client's transaction. {}, {}, {}, {}, {}",
                            client,
                            tx,
                            amount,
                            available,
                            e
                        );
                    };
                }
                TransactionEvent::Dispute { client, tx } => {
                    let client_book = self
                        .book
                        .entry(client)
                        .or_insert_with(|| ClientBook::default());
                    // dispute the transaction from the client book
                    if let Err(e) = client_book.try_dispute(tx) {
                        let available = client_book.account().available();
                        log::error!(
                            "Unabled to dispute partner's transaction. {}, {}, {}, {}",
                            client,
                            available,
                            tx,
                            e
                        );
                    };
                }
                TransactionEvent::Resolve { client, tx } => {
                    let client_book = self
                        .book
                        .entry(client)
                        .or_insert_with(|| ClientBook::default());
                    // Resolve a dispute from the client book
                    if let Err(e) = client_book.try_resolve(tx) {
                        let available = client_book.account().available();
                        log::error!(
                            "Unabled to resolve partner's transaction. {}, {}, {}, {}",
                            client,
                            available,
                            tx,
                            e
                        );
                    };
                }
                TransactionEvent::Chargeback { client, tx } => {
                    let client_book = self
                        .book
                        .entry(client)
                        .or_insert_with(|| ClientBook::default());
                    // Chargeback a dispute from the client book
                    if let Err(e) = client_book.try_chargeback(tx) {
                        let available = client_book.account().available();
                        log::error!(
                            "Unabled to chargeback partner's transaction. {}, {}, {}, {}",
                            client,
                            available,
                            tx,
                            e
                        );
                    };
                }
            }
        }
        // convert the book into the expected client result
        let r = self.book.async_into().await;
        Ok(r)
    }
}
