pub(crate) use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::{AddAssign, SubAssign};

#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Types {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct TransactionRecord {
    r#type: Types,
    client: Client,
    tx: Tx,
    amount: Option<Amount>,
}

/// Represents the clientId: wrapper around u16.
#[derive(
    Eq,
    PartialEq,
    std::hash::Hash,
    serde::Deserialize,
    serde::Serialize,
    Copy,
    Clone,
    Default,
    Debug,
)]
pub struct Client(u16);
impl std::fmt::Display for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Client ID: {}", self.0)
    }
}
/// Represents the TxId: wrapper around u32.
#[derive(
    serde::Deserialize,
    serde::Serialize,
    Copy,
    Clone,
    Default,
    std::cmp::PartialOrd,
    Eq,
    PartialEq,
    Hash,
    Debug,
)]
pub struct Tx(u32);
impl std::fmt::Display for Tx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Transaction ID: {}", self.0)
    }
}
/// Represents the Amount: wrapper around f32.
#[derive(
    serde::Deserialize,
    serde::Serialize,
    Copy,
    Clone,
    Default,
    std::cmp::PartialOrd,
    PartialEq,
    Debug,
)]

pub struct Amount(
    #[serde(serialize_with = "ser_four_places_precision_amount_f32")]
    #[serde(deserialize_with = "de_four_places_precision_amount_f32")]
    f32);
impl std::fmt::Display for Amount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Amount: {}", self.0)
    }
}

fn ser_four_places_precision_amount_f32<S>(v: &f32, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let v = f32::trunc(v * 10000.0) / 10000.0;
    s.serialize_f32(v)
}


fn de_four_places_precision_amount_f32<'de, D>(deserializer: D) -> Result<f32, D::Error>
where
	D: serde::Deserializer<'de>,
{
    struct FourPlacesPrecisionVisitor;
    impl<'de> serde::de::Visitor<'de> for FourPlacesPrecisionVisitor {
        type Value = f32;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            // NOTE for sake of example, we trunc addtional precision, so this will not be invoked
            formatter.write_str("The decimal amount must have a precision up at most to four places past the decimal")
        }

        fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            // NOTE for sake of example, we trunc addtional precision, and return Ok(_)
            // therefore expecting will not be invoked.
            Ok(f32::trunc(v * 10000.0) / 10000.0)
        }
    }

    // use our visitor to deserialize an `ActualValue`
    //deserializer.deserialize_any(JsonStringVisitor)

    deserializer.deserialize_f32(FourPlacesPrecisionVisitor)
}

/// Represents the Available amount: wrapper around Amount.
#[derive(
    serde::Deserialize,
    serde::Serialize,
    Copy,
    Clone,
    Default,
    std::cmp::PartialOrd,
    PartialEq,
    Debug,
)]
pub struct Available(Amount);
impl std::fmt::Display for Available {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Available Funds: {}", self.0 .0)
    }
}
/// Represents the Held amount: wrapper around Amount.
#[derive(
    serde::Deserialize,
    serde::Serialize,
    Copy,
    Clone,
    Default,
    std::cmp::PartialOrd,
    PartialEq,
    Debug,
)]
pub struct Held(Amount);
impl std::fmt::Display for Held {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Held {}", self.0)
    }
}
/// Represents the Total amount: wrapper around Amount.
#[derive(
    serde::Deserialize,
    serde::Serialize,
    Copy,
    Clone,
    Default,
    std::cmp::PartialOrd,
    PartialEq,
    Debug,
)]
pub struct Total(Amount);
impl std::fmt::Display for Total {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Total {}", self.0)
    }
}
/// Represents the Deposit amount: wrapper around Amount.
#[derive(
    serde::Deserialize,
    serde::Serialize,
    Copy,
    Clone,
    Default,
    std::cmp::PartialOrd,
    PartialEq,
    Debug,
)]
pub struct Deposit(Amount);
impl std::fmt::Display for Deposit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Deposit {}", self.0)
    }
}
impl std::convert::From<Amount> for Deposit {
    fn from(amount: Amount) -> Self {
        Deposit(amount)
    }
}

/// Represents the Withdrawal amount: wrapper around Amount.
#[derive(
    serde::Deserialize,
    serde::Serialize,
    Copy,
    Clone,
    Default,
    std::cmp::PartialOrd,
    PartialEq,
    Debug,
)]
pub struct Withdrawal(Amount);
impl std::fmt::Display for Withdrawal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Withdrawal {}", self.0)
    }
}

impl std::convert::From<Amount> for Withdrawal {
    fn from(amount: Amount) -> Self {
        Withdrawal(amount)
    }
}
/// Represents the Dispute amount: wrapper around Amount.
#[derive(
    serde::Deserialize,
    serde::Serialize,
    Copy,
    Clone,
    Default,
    std::cmp::PartialOrd,
    PartialEq,
    Debug,
)]
pub struct Dispute(Deposit);
impl std::fmt::Display for Dispute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Dispute {}", self.0)
    }
}
impl std::convert::Into<Dispute> for Deposit {
    fn into(self) -> Dispute {
        Dispute(self)
    }
}

#[derive(
    serde::Deserialize,
    serde::Serialize,
    Copy,
    Clone,
    Default,
    std::cmp::PartialOrd,
    PartialEq,
    Debug,
)]
/// Represents the Withdrawal amount: wrapper around Amount.
pub struct Resolve(Amount);
impl std::fmt::Display for Resolve {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Resolve {}", self.0)
    }
}
impl std::convert::Into<Resolve> for Amount {
    fn into(self) -> Resolve {
        Resolve(self)
    }
}

// helpful add assigns impls
impl AddAssign for Amount {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0;
    }
}
impl AddAssign for Available {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0;
    }
}
impl AddAssign for Total {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0;
    }
}
impl AddAssign for Held {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0;
    }
}
// helpful sub assigns impls
impl SubAssign for Amount {
    fn sub_assign(&mut self, other: Self) {
        self.0 -= other.0;
    }
}
impl SubAssign for Available {
    fn sub_assign(&mut self, other: Self) {
        self.0 -= other.0;
    }
}
impl SubAssign for Total {
    fn sub_assign(&mut self, other: Self) {
        self.0 -= other.0;
    }
}
impl SubAssign for Held {
    fn sub_assign(&mut self, other: Self) {
        self.0 -= other.0;
    }
}

/// Represents client transaction
// [repr(u8)] we could use #![feature(arbitrary_enum_discriminant)]
#[derive(Debug)]
pub enum ClientTransaction {
    Deposit(Deposit),
    Withdrawal(Withdrawal),
}

#[allow(unused)] // these implemented for fun
impl ClientTransaction {
    /// Check if the client transaction is deposit
    /// NOTE: this an extra method
    fn is_deposit(&self) -> bool {
        // note for fun: with [repr(u8)] #![feature(arbitrary_enum_discriminant)]
        // we can use unsafe transumte to cast the type or use the recent totally_speedy_transmute crate
        // and use bitwise operation to cast directly to true/false, which should save cpu pipeline from Jump instruction.
        // However am sure the code below will be optimized by the compiler
        match self {
            ClientTransaction::Deposit(_) => true,
            ClientTransaction::Withdrawal(_) => false,
        }
    }
    /// Check if the client transaction is withdrawal
    /// NOTE: this an extra method
    fn is_withdrawal(&self) -> bool {
        match self {
            ClientTransaction::Deposit(_) => false,
            ClientTransaction::Withdrawal(_) => true,
        }
    }
}

#[derive(Debug)]
pub enum PartnerTransaction {
    /// Partner disputing client's deposit transaction
    Dispute(Dispute),
    /// Partner resolving partner's dispute transaction
    Resolve(Dispute),
    /// Partner chraging back partner's dispute transaction
    Chargeback(Dispute),
}

impl std::fmt::Display for PartnerTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dispute(amount) => write!(f, "Dispute {}", amount),
            Self::Resolve(amount) => write!(f, "Resolve {}", amount),
            Self::Chargeback(amount) => write!(f, "Chargeback {}", amount),
        }
    }
}

/// Expected transaction data-structure
#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum TransactionEvent {
    Deposit {
        client: Client,
        tx: Tx,
        amount: Deposit,
    },
    Withdrawal {
        client: Client,
        tx: Tx,
        amount: Withdrawal,
    },
    Dispute {
        client: Client,
        tx: Tx,
    },
    Resolve {
        client: Client,
        tx: Tx,
    },
    Chargeback {
        client: Client,
        tx: Tx,
    },
}

impl std::convert::TryFrom<TransactionRecord> for TransactionEvent {
    type Error = anyhow::Error;
    fn try_from(transaction_record: TransactionRecord) -> Result<Self, Self::Error> {
        let ty = transaction_record.r#type;
        let client = transaction_record.client;
        let tx = transaction_record.tx;
        let mut amount_opt = transaction_record.amount;
        match ty {
            Types::Deposit => {
                // ensure amount is Some
                if let Some(amount) = amount_opt.take() {
                    Ok(TransactionEvent::Deposit {
                        client,
                        tx,
                        amount: amount.into(),
                    })
                } else {
                    anyhow::bail!("{}, {}, Deposit record should have amount", client, tx)
                }
            }
            Types::Withdrawal => {
                // ensure amount is Some
                if let Some(amount) = amount_opt.take() {
                    Ok(TransactionEvent::Withdrawal {
                        client,
                        tx,
                        amount: amount.into(),
                    })
                } else {
                    anyhow::bail!("{}, {}, Withdrawal record should have amount", client, tx)
                }
            }
            Types::Dispute => {
                // ensure amount is None
                if let None = amount_opt.take() {
                    Ok(TransactionEvent::Dispute { client, tx })
                } else {
                    anyhow::bail!("{}, {}, Dispute record shouldn't have amount", client, tx)
                }
            }
            Types::Resolve => {
                // ensure amount is None
                if let None = amount_opt.take() {
                    Ok(TransactionEvent::Resolve { client, tx })
                } else {
                    anyhow::bail!("{}, {}, Resolve record shouldn't have amount", client, tx)
                }
            }
            Types::Chargeback => {
                // ensure amount is None
                if let None = amount_opt.take() {
                    Ok(TransactionEvent::Chargeback { client, tx })
                } else {
                    anyhow::bail!(
                        "{}, {}, Chargeback record shouldn't have amount",
                        client,
                        tx
                    )
                }
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct ClientBook {
    /// The client's transactions
    transactions: HashMap<Tx, ClientTransaction>,
    /// The partner transactions for a given client
    partner_transactions: HashMap<Tx, PartnerTransaction>,
    /// The client's account
    account: ClientAccount,
}

impl From<HashMap<Client, ClientBook>> for Clients {
    fn from(mut book: HashMap<Client, ClientBook>) -> Self {
        let mut client_records = Vec::new();
        for (
            client,
            ClientBook {
                account:
                    ClientAccount {
                        available,
                        held,
                        total,
                        locked,
                    },
                ..
            },
        ) in book.drain()
        {
            let client_record = ClientRecord::new(client, available, held, total, locked);
            client_records.push(client_record)
        }
        client_records.into()
    }
}

impl ClientBook {
    /// Return ref to the client's transactions
    pub fn transactions(&self) -> &HashMap<Tx, ClientTransaction> {
        &self.transactions
    }
    /// Return ref to the partner's transactions
    pub fn partner_transactions(&self) -> &HashMap<Tx, PartnerTransaction> {
        &self.partner_transactions
    }
    /// Return ref to the client's account
    fn account(&self) -> &ClientAccount {
        &self.account
    }
    /// A deposit transaction,
    pub fn try_deposit(&mut self, tx: Tx, deposit_amount: Deposit) -> Result<(), anyhow::Error> {
        // ensure the account is not frozen/locked
        if self.account.is_locked() {
            anyhow::bail!("Cannot deposit. Account is frozen. please contact customer support.")
        }
        // ensure the transaction doesn't already exist
        if self.transactions.contains_key(&tx) {
            anyhow::bail!("{} already exist", tx)
        };
        // insert tx into the ClientTransactions
        self.transactions
            .insert(tx, ClientTransaction::Deposit(deposit_amount));
        // deposit the deposit amount into client's account
        self.account.deposit(deposit_amount);
        Ok(())
    }
    /// A withdrawal transaction,
    pub fn try_withdraw(
        &mut self,
        tx: Tx,
        withdrawal_amount: Withdrawal,
    ) -> Result<(), anyhow::Error> {
        // ensure the account is not frozen/locked
        if self.account.is_locked() {
            anyhow::bail!("Cannot withdraw. Account is frozen. please contact customer support.")
        }
        // ensure the transaction doesn't already exist
        if self.transactions.contains_key(&tx) {
            anyhow::bail!("Existing {}", tx)
        };
        // insert tx into the ClientTransactions
        self.transactions
            .insert(tx, ClientTransaction::Withdrawal(withdrawal_amount));
        // withdraw the withdrawal amount from the client's account
        self.account.try_withdraw(withdrawal_amount)?;
        Ok(())
    }
    /// dispute claim (for a client transaction)
    /// Assumption: once the account the is frozen, no further partners transactions can be executed
    pub fn try_dispute(&mut self, tx: Tx) -> Result<(), anyhow::Error> {
        if self.account.is_locked() {
            anyhow::bail!("Cannot dispute. Account is frozen, please contact business support.")
        }
        if let Some(client_transaction) = self.transactions.get(&tx) {
            if let ClientTransaction::Deposit(amount) = client_transaction {
                // insert tx dispute into the partner transactions
                self.partner_transactions
                    .insert(tx, PartnerTransaction::Dispute((*amount).into()));
                // dispute the deposit amount
                self.account.dispute(*amount);
                Ok(())
            } else {
                anyhow::bail!("Cannot dispute withdrawal {}", tx)
            }
        } else {
            anyhow::bail!("Dispute error on our partner's side {}", tx)
        }
    }
    /// resolve a dispute
    /// Assumption: once the account the is frozen, no further partners transactions can be executed
    pub fn try_resolve(&mut self, tx: Tx) -> Result<(), anyhow::Error> {
        if self.account.is_locked() {
            anyhow::bail!("Cannot resolve. Account is frozen, please contact business support.")
        }
        if let Some(partner_transaction) = self.partner_transactions.get_mut(&tx) {
            if let PartnerTransaction::Dispute(amount) = partner_transaction {
                // resolve the disputed amount
                self.account.resolve((*amount).into());
                // change dispute state to a resolve
                *partner_transaction = PartnerTransaction::Resolve(*amount);
                Ok(())
            } else {
                anyhow::bail!("Cannot resolve dispute {}", partner_transaction)
            }
        } else {
            anyhow::bail!("Resolve error on our partner's side {}", tx)
        }
    }
    /// chargeback a dispute
    /// Assumption: once the account the is frozen, no further partners transactions can be executed
    pub fn try_chargeback(&mut self, tx: Tx) -> Result<(), anyhow::Error> {
        if self.account.is_locked() {
            anyhow::bail!("Cannot chargeback. Account is frozen, please contact business support.")
        }
        if let Some(partner_transaction) = self.partner_transactions.get_mut(&tx) {
            if let PartnerTransaction::Dispute(amount) = partner_transaction {
                // chargeback the disputed amount
                self.account.chargeback(*amount);
                // change dispute state to a Chargeback
                *partner_transaction = PartnerTransaction::Chargeback(*amount);
                Ok(())
            } else {
                anyhow::bail!("Cannot chargeback dispute {}", partner_transaction)
            }
        } else {
            anyhow::bail!("Error on our partner's side {}", tx)
        }
    }
}
/// Client account
#[derive(Default, Debug)]
pub struct ClientAccount {
    available: Available,
    held: Held,
    total: Total,
    locked: bool,
}

impl ClientAccount {
    /// Check the available funds in the client's account
    pub fn available(&self) -> &Available {
        &self.available
    }
    /// A deposit is a credit to the client's asset account,
    /// meaning it should increase the available andtotal funds of the client account
    pub fn deposit(&mut self, amount: Deposit) {
        self.available.0 += amount.0;
        self.total.0 += amount.0;
    }
    /// A withdraw is a debit to the client's asset account,
    /// meaning it should decrease the available and total funds of the client account
    pub fn try_withdraw(&mut self, amount: Withdrawal) -> Result<(), anyhow::Error> {
        if self.available.0 < amount.0 {
            anyhow::bail!("a client does not have sufficient available funds")
        }
        self.available.0 -= amount.0;
        self.total.0 -= amount.0;
        Ok(())
    }
    /// Dispute claim amount from the account
    pub fn dispute(&mut self, amount: Deposit) {
        self.available.0 -= amount.0;
        self.held.0 += amount.0;
    }
    /// Resolve a dispute
    pub fn resolve(&mut self, amount: Dispute) {
        self.available.0 += amount.0 .0;
        self.held.0 -= amount.0 .0;
    }
    /// Chargeback a dispute
    pub fn chargeback(&mut self, amount: Dispute) {
        self.held.0 -= amount.0 .0;
        self.total.0 -= amount.0 .0;
        // froze the account.
        self.locked = true;
    }
    /// Helper method: Froze the account.
    #[allow(unused)] // in case in future we wanna froze an account due to some condition
    pub fn froze(&mut self) {
        self.locked = true;
    }
    /// Check if the account is locked/frozen.
    pub fn is_locked(&self) -> bool {
        self.locked
    }
}
/// Wrapper around Vec<ClientRecord>
#[derive(serde::Deserialize, serde::Serialize)]
pub struct Clients(Vec<ClientRecord>);
impl Clients {
    pub fn new() -> Self {
        Self(Vec::new())
    }
}
#[async_trait::async_trait]
impl AsyncFrom<Vec<ClientRecord>> for Clients {
    async fn async_from(book: Vec<ClientRecord>) -> Self {
        Clients::from(book)
    }
}
impl From<Vec<ClientRecord>> for Clients {
    fn from(vector: Vec<ClientRecord>) -> Self {
        Self(vector)
    }
}
impl std::iter::Extend<ClientRecord> for Clients {
    fn extend<T: IntoIterator<Item = ClientRecord>>(&mut self, iter: T) {
        self.0.extend(iter);
    }
}
impl IntoIterator for Clients {
    type Item = ClientRecord;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Client record data structure
#[derive(Default, serde::Deserialize, serde::Serialize)]
pub struct ClientRecord {
    client: Client,
    available: Available,
    held: Held,
    total: Total,
    locked: bool,
}

impl std::convert::From<Client> for ClientRecord {
    fn from(client: Client) -> Self {
        let mut client_record = Self::default();
        client_record.client = client;
        client_record
    }
}

impl ClientRecord {
    pub fn new(
        client: Client,
        available: Available,
        held: Held,
        total: Total,
        locked: bool,
    ) -> Self {
        Self {
            client,
            available,
            held,
            total,
            locked,
        }
    }
}

/// Dispatcher trait, should be implemented to dispatch the event from caller/producer to consumer(s)
pub trait Dispatch<T> {
    fn dispatch(&mut self, event: T) -> Result<(), anyhow::Error>;
}

#[derive(Clone)]
pub struct AccountantDispatcher {
    count: u16,
    accountant_handles: HashMap<u16, account::AccountantHandle>,
}

impl AccountantDispatcher {
    pub fn new(accountant_handles: HashMap<u16, account::AccountantHandle>) -> Self {
        Self {
            count: accountant_handles.len() as u16,
            accountant_handles,
        }
    }
}

impl std::ops::Deref for Client {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Dispatch<TransactionRecord> for AccountantDispatcher {
    fn dispatch(&mut self, event: TransactionRecord) -> Result<(), anyhow::Error> {
        let accountant_id = *event.client % self.count;
        if let Some(accountant_handle) = self.accountant_handles.get_mut(&accountant_id) {
            accountant_handle
                .send(TransactionEvent::try_from(event)?)
                .ok();
            Ok(())
        } else {
            let m = format!(
                "Unexpected: didn't find accountant handle for accountant_id: {}",
                accountant_id
            );
            anyhow::bail!(m)
        }
    }
}

#[async_trait::async_trait]
pub trait AsyncFrom<T> {
    async fn async_from(_: T) -> Self;
}
#[async_trait::async_trait]
impl AsyncFrom<HashMap<Client, ClientBook>> for Clients {
    async fn async_from(from: HashMap<Client, ClientBook>) -> Self {
        Self::from(from)
    }
}

#[async_trait::async_trait]
pub trait AsyncInto<T> {
    async fn async_into(self) -> T;
}

/// auto implementation;
#[async_trait::async_trait]
impl<A, T> AsyncInto<A> for T
where
    A: AsyncFrom<T>,
    T: Send,
{
    async fn async_into(self) -> A {
        A::async_from(self).await
    }
}

#[async_trait::async_trait]
pub trait AsyncMerge<T> {
    async fn async_merge(&mut self, with: T) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait]
impl<W> AsyncMerge<Clients> for csv_async::AsyncSerializer<W>
where
    W: tokio::io::AsyncWrite + Unpin + Send + Sync,
{
    async fn async_merge(&mut self, clients: Clients) -> Result<(), anyhow::Error> {
        for record in clients.into_iter().next() {
            self.serialize(record).await?
        }
        Ok(())
    }
}

mod account;
mod feed;

pub(crate) use account::Accountant;
pub(crate) use feed::Feed;
