#[derive(Debug, PartialEq)]
/// This enum represents the different consistency levels that can be used in Cassandra queries.
///
/// Consistency levels control the number of replicas that must respond to a query before it is considered successful.
/// These levels help balance between performance and consistency requirements.
///
/// Each variant corresponds to a different level of consistency:
/// - `Any`: No consistency, any replica can respond.
/// - `One`: One replica must respond (least consistent, fastest).
/// - `Two`: Two replicas must respond.
/// - `Three`: Three replicas must respond.
/// - `Quorum`: A quorum of replicas must respond (majority).
/// - `All`: All replicas must respond (most consistent, but slowest).
/// - `LocalQuorum`: A quorum of replicas must respond within the local datacenter.
/// - `EachQuorum`: A quorum of replicas must respond in each datacenter.
/// - `Serial`: Used for lightweight transactions, ensuring consistency during writes.
/// - `LocalSerial`: Like `Serial`, but within the local datacenter.
/// - `LocalOne`: Ensures consistency for one replica in the local datacenter.
pub enum Consistency {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    Serial = 0x0008,
    LocalSerial = 0x0009,
    LocalOne = 0x000A,
}
