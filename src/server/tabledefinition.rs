use super::{
    clusteringkey::ClusteringKey, columntypes::ColumnTypes, data::Data, partitionkey::PartitionKey,
    sstable::SSTable,
};

pub type TableDefinition = (
    String,
    Data,
    Vec<String>,
    PartitionKey,
    ClusteringKey,
    ColumnTypes,
    SSTable,
);
