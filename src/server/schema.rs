use crate::{
    errors::error_types::ErrorTypes,
    protocol::{
        protocol_notations::consistency::Consistency,
        query_parser::{clause::Clause, parser_impl::parse_conditions, relation::Relation},
    },
    server::{
        gossiper::get_gossiper,
        keyspace::Keyspace,
        log_type::LogType,
        mem_table::{is_tombstone, MemTable},
        nodes::write_log_message,
    },
};
use chrono::{DateTime, FixedOffset};

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    net::TcpStream,
    sync::{Arc, Mutex, MutexGuard},
    thread::{self},
};

use super::{
    address::Address,
    node_message::{NodeMessage, SchemaChange},
    selectquery::{self, SelectQuery},
};
use chksum_md5 as md5;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug)]

/// This struct represents the schema of the node. It contains the version, the keyspaces, the actual keyspace and the commit log.
pub struct Schema {
    version: i32,
    keyspaces: HashMap<Keyspace, HashMap<String, Arc<Mutex<MemTable>>>>,
    actual_keyspace: Option<Keyspace>,
    port: String,
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let keyspaces: HashMap<_, HashMap<_, _>> = self
            .keyspaces
            .iter()
            .map(|(keyspace, tables)| {
                let tables = tables
                    .iter()
                    .map(|(name, memtable)| {
                        let memtable = memtable.lock().unwrap();
                        (name.clone(), memtable.clone())
                    })
                    .collect();
                (keyspace.clone(), tables)
            })
            .collect();

        (&self.version, &keyspaces, &self.actual_keyspace).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (version, keyspaces, actual_keyspace): (
            i32,
            HashMap<Keyspace, HashMap<String, MemTable>>,
            Option<Keyspace>,
        ) = Deserialize::deserialize(deserializer)?;

        // Reconstruir los `Arc<Mutex<MemTable>>` a partir de `MemTable`
        let keyspaces = keyspaces
            .into_iter()
            .map(|(keyspace, tables)| {
                let tables = tables
                    .into_iter()
                    .map(|(name, memtable)| (name, Arc::new(Mutex::new(memtable))))
                    .collect();
                (keyspace, tables)
            })
            .collect();

        Ok(Schema {
            version,
            keyspaces,
            actual_keyspace,
            port: "".to_string(),
        })
    }
}

impl Schema {
    pub fn new(port: &String) -> Result<Schema, ErrorTypes> {
        match Self::read_schema(port) {
            Ok(schema) => Ok(schema),
            _ => Ok(Schema {
                version: 0,
                keyspaces: HashMap::new(),
                actual_keyspace: None,
                port: port.to_string(),
            }),
        }
    }
    /// This function is responsible for setting a keyspace in the node.
    pub fn set_keyspace(&mut self, keyspace: &String) -> Result<Keyspace, ErrorTypes> {
        for key in self.keyspaces.keys() {
            if key.get_name() == keyspace {
                self.actual_keyspace = Some(key.clone());
                return Ok(key.clone());
            }
        }
        Err(ErrorTypes::new(540, "Keyspace not found".to_string()))
    }

    pub fn set_id(&mut self, id: &String) {
        self.port = id.to_string();
        for (_, tables) in self.keyspaces.iter_mut() {
            for (name, table) in tables.iter_mut() {
                table.lock().unwrap().set_id(id, name);
            }
        }
    }

    /// This function is responsible for incrementing the version of the schema.
    fn increment_version(&mut self) {
        self.version += 1;
    }

    /// This function is responsible for getting the primary key of a table.
    fn get_pk(&self, table_name: &str) -> Result<Vec<usize>, ErrorTypes> {
        let k_s = match &self.actual_keyspace {
            Some(k_s) => k_s,
            _ => return Err(ErrorTypes::new(541, "Keyspace not selected".to_string())),
        };
        let hash_mt = match self.keyspaces.get(k_s) {
            Some(hash_mt) => hash_mt,
            _ => return Err(ErrorTypes::new(542, "Keyspace not found".to_string())),
        };
        match hash_mt.get(table_name) {
            Some(table) => {
                let table = table.lock().unwrap();
                let p_k = table.get_pk().clone();
                drop(table);
                Ok(p_k)
            }
            _ => Err(ErrorTypes::new(543, "Table not found".to_string())),
        }
    }
    ///This function is responsible for creating a table in the node.
    pub fn create_table(
        &mut self,
        table_name: &String,
        columns_type: Vec<(String, String)>,
        clustering_key: Vec<String>,
        primary_key: Vec<String>,
        port: String,
    ) -> Result<MemTable, ErrorTypes> {
        if self.actual_keyspace.is_some() {
            match self
                .keyspaces
                .get_mut(&self.actual_keyspace.clone().unwrap())
            {
                Some(keyspaces) => {
                    let memtable = MemTable::new(
                        columns_type,
                        primary_key,
                        table_name.clone(),
                        clustering_key,
                        port,
                    );
                    keyspaces.insert(
                        table_name.to_string(),
                        Arc::new(Mutex::new(memtable.clone())),
                    );
                    self.increment_version();
                    Ok(memtable)
                }
                None => Err(ErrorTypes::new(544, "Keyspace not found".to_string())),
            }
        } else {
            Err(ErrorTypes::new(545, "Keyspace not selected".to_string()))
        }
    }

    /// This function is responsible for creating a keyspace in the node.
    pub fn create_keyspace(
        &mut self,
        keyspace_name: &String,
        replication: usize,
    ) -> Result<Keyspace, ErrorTypes> {
        for key in self.keyspaces.keys() {
            if key.get_name() == keyspace_name {
                return Err(ErrorTypes::new(546, "Keyspace already exists".to_string()));
            }
        }
        let new_keyspace = Keyspace::new(keyspace_name.to_string(), replication);
        self.keyspaces.insert(new_keyspace.clone(), HashMap::new());
        self.increment_version();
        Ok(new_keyspace)
    }

    pub fn execute_update(
        &mut self,
        table_name: String,
        column_value: HashMap<String, String>,
        conditions: Clause,
        address: String,
        consistency: Consistency,
    ) -> Result<(), ErrorTypes> {
        let replication = self.get_replication()?;
        let table = self.get_table(&table_name)?;

        let p_k = table
            .lock()
            .unwrap()
            .get_partition_key()
            .iter()
            .map(|(x, _)| x.clone())
            .collect::<Vec<String>>();
        let mut result: Vec<&String> = Vec::new();
        search_pk(&conditions, &mut result, &p_k);
        let gossiper = get_gossiper();
        let shared_table = Arc::clone(table);
        if let (Some(node), key) = gossiper.get_node(result) {
            update(
                address.clone(),
                key,
                table_name.clone(),
                column_value.clone(),
                conditions.clone(),
                &node,
                Arc::clone(table),
            );
            let replicas = gossiper.get_replicas(key, replication, &node)?;

            if consistency == Consistency::One {
                thread::spawn(move || {
                    for node in replicas.iter() {
                        update(
                            address.clone(),
                            key,
                            table_name.clone(),
                            column_value.clone(),
                            conditions.clone(),
                            node,
                            Arc::clone(&shared_table),
                        );
                    }
                });
                return Ok(());
            }

            if consistency == Consistency::Quorum {
                let mut replicas_completed = 0;
                let needed = (replication - 1) / 2 + if (replication - 1) % 2 == 0 { 0 } else { 1 };
                for node in replicas.iter() {
                    if replicas_completed == needed {
                        break;
                    } else {
                        update(
                            address.clone(),
                            key,
                            table_name.clone(),
                            column_value.clone(),
                            conditions.clone(),
                            node,
                            Arc::clone(table),
                        );
                    }
                    replicas_completed += 1;
                }
                let address_clone = address.clone();
                let table_name = table_name.clone();
                thread::spawn(move || {
                    for node in replicas[replicas_completed..].iter() {
                        update(
                            address_clone.clone(),
                            key,
                            table_name.clone(),
                            column_value.clone(),
                            conditions.clone(),
                            node,
                            Arc::clone(&shared_table),
                        );
                    }
                });
                return Ok(());
            }
        }
        Err(ErrorTypes::new(547, "Error getting node".to_string()))
    }

    /// This function is responsible for executing the select query.
    pub fn execute_select(
        &mut self,
        info_select: (String, Clause, Vec<String>, Vec<String>),
        address: &Address,
        consistency: Consistency,
    ) -> Result<Vec<Vec<String>>, ErrorTypes> {
        let replication = self.get_replication()?;
        let table = self.get_table(&info_select.0)?;
        let table_lock = table.lock().unwrap();
        let p_k = table_lock
            .get_partition_key()
            .iter()
            .map(|(x, _)| x.clone())
            .collect::<Vec<String>>();
        let mut result: Vec<&String> = Vec::new();
        search_pk(&info_select.1, &mut result, &p_k);
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut found = false;
        let gossiper = get_gossiper();
        if let (Some(node), key) = gossiper.get_node(result) {
            let mut replicas = gossiper.get_replicas(key, replication, &node)?;
            replicas.insert(0, node.clone());
            let mut node = node;
            for replica in replicas.iter() {
                if gossiper.is_down(replica) {
                    continue;
                }
                let query = SelectQuery {
                    conditions: &info_select.1,
                    selected_columns: &info_select.2,
                    order: &info_select.3,
                    table_name: &info_select.0,
                    needs_ts: true,
                    needs_tb: true,
                };

                match select(address.clone(), replica, &table_lock, query) {
                    Ok(rows_) => {
                        rows = rows_;
                        found = true;
                        node = replica.to_string();
                        break;
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
            if !found {
                return Err(ErrorTypes::new(548, "Unrecheable data".to_string()));
            }
            drop(table_lock);
            if consistency == Consistency::One {
                let rows_clone = rows.clone();
                let node_clone = node.clone();
                let table = Arc::clone(table);
                let i_adr = address.i_address.clone();
                thread::spawn(move || {
                    check_read_repair(
                        info_select,
                        rows_clone,
                        i_adr,
                        &node_clone,
                        key,
                        table,
                        replicas.clone(),
                    )
                });
                let mut send_rows = Vec::new();
                send_rows.push(rows.remove(0));
                for mut row in rows {
                    if is_tombstone(&row) {
                        continue;
                    }
                    row.pop();
                    send_rows.push(row);
                }
                return Ok(send_rows);
            }
            if consistency == Consistency::Quorum {
                let rows_clone = rows.clone();
                let table = Arc::clone(table);
                let replicas_clone = replicas.clone();
                let failed = check_read_repair(
                    info_select.clone(),
                    rows_clone.clone(),
                    address.i_address.clone(),
                    &node,
                    key,
                    Arc::clone(&table),
                    replicas.clone(),
                );
                if failed == 0 {
                    let mut send_rows = Vec::new();
                    send_rows.push(rows.remove(0));
                    for mut row in rows {
                        if is_tombstone(&row) {
                            continue;
                        }
                        row.pop();
                        send_rows.push(row);
                    }
                    return Ok(send_rows);
                } else {
                    write_log_message(&address.i_port, LogType::Info, "Read repair".to_string());

                    return read_repair(
                        rows,
                        replicas_clone,
                        &node,
                        info_select,
                        address,
                        &table,
                        key,
                    );
                }
            }
        }
        Err(ErrorTypes::new(530, "Error getting node".to_string()))
    }

    pub fn execute_delete(
        &mut self,
        table_name: String,
        conditions: Clause,
        address: String,
        consistency: Consistency,
    ) -> Result<(), ErrorTypes> {
        let replication = self.get_replication()?;
        let table = self.get_table(&table_name)?;
        let gossiper = get_gossiper();

        let p_k = table
            .lock()
            .unwrap()
            .get_partition_key()
            .iter()
            .map(|(x, _)| x.clone())
            .collect::<Vec<String>>();

        let mut result: Vec<&String> = Vec::new();
        search_pk(&conditions, &mut result, &p_k);

        if let (Some(node), key) = gossiper.get_node(result) {
            delete(
                address.clone(),
                table_name.clone(),
                conditions.clone(),
                &node,
                Arc::clone(table),
            );

            let replicas = gossiper.get_replicas(key, replication, &node)?;
            let shared_table = Arc::clone(table);
            let address_clone = address.clone();
            let table_name = table_name.clone();
            if consistency == Consistency::One {
                thread::spawn(move || {
                    for node in replicas.iter() {
                        delete(
                            address_clone.clone(),
                            table_name.clone(),
                            conditions.clone(),
                            node,
                            Arc::clone(&shared_table),
                        );
                    }
                });
                return Ok(());
            }

            if consistency == Consistency::Quorum {
                let mut replicas_completed = 0;
                let needed = (replication - 1) / 2 + if (replication - 1) % 2 == 0 { 0 } else { 1 };
                for node in replicas.iter() {
                    if replicas_completed == needed {
                        break;
                    } else {
                        delete(
                            address.clone(),
                            table_name.clone(),
                            conditions.clone(),
                            node,
                            Arc::clone(table),
                        );
                    }
                    replicas_completed += 1;
                }
                thread::spawn(move || {
                    for node in replicas[replicas_completed..].iter() {
                        delete(
                            address_clone.clone(),
                            table_name.clone(),
                            conditions.clone(),
                            node,
                            Arc::clone(&shared_table),
                        );
                    }
                });
                return Ok(());
            }
        }
        Err(ErrorTypes::new(549, "Error getting node".to_string()))
    }

    /// This function is responsible for executing the insert query.
    pub fn execute_insert(
        &mut self,
        table_name: String,
        values: Vec<Vec<String>>,
        columns: Vec<String>,
        address: &Address,
        consistency: Consistency,
    ) -> Result<(), ErrorTypes> {
        let gossiper = get_gossiper();
        let replication = self.get_replication()?;
        let p_k = self.get_pk(&table_name)?;
        let table = self.get_table(&table_name)?;
        for row in values {
            let mut values_to_hash = Vec::new();
            for (i, value) in row.iter().enumerate() {
                if p_k.contains(&i) {
                    values_to_hash.push(value);
                }
            }
            if let (Some(node), key) = gossiper.get_node(values_to_hash) {
                insert(
                    address,
                    key,
                    row.clone(),
                    &table_name,
                    &columns,
                    Arc::clone(table),
                    &node,
                );

                let shared_table_clone = Arc::clone(table);

                let replicas = gossiper.get_replicas(key, replication, &node)?;
                if consistency == Consistency::One {
                    let address_clone = address.clone();
                    let table_name = table_name.clone();
                    let columns = columns.clone();
                    thread::spawn(move || {
                        for replica in replicas.iter() {
                            insert(
                                &address_clone.clone(),
                                key,
                                row.clone(),
                                &table_name.clone(),
                                &columns,
                                Arc::clone(&shared_table_clone),
                                replica,
                            );
                        }
                    });
                } else if consistency == Consistency::Quorum {
                    let mut replicas_completed = 0;

                    let needed =
                        (replication - 1) / 2 + if (replication - 1) % 2 == 0 { 0 } else { 1 };
                    for node in replicas.iter() {
                        if replicas_completed == needed {
                            break;
                        } else {
                            insert(
                                address,
                                key,
                                row.clone(),
                                &table_name.clone(),
                                &columns,
                                Arc::clone(table),
                                node,
                            );
                        }
                        replicas_completed += 1;
                    }
                    let table_name = table_name.clone();
                    let address_clone = address.clone();
                    let columns = columns.clone();
                    thread::spawn(move || {
                        for replica in replicas[replicas_completed..].iter() {
                            insert(
                                &address_clone.clone(),
                                key,
                                row.clone(),
                                &table_name.clone(),
                                &columns,
                                Arc::clone(&shared_table_clone),
                                replica,
                            );
                        }
                    });
                }
            }
        }
        Ok(())
    }

    ///This function is responsible for executing the node message.
    pub fn execute_node_message(
        &mut self,
        message: NodeMessage,
        client_stream: &mut TcpStream,
    ) -> Result<(), ErrorTypes> {
        match message {
            NodeMessage::SchemaChange(schema_change) => match schema_change {
                SchemaChange::CreateKeyspace(keyspace) => {
                    self.keyspaces.insert(keyspace, HashMap::new());
                    self.increment_version();
                    let msg = NodeMessage::Confirmation();
                    client_stream.write_all(&msg.to_bytes()).unwrap();
                    Ok(())
                }
                SchemaChange::CreateTable(mut memtable) => {
                    let table_name = memtable.table_name.clone();
                    if let Some(keyspace) = self.actual_keyspace.clone() {
                        memtable.set_id(&self.port, &table_name);
                        self.keyspaces
                            .get_mut(&keyspace)
                            .unwrap()
                            .insert(table_name.clone(), Arc::new(Mutex::new(*memtable)));
                        self.increment_version();
                        let msg = NodeMessage::Confirmation();
                        client_stream.write_all(&msg.to_bytes()).unwrap();
                        return Ok(());
                    }
                    Err(ErrorTypes::new(550, "Keyspace not selected".to_string()))
                }
                SchemaChange::UseKeyspace(keyspace) => {
                    if self.keyspaces.contains_key(&keyspace) {
                        self.actual_keyspace = Some(keyspace);
                        self.increment_version();
                        let msg = NodeMessage::Confirmation();
                        client_stream.write_all(&msg.to_bytes()).unwrap();
                        Ok(())
                    } else {
                        Err(ErrorTypes::new(551, "Keyspace not found".to_string()))
                    }
                }
            },
            NodeMessage::Insert(columns, values, table_name, key) => {
                write_log_message(
                    &self.port,
                    LogType::Info,
                    format!("Insert {:?} in {}", values, table_name),
                );
                let mut table = self.get_table(&table_name)?.lock().unwrap();
                table.insert_row(key, values.clone(), columns, None, None)?;
                let msg = NodeMessage::Confirmation();
                client_stream.write_all(&msg.to_bytes()).unwrap();
                Ok(())
            }
            NodeMessage::SelectRequest(
                conditions,
                selected_columns,
                order,
                table_name,
                needs_ts,
            ) => {
                let table = self.get_table(&table_name)?.lock().unwrap();
                let result =
                    table.execute_select(&conditions, &selected_columns, &order, needs_ts, true)?;
                let response = NodeMessage::SelectResponse(result);
                client_stream.write_all(&response.to_bytes()).unwrap();
                Ok(())
            }
            NodeMessage::Update(key, table_name, column_value, conditions) => {
                let mut table = self.get_table(&table_name)?.lock().unwrap();
                table.insert_row(key, vec![], vec![], Some(conditions), Some(column_value))?;
                drop(table);
                let msg = NodeMessage::Confirmation();
                client_stream.write_all(&msg.to_bytes()).unwrap();
                Ok(())
            }
            NodeMessage::Delete(table_name, conditions) => {
                let mut table = self.get_table(&table_name)?.lock().unwrap();
                table.execute_delete(conditions)?;
                let msg = NodeMessage::Confirmation();
                client_stream.write_all(&msg.to_bytes()).unwrap();
                Ok(())
            }
            NodeMessage::ChecksumRequest(conditions, selected_columns, order, table_name) => {
                let table = self.get_table(&table_name)?.lock().unwrap();
                let mut result =
                    table.execute_select(&conditions, &selected_columns, &order, false, true)?;
                result.remove(0);
                if let Ok(checksum) = md5::chksum(
                    result
                        .iter()
                        .flatten()
                        .map(|s| s.to_string())
                        .collect::<String>(),
                ) {
                    let msg = NodeMessage::ChecksumResponse(checksum.to_hex_lowercase());
                    client_stream.write_all(&msg.to_bytes()).unwrap();
                    Ok(())
                } else {
                    Err(ErrorTypes::new(
                        552,
                        "Error calculating checksum".to_string(),
                    ))
                }
            }
            NodeMessage::TransferFromNode(node) => self.transfer_from_node(&node),
            NodeMessage::RemoveNode(node) => {
                write_log_message(&self.port, LogType::Info, format!("Removing {}", node));
                let gossiper = get_gossiper();
                gossiper.remove_node(&node);

                Err(ErrorTypes::new(565, "Removed node".to_string()))
            }
            _ => Err(ErrorTypes::new(553, "Unexpected message".to_string())),
        }
    }

    fn transfer_from_node(&mut self, node: &String) -> Result<(), ErrorTypes> {
        let gossiper = get_gossiper();
        for keyspace in self.keyspaces.keys() {
            let rf = keyspace.replication;
            let partitions = gossiper.get_partitions_remove(node, rf);
            for table in self.keyspaces.get(keyspace).unwrap().values() {
                let mut table_lock = table.lock().unwrap();
                for (obj, partition) in partitions.iter() {
                    let rows = table_lock.get_rows(partition);
                    for (key, row) in rows {
                        table_lock.delete_rows(&key);
                        let msg = NodeMessage::Insert(
                            table_lock.columns.clone(),
                            row.clone(),
                            table_lock.table_name.clone(),
                            key,
                        );
                        if let Some(sender) = gossiper.get_sender(obj) {
                            if sender.send(msg.to_bytes()).is_err() {
                                return Err(ErrorTypes::new(
                                    553,
                                    "Error sending message to node".to_string(),
                                ));
                            }
                            let p = obj.split(":").collect::<Vec<&str>>();
                            write_log_message(
                                &p[1].to_string(),
                                LogType::Info,
                                format!("Sending {:?} to {}", row, obj),
                            );
                        }
                    }
                }
            }
        }
        let p = node.split(":").collect::<Vec<&str>>();
        write_log_message(
            &p[1].to_string(),
            LogType::Info,
            "Removing myself".to_string(),
        );
        let neighours = gossiper.get_neighbours();
        let msg = NodeMessage::RemoveNode(node.to_string());
        for node in neighours.iter() {
            if let Some(sender) = gossiper.get_sender(node) {
                if sender.send(msg.to_bytes()).is_err() {
                    return Err(ErrorTypes::new(
                        553,
                        "Error sending message to node".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn new_node(&self, new_node: &String, local_address: &String) {
        let p = local_address.split(":").collect::<Vec<&str>>();
        write_log_message(
            &p[1].to_string(),
            LogType::Info,
            format!("New node {}", new_node),
        );
        for keyspace in self.keyspaces.keys() {
            let rf = keyspace.replication;
            let gossiper = get_gossiper();
            let partitions = gossiper.get_partitions(new_node, local_address, rf);
            for table in self.keyspaces.get(keyspace).unwrap().values() {
                let mut table_lock = table.lock().unwrap();
                let rows = table_lock.get_rows(&partitions);
                for (key, row) in rows {
                    table_lock.delete_rows(&key);
                    let msg = NodeMessage::Insert(
                        table_lock.columns.clone(),
                        row.clone(),
                        table_lock.table_name.clone(),
                        key,
                    );

                    loop {
                        if let Some(sender) = gossiper.get_sender(new_node) {
                            if sender.send(msg.to_bytes()).is_ok() {
                                write_log_message(
                                    &p[1].to_string(),
                                    LogType::Info,
                                    format!(
                                        "Sending {:?} to {} {}",
                                        row, new_node, table_lock.table_name
                                    ),
                                );
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
    fn get_table(&mut self, table_name: &str) -> Result<&Arc<Mutex<MemTable>>, ErrorTypes> {
        let k_s = match &self.actual_keyspace {
            Some(k_s) => k_s,
            _ => return Err(ErrorTypes::new(554, "Keyspace not selected".to_string())),
        };
        let hash_mt = match self.keyspaces.get_mut(k_s) {
            Some(hash_mt) => hash_mt,
            _ => return Err(ErrorTypes::new(555, "Keyspace not found".to_string())),
        };
        match hash_mt.get(&table_name.to_owned()) {
            Some(table) => Ok(table),
            _ => Err(ErrorTypes::new(556, "Table not found".to_string())),
        }
    }

    fn get_replication(&self) -> Result<usize, ErrorTypes> {
        let k_s = match &self.actual_keyspace {
            Some(k_s) => k_s,
            _ => return Err(ErrorTypes::new(557, "Keyspace not selected".to_string())),
        };
        Ok(k_s.replication)
    }

    pub fn save_schema(&self) -> Result<(), ErrorTypes> {
        let serialized = serde_json::to_string(&self).unwrap();
        let mut file = std::fs::File::create("schema.json").unwrap();
        file.write_all(serialized.as_bytes()).unwrap();
        Ok(())
    }
    pub fn read_schema(id: &String) -> Result<Schema, ErrorTypes> {
        if let Ok(file) = std::fs::File::open("schema.json") {
            if let Ok(mut schema) = serde_json::from_reader::<File, Schema>(file) {
                schema.set_id(id);
                return Ok(schema);
            }
            return Err(ErrorTypes::new(558, "There is not a schema.".to_string()));
        }
        Err(ErrorTypes::new(559, "There is not a schema.".to_string()))
    }
}

fn redirect_select(
    conditions: &Clause,
    selected_columns: &[String],
    order: &[String],
    node: &String,
    table_name: &str,
    needs_ts: bool,
) -> Result<Vec<Vec<String>>, ErrorTypes> {
    let msg = NodeMessage::SelectRequest(
        conditions.clone(),
        selected_columns.to_owned(),
        order.to_owned(),
        table_name.to_owned(),
        needs_ts,
    );
    let gossiper = get_gossiper();
    if let Some(sender) = gossiper.get_sender(node) {
        if sender.send(msg.to_bytes()).is_err() {
            return Err(ErrorTypes::new(
                560,
                "Error sending message to node".to_string(),
            ));
        }
        let bytes = gossiper.receive(node);
        if bytes.is_empty() {
            return Err(ErrorTypes::new(
                565,
                "Couldn't receive the message".to_string(),
            ));
        }
        let message = NodeMessage::from_bytes(bytes[1..].to_vec());

        match message {
            NodeMessage::SelectResponse(result) => return Ok(result),
            _ => return Err(ErrorTypes::new(561, "Unexpected message".to_string())),
        }
    }

    Err(ErrorTypes::new(562, "Error getting sender".to_string()))
}

fn get_checksum(
    conditions: &Clause,
    selected_columns: &[String],
    order: &[String],
    node: &String,
    table_name: &str,
    address: &String,
    table: &Arc<Mutex<MemTable>>,
) -> Result<String, ErrorTypes> {
    if address == node {
        let result = table.lock().unwrap().execute_select(
            conditions,
            selected_columns,
            order,
            false,
            false,
        )?;
        let mut result = result.clone();
        result.remove(0);
        if let Ok(checksum) = md5::chksum(
            result
                .iter()
                .flatten()
                .map(|s| s.to_string())
                .collect::<String>(),
        ) {
            return Ok(checksum.to_hex_lowercase());
        }
    }

    let msg = NodeMessage::ChecksumRequest(
        conditions.clone(),
        selected_columns.to_owned(),
        order.to_owned(),
        table_name.to_owned(),
    );
    let gossiper = get_gossiper();
    if let Some(sender) = gossiper.get_sender(node) {
        if sender.send(msg.to_bytes()).is_err() {
            return Err(ErrorTypes::new(
                563,
                "Error sending message to node".to_string(),
            ));
        }
        let bytes = gossiper.receive(node);
        let message = NodeMessage::from_bytes(bytes[1..].to_vec());
        match message {
            NodeMessage::ChecksumResponse(checksum) => return Ok(checksum),
            _ => return Err(ErrorTypes::new(564, "Unexpected message".to_string())),
        }
    }

    Err(ErrorTypes::new(565, "Error getting sender".to_string()))
}

///This function is responsible for redirecting the insert to the correct node.
fn redirect_insert(
    values: Vec<String>,
    columns: &[String],
    node: &String,
    key: u128,
    table_name: String,
) -> Result<(), ErrorTypes> {
    let msg = NodeMessage::Insert(columns.to_vec(), values, table_name, key);
    let gossiper = get_gossiper();
    if let Some(sender) = gossiper.get_sender(node) {
        if sender.send(msg.to_bytes()).is_err() {
            return Err(ErrorTypes::new(
                566,
                "Error sending message to node".to_string(),
            ));
        }
        let id = node.split(":").collect::<Vec<&str>>()[1].to_string();
        write_log_message(&id, LogType::Info, "Inserting".to_string());
        let bytes = gossiper.receive(node);
        if bytes.is_empty() {
            return Err(ErrorTypes::new(
                565,
                "Couldn't receive the message".to_string(),
            ));
        }
        let message = NodeMessage::from_bytes(bytes[1..].to_vec());

        match message {
            NodeMessage::Confirmation() => return Ok(()),
            _ => return Err(ErrorTypes::new(567, "Unexpected message".to_string())),
        }
    }
    Err(ErrorTypes::new(568, "Error getting sender".to_string()))
}

///This function is responsible for searching the primary key in the conditions of a select query.
fn search_pk<'a>(clause: &'a Clause, result: &mut Vec<&'a String>, pk: &Vec<String>) {
    match clause {
        Clause::And { left, right } => {
            search_pk(left, result, pk);
            search_pk(right, result, pk);
        }
        Clause::Or { left, right } => {
            search_pk(left, result, pk);
            search_pk(right, result, pk);
        }
        Clause::Not { right } => search_pk(right, result, pk),
        Clause::Term {
            relation: Relation::Equal { v1, v2 },
        } => {
            if pk.contains(v1) {
                result.push(v2);
            }
        }
        _ => {}
    }
}

fn redirect_update(
    node: &String,
    key: u128,
    table_name: String,
    column_value: HashMap<String, String>,
    conditions: Clause,
) -> Result<(), ErrorTypes> {
    let msg = NodeMessage::Update(key, table_name, column_value, conditions);
    let gossiper = get_gossiper();
    if let Some(sender) = gossiper.get_sender(node) {
        if sender.send(msg.to_bytes()).is_err() {
            return Err(ErrorTypes::new(
                569,
                "Error sending message to node".to_string(),
            ));
        }
        let id = node.split(":").collect::<Vec<&str>>()[1].to_string();
        write_log_message(&id, LogType::Info, "Updating".to_string());
        let bytes = gossiper.receive(node);
        if bytes.is_empty() {
            return Err(ErrorTypes::new(
                565,
                "Couldn't receive the message".to_string(),
            ));
        }
        let message = NodeMessage::from_bytes(bytes[1..].to_vec());

        match message {
            NodeMessage::Confirmation() => return Ok(()),
            _ => return Err(ErrorTypes::new(561, "Unexpected message".to_string())),
        }
    }
    Err(ErrorTypes::new(570, "Error getting sender".to_string()))
}

fn redirect_delete(
    node: &String,
    table_name: String,
    conditions: Clause,
) -> Result<(), ErrorTypes> {
    let msg = NodeMessage::Delete(table_name, conditions);
    let gossiper = get_gossiper();
    if let Some(sender) = gossiper.get_sender(node) {
        if sender.send(msg.to_bytes()).is_err() {
            return Err(ErrorTypes::new(
                571,
                "Error sending message to node".to_string(),
            ));
        }
        let id = node.split(":").collect::<Vec<&str>>()[1].to_string();
        write_log_message(&id, LogType::Info, "Deleting".to_string());
        let bytes = gossiper.receive(node);
        if bytes.is_empty() {
            return Err(ErrorTypes::new(
                565,
                "Couldn't receive the message".to_string(),
            ));
        }
        let message = NodeMessage::from_bytes(bytes[1..].to_vec());

        match message {
            NodeMessage::Confirmation() => return Ok(()),
            _ => return Err(ErrorTypes::new(572, "Unexpected message".to_string())),
        }
    }
    Err(ErrorTypes::new(573, "Error getting sender".to_string()))
}

fn insert(
    address: &Address,
    key: u128,
    row: Vec<String>,
    table_name: &str,
    columns: &[String],
    table: Arc<Mutex<MemTable>>,
    node: &String,
) {
    if *node == address.i_address {
        if table
            .lock()
            .unwrap()
            .insert_row(key, row.clone(), columns.to_vec(), None, None)
            .is_ok()
        {
            write_log_message(&address.i_port, LogType::Info, "Inserting".to_string());
        }
        return;
    }
    if redirect_insert(row.clone(), columns, node, key, table_name.to_owned()).is_ok() {
        write_log_message(
            &address.i_port,
            LogType::Info,
            format!("Redirecting insert to {}", node),
        );
    }
}

fn update(
    address: String,
    key: u128,
    table_name: String,
    column_value: HashMap<String, String>,
    conditions: Clause,
    node: &String,
    table: Arc<Mutex<MemTable>>,
) {
    if address == *node {
        let _ = table.lock().unwrap().insert_row(
            key,
            vec![],
            vec![],
            Some(conditions.clone()),
            Some(column_value.clone()),
        );
        let id = address.split(":").collect::<Vec<&str>>()[1].to_string();
        write_log_message(&id, LogType::Info, "Updating".to_string());
    } else {
        let _ = redirect_update(
            node,
            key,
            table_name.clone(),
            column_value.clone(),
            conditions.clone(),
        );
        let id = address.split(":").collect::<Vec<&str>>()[1].to_string();
        write_log_message(
            &id,
            LogType::Info,
            format!("Redirecting update to {}", node),
        );
    }
}

fn delete(
    address: String,
    table_name: String,
    conditions: Clause,
    node: &String,
    table: Arc<Mutex<MemTable>>,
) {
    if address == *node {
        let _ = table.lock().unwrap().execute_delete(conditions.clone());
        let id = address.split(":").collect::<Vec<&str>>()[1].to_string();
        write_log_message(&id, LogType::Info, "Deleting".to_string());
    } else {
        let _ = redirect_delete(node, table_name.clone(), conditions.clone());
        let id = address.split(":").collect::<Vec<&str>>()[1].to_string();
        write_log_message(
            &id,
            LogType::Info,
            format!("Redirecting delete to {}", node),
        );
    }
}

fn select(
    address: Address,
    node: &String,
    table: &MutexGuard<MemTable>,
    query: selectquery::SelectQuery,
) -> Result<Vec<Vec<String>>, ErrorTypes> {
    if address.i_address == *node {
        return table.execute_select(
            query.conditions,
            query.selected_columns,
            query.order,
            query.needs_ts,
            query.needs_tb,
        );
    }
    redirect_select(
        query.conditions,
        query.selected_columns,
        query.order,
        node,
        query.table_name,
        query.needs_ts,
    )
}

fn check_read_repair(
    info_select: (String, Clause, Vec<String>, Vec<String>),
    rows: Vec<Vec<String>>,
    address: String,
    node: &String,
    _key: u128,
    table: Arc<Mutex<MemTable>>,
    replicas: Vec<String>,
) -> usize {
    let mut failed = 0;
    let mut rows_no_ts = rows.clone();
    rows_no_ts.iter_mut().for_each(|x| {
        x.pop();
    });
    rows_no_ts.remove(0);
    let mut set = HashSet::new();
    if let Ok(checksum) = md5::chksum(
        rows_no_ts
            .iter()
            .flatten()
            .map(|s| s.to_string())
            .collect::<String>(),
    ) {
        set.insert(checksum.to_hex_lowercase());
    }
    for replica in replicas.iter() {
        if replica == node {
            continue;
        }
        if let Ok(checksum_replica) = get_checksum(
            &info_select.1,
            &info_select.2,
            &[],
            replica,
            &info_select.0,
            &address,
            &table,
        ) {
            if !set.contains(&checksum_replica) {
                failed += 1;
            }
        }
    }
    failed
}

fn read_repair(
    mut rows: Vec<Vec<String>>,
    replicas: Vec<String>,
    node: &String,
    info_select: (String, Clause, Vec<String>, Vec<String>),
    address: &Address,
    table: &Arc<Mutex<MemTable>>,
    key: u128,
) -> Result<Vec<Vec<String>>, ErrorTypes> {
    let mut hash = HashMap::new();
    rows.remove(0);
    let len = rows.len();
    hash.insert(node, rows);
    for replica in replicas.iter() {
        if replica == node {
            continue;
        }
        let query = SelectQuery {
            conditions: &info_select.1,
            selected_columns: &info_select.2,
            order: &info_select.3,
            table_name: &info_select.0,
            needs_ts: true,
            needs_tb: true,
        };
        if let Ok(mut rows) = select(address.clone(), replica, &table.lock().unwrap(), query) {
            rows.remove(0);
            hash.insert(replica, rows);
        }
    }
    let mut pointers = vec![0; replicas.len()];
    let mut new_rows = Vec::new();
    while pointers[0] < len {
        let mut to_repair: Vec<&String> = Vec::new();
        let mut to_insert: Vec<&String> = Vec::new();
        let mut max_ts: (&String, DateTime<FixedOffset>, Vec<String>) =
            (&Default::default(), Default::default(), Vec::new());
        for (i, (replica, rows)) in hash.iter().enumerate() {
            if let Some(row) = rows.get(pointers[i]) {
                if let Ok(ts) = DateTime::parse_from_rfc3339(row.last().unwrap()) {
                    let row = row[0..row.len() - 1].to_vec();
                    if i == 0 {
                        max_ts = (replica, ts, row.to_vec());
                    } else {
                        if ts > max_ts.1 {
                            to_repair.push(max_ts.0);
                            max_ts = (replica, ts, row.clone());
                        }
                        if row != max_ts.2 {
                            to_repair.push(*replica);
                        }
                    }
                }
            } else {
                to_insert.push(*replica);
            }
        }
        for pointer in pointers.iter_mut() {
            *pointer += 1;
        }
        new_rows.push(max_ts.2.clone());
        if to_repair.is_empty() {
            continue;
        }
        write_log_message(
            &address.i_port,
            LogType::Info,
            format!(
                "I send to {:?}, the row: {:?} to repair",
                to_repair, max_ts.2
            ),
        );
        write_log_message(
            &address.i_port,
            LogType::Info,
            format!("I send to {:?} to insert", to_insert),
        );
        let mut hash = HashMap::new();
        for (i, column) in info_select.2.iter().enumerate() {
            hash.insert(column.to_string(), max_ts.2[i].to_string());
        }
        let pk = table.lock().unwrap().get_primary_key();
        let mut condition = Vec::new();
        let mut columns_to_update = info_select.2.clone();
        for (i, (column, pos)) in pk.iter().enumerate() {
            if hash.contains_key(column) {
                hash.remove(column);
            }
            columns_to_update.remove(*pos);
            condition.push(format!("{} = {}", column, max_ts.2[*pos]));
            if i == pk.len() - 1 {
                break;
            }
            condition.push("AND".to_string());
        }

        let conditiona = parse_conditions(condition).unwrap();
        for node in to_repair.iter() {
            update(
                address.i_address.clone(),
                key,
                info_select.0.clone(),
                hash.clone(),
                conditiona.clone(),
                node,
                Arc::clone(table),
            );
        }
        for node in to_insert.iter() {
            insert(
                address,
                key,
                max_ts.2.clone(),
                &info_select.0,
                &info_select.2.clone(),
                Arc::clone(table),
                node,
            );
        }
    }
    Ok(new_rows)
}
