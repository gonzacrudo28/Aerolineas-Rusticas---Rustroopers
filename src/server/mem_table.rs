use crate::server::sstable::{clean_line, meets_conditions, SSTable};
use crate::{
    errors::error_types::ErrorTypes, protocol::query_parser::clause::Clause,
    server::sstable::sort_by_columns,
};

use chrono::DateTime;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fs::File;
use std::io::BufReader;
use std::str::FromStr;
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{BufRead, Write},
};

use super::tabledefinition::TableDefinition;
const MAX_ENTRIES: usize = 1;
#[derive(Clone, Debug)]
/// This struct represents a MemTable, where data is a Hashmap, Key is a u128 (token range) and Value is a Vec of Vec of Strings (rows).
pub struct MemTable {
    pub table_name: String,
    pub data: HashMap<u128, Vec<Vec<String>>>,
    pub columns: Vec<String>,
    pub partition_key: Vec<(String, usize)>,
    pub clustering_key: Vec<(String, usize)>,
    pub columns_type: Vec<(String, String)>,
    pub max_entries: usize,
    pub ss_tables: SSTable,
    pub id: String,
}

impl Serialize for MemTable {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let data = self.data.keys().collect::<Vec<&u128>>();
        (
            &self.table_name,
            &data,
            &self.columns,
            &self.partition_key,
            &self.clustering_key,
            &self.columns_type,
            &self.ss_tables,
        )
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MemTable {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let table_definition: TableDefinition = Deserialize::deserialize(deserializer)?;
        let (table_name, data, columns, partition_key, clustering_key, columns_type, ss_tables) =
            table_definition;

        let data = data
            .iter()
            .map(|k| (*k, Vec::new()))
            .collect::<HashMap<u128, Vec<Vec<String>>>>();

        Ok(MemTable {
            table_name,
            data,
            columns,
            partition_key,
            clustering_key,
            columns_type,
            ss_tables,
            id: "".to_string(),
            max_entries: MAX_ENTRIES,
        })
    }
}

impl MemTable {
    /// This function creates a new MemTable
    pub fn new(
        columns_type: Vec<(String, String)>,
        partition_key: Vec<String>,
        table_name: String,
        clustering_key: Vec<String>,
        id: String,
    ) -> MemTable {
        MemTable {
            id: id.clone(),
            table_name: table_name.clone(),
            data: HashMap::new(),
            columns: columns_type.iter().map(|(name, _)| name.clone()).collect(),
            partition_key: Self::make_partition_key(partition_key, &columns_type),
            clustering_key: Self::make_clustering_key(clustering_key, &columns_type),
            max_entries: MAX_ENTRIES,
            ss_tables: SSTable::new(format!("{}_{}_sstable.csv", id, table_name)),
            columns_type,
        }
    }

    pub fn get_primary_key(&self) -> Vec<(String, usize)> {
        let mut vec = Vec::new();
        for a in &self.partition_key {
            vec.push(a.clone());
        }
        for (a, b) in &self.clustering_key {
            let i = b - 1;
            vec.push((a.clone(), i));
        }
        vec
    }

    pub fn get_pk(&self) -> Vec<usize> {
        self.get_partition_key()
            .iter()
            .map(|(_, x)| *x)
            .collect::<Vec<usize>>()
    }

    /// This function sets the id
    pub fn set_id(&mut self, id: &String, name: &String) {
        self.id = id.to_string();
        self.ss_tables
            .set_route(format!("{}_{}_sstable.csv", id, name));
    }

    ///This function creates the partition key
    fn make_partition_key(
        primary_key: Vec<String>,
        columns: &[(String, String)],
    ) -> Vec<(String, usize)> {
        let mut keys = Vec::new();
        for (i, column) in columns.iter().enumerate() {
            let (column_name, _) = column;
            if primary_key.contains(column_name) {
                keys.push((column_name.to_string(), i));
            }
        }
        keys
    }

    /// This function returns the partition key
    pub fn get_partition_key(&self) -> Vec<(String, usize)> {
        self.partition_key.clone()
    }

    /// This function parses the clustering key
    fn make_clustering_key(
        clustering_key: Vec<String>,
        columns: &[(String, String)],
    ) -> Vec<(String, usize)> {
        let mut keys: Vec<(String, usize)> = Vec::new();
        for key in clustering_key.iter() {
            for (i, column) in columns.iter().enumerate() {
                let (column_name, _) = column;
                if key == column_name {
                    keys.push((key.to_string(), i + 1));
                }
            }
        }
        keys
    }
    /// This function returns the clustering key
    pub fn get_clustering_key(&self) -> Vec<(String, usize)> {
        self.clustering_key
            .iter()
            .map(|(x, y)| (x.clone(), y - 1))
            .collect()
    }
    /// This function devuelve un hash con las columnas y sus indices
    pub fn get_columns_index(&self) -> HashMap<String, usize> {
        let mut hash = HashMap::new();

        for (i, column) in self.columns.iter().enumerate() {
            hash.insert(column.clone(), i);
        }
        hash
    }

    /// This function devuelve un hash con los indices y sus columnas
    pub fn get_index_column(&self) -> HashMap<usize, String> {
        let mut hash = HashMap::new();
        for (i, column) in self.columns.iter().enumerate() {
            hash.insert(i, column.clone());
        }
        hash
    }

    /// This function gets the rows of the id from the MemTable
    pub fn get_row(&self, key: u128) -> Option<Vec<Vec<String>>> {
        match self.data.get(&key) {
            Some(rows) => {
                let mut res = Vec::new();
                for mut row in rows.clone() {
                    row.remove(0);
                    res.push(row);
                }
                Some(res)
            }
            None => None,
        }
    }

    /// This function gets the rows of the id from the MemTable
    pub fn get_row_no_ts(&self, key: u128) -> Option<Vec<Vec<String>>> {
        match self.data.get(&key) {
            Some(rows) => {
                let mut res = Vec::new();
                for mut row in rows.clone() {
                    row.remove(0);
                    row.pop();
                    res.push(row);
                }
                Some(res)
            }
            None => None,
        }
    }

    /// This function deletes a row from the MemTable
    pub fn delete_row(&mut self, key: u128, row: &Vec<String>) -> bool {
        if let Some(rows) = self.data.get_mut(&key) {
            if let Some(pos) = rows.iter().position(|r| &clean_line(r.join(",")) == row) {
                rows.remove(pos);
                if rows.is_empty() {
                    self.data.remove(&key);
                }
                return true;
            }
        }
        false
    }

    /// This function flushes the MemTable to the SSTable file.
    pub fn flush(&mut self) -> Result<(), ErrorTypes> {
        let _ = match OpenOptions::new()
            .append(true)
            .create(true)
            .open(self.ss_tables.get_route())
        {
            Ok(file) => file,
            Err(_) => {
                return Err(ErrorTypes::new(
                    500,
                    "Error opening SSTable file".to_string(),
                ));
            }
        };
        self.compact_sstable()?;

        self.data.clear();
        self.max_entries = MAX_ENTRIES;
        Ok(())
    }

    /// This function sorts the lines that are going to be written in the SSTable
    fn sort_lines(&self, lines: Vec<Vec<String>>) -> Vec<Vec<String>> {
        let mut lines = lines;
        let primary_key = self.clustering_key[0].1;
        lines.sort_by(|a, b| {
            let a_key = a[0].parse::<u128>().unwrap();
            let b_key = b[0].parse::<u128>().unwrap();

            match a_key.cmp(&b_key) {
                std::cmp::Ordering::Equal => {
                    let a_clustering = a[primary_key].replace("-", "").parse::<i32>().unwrap();
                    let b_clustering = b[primary_key].replace("-", "").parse::<i32>().unwrap();
                    a_clustering.cmp(&b_clustering)
                }
                _ => a_key.cmp(&b_key),
            }
        });
        lines
    }

    /// This function filters the lines that are going to be written in the SSTable
    fn filter_lines(&self, lines: Vec<Vec<String>>) -> Vec<Vec<String>> {
        let mut res_lines = Vec::new();
        let mut hash: HashMap<String, (DateTime<chrono::Utc>, Vec<String>)> = HashMap::new();
        for line in lines {
            if hash.contains_key(&line[1]) {
                let timestamp =
                    chrono::DateTime::<chrono::Utc>::from_str(line.last().unwrap()).unwrap();
                let timestamp_hash = hash.get(&line[1]).unwrap().0;
                if timestamp > timestamp_hash {
                    hash.insert(
                        line[1].clone(),
                        (
                            chrono::DateTime::from_str(line.last().unwrap()).unwrap(),
                            line.clone(),
                        ),
                    );
                }
            } else {
                hash.insert(
                    line[1].clone(),
                    (
                        chrono::DateTime::from_str(line.last().unwrap()).unwrap(),
                        line.clone(),
                    ),
                );
            }
        }
        for (_, (_, line)) in hash {
            res_lines.push(line);
        }
        res_lines
    }

    /// This function compacts the SSTable
    fn compact_sstable(&mut self) -> Result<(), ErrorTypes> {
        let mut all_lines = self.get_sstables_lines(self.ss_tables.get_route())?;
        let data_sorted = order_hash(&self.data);
        for (_, rows) in data_sorted {
            all_lines.extend(rows);
        }
        all_lines = self.filter_lines(all_lines);
        all_lines = self.sort_lines(all_lines);
        let mut new_sstable = self.open_compact_files()?;

        let res_lines = filter_lines_timestamp(&all_lines);
        for line in res_lines {
            writeln!(new_sstable, "{}", line).unwrap();
        }
        rename_file(
            self.ss_tables.get_route(),
            format!("{}_sstable_compact.csv", self.id),
        );
        Ok(())
    }

    /// This function opens the files that are going to be used in the compact
    fn open_compact_files(&self) -> Result<File, ErrorTypes> {
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(format!("{}_sstable_compact.csv", self.id))
            .map_err(|_| ErrorTypes::new(501, "Could not open the file".to_string()))
    }

    /// This function gets the lines of the actual SSTable
    fn get_sstables_lines(&self, filename: String) -> Result<Vec<Vec<String>>, ErrorTypes> {
        let file = File::open(filename).map_err(|_| ErrorTypes::new(502, "Fallos".to_string()))?;
        let reader = BufReader::new(file);
        let mut lines = Vec::new();

        for line in reader.lines() {
            let line = line.map_err(|_| ErrorTypes::new(503, "Fallos".to_string()))?;
            lines.push(line.split(",").map(|s| s.to_string()).collect());
        }
        Ok(lines)
    }

    fn get_newest(&self, lines: Vec<Vec<String>>) -> Vec<Vec<String>> {
        let mut res_lines = Vec::new();
        let mut hash: HashMap<String, (DateTime<chrono::Utc>, Vec<String>)> = HashMap::new();
        let mut first = true;
        for line in lines {
            if first {
                first = false;
                continue;
            }
            if hash.contains_key(&line[0]) {
                let timestamp =
                    chrono::DateTime::<chrono::Utc>::from_str(line.last().unwrap()).unwrap();
                let timestamp_hash = hash.get(&line[0]).unwrap().0;
                if timestamp > timestamp_hash {
                    hash.insert(
                        line[0].clone(),
                        (
                            chrono::DateTime::from_str(line.last().unwrap()).unwrap(),
                            line.clone(),
                        ),
                    );
                }
            } else {
                hash.insert(
                    line[0].clone(),
                    (
                        chrono::DateTime::from_str(line.last().unwrap()).unwrap(),
                        line.clone(),
                    ),
                );
            }
        }
        for (_, (_, line)) in hash {
            res_lines.push(line);
        }
        res_lines
    }

    /// This function handles the select query. First, it checks if there is any row needed in the memtable, after that it does the same with te sstable.
    pub fn execute_select(
        &self,
        conditions: &Clause,
        selected_columns: &[String],
        order: &[String],
        need_ts: bool,
        include_tombstones: bool,
    ) -> Result<Vec<Vec<String>>, ErrorTypes> {
        let mut result = clean_rows_select(self.find_rows(conditions, true)?);
        let mut selected_columns = selected_columns.to_vec();
        if selected_columns == ["*"] {
            selected_columns = self.columns.clone();
        }
        result.extend(clean_rows_select(
            self.ss_tables
                .execute_select(conditions, &selected_columns)?,
        ));
        let mut filtered_lines: Vec<Vec<String>> = self.get_newest(result);
        if !order.is_empty() {
            match sort_by_columns(order, filtered_lines, &self.columns) {
                Ok(r) => filtered_lines = r,
                _ => return Err(ErrorTypes::new(504, "Invalid sorting".to_string())),
            };
        } else {
            let ck = self
                .clustering_key
                .iter()
                .map(|(x, _)| x.clone())
                .collect::<Vec<String>>();
            match sort_by_columns(&ck, filtered_lines, &self.columns) {
                Ok(r) => filtered_lines = r,
                _ => return Err(ErrorTypes::new(505, "Invalid sorting".to_string())),
            }
        }
        if !include_tombstones {
            let mut res = Vec::new();
            for row in filtered_lines {
                if row.iter().any(|x| x == "X") {
                    continue;
                }
                res.push(row);
            }
            filtered_lines = res;
        }
        if selected_columns.len() == 1 && selected_columns[0] == "*" {
            filtered_lines.insert(0, self.columns.clone());
            if need_ts {
                filtered_lines[0].push("timestamp".to_string());
                return Ok(filtered_lines);
            }
            for row in &mut filtered_lines {
                row.pop();
            }
            return Ok(filtered_lines);
        } else if selected_columns.len() == 1 {
            return Err(ErrorTypes::new(506, "Invalid fields".to_string()));
        }
        let mut filtered: Vec<Vec<String>> = match field_filter(
            filtered_lines,
            self.columns.clone(),
            selected_columns.to_vec(),
            need_ts,
        ) {
            Ok(f) => f,
            Err(e) => return Err(e),
        };
        filtered.insert(0, selected_columns.to_vec());
        Ok(filtered)
    }

    /// This function handles the delete query.
    pub fn execute_delete(&mut self, conditions: Clause) -> Result<(), ErrorTypes> {
        let mut rows_to_delete = self.find_rows(&conditions, false)?;
        rows_to_delete.remove(0);
        let mut rows: Vec<(u128, Vec<String>)> =
            self.ss_tables.execute_select(&conditions, &self.columns)?;
        rows.extend(rows_to_delete);
        let c_k: Vec<(String, usize)> = self.get_clustering_key().clone();
        let clustering: Vec<&usize> = c_k.iter().map(|(_x, y)| y).collect();
        let p_k: Vec<(String, usize)> = self.partition_key.clone();
        let primary: Vec<&usize> = p_k.iter().map(|(_x, y)| y).collect();

        for (key, row) in rows {
            let mut columns = Vec::new();
            for (i, value) in row.iter().enumerate() {
                if primary.contains(&&i) || clustering.contains(&&i) {
                    columns.push(value.to_string());
                } else {
                    columns.push("X".to_string());
                }
            }
            self.insert_row(key, columns, vec![], None, None)?;
        }
        Ok(())
    }

    /// This function finds the rows that meet the conditions in the MemTable
    pub fn find_rows(
        &self,
        conditions: &Clause,
        need_ts: bool,
    ) -> Result<Vec<(u128, Vec<String>)>, ErrorTypes> {
        let mut result = Vec::new();
        let mut hash: HashMap<&String, String> = HashMap::new();
        for key in self.data.keys() {
            let rows = match self.get_row(*key) {
                Some(rows) => rows,
                _ => return Err(ErrorTypes::new(587, "Error getting rows".to_string())),
            };
            for mut row in rows {
                let time_stamp = row.last().unwrap().to_string();
                row.pop();

                for (i, column) in self.columns.iter().enumerate() {
                    hash.insert(column, row[i].clone());
                }
                if is_tombstone(&row) {
                    if need_ts {
                        let mut new_row = row.clone();
                        new_row.push(time_stamp);
                        result.push((*key, new_row));
                    } else {
                        result.push((*key, row));
                    }
                    continue;
                }
                match meets_conditions(&hash, conditions) {
                    Ok(true) => {
                        if need_ts {
                            let mut new_row = row.clone();
                            new_row.push(time_stamp);
                            result.push((*key, new_row));
                        } else {
                            result.push((*key, row))
                        }
                    }
                    Ok(false) => continue,
                    _ => {
                        return Err(ErrorTypes::new(
                            507,
                            "Checking line conditions failed".to_string(),
                        ))
                    }
                }
                hash.clear();
            }
        }
        result.insert(0, (0, self.columns.clone()));
        Ok(result)
    }

    /// This function inserts a row in the MemTable
    pub fn insert_row(
        &mut self,
        key: u128,
        columns: Vec<String>,
        columns_inserted: Vec<String>,
        clause: Option<Clause>,
        columns_update: Option<HashMap<String, String>>,
    ) -> Result<(), ErrorTypes> {
        if self.max_entries == 0 {
            self.flush()?;
        }
        self.data.entry(key).or_default();
        if let Some(clause) = clause {
            self.update_memtable(clause, &columns_update.unwrap())?;
            Ok(())
        } else {
            let mut row_time_id = self.check_line(columns, &columns_inserted);
            row_time_id.push(chrono::Utc::now().to_rfc3339());
            row_time_id.insert(0, key.to_string());
            let vec = self.data.get_mut(&key).unwrap();
            vec.push(row_time_id);
            self.order_data_vec(key);
            self.max_entries -= 1;
            Ok(())
        }
    }

    /// This function updates the MemTable
    fn update_memtable(
        &mut self,
        clause: Clause,
        columns_update: &HashMap<String, String>,
    ) -> Result<(), ErrorTypes> {
        check_update_columns(&self.partition_key, columns_update)?;
        let mut rows_to_update = self.find_rows(&clause, false)?;
        rows_to_update.remove(0);
        for (key, row) in &rows_to_update {
            self.delete_row(*key, row);
        }
        if !rows_to_update.is_empty() {
            let rows_updated = self.update_rows(rows_to_update, columns_update)?;
            for (key, mut row) in rows_updated {
                row.push(chrono::Utc::now().to_rfc3339());
                row.insert(0, key.to_string());
                let vec = self.data.entry(key).or_default();
                vec.push(row);
                self.order_data_vec(key);
            }
            Ok(())
        } else {
            self.update_sstable_rows(clause, columns_update)?;
            Ok(())
        }
    }

    /// This function orders the data of the MemTable
    fn order_data_vec(&mut self, partition_key: u128) {
        let vec = self.data.get_mut(&partition_key).unwrap();

        if !self.clustering_key.is_empty() {
            let key = self.clustering_key[0].1;

            let (_, column_type) = &self.columns_type[key - 1];
            if column_type == "int" {
                vec.sort_by(|a, b| {
                    let a = a[key].parse::<i32>().unwrap();
                    let b = b[key].parse::<i32>().unwrap();
                    a.cmp(&b)
                });
            } else if column_type == "date" {
                vec.sort_by(|a, b| {
                    let a_parsed = a[key].replace("-", "").parse::<i32>();
                    let b_parsed = b[key].replace("-", "").parse::<i32>();
                    match (a_parsed, b_parsed) {
                        (Ok(a), Ok(b)) => a.cmp(&b),
                        _ => std::cmp::Ordering::Equal,
                    }
                });
            } else {
                vec.sort_by(|a, b| a[key].cmp(&b[key]));
            }
        }
    }

    /// This function updates the rows
    pub fn update_rows(
        &self,
        rows_to_update: Vec<(u128, Vec<String>)>,
        columns_update: &HashMap<String, String>,
    ) -> Result<Vec<(u128, Vec<String>)>, ErrorTypes> {
        let mut rows_updated = Vec::new();
        let hash_campos = self.get_index_column();
        for (key, valores) in rows_to_update {
            let mut row_updated = Vec::new();
            for (i, _column) in self.columns.iter().enumerate() {
                let campo = hash_campos.get(&i).unwrap();
                if columns_update.contains_key(campo) {
                    row_updated.push(columns_update.get(campo).unwrap().clone());
                } else {
                    row_updated.push(valores[i].clone());
                }
            }
            rows_updated.push((key, row_updated));
        }
        Ok(rows_updated)
    }

    /// This function updates the rows in the SSTable
    pub fn update_sstable_rows(
        &mut self,
        clause: Clause,
        columns_update: &HashMap<String, String>,
    ) -> Result<(), ErrorTypes> {
        let updatable_sstables_rows = self.ss_tables.execute_select(&clause, &self.columns)?;
        let mut sstables_updated = Vec::new();
        for (key, row) in updatable_sstables_rows {
            sstables_updated.push(self.update_rows(vec![(key, row)], columns_update)?[0].clone());
        }
        for (key, row) in sstables_updated {
            self.insert_row(key, row, self.columns.clone(), None, None)?;
        }

        Ok(())
    }

    ///This function checks if the long of the line its correct and if not, it fills it with empty strings
    fn check_line(&mut self, columns: Vec<String>, columns_names: &[String]) -> Vec<String> {
        let indexes = self.get_columns_index();
        let mut correct_line: Vec<String> = Vec::new();
        for _ in 0..indexes.len() {
            correct_line.push("".to_string());
        }

        for i in 0..columns.len() {
            let index = indexes.get(&columns_names[i]).unwrap();
            correct_line[*index] = columns[i].clone();
        }

        correct_line
    }
    /// This function returns the rows that the actual node has to transfer to the new node.
    pub fn get_rows(&self, partitions: &Vec<(u128, u128)>) -> Vec<(u128, Vec<String>)> {
        if partitions.is_empty() {
            return vec![];
        }
        let mut rows = self.find_rows(&Clause::Placeholder, true).unwrap();
        rows.extend(
            self.ss_tables
                .execute_select(&Clause::Placeholder, &self.columns)
                .unwrap(),
        );
        rows.remove(0);
        let rows_grouped = self.group_by_primary_key(clean_rows_select(rows.to_vec()));
        let mut res = Vec::new();
        for rows_ in rows_grouped {
            let rows_filtered = self.filter_lines(rows_);
            for mut row in rows_filtered {
                for (key, row_) in rows.iter() {
                    if &row == row_ {
                        row.pop();
                        res.push((*key, row.clone()));
                    }
                }
            }
        }
        let mut vec = Vec::new();
        for (start, end) in partitions {
            for (key, row) in &res {
                if key >= start && key <= end {
                    vec.push((*key, row.clone()));
                }
            }
        }
        vec
    }

    fn group_by_primary_key(&self, rows: Vec<Vec<String>>) -> Vec<Vec<Vec<String>>> {
        let pk = self.get_primary_key();
        let mut hash: HashMap<Vec<String>, Vec<Vec<String>>> = HashMap::new();
        for row in rows {
            let mut key = Vec::new();
            for (_, index) in &pk {
                key.push(row[*index].clone());
            }
            hash.entry(key).or_default().push(row);
        }
        let mut res = Vec::new();
        for (_k, value) in hash {
            res.push(value);
        }
        res
    }

    /// This function deletes the rows that have the partition key given
    pub fn delete_rows(&mut self, partition_key: &u128) {
        self.data.remove(partition_key);
        let _ = self.delete_sstables_rows(partition_key);
    }

    fn delete_sstables_rows(&self, partition_key: &u128) -> Result<(), ErrorTypes> {
        let file = File::open(self.ss_tables.get_route())
            .map_err(|_| ErrorTypes::new(000, "The file could not be open".to_string()))?;
        let mut reader = BufReader::new(file);

        let temp_file = "temp_sstable.txt".to_string();
        filter_file_by_pk(&mut reader, &temp_file, partition_key)?;

        fs::remove_file(self.ss_tables.get_route())
            .map_err(|_| ErrorTypes::new(000, "The file could not be removed".to_string()))?;
        rename_file(self.ss_tables.get_route(), temp_file);
        Ok(())
    }
}

/// This function the sstable deleting the rows that have the partition key given
fn filter_file_by_pk(
    reader: &mut BufReader<File>,
    temp_file: &String,
    partition_key: &u128,
) -> Result<(), ErrorTypes> {
    let mut archivo_filtrado = File::create(temp_file)
        .map_err(|_| ErrorTypes::new(000, "The file could not be open".to_string()))?;
    let mut line = String::new();
    while reader
        .read_line(&mut line)
        .map_err(|_| ErrorTypes::new(000, "The file could not be open".to_string()))?
        > 0
    {
        let linea_ = line.trim_end();
        let arr_linea: Vec<&str> = linea_.split(',').collect();
        if arr_linea[0].parse::<u128>().unwrap() != *partition_key {
            writeln!(archivo_filtrado, "{}", linea_)
                .map_err(|_| ErrorTypes::new(000, "The file could not be written".to_string()))?;
        }
        line.clear();
    }
    Ok(())
}

/// This function renames a file.
fn rename_file(new_name: String, old_name: String) {
    std::fs::rename(old_name, new_name).unwrap();
}
/// This function checks if a row is a tombstone
pub fn is_tombstone(row: &[String]) -> bool {
    row.iter().any(|x| x == "X")
}

fn filter_lines_timestamp(all_lines: &[Vec<String>]) -> Vec<String> {
    let mut res_lines = Vec::new();
    let mut actual_key: Option<u128> = None;
    for line in all_lines.iter() {
        let line_key = line[0].parse::<u128>().unwrap();
        match actual_key {
            Some(key) => {
                if key != line_key {
                    actual_key = Some(line_key);
                }
            }
            None => {
                actual_key = Some(line_key);
            }
        }
        res_lines.push(line.join(","));
    }
    res_lines
}

/// This function orders the hash by the key.
fn order_hash(data: &HashMap<u128, Vec<Vec<String>>>) -> Vec<(u128, Vec<Vec<String>>)> {
    let mut keys: Vec<&u128> = data.keys().collect();
    keys.sort();

    let mut ordered_elements = Vec::new();
    for key in keys {
        if let Some(value) = data.get(key) {
            ordered_elements.push((*key, value.clone()));
        }
    }
    ordered_elements
}

/// This function cleans the rows of the select query
pub fn clean_rows_select(rows: Vec<(u128, Vec<String>)>) -> Vec<Vec<String>> {
    let mut res_rows = Vec::new();
    for row in rows {
        res_rows.push(row.1);
    }
    res_rows
}

/// This function checks if the columns to update are valid
fn check_update_columns(
    primary_keys: &[(String, usize)],
    columns_update: &HashMap<String, String>,
) -> Result<(), ErrorTypes> {
    for column in columns_update.keys() {
        if primary_keys
            .iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>()
            .contains(&column)
        {
            return Err(ErrorTypes::new(508, "Invalid column to update".to_string()));
        }
    }
    Ok(())
}

/// This function returns the selected data with only the selected fields
pub fn field_filter(
    data: Vec<Vec<String>>,
    columns: Vec<String>,
    selected_columns: Vec<String>,
    need_ts: bool,
) -> Result<Vec<Vec<String>>, ErrorTypes> {
    let mut column_map: HashMap<String, usize> = HashMap::new();
    for (i, col) in columns.iter().enumerate() {
        column_map.insert(col.clone(), i);
    }
    let mut indices: Vec<usize> = Vec::new();
    for col in selected_columns {
        if let Some(&indice) = column_map.get(&col) {
            indices.push(indice);
        } else {
            return Err(ErrorTypes::new(509, "Invalid fields".to_string()));
        }
    }
    let mut filtered_data: Vec<Vec<String>> = Vec::new();
    for row in data {
        let mut new_row = Vec::new();
        for i in indices.iter() {
            new_row.push(row[*i].clone());
        }
        if need_ts {
            new_row.push(row.last().unwrap().clone());
        }
        filtered_data.push(new_row);
    }
    Ok(filtered_data)
}

#[cfg(test)]
pub mod test {
    use std::vec;

    use crate::protocol::query_parser::relation::Relation;

    use super::*;
    /// This test checks if the select is done correctly with multiple rows.
    #[test]
    #[ignore]
    fn test_complex_select() {
        let columns = vec![
            ("id".to_string(), "int".to_string()),
            ("origin".to_string(), "text".to_string()),
            ("destination".to_string(), "text".to_string()),
            ("date".to_string(), "date".to_string()),
        ];
        let primary_key = vec!["destination".to_string()];
        let clustering_key = vec!["date".to_string()];
        let mut memtable = MemTable::new(
            columns,
            primary_key,
            "arrivals".to_string(),
            clustering_key,
            "2ff".to_string(),
        );
        let _ = memtable.insert_row(
            1,
            vec![
                "2".to_string(),
                "EZE".to_string(),
                "AEP".to_string(),
                "2024-11-02".to_string(),
            ],
            vec![
                "id".to_string(),
                "origin".to_string(),
                "destination".to_string(),
                "date".to_string(),
            ],
            None,
            None,
        );
        let _ = memtable.insert_row(
            2,
            vec![
                "3".to_string(),
                "EZE".to_string(),
                "MIA".to_string(),
                "2024-11-02".to_string(),
            ],
            vec![
                "id".to_string(),
                "origin".to_string(),
                "destination".to_string(),
                "date".to_string(),
            ],
            None,
            None,
        );
        let _ = memtable.insert_row(
            3,
            vec![
                "4".to_string(),
                "MIA".to_string(),
                "MEX".to_string(),
                "2024-11-02".to_string(),
            ],
            vec![
                "id".to_string(),
                "origin".to_string(),
                "destination".to_string(),
                "date".to_string(),
            ],
            None,
            None,
        );
        let _ = memtable.insert_row(
            4,
            vec![
                "5".to_string(),
                "AEP".to_string(),
                "MEX".to_string(),
                "2024-11-02".to_string(),
            ],
            vec![
                "id".to_string(),
                "origin".to_string(),
                "destination".to_string(),
                "date".to_string(),
            ],
            None,
            None,
        );
        let _ = memtable.insert_row(
            5,
            vec![
                "6".to_string(),
                "EXE".to_string(),
                "MEX".to_string(),
                "2024-11-02".to_string(),
            ],
            vec![
                "id".to_string(),
                "origin".to_string(),
                "destination".to_string(),
                "date".to_string(),
            ],
            None,
            None,
        );
        let _ = memtable.insert_row(
            6,
            vec![
                "7".to_string(),
                "EZE".to_string(),
                "MZA".to_string(),
                "2024-11-02".to_string(),
            ],
            vec![
                "id".to_string(),
                "origin".to_string(),
                "destination".to_string(),
                "date".to_string(),
            ],
            None,
            None,
        );
        let clause = Clause::Term {
            relation: Relation::Equal {
                v1: "origin".to_string(),
                v2: "EZE".to_string(),
            },
        };
        let selected_rows = memtable
            .execute_select(
                &clause,
                &["origin".to_string(), "destination".to_string()],
                &[],
                false,
                false,
            )
            .unwrap();

        assert_eq!(4, selected_rows.len());
        assert!(selected_rows.contains(&vec!["EZE".to_string(), "MZA".to_string()]));
        assert!(selected_rows.contains(&vec!["EZE".to_string(), "AEP".to_string()]));
        assert!(selected_rows.contains(&vec!["EZE".to_string(), "MIA".to_string()]));
    }

    /// This test checks if the delete is done correctly with multiple rows. You must set the MAX_ENTRIES to 2
    #[test]
    #[ignore]
    fn test_multiple_delete() {
        let columns = vec![
            ("id".to_string(), "int".to_string()),
            ("origin".to_string(), "text".to_string()),
            ("destination".to_string(), "text".to_string()),
            ("date".to_string(), "date".to_string()),
        ];
        let primary_key = vec!["destination".to_string()];
        let clustering_key = vec!["id".to_string()];
        let mut memtable = MemTable::new(
            columns,
            primary_key,
            "arrivals".to_string(),
            clustering_key,
            "2ff".to_string(),
        );
        let _ = memtable.insert_row(
            1,
            vec![
                "1".to_string(),
                "EZE".to_string(),
                "MZA".to_string(),
                "2024-11-02".to_string(),
            ],
            vec![
                "id".to_string(),
                "origin".to_string(),
                "destination".to_string(),
                "date".to_string(),
            ],
            None,
            None,
        );

        let _ = memtable.insert_row(
            1,
            vec![
                "2".to_string(),
                "EZE".to_string(),
                "AEP".to_string(),
                "2024-11-02".to_string(),
            ],
            vec![
                "id".to_string(),
                "origin".to_string(),
                "destination".to_string(),
                "date".to_string(),
            ],
            None,
            None,
        );

        let _ = memtable.insert_row(
            1,
            vec![
                "3".to_string(),
                "EZE".to_string(),
                "MIA".to_string(),
                "2024-11-02".to_string(),
            ],
            vec![
                "id".to_string(),
                "origin".to_string(),
                "destination".to_string(),
                "date".to_string(),
            ],
            None,
            None,
        );

        let _ = memtable.insert_row(
            1,
            vec![
                "4".to_string(),
                "MIA".to_string(),
                "MEX".to_string(),
                "2024-11-02".to_string(),
            ],
            vec![
                "id".to_string(),
                "origin".to_string(),
                "destination".to_string(),
                "date".to_string(),
            ],
            None,
            None,
        );

        let _ = memtable.insert_row(
            1,
            vec![
                "5".to_string(),
                "AEP".to_string(),
                "MEX".to_string(),
                "2024-11-02".to_string(),
            ],
            vec![
                "id".to_string(),
                "origin".to_string(),
                "destination".to_string(),
                "date".to_string(),
            ],
            None,
            None,
        );

        let clause = Clause::Term {
            relation: Relation::Equal {
                v1: "id".to_string(),
                v2: "5".to_string(),
            },
        };

        let _res_delete = memtable.execute_delete(clause.clone());

        let selected_rows = memtable
            .execute_select(
                &clause,
                &[
                    "id".to_string(),
                    "origin".to_string(),
                    "destination".to_string(),
                    "date".to_string(),
                ],
                &[],
                false,
                false,
            )
            .unwrap();
        assert_eq!(1, selected_rows.len()); // Only the line of the columns
    }
}
