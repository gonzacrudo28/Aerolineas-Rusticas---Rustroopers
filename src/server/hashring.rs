use crate::errors::error_types::ErrorTypes;
use murmur3::murmur3_x64_128;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Cursor;
use std::ops::Bound::Excluded;
use std::ops::Bound::Included;

const REPLICAS: i32 = 32;
pub const NODOS: usize = 8;
pub struct HashRing {
    pub node_ring: BTreeMap<u128, String>,
    pub quantity: usize,
}
///This struct represents a HashRing of data and nodes to implement the Consistent Hashing algorithm.
impl Default for HashRing {
    fn default() -> Self {
        Self::new()
    }
}

impl HashRing {
    pub fn new() -> HashRing {
        HashRing {
            node_ring: BTreeMap::new(),
            quantity: 0,
        }
    }
    pub fn hash<T: AsRef<[u8]>>(key: T) -> u128 {
        murmur3_x64_128(&mut Cursor::new(key), 0).unwrap()
    }
    ///This function adds a node to the HashRing.
    pub fn add_node(&mut self, node: String) {
        if self.node_ring.values().any(|v| v == &node) {
            return;
        }
        for i in 0..REPLICAS {
            let vnode = format!("{}-{}", node, i);
            let hash = Self::hash(&vnode);
            self.node_ring.insert(hash, node.clone());
        }
        self.quantity += 1;
    }

    ///This function removes a node from the HashRing.
    pub fn remove_node(&mut self, node: String) {
        for i in 0..REPLICAS {
            let vnode = format!("{}-{}", node, i);
            let hash = Self::hash(&vnode);
            self.node_ring.remove(&hash);
        }
        self.quantity -= 1;
    }
    ///This function returns the node that is responsible of the key.
    pub fn get_node(&self, key: Vec<&String>) -> (Option<String>, u128) {
        let hash = Self::hash(key.iter().map(|s| s.as_str()).collect::<String>());
        let node = self
            .node_ring
            .range(hash..)
            .next()
            .map(|(_, v)| v.to_string());
        (
            node.or_else(|| self.node_ring.values().next().map(|v| v.to_string())),
            hash,
        )
    }

    /// This function returns the partitions that the local node is responsible of transfering to the new node.
    pub fn get_partitions(&self, node: &String, local: &String, rf: usize) -> Vec<(u128, u128)> {
        let mut partitions = Vec::new();
        let mut used = HashSet::new();
        let mut vnodes = Vec::new();
        for i in 0..REPLICAS {
            let vnode = format!("{}-{}", node, i);
            let hash = Self::hash(&vnode);
            vnodes.push(hash);
        }
        vnodes.sort_by(|a, b| b.cmp(a));
        for vnode in vnodes {
            let mut j = 0;
            let next = self.get_next(vnode, node);
            let next_value = self.node_ring.get(&next).unwrap();
            let mut previous = vnode;
            let replicas = self.get_replicas(next, rf, next_value);
            let mut replicas = replicas.unwrap();
            replicas.insert(0, next_value.to_string());
            if !replicas.contains(local) || used.contains(&vnode) {
                continue;
            }
            while j < rf {
                let prev = self.get_previous(previous, node, &mut used);
                let range = (prev, previous);

                previous = prev;
                if replicas[replicas.len() - 1 - j] != *local {
                    j += 1;
                    continue;
                }
                partitions.push(range);
                j += 1;
            }
        }
        partitions
    }

    pub fn get_partitions_remove(
        &self,
        node: &String,
        rf: usize,
    ) -> HashMap<String, Vec<(u128, u128)>> {
        let mut partitions = HashMap::new();
        let mut used = HashSet::new();
        let mut vnodes = Vec::new();
        for i in 0..REPLICAS {
            let vnode = format!("{}-{}", node, i);
            let hash = Self::hash(&vnode);
            vnodes.push(hash);
        }
        vnodes.sort_by(|a, b| b.cmp(a));
        for vnode in vnodes {
            let mut j = 0;
            let next = self.get_next(vnode, node);
            let next_value = self.node_ring.get(&next).unwrap();
            let mut previous = vnode;
            let replicas = self.get_replicas(next, rf, next_value);
            let mut replicas = replicas.unwrap();
            replicas.insert(0, next_value.to_string());
            if used.contains(&vnode) {
                continue;
            }
            while j < rf {
                let prev = self.get_previous(previous, node, &mut used);
                let range = (prev, previous);

                previous = prev;

                partitions
                    .entry(replicas[replicas.len() - 1 - j].to_string())
                    .or_insert(Vec::new())
                    .push(range);
                j += 1;
            }
        }
        partitions
    }

    fn get_next(&self, key: u128, value: &String) -> u128 {
        let mut next = key;

        for (k, v) in self.node_ring.range((Excluded(key), Included(u128::MAX))) {
            if v == value {
                continue;
            }
            next = *k;
            return next;
        }

        for (k, v) in self.node_ring.range((Included(0), Included(key))) {
            if v == value {
                continue;
            }
            next = *k;
            return next;
        }

        next
    }

    fn get_previous(&self, key: u128, value: &String, used: &mut HashSet<u128>) -> u128 {
        let mut previous = key;

        for (k, v) in self.node_ring.range((Included(0), Excluded(key))).rev() {
            if v == value {
                used.insert(*k);
                continue;
            }
            previous = *k;
            return previous;
        }

        for (k, v) in self
            .node_ring
            .range((Included(key), Included(u128::MAX)))
            .rev()
        {
            if v == value {
                continue;
            }
            previous = *k;
            return previous;
        }

        previous
    }
    ///This function returns the replicas of the node that is responsible of the key.
    pub fn get_replicas(
        &self,
        mut key: u128,
        rf: usize,
        local: &String,
    ) -> Result<Vec<String>, ErrorTypes> {
        if self.node_ring.len() - 1 < rf {
            return Err(ErrorTypes::Error {
                code: 543,
                message: "There are not enough nodes to complete the replication factor"
                    .to_string(),
            });
        }
        let mut nodes: Vec<String> = Vec::new();

        while nodes.len() < rf - 1 {
            let last = match self.node_ring.last_key_value() {
                Some((last, _)) => last,
                _ => {
                    return Err(ErrorTypes::Error {
                        code: 580,
                        message: "There are not enough nodes to complete the replication factor"
                            .to_string(),
                    })
                }
            };
            let node = self
                .node_ring
                .range((Excluded(key), Included(*last)))
                .next();

            if let Some((current_key, next_node)) = node {
                if !nodes.contains(next_node) && next_node != local {
                    nodes.push(next_node.to_string());
                }
                key = *current_key;
            } else {
                let first_node = self.node_ring.range(0..).next();
                if let Some((first_key, first_node)) = first_node {
                    if !nodes.contains(first_node) && first_node != local {
                        nodes.push(first_node.to_string());
                    }
                    key = *first_key;
                }
            }
        }
        Ok(nodes)
    }
}
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_replicas() {
        let mut ring = HashRing::new();
        ring.add_node("127.0.0.1:8080".to_string());
        ring.add_node("127.0.0.1:8081".to_string());
        ring.add_node("127.0.0.1:8082".to_string());
        ring.add_node("127.0.0.1:8083".to_string());
        ring.add_node("127.0.0.1:8084".to_string());
        ring.add_node("127.0.0.1:8085".to_string());
        ring.add_node("127.0.0.1:8086".to_string());
        ring.add_node("127.0.0.1:8087".to_string());
        ring.add_node("127.0.0.1:8088".to_string()); //MLO RHO, HER

        let (node, _) = ring.get_node(vec![&"MLO".to_string()]);
        let node = node.unwrap();

        assert_eq!(node, "127.0.0.1:8088")
    }
}
