//! Pluggable backend traits for reactive data structures (M5.A — D178).
//!
//! Each structure delegates storage to a backend trait. Default implementations
//! use `Vec<T>` / `HashMap<K, V>`. `imbl`-backed persistent backends are
//! deferred until bench evidence justifies (D178).

use std::collections::HashMap;
use std::hash::Hash;

// ---------------------------------------------------------------------------
// LogBackend
// ---------------------------------------------------------------------------

/// Storage backend for [`crate::ReactiveLog`].
pub trait LogBackend<T: Clone>: Send + Sync {
    fn size(&self) -> usize;
    fn at(&self, index: i64) -> Option<T>;
    fn append(&mut self, value: T);
    fn append_many(&mut self, values: Vec<T>);
    fn clear(&mut self) -> usize;
    fn trim_head(&mut self, n: usize) -> usize;
    fn to_vec(&self) -> Vec<T>;
}

/// Default `Vec`-backed log backend with optional ring-buffer cap.
pub struct VecLogBackend<T> {
    data: Vec<T>,
    max_size: Option<usize>,
}

impl<T> VecLogBackend<T> {
    #[must_use]
    pub fn new(max_size: Option<usize>) -> Self {
        Self {
            data: Vec::new(),
            max_size,
        }
    }
}

impl<T: Clone + Send + Sync> LogBackend<T> for VecLogBackend<T> {
    fn size(&self) -> usize {
        self.data.len()
    }

    fn at(&self, index: i64) -> Option<T> {
        let len = self.data.len() as i64;
        let idx = if index < 0 { len + index } else { index };
        if idx < 0 || idx >= len {
            None
        } else {
            Some(self.data[idx as usize].clone())
        }
    }

    fn append(&mut self, value: T) {
        self.data.push(value);
        if let Some(max) = self.max_size {
            if self.data.len() > max {
                let excess = self.data.len() - max;
                self.data.drain(..excess);
            }
        }
    }

    fn append_many(&mut self, values: Vec<T>) {
        self.data.extend(values);
        if let Some(max) = self.max_size {
            if self.data.len() > max {
                let excess = self.data.len() - max;
                self.data.drain(..excess);
            }
        }
    }

    fn clear(&mut self) -> usize {
        let count = self.data.len();
        self.data.clear();
        count
    }

    fn trim_head(&mut self, n: usize) -> usize {
        let actual = n.min(self.data.len());
        self.data.drain(..actual);
        actual
    }

    fn to_vec(&self) -> Vec<T> {
        self.data.clone()
    }
}

// ---------------------------------------------------------------------------
// ListBackend
// ---------------------------------------------------------------------------

/// Storage backend for [`crate::ReactiveList`].
pub trait ListBackend<T: Clone>: Send + Sync {
    fn size(&self) -> usize;
    fn at(&self, index: i64) -> Option<T>;
    fn append(&mut self, value: T);
    fn append_many(&mut self, values: Vec<T>);
    fn insert(&mut self, index: usize, value: T);
    fn insert_many(&mut self, index: usize, values: Vec<T>);
    /// Remove and return element at `index`. Negative indices count from end.
    fn pop(&mut self, index: i64) -> Option<T>;
    fn clear(&mut self) -> usize;
    fn to_vec(&self) -> Vec<T>;
}

/// Default `Vec`-backed list backend.
pub struct VecListBackend<T> {
    data: Vec<T>,
}

impl<T> VecListBackend<T> {
    #[must_use]
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

impl<T> Default for VecListBackend<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Send + Sync> ListBackend<T> for VecListBackend<T> {
    fn size(&self) -> usize {
        self.data.len()
    }

    fn at(&self, index: i64) -> Option<T> {
        let len = self.data.len() as i64;
        let idx = if index < 0 { len + index } else { index };
        if idx < 0 || idx >= len {
            None
        } else {
            Some(self.data[idx as usize].clone())
        }
    }

    fn append(&mut self, value: T) {
        self.data.push(value);
    }

    fn append_many(&mut self, values: Vec<T>) {
        self.data.extend(values);
    }

    fn insert(&mut self, index: usize, value: T) {
        self.data.insert(index, value);
    }

    fn insert_many(&mut self, index: usize, values: Vec<T>) {
        for (i, v) in values.into_iter().enumerate() {
            self.data.insert(index + i, v);
        }
    }

    fn pop(&mut self, index: i64) -> Option<T> {
        let len = self.data.len() as i64;
        let idx = if index < 0 { len + index } else { index };
        if idx < 0 || idx >= len {
            None
        } else {
            Some(self.data.remove(idx as usize))
        }
    }

    fn clear(&mut self) -> usize {
        let count = self.data.len();
        self.data.clear();
        count
    }

    fn to_vec(&self) -> Vec<T> {
        self.data.clone()
    }
}

// ---------------------------------------------------------------------------
// MapBackend
// ---------------------------------------------------------------------------

/// Storage backend for [`crate::ReactiveMap`].
pub trait MapBackend<K: Clone + Eq + Hash, V: Clone>: Send + Sync {
    fn size(&self) -> usize;
    fn has(&self, key: &K) -> bool;
    fn get(&self, key: &K) -> Option<V>;
    fn set(&mut self, key: K, value: V);
    fn set_many(&mut self, entries: Vec<(K, V)>);
    fn delete(&mut self, key: &K) -> bool;
    fn delete_many(&mut self, keys: &[K]) -> usize;
    fn clear(&mut self) -> usize;
    fn to_vec(&self) -> Vec<(K, V)>;
}

/// Default `HashMap`-backed map backend.
pub struct HashMapBackend<K, V> {
    data: HashMap<K, V>,
}

impl<K, V> HashMapBackend<K, V> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
}

impl<K, V> Default for HashMapBackend<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Clone + Eq + Hash + Send + Sync, V: Clone + Send + Sync> MapBackend<K, V>
    for HashMapBackend<K, V>
{
    fn size(&self) -> usize {
        self.data.len()
    }

    fn has(&self, key: &K) -> bool {
        self.data.contains_key(key)
    }

    fn get(&self, key: &K) -> Option<V> {
        self.data.get(key).cloned()
    }

    fn set(&mut self, key: K, value: V) {
        self.data.insert(key, value);
    }

    fn set_many(&mut self, entries: Vec<(K, V)>) {
        for (k, v) in entries {
            self.data.insert(k, v);
        }
    }

    fn delete(&mut self, key: &K) -> bool {
        self.data.remove(key).is_some()
    }

    fn delete_many(&mut self, keys: &[K]) -> usize {
        keys.iter()
            .filter(|k| self.data.remove(*k).is_some())
            .count()
    }

    fn clear(&mut self) -> usize {
        let count = self.data.len();
        self.data.clear();
        count
    }

    fn to_vec(&self) -> Vec<(K, V)> {
        self.data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// IndexBackend
// ---------------------------------------------------------------------------

/// A row in a reactive index: primary key, secondary sort key, value.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde-support",
    derive(serde::Serialize, serde::Deserialize)
)]
pub struct IndexRow<K, V> {
    pub primary: K,
    pub secondary: String,
    pub value: V,
}

/// Storage backend for [`crate::ReactiveIndex`].
pub trait IndexBackend<K: Clone + Eq + Hash, V: Clone>: Send + Sync {
    fn size(&self) -> usize;
    fn has(&self, primary: &K) -> bool;
    fn get(&self, primary: &K) -> Option<V>;
    /// Get the full row (primary + secondary + value) by primary key.
    fn get_row(&self, primary: &K) -> Option<IndexRow<K, V>>;
    /// Returns `true` if inserted (new key), `false` if updated.
    fn upsert(&mut self, primary: K, secondary: String, value: V) -> bool;
    fn upsert_many(&mut self, rows: Vec<(K, String, V)>) -> usize;
    fn delete(&mut self, primary: &K) -> bool;
    fn delete_many(&mut self, primaries: &[K]) -> usize;
    fn clear(&mut self) -> usize;
    fn to_ordered(&self) -> Vec<IndexRow<K, V>>;
    fn to_primary_map(&self) -> Vec<(K, V)>;
}

/// Default sorted-`Vec` + `HashMap`-backed index backend.
///
/// Maintains a sorted Vec of `IndexRow` ordered by `(secondary, primary_str)`
/// plus a parallel HashMap for O(1) primary lookup.
pub struct VecIndexBackend<K, V> {
    rows: Vec<IndexRow<K, V>>,
    primary_map: HashMap<K, V>,
    /// Parallel map for O(1) secondary lookup by primary key.
    secondary_map: HashMap<K, String>,
}

impl<K, V> VecIndexBackend<K, V> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            primary_map: HashMap::new(),
            secondary_map: HashMap::new(),
        }
    }
}

impl<K, V> Default for VecIndexBackend<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Clone + Eq + Hash + Send + Sync + ToString, V: Clone + Send + Sync> IndexBackend<K, V>
    for VecIndexBackend<K, V>
{
    fn size(&self) -> usize {
        self.primary_map.len()
    }

    fn has(&self, primary: &K) -> bool {
        self.primary_map.contains_key(primary)
    }

    fn get(&self, primary: &K) -> Option<V> {
        self.primary_map.get(primary).cloned()
    }

    fn get_row(&self, primary: &K) -> Option<IndexRow<K, V>> {
        let value = self.primary_map.get(primary)?;
        let secondary = self.secondary_map.get(primary)?;
        Some(IndexRow {
            primary: primary.clone(),
            secondary: secondary.clone(),
            value: value.clone(),
        })
    }

    fn upsert(&mut self, primary: K, secondary: String, value: V) -> bool {
        let is_new = !self.primary_map.contains_key(&primary);
        if !is_new {
            // Remove old row from sorted vec.
            self.rows.retain(|r| r.primary != primary);
        }
        self.primary_map.insert(primary.clone(), value.clone());
        self.secondary_map
            .insert(primary.clone(), secondary.clone());
        let row = IndexRow {
            primary,
            secondary,
            value,
        };
        let pos = self
            .rows
            .binary_search_by(|r| {
                r.secondary
                    .cmp(&row.secondary)
                    .then_with(|| r.primary.to_string().cmp(&row.primary.to_string()))
            })
            .unwrap_or_else(|p| p);
        self.rows.insert(pos, row);
        is_new
    }

    fn upsert_many(&mut self, rows: Vec<(K, String, V)>) -> usize {
        let mut inserted = 0;
        for (k, s, v) in rows {
            if self.upsert(k, s, v) {
                inserted += 1;
            }
        }
        inserted
    }

    fn delete(&mut self, primary: &K) -> bool {
        if self.primary_map.remove(primary).is_some() {
            self.secondary_map.remove(primary);
            self.rows.retain(|r| r.primary != *primary);
            true
        } else {
            false
        }
    }

    fn delete_many(&mut self, primaries: &[K]) -> usize {
        primaries.iter().filter(|k| self.delete(k)).count()
    }

    fn clear(&mut self) -> usize {
        let count = self.primary_map.len();
        self.rows.clear();
        self.primary_map.clear();
        self.secondary_map.clear();
        count
    }

    fn to_ordered(&self) -> Vec<IndexRow<K, V>> {
        self.rows.clone()
    }

    fn to_primary_map(&self) -> Vec<(K, V)> {
        self.primary_map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}
