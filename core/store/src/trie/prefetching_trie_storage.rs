use crate::trie::POISONED_LOCK_ERR;
use crate::{DBCol, StorageError, Store, TrieCache, TrieCachingStorage, TrieStorage};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::TrieNodesCount;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::error;

const MAX_QUEUED_WORK_ITEMS: usize = 16 * 1024;
const MAX_PREFETCH_STAGING_MEMORY: usize = 200 * 1024 * 1024;
/// How much memory capacity is reserved for each prefetch request.
/// Set to 4MiB, the same as `max_length_storage_value`.
const PREFETCH_RESERVED_BYTES_PER_SLOT: usize = 4 * 1024 * 1024;

/// Storage used by I/O threads to prefetch data.
///
/// This implements `TrieStorage` and therefore can be used inside a `Trie`.
/// Prefetching runs through the normal trie lookup code and only the backing
/// trie storage behaves differently.
///
/// `TriePrefetchingStorage` instances are always linked to a parent
/// `TrieCachingStorage`. They share a shard cache, to avoid reading anything
/// from the DB that is already cached.
/// They communicate through `PrefetchStagingArea` exclusively.
///
/// Each I/O threads will have its own copy of `TriePrefetchingStorage`, so
/// this should remain a cheap object.
#[derive(Clone)]
struct TriePrefetchingStorage {
    /// Store is shared with parent `TrieCachingStorage`.
    store: Store,
    shard_uid: ShardUId,
    /// Shard cache is shared with parent `TrieCachingStorage`. But the
    /// pre-fetcher uses this in read-only mode to avoid premature evictions.
    shard_cache: TrieCache,
    /// Shared with parent `TrieCachingStorage`.
    prefetching: PrefetchStagingArea,
}

/// This type is shared between runtime crate and store crate.
///
/// The former creates `PrefetchApi` and puts requests in, the latter serves requests.
/// With this API, the store does not know about receipts etc, and the runtime
/// does not know about the trie structure. The only thing they share is this object.
#[derive(Clone)]
pub struct PrefetchApi {
    /// Bounded, shared queue for all IO threads to take work from.
    ///
    /// Work items are defined as `TrieKey` because currently the only
    /// work is to prefetch a trie key. If other IO work is added, consider
    /// changing the queue to an enum.
    work_queue: Arc<crossbeam::queue::ArrayQueue<TrieKey>>,
    /// Prefetching IO threads will insert fetched data here. This is also used
    /// to mark what is already being fetched, to avoid fetching the same data
    /// multiple times.
    prefetching: PrefetchStagingArea,
    /// Set to true to stop all io threads.
    stop_io: Arc<AtomicBool>,
}

/// Staging area for in-flight prefetch requests and a buffer for prefetched data.
///
/// Before starting a pre-fetch, a slot is reserved for it. Once the data is
/// here, it will be put in that slot. The parent `TrieCachingStorage` needs
/// to take it out and move it to the shard cache.
///
/// A shared staging area is the interface between `TrieCachingStorage` and
/// `TriePrefetchingStorage`. The parent simply checks the staging area before
/// going to the DB. Otherwise, no communication between the two is necessary.
///
/// This design also ensures the shard cache works exactly the same with or
/// without the prefetcher, because the order in which it sees accesses is
/// independent of the prefetcher.
#[derive(Default, Clone)]
pub(crate) struct PrefetchStagingArea(Arc<Mutex<InnerPrefetchStagingArea>>);

#[derive(Default)]
struct InnerPrefetchStagingArea {
    slots: HashMap<CryptoHash, PrefetchSlot>,
    size_bytes: usize,
}

/// Result when atomically accessing the prefetch staging area.
pub(crate) enum PrefetcherResult {
    SlotReserved,
    Pending,
    Prefetched(Arc<[u8]>),
    MemoryLimitReached,
}

/// Type used interanlly in the staging area to keep track of requests.
#[derive(Clone, Debug)]
enum PrefetchSlot {
    PendingPrefetch,
    PendingFetch,
    Done(Arc<[u8]>),
}

impl TrieStorage for TriePrefetchingStorage {
    // Note: This is the tricky bit of the implementation.
    // We have to retrieve data only once in many threads, so all IO threads
    // have to go though the staging area and check for inflight requests.
    // The shard cache mutex plus the prefetch staging area mutex are used for
    // that in combination. Let's call the first lock S and the second P.
    // The rules for S and P are:
    // 1. To avoid deadlocks, S must always be requested before P, if they are
    //    held at the same time.
    // 2. When looking up if something is already in the shard cache, S must not
    //    be released until the staging area is updated by the current thread.
    //    Otherwise, there will be race conditions that could lead to multiple
    //    threads looking up the same value from DB.
    // 3. IO threads should release S and P as soon as possible, as they can
    //    block the main thread otherwise.
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        // Try to get value from shard cache containing most recently touched nodes.
        let mut shard_cache_guard = self.shard_cache.0.lock().expect(POISONED_LOCK_ERR);
        if let Some(val) = shard_cache_guard.get(hash) {
            return Ok(val);
        }

        // If data is already being prefetched, wait for that instead of sending a new request.
        let prefetch_state = PrefetchStagingArea::get_and_set_if_empty(
            &self.prefetching,
            hash.clone(),
            PrefetchSlot::PendingPrefetch,
        );
        // Keep lock until here to avoid race condition between shard cache insertion and reserving prefetch slot.
        std::mem::drop(shard_cache_guard);

        match prefetch_state {
            PrefetcherResult::SlotReserved => {
                let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
                let value: Arc<[u8]> = self
                    .store
                    .get(DBCol::State, key.as_ref())
                    .map_err(|_| StorageError::StorageInternalError)?
                    .ok_or_else(|| {
                        StorageError::StorageInconsistentState("Trie node missing".to_string())
                    })?
                    .into();

                self.prefetching.insert_fetched(hash.clone(), value.clone());
                Ok(value)
            }
            PrefetcherResult::Prefetched(value) => Ok(value),
            PrefetcherResult::Pending => {
                // yield once before calling `block_get` that will check for data to be present again.
                std::thread::yield_now();
                self.prefetching
                    .blocking_get(hash.clone())
                    .or_else(|| {
                        // `blocking_get` will return None if the prefetch slot has been removed
                        // by the main thread and the value inserted into the shard cache.
                        let mut guard = self.shard_cache.0.lock().expect(POISONED_LOCK_ERR);
                        guard.get(hash)
                    })
                    .ok_or_else(|| {
                        // This could only happen if this thread started prefetching a value
                        // while also another thread was already prefetching it. Then the other
                        // other thread finished, the main thread takes it out, and moves it to
                        // the shard cache. And then this current thread gets delayed for long
                        // enough that the value gets evicted from the shard cache again before
                        // this thread has a change to read it.
                        // In this rare occasion, we shall abort the current prefetch request and
                        // move on to the next.
                        StorageError::StorageInconsistentState("Prefetcher failed".to_owned())
                    })
            }
            PrefetcherResult::MemoryLimitReached => {
                Err(StorageError::StorageInconsistentState("Prefetcher failed".to_owned()))
            }
        }
    }

    fn get_trie_nodes_count(&self) -> TrieNodesCount {
        unimplemented!()
    }
}

impl TriePrefetchingStorage {
    pub(crate) fn new(
        store: Store,
        shard_uid: ShardUId,
        shard_cache: TrieCache,
        prefetching: PrefetchStagingArea,
    ) -> Self {
        Self { store, shard_uid, shard_cache, prefetching }
    }
}

impl PrefetchStagingArea {
    /// Release a slot in the prefetcher staging area.
    ///
    /// This must only be called after inserting the value to the shard cache.
    /// Otherwise, the following scenario becomes possible:
    /// 1: Main thread removes a value from the prefetch staging area.
    /// 2: IO thread misses in the shard cache on the same key and starts fetching it again.
    /// 3: Main thread value is inserted in shard cache.
    pub(crate) fn release(&self, key: &CryptoHash) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        let dropped = guard.slots.remove(key);
        // `Done` is the result after a successful prefetch.
        // `PendingFetch` means the value has been read without a prefetch.
        // `None` means prefetching was stopped due to memory limits.
        debug_assert!(
            dropped.is_none()
                || prefetch_state_matches(
                    PrefetchSlot::Done(Arc::new([])),
                    dropped.as_ref().unwrap()
                )
                || prefetch_state_matches(PrefetchSlot::PendingFetch, dropped.as_ref().unwrap()),
        );
        match dropped {
            Some(PrefetchSlot::Done(value)) => guard.size_bytes -= value.len(),
            Some(PrefetchSlot::PendingFetch) => {
                guard.size_bytes -= PREFETCH_RESERVED_BYTES_PER_SLOT
            }
            None => (),
            _ => {
                error!(target: "prefetcher", "prefetcher bug detected, trying to release {dropped:?}");
            }
        }
    }

    /// Block until value is prefetched and then return it.
    ///
    /// Note: This function could return a future and become async.
    /// DB requests are all blocking, unfortunately, so the benefit seems small.
    /// The main benefit would be if many IO threads end up prefetching the
    /// same data and thus are waiting on each other rather than the DB.
    /// Of course, that would require prefetching to be moved into an async environment,
    pub(crate) fn blocking_get(&self, key: CryptoHash) -> Option<Arc<[u8]>> {
        loop {
            match self.0.lock().expect(POISONED_LOCK_ERR).slots.get(&key) {
                Some(PrefetchSlot::Done(value)) => return Some(value.clone()),
                Some(_) => (),
                None => return None,
            }
            std::thread::sleep(std::time::Duration::from_micros(1));
        }
    }

    /// Get prefetched value if available and otherwise atomically set
    /// prefetcher state to being fetched by main thread.
    pub(crate) fn get_or_set_fetching(&self, key: CryptoHash) -> PrefetcherResult {
        self.get_and_set_if_empty(key, PrefetchSlot::PendingFetch)
    }

    fn insert_fetched(&self, key: CryptoHash, value: Arc<[u8]>) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        guard.size_bytes -= PREFETCH_RESERVED_BYTES_PER_SLOT;
        guard.size_bytes += value.len();
        let pending = guard.slots.insert(key, PrefetchSlot::Done(value));
        debug_assert!(prefetch_state_matches(PrefetchSlot::PendingPrefetch, &pending.unwrap()));
    }

    /// Get prefetched value if available and otherwise atomically insert the
    /// given `PrefetchSlot` if no request is pending yet.
    fn get_and_set_if_empty(
        &self,
        key: CryptoHash,
        set_if_empty: PrefetchSlot,
    ) -> PrefetcherResult {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        let size_bytes = guard.size_bytes;
        match guard.slots.entry(key) {
            Entry::Occupied(entry) => match entry.get() {
                PrefetchSlot::Done(value) => PrefetcherResult::Prefetched(value.clone()),
                PrefetchSlot::PendingPrefetch | PrefetchSlot::PendingFetch => {
                    PrefetcherResult::Pending
                }
            },
            Entry::Vacant(entry) => {
                let full =
                    size_bytes > MAX_PREFETCH_STAGING_MEMORY - PREFETCH_RESERVED_BYTES_PER_SLOT;
                if full {
                    return PrefetcherResult::MemoryLimitReached;
                }
                entry.insert(set_if_empty);
                guard.size_bytes += PREFETCH_RESERVED_BYTES_PER_SLOT;
                PrefetcherResult::SlotReserved
            }
        }
    }
}

impl PrefetchApi {
    pub fn new(parent: &TrieCachingStorage) -> Self {
        Self {
            work_queue: Arc::new(crossbeam::queue::ArrayQueue::new(MAX_QUEUED_WORK_ITEMS)),
            prefetching: parent.prefetching.clone(),
            stop_io: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns the trie key back if queue is full.
    pub fn prefetch_trie_key(&self, trie_key: TrieKey) -> Result<(), TrieKey> {
        self.work_queue.push(trie_key)
    }

    pub fn start_io_thread(
        &self,
        parent: &TrieCachingStorage,
        trie_root: CryptoHash,
    ) -> std::thread::JoinHandle<()> {
        let prefetcher_storage = TriePrefetchingStorage::new(
            parent.store.clone(),
            parent.shard_uid,
            parent.shard_cache.clone(),
            self.prefetching.clone(),
        );
        let stop_io = self.stop_io.clone();
        let work_queue = self.work_queue.clone();
        std::thread::spawn(move || {
            // `Trie` cannot be sent across threads but `TriePrefetchingStorage` can.
            //  Therefore, construct `Trie` in the new thread.
            let prefetcher_trie = crate::Trie::new(Box::new(prefetcher_storage), trie_root, None);

            // Keep looping until signalled to stop.
            while !stop_io.load(Ordering::Acquire) {
                if let Some(trie_key) = work_queue.pop() {
                    let storage_key = trie_key.to_vec();
                    if let Ok(Some(_value)) = prefetcher_trie.get(&storage_key) {
                        near_o11y::io_trace!(count: "prefetch");
                    } else {
                        // This may happen in rare occasions and can be ignored safely.
                        // See comments in `TriePrefetchingStorage::retrieve_raw_bytes`.
                        near_o11y::io_trace!(count: "prefetch_failure");
                    }
                } else {
                    std::thread::sleep(Duration::from_micros(10));
                }
            }
        })
    }

    /// Removes all queue up prefetch requests.
    pub fn clear(&self) {
        while let Some(_dropped) = self.work_queue.pop() {}
    }

    /// Stops IO threads after they finish their current task.
    ///
    /// Queued up work will not be finished. But trie keys that are already
    /// being fetched will finish.
    pub fn stop(&self) {
        self.stop_io.store(true, Ordering::Release);
    }
}

fn prefetch_state_matches(expected: PrefetchSlot, actual: &PrefetchSlot) -> bool {
    match (expected, actual) {
        (PrefetchSlot::PendingPrefetch, PrefetchSlot::PendingPrefetch)
        | (PrefetchSlot::PendingFetch, PrefetchSlot::PendingFetch)
        | (PrefetchSlot::Done(_), PrefetchSlot::Done(_)) => true,
        _ => false,
    }
}

/// Implementation to make testing from runtime possible.
///
/// Prefetching by design has no visible side-effects.
/// To nevertheless test the functionality on the API level,
/// a minimal set of functions is required to check the inner
/// state of the prefetcher.
#[cfg(feature = "test_features")]
mod tests {
    use super::{PrefetchApi, PrefetchSlot};
    use crate::TrieCachingStorage;

    impl PrefetchApi {
        /// Returns the number of prefetched values currently staged.
        pub fn num_prefetched_and_staged(&self) -> usize {
            self.prefetching
                .0
                .lock()
                .unwrap()
                .slots
                .iter()
                .filter(|(_key, slot)| match slot {
                    PrefetchSlot::PendingPrefetch | PrefetchSlot::PendingFetch => false,
                    PrefetchSlot::Done(_) => true,
                })
                .count()
        }

        pub fn work_queued(&self) -> bool {
            self.work_queue.len() > 0
        }
    }

    impl TrieCachingStorage {
        pub fn clear_cache(&self) {
            self.shard_cache.clear();
        }
    }
}