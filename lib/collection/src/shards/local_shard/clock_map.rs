use std::collections::{hash_map, HashMap};
use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{cmp, fs, io};

use serde::{Deserialize, Serialize};

use crate::operations::types::CollectionError;
use crate::operations::ClockTag;
use crate::shards::shard::PeerId;

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(transparent)]
pub struct ClockMap {
    clocks: HashMap<Key, Clock>,
}

impl ClockMap {
    pub fn load_or_default(path: &Path) -> Result<Self> {
        let result = Self::load(path);

        if let Err(Error::Io(err)) = &result {
            if err.kind() == io::ErrorKind::NotFound {
                return Ok(Self::default());
            }
        }

        result
    }

    pub fn load(path: &Path) -> Result<Self> {
        let file = fs::File::open(path)?;
        let clock_map = serde_json::from_reader(io::BufReader::new(file))?;
        Ok(clock_map)
    }

    pub fn store(&self, path: &Path) -> Result<()> {
        let file = atomicwrites::AtomicFile::new(path, atomicwrites::AllowOverwrite);

        file.write(|file| -> Result<_> {
            let mut writer = io::BufWriter::new(file);
            serde_json::to_writer(&mut writer, &self)?;
            writer.flush()?;

            Ok(())
        })?;

        Ok(())
    }

    /// Advance clock referenced by the `clock_tag` to `clock_tag.clock_tick`, if it's newer than
    /// the current tick tracked by the clock, or correct `clock_tag.clock_tick` if it's older than
    /// the current tick.
    ///
    /// If the clock is not yet tracked by the `ClockMap`, it is initialized to
    /// the `clock_tag.clock_tick` and added to the `ClockMap`.
    ///
    /// Returns whether operation should be accepted by the local shard and written into the WAL
    /// and applied to the storage, or rejected.
    ///
    /// Operations with `clock_tag.clock_tick = 0` is a special case that is *always* accepted,
    /// *and* its `clock_tag.clock_tick` is *always* corrected and should be written into WAL with
    /// the corrected clock tag!
    #[must_use = "operation accept status must be used"]
    pub fn advance_clock_and_correct_tag(&mut self, clock_tag: &mut ClockTag) -> bool {
        let (clock_updated, current_tick) = self.advance_clock_impl(clock_tag);

        // We "accept" an operation, if it has `clock_tick` that is "newer" than `current_tick` in `ClockMap`
        // (e.g., if `advance_clock_impl` *updated* the clock and returned `clock_updated = true`).
        //
        // If we "reject" an operation (because it has `clock_tick` that is "older" than `current_tick` in `ClockMap`),
        // we have to update its clock tag with `current_tick`, so that it can be "echoed" back to the node.
        //
        // And we also *always* accept all operations with `clock_tick = 0` and *always* update their clock tags.

        let _operation_accepted = clock_updated || clock_tag.clock_tick == 0 || clock_tag.force;
        let update_tag = (!clock_updated || clock_tag.clock_tick == 0) && !clock_tag.force;

        if _operation_accepted && !update_tag {
            log::trace!("Accepting clock tag {clock_tag:?} (current tick: {current_tick})");
        }

        if update_tag {
            if _operation_accepted {
                log::trace!("Updating clock tag {clock_tag:?} (current tick: {current_tick})");
            } else {
                log::trace!("Rejecting clock tag {clock_tag:?} (current tick: {current_tick})")
            }

            clock_tag.clock_tick = current_tick;
        }

        // TODO: now accepts everything, start rejecting old clock values here
        // TODO: return operation_accepted
        true
    }

    /// Advance clock referenced by the `clock_tag` to `clock_tag.clock_tick`, if it's newer than
    /// the current tick tracked by the clock.
    ///
    /// If the clock is not yet tracked by the `ClockMap`, it is initialized to
    /// the `clock_tag.clock_tick` and added to the `ClockMap`.
    pub fn advance_clock(&mut self, clock_tag: &ClockTag) {
        let _ = self.advance_clock_impl(clock_tag);
    }

    /// Advance clock referenced by the `clock_tag` to `clock_tag.clock_tick`, if it's newer than
    /// the current tick tracked by the clock.
    ///
    /// If the clock is not yet tracked by the `ClockMap`, it is initialized to
    /// the `clock_tag.clock_tick` and added to the `ClockMap`.
    ///
    /// Returns whether the clock was *initialized-or-updated* and the current tick.
    #[must_use = "clock update status and current tick must be used"]
    fn advance_clock_impl(&mut self, clock_tag: &ClockTag) -> (bool, u64) {
        let key = Key::from_tag(clock_tag);
        let new_tick = clock_tag.clock_tick;

        match self.clocks.entry(key) {
            hash_map::Entry::Occupied(entry) => entry.get().advance_to(new_tick),
            hash_map::Entry::Vacant(entry) => {
                entry.insert(Clock::new(new_tick));
                (true, new_tick)
            }
        }
    }

    /// Create a recovery point from the current clock map state
    pub fn to_recovery_point(&self) -> RecoveryPoint {
        RecoveryPoint {
            clocks: self
                .clocks
                .iter()
                .map(|(key, clock)| (*key, clock.current_tick.load(Ordering::Relaxed)))
                .collect(),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
struct Key {
    peer_id: PeerId,
    clock_id: u32,
}

impl Key {
    pub fn new(peer_id: PeerId, clock_id: u32) -> Self {
        Self { peer_id, clock_id }
    }

    pub fn from_tag(clock_tag: &ClockTag) -> Self {
        Self {
            peer_id: clock_tag.peer_id,
            clock_id: clock_tag.clock_id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Clock {
    current_tick: AtomicU64,
}

impl Clock {
    pub fn new(tick: u64) -> Self {
        Self {
            current_tick: tick.into(),
        }
    }

    /// Advance clock to `new_tick`, if `new_tick` is newer than current tick.
    ///
    /// Returns whether the clock was updated and the current tick.
    #[must_use = "clock update status and current tick must be used"]
    pub fn advance_to(&self, new_tick: u64) -> (bool, u64) {
        let old_tick = self.current_tick.fetch_max(new_tick, Ordering::Relaxed);

        let _clock_updated = old_tick < new_tick;
        let current_tick = cmp::max(old_tick, new_tick);

        // TODO: now accepts everything, start rejecting old clock values here
        // TODO: return (clock_updated, current_tick)
        (true, current_tick)
    }
}

#[derive(Debug, Clone, Default)]
pub struct RecoveryPoint {
    clocks: HashMap<Key, u64>,
}

impl RecoveryPoint {
    /// Extend this recovery point with new clocks from `clock_map`
    ///
    /// Clocks that we already have in this recovery point are not updated, regardless of their
    /// tick value.
    pub fn extend_with_missing_clocks(&mut self, clock_map: &ClockMap) {
        for (key, clock) in &clock_map.clocks {
            self.clocks
                .entry(*key)
                .or_insert_with(|| clock.current_tick.load(Ordering::Relaxed));
        }
    }
}

impl From<RecoveryPoint> for api::grpc::qdrant::RecoveryPoint {
    fn from(value: RecoveryPoint) -> Self {
        Self {
            clocks: value
                .clocks
                .into_iter()
                .map(|(key, tick)| ClockTag::new(key.peer_id, key.clock_id, tick).into())
                .collect(),
        }
    }
}

impl From<api::grpc::qdrant::RecoveryPoint> for RecoveryPoint {
    fn from(value: api::grpc::qdrant::RecoveryPoint) -> Self {
        Self {
            clocks: value
                .clocks
                .into_iter()
                .map(|tag| (Key::new(tag.peer_id, tag.clock_id), tag.clock_tick))
                .collect(),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

impl From<atomicwrites::Error<Error>> for Error {
    fn from(err: atomicwrites::Error<Error>) -> Self {
        match err {
            atomicwrites::Error::Internal(err) => err.into(),
            atomicwrites::Error::User(err) => err,
        }
    }
}

impl From<Error> for CollectionError {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(err) => err.into(),
            Error::SerdeJson(err) => err.into(),
        }
    }
}
