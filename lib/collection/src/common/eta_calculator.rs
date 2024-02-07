use std::time::{Duration, Instant};

/// A progress ETA calculator.
/// Calculates the ETA roughly based on the last ten seconds of measurements.
pub struct EtaCalculator {
    ring: [(Instant, usize); Self::SIZE],
    ring_pos: usize,
}

impl EtaCalculator {
    const SIZE: usize = 16;
    const DURATION: Duration = Duration::from_millis(625);

    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            ring: [(now, 0); Self::SIZE],
            ring_pos: 0,
        }
    }

    /// Capture the current progress and time.
    pub fn set_progress(&mut self, current_progress: usize) {
        if current_progress < self.ring[self.ring_pos].1 {
            // Progress went backwards, reset the state.
            *self = Self::new();
        }
        let now = Instant::now();
        if now - self.ring[(self.ring_pos + Self::SIZE - 1) % Self::SIZE].0 >= Self::DURATION {
            self.ring_pos = (self.ring_pos + 1) % Self::SIZE;
        }
        self.ring[self.ring_pos] = (now, current_progress);
    }

    /// Calculate the ETA to reach the target progress.
    pub fn estimate(&self, target_progress: usize) -> Duration {
        let now = Instant::now();
        let (_, current_progress) = self.ring[self.ring_pos];
        let (prev_time, old_progress) = self.ring[(self.ring_pos + Self::SIZE - 1) % Self::SIZE];
        match target_progress.checked_sub(current_progress) {
            None => Duration::from_secs(0),
            Some(value_diff) => {
                let rate = value_diff as f64 / (current_progress - old_progress) as f64;
                let time_diff = (now - prev_time).as_secs_f64();
                Duration::try_from_secs_f64(time_diff * rate).unwrap_or(Duration::MAX)
            }
        }
    }
}
