//! QoS levels shared by the broker core.

/// Delivery guarantee requested by a publisher or granted to a subscriber.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(u8)]
pub enum QoS {
    /// At most once.
    AtMostOnce = 0,
    /// At least once.
    AtLeastOnce = 1,
    /// Exactly once.
    #[default]
    ExactlyOnce = 2,
}

impl QoS {
    /// Wire value `0`, `1`, or `2`.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Returns the lower of the two levels.
    pub fn min(self, other: Self) -> Self {
        if self <= other { self } else { other }
    }
}

impl TryFrom<u8> for QoS {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::AtMostOnce),
            1 => Ok(Self::AtLeastOnce),
            2 => Ok(Self::ExactlyOnce),
            other => Err(other),
        }
    }
}
