//! AI Trading Agent - High-performance event-driven trading system
//!
//! This crate provides a comprehensive trading agent framework with event-driven
//! architecture, supporting backtesting, paper trading, and live trading modes.

pub mod event_bus;
pub mod events;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
