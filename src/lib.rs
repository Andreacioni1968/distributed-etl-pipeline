//! CAPSULA - Binary-first transport protocol for distributed data meshes.
//!
//! Provides cryptographically verifiable data structures for bidirectional 
//! communication between Core Controllers and Edge Nodes (Workers).
//! Optimized for transporting Apache Parquet payloads with attached JSON metadata.

use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// Message routing direction in the hub-and-spoke mesh.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Direction {
    CoreToEdge,
    EdgeToCore,
}

/// Supported distributed operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Operation {
    // Stage 1: Distributed Computation
    CalcIndicators,
    CalcFiboGeometry,
    DetectPatterns,

    // Stage 2: Aggregation & Logic
    GenMandelbrotWisdom,
    RunWalkerSim,

    // Stage 3: Edge Data Ingestion
    FetchCandles,
    FetchTicker,
    FetchAll,

    // Diagnostics & Custom
    Ping,
    Custom(String),
}

/// Execution status for asynchronous callbacks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Status {
    Ok,
    Error,
    Partial,
    Pending,
}

/// JSON-serializable envelope metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapsulaMeta {
    // Routing & Identity
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub direction: Direction,

    // Workload Specifications
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub operation: Operation,
    pub params: Option<serde_json::Value>,

    // Callback & Telemetry (Populated on EdgeToCore)
    pub status: Option<Status>,
    pub rows_processed: Option<u64>,
    pub execution_ms: Option<u64>,
    pub error_msg: Option<String>,
    pub reply_to: Option<Uuid>,
}

/// The complete cryptographic payload envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capsula {
    pub meta: CapsulaMeta,
    pub payload: Vec<u8>,       // Typically compressed Apache Parquet binaries
    pub sha256: String,         // Hex-encoded integrity hash
}

impl Capsula {
    /// Constructs a new Capsula and automatically computes the SHA256 hash.
    pub fn new(meta: CapsulaMeta, payload: Vec<u8>) -> Self {
        let sha256 = Self::compute_hash(&payload);
        Self { meta, payload, sha256 }
    }

    /// Computes the SHA256 checksum of the binary payload.
    pub fn compute_hash(payload: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(payload);
        hex::encode(hasher.finalize())
    }

    /// Cryptographically verifies the integrity of the payload in transit.
    pub fn verify(&self) -> bool {
        Self::compute_hash(&self.payload) == self.sha256
    }

    /// Factory method for Core -> Edge dispatches.
    pub fn mission(
        symbol: &str,
        timeframe: &str,
        operation: Operation,
        payload: Vec<u8>,
        params: Option<serde_json::Value>,
    ) -> Self {
        let meta = CapsulaMeta {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            direction: Direction::CoreToEdge,
            symbol: Some(symbol.to_string()),
            timeframe: Some(timeframe.to_string()),
            operation,
            params,
            status: None,
            rows_processed: None,
            execution_ms: None,
            error_msg: None,
            reply_to: None,
        };
        Self::new(meta, payload)
    }

    /// Factory method for Edge -> Core callbacks.
    pub fn reply(
        original_id: Uuid,
        status: Status,
        payload: Vec<u8>,
        rows_processed: Option<u64>,
        execution_ms: Option<u64>,
        error_msg: Option<String>,
    ) -> Self {
        let meta = CapsulaMeta {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            direction: Direction::EdgeToCore,
            symbol: None,
            timeframe: None,
            operation: Operation::Custom("reply".to_string()),
            params: None,
            status: Some(status),
            rows_processed,
            execution_ms,
            error_msg,
            reply_to: Some(original_id),
        };
        Self::new(meta, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mission() {
        let payload = vec![1, 2, 3, 4, 5];
        let capsula = Capsula::mission("BTCUSD", "H1", Operation::CalcIndicators, payload.clone(), None);

        assert_eq!(capsula.meta.direction, Direction::CoreToEdge);
        assert_eq!(capsula.meta.symbol, Some("BTCUSD".to_string()));
        assert!(capsula.verify()); // Cryptographic check passes
    }

    #[test]
    fn test_verify_integrity() {
        let payload = vec![1, 2, 3, 4, 5];
        let mut capsula = Capsula::mission("BTCUSD", "H1", Operation::Ping, payload, None);
        
        assert!(capsula.verify());

        // Simulate network corruption
        capsula.payload[0] = 99;
        assert!(!capsula.verify());
    }
}
