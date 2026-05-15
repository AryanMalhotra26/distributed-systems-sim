# Distributed Systems Simulation

A Java implementation of core distributed algorithms — leader election, consensus protocols, and fault-tolerant control flow — with comprehensive JUnit test suites covering edge cases and simulated network failures.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Java |
| Testing | JUnit 5 |
| Concurrency | Java Multithreading, ExecutorService |
| Messaging | Message Passing (simulated) |
| Build | Maven |

## Architecture

```
Node A ──┐
Node B ──┼──► Message Bus ──► Consensus Engine ──► Committed State
Node C ──┘         │
                   └──► Fault Detector ──► Leader Election
```

Each node runs as an independent thread, communicating via a shared message-passing layer that simulates real network conditions including delays, partitions, and crashes.

## Algorithms Implemented

### Leader Election
- **Ring-based election** (Chang-Roberts algorithm) — O(n log n) messages
- **Bully algorithm** — elects highest-ID active node
- Handles node crashes mid-election with automatic re-election

### Consensus
- **Paxos-style** consensus with Prepare/Promise/Accept/Commit phases
- Majority quorum enforcement across configurable node counts
- Safe under concurrent proposals and node failures

### Fault Tolerance
- Heartbeat-based failure detection with configurable timeouts
- Automatic re-election when leader becomes unresponsive
- State recovery after network partition heals

## Key Features

- Fully simulated network topology — no real sockets required
- Configurable fault injection: crash nodes, delay messages, drop packets
- Thread-safe node state management with Java concurrency primitives
- Deterministic test replay via seeded randomness

## Test Coverage

```
✅ Leader election — single failure, concurrent failures, network partition
✅ Consensus — happy path, competing proposers, quorum loss scenarios  
✅ Fault detection — timeout accuracy, false-positive prevention
✅ Recovery — re-election after crash, state consistency post-partition
```

## Setup

```bash
git clone https://github.com/AryanMalhotra26/distributed-systems-sim.git
cd distributed-systems-sim
mvn clean test          # Run all JUnit test suites
mvn exec:java           # Run simulation demo
```

## Results

- Consensus achieved in < 5 message rounds under normal conditions
- Leader election completes within O(n) rounds for bully algorithm
- 100% test pass rate across 40+ JUnit test cases covering edge cases and fault scenarios
