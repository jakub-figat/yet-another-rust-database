# YARD - Yet Another Rust Database

Implementation of distributed, wide-column NoSQL database based on `io_uring` and thread-per-core concurrency model.
For learning purposes.

### Done:
- Client/Server communication with protobuf
- Networking
- Client side macro for using structs as models
- Connection pooling
- Transactions
- Concurrent batch requests from client
- Table schema and validation
- SStables
- Commit log

### Work in progress:
- Tests
- Compaction

