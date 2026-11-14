# Raft

A Raft consensus implementation for the MIT 6.824 lab. Servers elect a leader and agree on a shared log of commands, even when servers crash or the network drops messages.

Some key features that I implemented:

- Leader elections with randomized timeouts and heartbeats
- Log replication and applying commands in order after a majority agrees
- Log conflict recovery when disconnected followers rejoin
- Persistent terms, votes, and log entries for recovery after crashes
- Log compaction with snapshots and snapshot transfer to followers that fall behind

The full Raft lab test suite passes with Go's race detector enabled, including elections, replication, persistence, and snapshots.
