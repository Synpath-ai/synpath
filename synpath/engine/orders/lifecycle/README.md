# Order lifecycles

The four order types in the SQLite storage experiment. These documents define
behavior and identify what the current checkpoint implements.

- [Market](market.md)
- [Limit](limit.md)
- [Stop market](stop-market.md)
- [Stop limit](stop-limit.md)

[Shared storage and execution ownership](../../../../docs/order-storage.md)
owns identity, dispatch persistence, transaction boundaries, and recovery.
