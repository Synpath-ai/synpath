# Order lifecycles

The four instruction types implemented on SQLite and PostgreSQL. These documents define
behavior and identify what the current checkpoint implements.

- [Market](market.md)
- [Limit](limit.md)
- [Stop market](stop-market.md)
- [Stop limit](stop-limit.md)

A stop's time in force is one for the whole order (armed, and after trigger
its child), as in IBKR and FIX: GTC, GTD or DAY; IOC and FOK are refused.

[Shared storage and execution ownership](../../README.md)
owns identity, dispatch persistence, transaction boundaries, and recovery.
