# Order lifecycles

These documents explain order behavior and distinguish current implementation from draft proposals. The four instruction types below have the SQLite and PostgreSQL lifecycle work; routed bucket orders still use the existing managed-order path.

- [Market](market.md)
- [Limit](limit.md)
- [Stop market](stop-market.md)
- [Stop limit](stop-limit.md)
- [Shared managed states](managed-states.md): current internal states, public statuses and progress
- [Routed bucket order](routed.md): current transitions and lifecycle gaps
- [Routed lifecycle improvement plan](routed-lifecycle-plan.md): draft decisions and offline acceptance cases

A stop's time in force is one for the whole order (armed, and after trigger
its child), as in IBKR and FIX: GTC, GTD or DAY; IOC and FOK are refused.

[Shared storage and execution ownership](../../README.md)
owns identity, dispatch persistence, transaction boundaries, and recovery.
