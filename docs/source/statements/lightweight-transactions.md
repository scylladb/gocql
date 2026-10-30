# Lightweight transactions

Set statement Kind to `ConditionalStatement` when known. Server conditional
metadata and SERIAL/LOCAL_SERIAL consistency also force safe conditional
classification. Conditional requests never speculate and use deterministic
replica ordering.

Use `CASRows(decoder)` to decode the applied flag and the returned row through a
typed `CASResult[R]`. For conditional batches, call `RunBatch` with this decoder.
The ordinary `Options` carries regular and serial consistency; profile defaults
apply when an explicit override is absent. Inspect every returned row and close
the result page/rows even when the condition was not applied.
