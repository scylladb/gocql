# Speculative execution

Configure `Profile.Speculation` with a `SpeculationPolicy` that returns a delay
and whether another attempt is allowed from the attempt number and
MetricsSnapshot. Explicitly idempotent ordinary requests are eligible;
unknown/conditional statements, SERIAL/LOCAL_SERIAL requests and batches never
speculate. Concurrent branches share serialized plan advancement and the same
encoded input owner.

Losing attempts drain before borrowed input can be reused. Request results and
observers account for every started attempt. Disabled speculation allocates no
scheduler/timer and fetches no speculation metrics snapshot. Streaming queries disable
speculation and prefetch, reserving one configured connection through paging.
