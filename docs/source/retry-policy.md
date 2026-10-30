# Retry policy

`Profile.Retry` implements `RetryPolicy.Decide(AttemptError) RetryDecision`.
`SafeRetryPolicy` bounds attempts and retries only when outcome/idempotence and
server evidence permit. Custom policies can explicitly choose downgrading; achieved consistency is
reported. Decisions select StopRetry,
RetrySame, RetryNext or IgnoreWriteError, with optional consistency and delay.

Retries reuse encoded binding inputs and compiled result plans. They do not
invoke application encoders again. Unknown-outcome writes require explicit
idempotence; deterministic errors never retry. Streaming may retry only before
any application output begins and never replays a delivered row.

Request observers see one terminal request event and one per started attempt.
Callbacks are synchronous and must be prompt; disabled observation avoids event
construction. Inspect errors with errors.Is/errors.As to retain server details.
