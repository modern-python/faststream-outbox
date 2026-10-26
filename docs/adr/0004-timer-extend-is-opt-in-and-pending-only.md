# Extending a timer is opt-in, only moves the deadline later, and skips leased rows

`publish(..., timer_id=..., timer_conflict="extend")` updates the live `(queue, timer_id)` row in the
same `INSERT ... ON CONFLICT DO UPDATE` statement that would otherwise insert it. The default stays
`"ignore"`, the old `ON CONFLICT DO NOTHING`, because callers that replay a publish rely on a
duplicate being a no-op; making every duplicate an update would postpone or rewrite their timers on
replay. The update sets `next_attempt_at` to `GREATEST` of the stored and requested deadlines, and
takes the new payload and headers and clears the retry history only when the requested deadline is
later. That makes concurrent and out-of-order heartbeats safe without any ordering between callers,
at the cost of never moving a timer earlier; a mode that can is a new `TimerConflict` value, not a
change to this one. The conditions live in `CASE` expressions rather than the `DO UPDATE ... WHERE`,
so a stale extend still returns the row id and every extend writes a row version even when nothing
changes. The `WHERE` holds only `acquired_token IS NULL`: a leased row is never updated, and the
call returns `None`. Clearing or rotating the lease so the update could land would stop the old
worker's completion from deleting the refreshed row, but it cannot undo what that worker's handler
already did, so the library reports the claim instead of hiding it. Because the update locks the
row and the claim uses `SKIP LOCKED`, an extend and a claim are always ordered: either the claim
sees the new deadline or the extend sees the lease. Deciding whether a heartbeat should win over an
expiry that is already running is left to the application's handler.
