# HVFHV — Design walkthrough results

Date: 2026-11-09. Method: manual state traces and independent arithmetic on the written contracts. Status values here describe specification reasoning only, not executable integration tests. T01–T29 remain NOT RUN.

## W01 — A retry after a later rollback

Initial selection `(A, generation 7)`. Operation key K selects B at generation 8 and commits, but the caller loses the acknowledgement. An operator subsequently selects A at generation 9. Retrying K must return the existing event `(A→B, 7→8)` as historical success and report that current is now A/9. It must not reselect B. A stale new operation expecting A/7 fails even though the release ID again equals A. Expected contract result: internally consistent with unique operation key and generation comparison. Relates to T18/T19.

## W02 — Revised source file

Plan contains month M at revision X with n rows. Discovery observes revision Y with m rows. A fresh candidate removes X's contribution, inserts Y once and recomputes dependents. Expected candidate count is old total minus n plus m, not old total plus m. A repeated Y acquisition creates another observation, not another contribution. The source locator of a row in Y does not imply correspondence with X. Expected contract result: consistent with R03/R05/R06 and T02/T06/T13.

## W03 — Warning versus computation failure

Assume all structural rules PASS. Q02 records five negative intervals and returns FAIL at WARN severity: publication can proceed with explicit exclusions. Replacing Q02's result with ERROR, removing it, duplicating it, or returning an unapproved NOT_APPLICABLE blocks S07/publication. A BLOCK/NOT_APPLICABLE is never accepted. Expected contract result: gate truth table separates imperfect source data from an incomplete evaluation. Relates to T26.

## W04 — New publication with a gap

Current ends at September. Discovery sees November available but October unavailable. The twelve consecutive months ending in November contain a gap; no new complete-window plan can be selected. The loop still checks November after October's 404 and records it as available. If October later arrives, a fresh plan can freeze both revisions. Availability is distinct from inclusion. Expected contract result: consistent with discovery frontier and T25; no twelve-nonconsecutive-file substitution.

## W05 — Finalized original and lost registry write

Download completes, full decode and hash pass, provenance record is synchronized, and the blob is finalized. The process stops before source revision registration. Reconciliation verifies digest and provenance before registering reuse. Without provenance it leaves an unassigned blob, even if its filename looks plausible. A plan already frozen cannot acquire this revision retroactively. Expected contract result: consistent with T29 and frozen-plan identity.

## W06 — Rejection while a cached query is in flight

R1 is VERIFIED when a query starts. R1 is subsequently REJECTED. The completion check denies presenting the result and the next cache hit is denied outside the cached function. Merely superseding R1 with R2 while R1 remains VERIFIED does not invalidate its pinned data. Previously displayed values cannot be recalled; the next render surfaces the rejection. Expected contract result: matches T21/T27 without claiming instantaneous browser revocation.

## W07 — Interval and percentile arithmetic

For eligible values sorted as `[0, 0.2, 1, 10]`, continuous quantile rank is `h=(n-1)p` on zero-based indices. At p=0.5, h=1.5, interpolate 0.2 and 1 to obtain 0.6. At p=0.95, h=2.85, interpolate 1 and 10 to obtain 8.65. Adding one missing pair and one negative pair gives total=6, eligible=4, missing=1, negative=1; quantiles are unchanged. Empty eligible set yields NULL, not zero. This is hand calculation, not engine output. Relates to T08/T12.

## W08 — Resource envelope

If each retained database and candidate used 15 GiB, four copies consume 60 GiB. With assumed 6 GiB originals and a reserved 16 GiB spill allowance, the envelope already exceeds the 80 GiB budget before logs/headroom. The correct action is resource failure or an explicit design amendment, not dropping a retained release or sampling source rows silently. These values are illustrative inputs, not measured sizes. Relates to T22 and the technical-readiness capacity analysis.

## Conclusion and limits

The eight traces have defined outcomes under the amended contracts. They are stronger than a file-existence check but weaker than an executable model or runtime test. No process, SQL engine, HTTP server or filesystem failure was exercised. Implementation verification must test the same outcomes against actual behavior, including race timing and rollback evidence.
