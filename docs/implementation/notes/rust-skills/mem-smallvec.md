# SmallVec for key-column scratch (PR 11.13)

> **Status:** declined — keep `PgRelation::key_columns()` returning its existing `Vec<&str>` and do
> not add or promote a `smallvec` dependency.

## Evidence

The final `loader/keycols` Criterion verification measured median times of **41.554 ns** for a
one-key relation and **45.635 ns** for a three-key relation on the documented Apple M2 benchmark
machine. An initial run measured 43.083 ns and 43.061 ns; Criterion reported no detectable change
between runs. The fastest committed loader transform-cycle median is **25.1 ms**, making key
collection about 0.00017–0.00018% of that cycle (over 550,000× smaller). The two key counts are also
indistinguishable within their confidence intervals.

DuckDB windowing and merge work dominates the loader profile. Avoiding this tiny allocation cannot
materially affect throughput, while `SmallVec` would add an API/storage branch and expand the direct
dependency surface.

## Reversal condition

Revisit only if end-to-end profiling attributes a material share of loader cycle time or allocation
pressure to repeated key-column collection. A new proposal must include that profile, the observed
key-count distribution, and an isolated before/after benchmark whose confidence intervals show a
real improvement. Until then, keep the ordinary `Vec` and make no iterator or inline-vector rewrite.
