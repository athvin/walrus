# Explicit scratch ownership supersedes thread-local scratch (PR 15.2)

> **Status:** superseded by PR 11.6. Walrus keeps reusable scratch on the object that owns the work;
> it does not introduce ambient per-thread state. PR 11.17 independently records and preserves the
> zero-thread-local decision.

## What the rule offers

The `conc-thread-local` rule replaces unsafe `static mut` scratch, caches, and accumulators with a
safe independent value per thread, using `Cell` or `RefCell`. That is the right repair when mutable
state truly has thread scope and no narrower owner. Walrus has neither side of that precondition:

```console
$ rg -n 'static mut' crates tests --glob '*.rs' | wc -l
0
$ rg -n 'thread_local!' crates tests --glob '*.rs' | wc -l
0
```

## PR 11.6 already owns the allocation result

PR 11.6 (`perf(PR 11.6): reuse append-loop scratch buffers`) put both reusable strings directly on
`pg_to_arrow::batch::BatchBuilder`:

- `meta_buf` assembles each row's metadata JSON and is cleared and refilled by `append_meta`;
- `ts_buf` is passed as `&mut String` through `append_value`, range, and multirange paths, then
  cleared and refilled by the temporal parsers.

A `BatchBuilder` owns exactly one relation's in-progress Arrow batch. Its scratch therefore shares
the lifetime and movement of the work, retains allocation capacity across rows, and is released with
the builder. No lookup, runtime borrow check, thread affinity, or hidden reset protocol is needed.

Replacing those fields with `thread_local!` would not remove an allocation that remains: it would
move already-reused capacity into ambient state. It would also let unrelated builders on the same
runtime thread contend for one re-entrant `RefCell`, while a builder migrating between runtime
threads would silently switch scratch instances. Explicit ownership makes neither assumption.

## PR 11.17 owns the ambient-state decision

PR 11.17's `docs/implementation/notes/rust-skills/mem-arena-allocator.md` evaluates the broader
thread-local scratch-arena pattern and declines it. The note records the same `meta_buf`/`ts_buf`
standard-library reuse as the matching implementation, plus the production zero-`thread_local!`
invariant. PR 15.2 therefore resolves the corpus overlap in favor of that established final shape;
it does not create a second scratch strategy.

## Reversal condition

Reconsider thread-local scratch only if profiling identifies a synchronous hot path with all three
properties: it has no natural owning builder or request object, repeated allocation is a demonstrated
bottleneck in the representative workload, and execution is intentionally pinned to one thread for
the entire reuse lifetime. The proposal must benchmark thread-local storage against adding explicit
ownership first. Without all three facts, PR 11.6 remains the owner of the allocation fix and PR
11.17 remains the owner of the ambient-state decision.
