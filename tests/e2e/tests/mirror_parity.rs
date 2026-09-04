#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "acceptance test — failures should stop at the exact setup or parity boundary"
)]
//! Acceptance proof for the reusable source-to-DuckLake parity framework.
//!
//! `just acceptance` owns an isolated Compose stack and runs this target. The test keeps the target
//! table quiescent after each sentinel, waits on control-plane watermarks rather than sleeping, and
//! compares the source with the public DuckLake view after every named scenario step.
#![cfg(feature = "it")]

use e2e::{Harness, ScenarioStep, TableExpectation, TableId, TableParity};
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(180);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the compose backing services; run `just acceptance`"]
async fn dml_and_column_evolution_keep_exact_logical_parity() {
    let target = TableId::new("public", "mirror_parity");
    let mut harness = Harness::start_scenario(
        "DROP TABLE IF EXISTS public.mirror_parity; \
         CREATE TABLE public.mirror_parity ( \
             id bigint PRIMARY KEY, \
             status text NOT NULL, \
             amount numeric(12,2), \
             active boolean, \
             legacy text \
         ); \
         INSERT INTO public.mirror_parity (id, status, amount, active, legacy) VALUES \
             (1, 'created', 10.25, true, 'keep-until-drop'), \
             (2, 'delete-me', NULL, false, NULL), \
             (3, 'nullable', -3.50, NULL, 'old');",
    )
    .await
    .expect("start isolated source, sink, loader, and DuckLake");

    let dml = ScenarioStep::new(
        "insert update delete",
        "BEGIN; \
         INSERT INTO public.mirror_parity (id, status, amount, active, legacy) \
             VALUES (4, 'inserted', 99.99, true, 'new'); \
         UPDATE public.mirror_parity \
             SET status = 'updated', amount = 11.75, active = false WHERE id = 1; \
         DELETE FROM public.mirror_parity WHERE id = 2; \
         COMMIT;",
        "INSERT INTO public.mirror_parity (id, status, amount, active, legacy) \
         VALUES (100, 'dml-sentinel', 0.00, true, 'sentinel')",
    )
    .converge_on(target.clone())
    .expect(TableExpectation::Present(TableParity::auto(
        "public",
        "mirror_parity",
    )));
    harness.run_step(&dml, DEADLINE).await.expect("DML parity");

    let add_column = ScenarioStep::new(
        "add column",
        "ALTER TABLE public.mirror_parity ADD COLUMN note text; \
         UPDATE public.mirror_parity SET note = 'backfilled' WHERE id IN (1, 3);",
        "INSERT INTO public.mirror_parity (id, status, amount, active, legacy, note) \
         VALUES (101, 'add-column-sentinel', 1.01, NULL, NULL, 'new shape')",
    )
    .converge_on(target.clone())
    .expect(TableExpectation::Present(TableParity::auto(
        "public",
        "mirror_parity",
    )));
    harness
        .run_step(&add_column, DEADLINE)
        .await
        .expect("ADD COLUMN parity");

    let drop_column = ScenarioStep::new(
        "drop column",
        "ALTER TABLE public.mirror_parity DROP COLUMN note; \
         UPDATE public.mirror_parity SET status = 'after-drop' WHERE id = 3;",
        "INSERT INTO public.mirror_parity (id, status, amount, active, legacy) \
         VALUES (102, 'drop-column-sentinel', 2.02, false, 'trailing-column-removed')",
    )
    .converge_on(target)
    .expect(TableExpectation::Present(TableParity::auto(
        "public",
        "mirror_parity",
    )));
    harness
        .run_step(&drop_column, DEADLINE)
        .await
        .expect("DROP COLUMN parity");

    harness
        .assert_managed_inventory()
        .await
        .expect("source, registry, and DuckLake table inventories agree");
}
