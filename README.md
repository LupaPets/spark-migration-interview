# Veterinary migration platform

A compact, fictional migration codebase used for a live Scala / Spark interview. Five veterinary systems feed the same target data model. All schemas and records are synthetic; no production services or credentials are used.

## Live session

No preparation, installation or advance reading is expected. The interviewer shares the PR and editor. Start with the PR's source pipeline, follow its dependencies as needed, and think aloud. Ask questions, prioritize your findings, then implement a small fix or regression test together. There is no expectation to inspect every file or fix everything.

The proposed change adds historical invoice support to an existing pipeline. Review it as a production change: consider both the source being changed and the other consumers of shared code.

## Finding your way around

- `migration/Main.scala`: entry point and orchestration.
- `core/`: pipeline contract, registry, configuration and batch output.
- `pipelines/systemone/` through `systemfive/`: existing source adapters with different schemas.
- `model/TargetModel.scala`: the shared client, pet, invoice and payment model.
- `shared/`: identity generation, related-entity mappings and enrichment.
- `output/`: common partitioned storage writer.
- `validation/`: financial reconciliation.
- `reporting/`: bounded previews, source profiles and target inventory.
- `demo/` and `src/test/`: synthetic exports and cross-pipeline contract checks.

See [architecture](docs/architecture.md), [target contract](docs/target-contract.md), and each source's README when you need more detail. These are references for the live conversation.

## Local execution

For the interviewer; candidates need no setup. Use JDK 17 or 21 and sbt.

```sh
sbt compile
sbt "Test / runMain migration.PipelineContractCheck"
sbt "Test / runMain migration.PlatformSupportCheck"
sbt "runMain migration.Main vet_system_one"
sbt "runMain migration.Main vet_system_two"
```

The default demo only reads synthetic data and displays results. An optional second argument is a local output root; the writer updates clinic_a there. Use a new disposable directory when exploring writes. The shared writer is not a transactional multi-table store.

Use `--clinic clinic_b` to change the output scope, `--preview 5` to limit displayed invoices, and `--validate` to display identity and foreign-key diagnostics. Validation is a diagnostic report, not an automatic write gate. Invoice datasets are persisted during repeated reporting and released even if a consumer fails.

`main` is a working platform baseline. The interview PR intentionally introduces defects. Contract checks pass on main; existing checks may fail on the PR branch, and they are not comprehensive.
