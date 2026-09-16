# Veterinary migration review

A fictional Scala / Apache Spark pull-request interview. All names, records and schemas are synthetic. No production services or credentials are needed.

## Your task

Review the proposed migration as if it were going into production. Spend about 35 minutes on `InvoiceMigration.scala`, then discuss your highest-priority findings for 10 minutes. `FollowUpTransforms.scala` is an optional extension for a longer session.

For each finding, identify the location, a concrete triggering input or scale, the effect, and a practical fix or regression test. Prioritize data loss, incorrect financial totals and operational failures. You do not need to rewrite the pipeline. Explain which changes you would block and which need measurements first. Reasonable code should not need to change merely because it looks unusual.

## Business contract

- Merge exports from `vet_system_one` and `vet_system_two`. Invoice IDs are unique within a source and clinic; the full key is `(source, clinic_id, invoice_id)`.
- Preserve every invoice, including invoices with absent, unknown or inactive vets. Only attach details of active vets.
- Vet IDs are globally unique in this exercise, but the vet export contains historical versions. Pick the greatest `(updated_at, revision)` per vet before enrichment. Revision breaks timestamp ties.
- Amounts are integer minor units (`amount_cents`), already inclusive of tax. Report exact totals per clinic. Inputs fit a signed 64-bit sum.
- Clinic IDs are globally unique. The clinic reference has exactly one row per clinic, at most 200 rows, and every invoice clinic is present.
- A rerun for one clinic must leave other clinics' existing output intact. Output is Parquet partitioned by clinic, using Spark's default static partition overwrite configuration.
- Format calendar dates as `yyyy-MM-dd`. Dates are ISO dates without times or time zones.
- Missing quantities remain unknown, not zero. An invoice count includes invoices without a vet.

## Production profile

The full input is 80 million invoices, roughly 30 GB before JVM object overhead. One clinic accounts for 45% of rows. Some exports use `UNKNOWN` as the vet ID for about 35% of invoices. The cluster has a 4 GB driver and multiple executors. Small fixtures are deliberately unrepresentative of production volume.

The optional follow-up's `readForClinic` consumes the canonical schema, including nullable `qty` and a large `notes` column. Its audit output promises one row per invoice; its recent-vet selector uses the same versioning contract above.

## Running locally

Use JDK 17 or 21 and sbt. Versions are pinned in `build.sbt` and `project/build.properties`.

```sh
sbt compile
sbt "Test / runMain interview.FixtureCheck"
```

On the PR branch, `sbt "runMain interview.Smoke"` runs a small, read-only example. This is a smoke demonstration, not a correctness test. Review the write methods statically; no production storage is configured. `main` contains only the exercise contract, synthetic fixtures and fixture checks; the PR adds the proposed implementation.
