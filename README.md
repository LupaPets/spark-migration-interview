# Veterinary migration review

A fictional Scala / Apache Spark pull-request interview. All names, records and schemas are synthetic. No production services or credentials are needed.

## Your task

This is a live, shared-screen review and coding exercise. No preparation, installation or advance reading is expected. The interviewer will introduce the context, open `InvoiceMigration.scala` and work through it with you.

Read the code together and think aloud: what does it do, what would you check before shipping it, and which change would you make first? Ask questions as you go. After discussing a finding, implement a small correction or regression test together in the interviewer's environment. There is no expectation to find or fix everything. `FollowUpTransforms.scala` is available if there is time for a deeper discussion.

For each finding, explain a concrete triggering input or scale, the effect, and a practical fix or regression test. Prioritize data loss, incorrect financial totals and operational failures. Explain which changes you would block and which need measurements first. Reasonable code should not need to change merely because it looks unusual.

## Context to start the conversation

We are combining invoice exports from two fictional veterinary systems. Every invoice must survive enrichment, totals must remain correct, and rerunning one clinic must preserve other clinics' output. The data is much larger than the driver's memory. The detailed contract below is a reference to consult during the conversation, not required advance reading.

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

These commands are for the interviewer or for optional exploration after the session. Candidates do not need to set up a local environment.

Use JDK 17 or 21 and sbt. Versions are pinned in `build.sbt` and `project/build.properties`.

```sh
sbt compile
sbt "Test / runMain interview.FixtureCheck"
```

On the PR branch, `sbt "runMain interview.Smoke"` runs a small, read-only example. This is a smoke demonstration, not a correctness test. Review the write methods statically; no production storage is configured. `main` contains only the exercise contract, synthetic fixtures and fixture checks; the PR adds the proposed implementation.
