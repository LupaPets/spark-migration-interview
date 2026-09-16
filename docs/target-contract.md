# Target contract

All five systems emit the types in model/TargetModel.scala.

| Entity | Source key | Relationship |
|---|---|---|
| Client | source + clinic + client ID | — |
| Pet | source + clinic + pet ID | client_id resolves to Client.id |
| StoreInvoice | source + clinic + invoice ID | client_id resolves to Client.id |
| Payment | source + clinic + payment ID | invoice_id resolves to StoreInvoice.id |

Stable IDs must remain unchanged across reruns and must distinguish clinics that reuse source IDs. StableIds includes the entity kind so unrelated entity IDs cannot collide.

Every source invoice must survive enrichment, even if its vet is missing, inactive or UNKNOWN. Vet IDs are globally unique in this exercise; the reference export contains historical versions. Choose the greatest (updated_at, revision), then attach details only if that selected version is active. The clinic reference is unique, complete and bounded to 200 rows.

Amounts are signed integer minor units inclusive of tax. Negative invoices are credit notes/refunds and must retain their sign. Source system two supplies decimal major units and converts during loading. Totals fit a signed 64-bit sum. Count every invoice, including those without a vet.

Dates are ISO calendar dates without times/time zones. Rerunning a clinic must preserve other clinics' output. Output is Parquet partitioned by clinic_id under a shared table root. Spark's session default is static partition overwrite; the baseline writer overrides that per write.

## Scale

Full exports contain about 80 million invoices / 30 GB before JVM object overhead. One clinic holds 45% of rows. About 35% of invoice vet IDs may be UNKNOWN. The driver has 4 GB and there are multiple executors. No single output file is required. The audit reader benefits from files around 128 MB.

The fixture intentionally covers tiny inputs only. Runtime tests cannot establish that the design scales to this profile.
