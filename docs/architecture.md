# Architecture

The registry selects a source-specific subclass of BasePipeline. Each adapter loads its own invoice schema into InvoiceInput. BasePipeline then builds a MigrationBatch with the same four target entities, regardless of source.

```text
Main -> PipelineRegistry -> BasePipeline.run
                             |-- systemone.InvoiceLoader --|
                             |-- systemtwo.Pipeline -------|-> InvoiceInput
                             |-- systemthree.Pipeline -----|     |
                             |-- systemfour.Pipeline -------|     |
                             |-- systemfive.Pipeline -------|     |
                             |                              InvoiceMappings
                             |                                  |
                             |-- EntityMappings ----------> MigrationBatch
                                                               |
                                          Reconciliation / BatchWriter
```

SourceTables is the input boundary. The demo supplies in-memory exports; a production implementation would resolve staged tables. The client, pet and payment exports have already been normalized by ingestion in this exercise. Invoice adapters still handle five distinct source formats.

A subclass may override transformInvoices for source-specific behavior. Shared mappings are the default and changes there affect every adapter using them. EntityMappings constructs foreign keys with the same StableIds helper as invoice mappings. Follow those consumers before changing ID inputs.

BatchWriter replaces the requested clinic partition independently in each target table. It is not a cross-table transaction. The caller owns sequencing and retry orchestration. Audit writes go to a distinct run path. MigrationBatch datasets are lazy; building the batch does not execute a Spark job.

The demo loads both clinic_a and clinic_b to exercise tenant isolation and repeated source IDs. PipelineConfig.clinicId controls which partition is published. The current job contract permits loading a multi-clinic export.
