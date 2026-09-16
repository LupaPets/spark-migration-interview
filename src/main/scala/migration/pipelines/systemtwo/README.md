# vet_system_two

Sales uses clinic, document, owner, clinician, gross and issued. Gross is decimal major currency units, including negative refunds. The loader converts to integer minor units and the shared InvoiceMappings handles the target model.

Client, pet and payment staging tables use the normalized ingestion schemas in SourceModel. There is no separate historical invoice table.
