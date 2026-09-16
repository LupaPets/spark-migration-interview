# vet_system_three

Ledger uses site, reference, account, staff, minor_units and date. Amounts are already signed minor units. The adapter only renames fields before the shared InvoiceMappings.

Client, pet and payment staging tables use the normalized ingestion schemas in SourceModel. There is no separate historical invoice table.
