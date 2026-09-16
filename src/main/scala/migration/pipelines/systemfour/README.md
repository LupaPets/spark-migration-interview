# vet_system_four

Documents stores location.id, customer.id and total.cents in nested structs, with id, vet_id and date at the top level. Amounts are signed minor units. The adapter flattens these into InvoiceInput before shared target mapping.

Related entities use the normalized ingestion schemas. There is no separate archive table.
