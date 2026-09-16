# vet_system_one

Existing source adapter, using shared entity mappings and invoice enrichment.

Current invoices already use InvoiceInput column names and signed minor units. The new historical feed, archived_invoices, contains older, disjoint invoice IDs. It has the same names and types but exports invoice_id before clinic_id. Current and archived rows must both be retained.

The new feature belongs in InvoiceLoader and this pipeline's transformation override when source-specific behavior is needed. Other source systems do not expose archived_invoices.
