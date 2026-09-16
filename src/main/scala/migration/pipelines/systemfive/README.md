# vet_system_five

Charges uses branch, number, customer, vet_id, cents, is_credit and date. Cents is nonnegative; is_credit determines the sign in the normalized invoice model. The shared target mapping must preserve that sign.

Related entities use the normalized ingestion schemas. There is no separate archive table.
