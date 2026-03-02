Payment Reconciliation and Validation Engine (AMEX/Citi)
This script processes normalized credit card transactions to generate bulk billing files compatible with AppFolio. Its primary function is to act as an intelligent audit filter that applies accounting business rules before authorizing any payment.

🚀 Key Features
Business Rules System (Internal Audit)
The script not only moves data but also validates the integrity of each transaction through three levels of control:
• 	Ownership Validation: Automatically identifies authorized core team members.
• 	Exception Filter (Happy Trailers HRS): Automatically blocks transactions linked to entities not defined as operating under the company.
• 	Reconciliation Alerts (RR Reiter Realty): Flags as ALERT any transaction from this company that does not include the RAS payment identifier in the company or GL account columns.

Data Recovery
Unlike previous processes that relied exclusively on the "RAS" label, this engine prioritizes the identity of the account holder. If a transaction belongs to an authorized member, the system processes it regardless of statement labels, ensuring legitimate charges are not lost (such as validation of specific amounts like 69.97).

Netting Intelligence
The script performs mathematical summation of charges and credits (refunds) under the following conditions:
• 	Groups by date, merchant, resolved vendor, and property.
• 	Status differentiation: Does not mix transactions marked as OK with those marked as ALERT, allowing clear review in the output file.
• 	Eliminates $0.00 balances resulting from immediate cancellations.

Entity Resolution (Fuzzy Match)
Uses fuzzy logic algorithms to:
• 	Vendors: Map raw bank names (e.g., "THE HOME DEPOT #123") to clean names from the official directory.
• 	Properties: Assign each expense to the correct property code in AppFolio based on GL account and mapping rules.
• 	Cash Accounts: Automatically determine the outgoing account (AMEX or Mastercard) based on the source file.

📊 Output Format (AppFolio Ready)
The file generated in  includes an enriched Description column:
Example:

This allows the accounting team to view the audit results directly in the financial software before approving payment.
