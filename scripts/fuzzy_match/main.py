import sys
import os
import pandas as pd

# Add the project root to the system path to resolve module import errors
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from scripts.fuzzy_match.resolvers import (
    resolve_property_code,
    resolve_vendor,
    resolve_gl_from_vendor,
    resolve_cash_account,
    # resolve_transaction_mappings is not used in this main script, but kept for context
)
from scripts.ingestion.assign_vendor_gl import apply_manual_rules

# --- Main Execution Block ---
def main():
  
    # --- Load Data ---
    print("Loading normalized data...")
    # Define file paths for easier management
    statement_path = "data/clean/normalized_statement_amex.csv"
    
    df = pd.read_csv(statement_path)
    gl_accounts = pd.read_csv("data/clean/normalized_gl_accounts.csv")
    property_directory = pd.read_csv("data/clean/normalized_property_directory.csv")
    vendor_directory = pd.read_csv("data/clean/normalized_vendor_directory.csv")
    vendor_gl_map_df = pd.read_csv("data/clean/vendor_gl_map.csv")
    
    # Create the vendor to GL mapping dictionary
    vendor_gl_map = {row["vendor"]: row["gl_account"] for _, row in vendor_gl_map_df.iterrows()}

    # --- Data Resolution Pipeline ---
    print("🔍 Resolving vendors...")
    df[["resolved_vendor", "vendor_match_score"]] = df.apply(
        lambda r: pd.Series(resolve_vendor(r, vendor_directory)),
        axis=1
    )

    print("Resolving properties...")
    # Using a more robust apply with a lambda to handle potential errors
    df[["resolved_property", "property_match_score", "property_source"]] = df.apply(
        lambda row: pd.Series(resolve_property_code(row["company"], property_directory)),
        axis=1
    )

    # Manual rules are part of the GL resolution, but can be applied here if needed for other steps
    # print("⚙️ Applying manual rules...")
    # df = apply_manual_rules(df) # This function seems to expect a DataFrame

    print("Resolving GL Accounts from vendors...")
    df[["resolved_gl_account", "gl_match_score", "gl_resolution_source"]] = df.apply(
        lambda r: pd.Series(resolve_gl_from_vendor(r["resolved_vendor"], vendor_gl_map, gl_accounts)),
        axis=1
    )

    # --- Build AppFolio-compatible DataFrame ---
    print("Building AppFolio output...")
    output_columns = [
        "Bill Property Code*", "Vendor Payee Name*", "Amount*", "Bill Account*",
        "Description", "Bill Date*", "Due Date*", "Posting Date*", "Bill Reference",
        "Bill Remarks", "Memo For Check", "Purchase Order Number", "Cash Account"
    ]
    new_df = pd.DataFrame() # Initialize empty DataFrame

    new_df["Vendor Payee Name*"] = df["resolved_vendor"]
    new_df["Amount*"] = df["amount"]
    new_df["Bill Property Code*"] = df["resolved_property"]
    new_df["Bill Account*"] = df["resolved_gl_account"]
    new_df["Cash Account"] = resolve_cash_account(statement_path)

    # Assign empty strings to required but unused columns
    for col in [
        "Description", "Bill Date*", "Due Date*", "Posting Date*",
        "Bill Reference", "Bill Remarks", "Memo For Check", "Purchase Order Number"
    ]:
        new_df[col] = ""

    # --- Handle Special Cases ---
    # Apply special rule for ACE Hardware where GL account is unresolved
    mask = (
        new_df["Bill Account*"].isnull() | (new_df["Bill Account*"].astype(str).str.strip() == "")
    ) & (
        new_df["Vendor Payee Name*"].str.contains("ACE Hardware", case=False, na=False)
    )
    new_df.loc[mask, "Bill Account*"] = "6435: General Repairs"

    # --- Save Output ---
    output_path = "data/clean/appfolio_ready_bulk.csv"
    new_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✅ File generated: {output_path}")

    # --- Log Unresolved GL Accounts for Review ---
    unresolved = df[df["gl_resolution_source"] == "unresolved"]
    if unresolved.empty:
        print("🎉 No unresolved GL accounts!")
    else:
        print("\n Unresolved GL accounts found. Review the following:")
        print(unresolved[["resolved_vendor", "amount", "gl_resolution_source"]].to_string(index=False))

if __name__ == "__main__":
    main()

