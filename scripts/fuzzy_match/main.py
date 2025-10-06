import sys
import os
import pandas as pd

# --- Add project root for imports ---
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from scripts.fuzzy_match.resolvers import (
    resolve_property_code,
    resolve_vendor,
    resolve_gl_from_vendor,
    resolve_cash_account,
)
from scripts.ingestion.assign_vendor_gl import apply_manual_rules


def main():
    # --- Load Data ---
    print("📥 Loading normalized data...")

    statement_path = "data/clean/normalized_statement_amex.csv"
    df = pd.read_csv(statement_path)
    gl_accounts = pd.read_csv("data/clean/normalized_gl_accounts.csv")
    property_directory = pd.read_csv("data/clean/normalized_property_directory.csv")
    vendor_directory = pd.read_csv("data/clean/normalized_vendor_directory.csv")
    vendor_gl_map_df = pd.read_csv("data/clean/vendor_gl_map.csv")

    # Map vendors to GL accounts
    vendor_gl_map = {row["vendor"]: row["gl_account"] for _, row in vendor_gl_map_df.iterrows()}

    # --- Resolution Pipeline ---
    print("🔍 Resolving vendors...")
    df[["resolved_vendor", "vendor_match_score"]] = df.apply(
        lambda r: pd.Series(resolve_vendor(r, vendor_directory)),
        axis=1
    )

    print("🏠 Resolving properties...")
    df[["resolved_property", "property_match_score", "property_source"]] = df.apply(
        lambda row: pd.Series(resolve_property_code(row["company"], property_directory)),
        axis=1
    )

    print("📘 Resolving GL Accounts from vendors...")
    df[["resolved_gl_account", "gl_match_score", "gl_resolution_source"]] = df.apply(
        lambda r: pd.Series(resolve_gl_from_vendor(r["resolved_vendor"], vendor_gl_map, gl_accounts)),
        axis=1
    )

    # --- Build AppFolio-compatible DataFrame ---
    print("🧾 Building AppFolio output...")

    new_df = pd.DataFrame()
    new_df["Vendor Payee Name*"] = df["resolved_vendor"]
    new_df["Amount*"] = df["amount"]
    new_df["Bill Property Code*"] = df["resolved_property"]
    new_df["Bill Account*"] = df["resolved_gl_account"]

    # --- Cash Account and Dynamic Description ---
    cash_account = resolve_cash_account(statement_path)
    new_df["Cash Account"] = cash_account

    if "amex" in cash_account.lower():
        description_text = "Amex Payment"
    elif "mastercard" in cash_account.lower():
        description_text = "Mastercard Payment"
    elif "bank of america" in cash_account.lower() or "boa" in cash_account.lower():
        description_text = "Bank of America Payment"
    elif "chase" in cash_account.lower():
        description_text = "Chase Payment"
    else:
        description_text = "Payment"

    new_df["Description"] = description_text

    # --- Fill required but unused columns ---
    for col in [
        "Bill Date*", "Due Date*", "Posting Date*",
        "Bill Reference", "Bill Remarks", "Memo For Check", "Purchase Order Number"
    ]:
        new_df[col] = ""

    # --- Handle Special Cases ---
    mask = (
        new_df["Bill Account*"].isnull() | (new_df["Bill Account*"].astype(str).str.strip() == "")
    ) & (
        new_df["Vendor Payee Name*"].str.contains("THE SHERWIN-WILLIAMSCLEVELAND", case=False, na=False)
    )
    new_df.loc[mask, "Bill Account*"] = "6435: General Repairs"

    # --- Save Output ---
    output_path = "data/clean/appfolio_ready_bulk.csv"
    new_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✅ File generated: {output_path}")

    # --- Log Unresolved GL Accounts ---
    unresolved = df[df["gl_resolution_source"] == "unresolved"]
    if unresolved.empty:
        print("🎉 No unresolved GL accounts!")
    else:
        print("\n⚠️ Unresolved GL accounts found. Review the following:")
        print(unresolved[["resolved_vendor", "amount", "gl_resolution_source"]].to_string(index=False))


if __name__ == "__main__":
    main()
