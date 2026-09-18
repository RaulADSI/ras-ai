import sqlite3
import uuid
import hashlib
import os
from typing import NamedTuple, Tuple, Set

class ExportSnapshotItem(NamedTuple):
    allocation_id: int
    bank_id: str
    amount_cents: int
    currency: str
    date: str

class Ledger:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def reserve_batch(self) -> Tuple[str, Tuple[ExportSnapshotItem, ...]]:
        batch_id = f"batch_{uuid.uuid4().hex[:8]}"
        snapshot = []
        
        try:
            self.conn.execute("BEGIN IMMEDIATE;")
            
            cursor = self.conn.execute("""
                SELECT a.id, t.bank_id, a.amount_cents, t.currency, t.original_date
                FROM allocations a
                JOIN transactions t ON a.transaction_id = t.id
                WHERE a.eligibility_state = 'READY_TO_IMPORT'
                  AND a.id NOT IN (
                      SELECT allocation_id FROM export_items 
                      WHERE delivery_state IN ('RESERVED', 'EXPORTED', 'IMPORTED', 'IMPORT_FAILED_REVIEW')
                  )
            """)
            rows = cursor.fetchall()
            
            if not rows:
                self.conn.execute("ROLLBACK;")
                return None, tuple()

            self.conn.execute("INSERT INTO export_batches (batch_id) VALUES (?)", (batch_id,))
            
            for row in rows:
                alloc_id, bank_id, amount, currency, date = row
                self.conn.execute(
                    "INSERT INTO export_items (batch_id, allocation_id, delivery_state) VALUES (?, ?, ?)",
                    (batch_id, alloc_id, 'RESERVED')
                )
                snapshot.append(ExportSnapshotItem(alloc_id, bank_id, amount, currency, date))
                
            self.conn.execute("COMMIT;")
            return batch_id, tuple(snapshot)
            
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise

    def mark_exported(self, batch_id: str, file_path: str, snapshot: Tuple[ExportSnapshotItem, ...]):
        if not os.path.exists(file_path):
            raise FileNotFoundError("Faayilli CSV hin argamne.")

        with open(file_path, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()

        try:
            self.conn.execute("BEGIN IMMEDIATE;")
            
            cursor = self.conn.execute("UPDATE export_batches SET file_hash = ? WHERE batch_id = ?", (file_hash, batch_id))
            if cursor.rowcount == 0:
                raise ValueError("Batch ID sirrii miti.")

            cursor = self.conn.execute("UPDATE export_items SET delivery_state = 'EXPORTED' WHERE batch_id = ? AND delivery_state = 'RESERVED'", (batch_id,))
            if cursor.rowcount != len(snapshot):
                raise ValueError("Baay'inni 'allocation' sirrii miti.")
                
            self.conn.execute("COMMIT;")
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise

    def confirm_import(self, batch_id: str, successful_refs: dict[int, str], failed_allocs: Set[int]):
        success_set = set(successful_refs.keys())
        if not success_set.isdisjoint(failed_allocs):
            raise ValueError("ID wal irra bu'e jira.")

        try:
            self.conn.execute("BEGIN IMMEDIATE;")
            
            for alloc_id, ref in successful_refs.items():
                cursor = self.conn.execute("""
                    UPDATE export_items 
                    SET delivery_state = 'IMPORTED', appfolio_reference = ? 
                    WHERE batch_id = ? AND allocation_id = ? AND delivery_state = 'EXPORTED'
                """, (ref, batch_id, alloc_id))
                if cursor.rowcount == 0:
                    raise ValueError(f"Allocation {alloc_id} sirrii miti ykn haalli isaa 'EXPORTED' miti.")
                
            for alloc_id in failed_allocs:
                cursor = self.conn.execute("""
                    UPDATE export_items 
                    SET delivery_state = 'IMPORT_FAILED_REVIEW' 
                    WHERE batch_id = ? AND allocation_id = ? AND delivery_state = 'EXPORTED'
                """, (batch_id, alloc_id))
                if cursor.rowcount == 0:
                    raise ValueError(f"Allocation {alloc_id} sirrii miti ykn haalli isaa 'EXPORTED' miti.")
                
            self.conn.execute("COMMIT;")
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise