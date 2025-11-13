import random
import time

from ccm_model.Transaction import Transaction
from ccm_model.Response import Response
from ccm_model.Enums import Action, TransactionStatus
from ccm_model.DeadlockDetector import DeadlockDetector
from ccm_model.LockManager import LockManager

# sementara
class Row:
    def __init__(self, name: str):
        self.name = name

class ConcurrencyControlManager:
    def __init__(self):
        self.transaction_table: dict[int, Transaction] = {}
        self.lock_table: dict[int, Transaction] = {}
        self.deadlock_detector: DeadlockDetector = DeadlockDetector()
        self.lock_manager: LockManager = LockManager()
        self._next_tid = 1

    def begin_transaction(self) -> int:
        """Memulai transaksi baru dan mengembalikan transaction_id."""
        print("[CCM] Begin transaction called")

        transaction_id = random.randint(1, 100)
        while transaction_id in self.transaction_table:
            transaction_id = random.randint(1, 100)

        transaction = Transaction(
            transaction_id=transaction_id,
            status=TransactionStatus.ACTIVE,
            start_time=time.time(),
        )
        self.transaction_table[transaction_id] = transaction

        return transaction_id

    def log_object(self, object: Row, transaction_id: int) -> None:
        """Mencatat objek (Row) yang diakses oleh transaksi."""
        print(f"Mencatat akses ke {object.name} oleh Transaksi {transaction_id}")

    def validate_object(self, object: Row, transaction_id: int, action: Action) -> Response:
        """Memvalidasi apakah transaksi boleh melakukan aksi tertentu pada objek."""
        print(f"Memvalidasi {action.name} pada {object.name} untuk T{transaction_id}")
        return Response(True, f"{action.name} pada {object.name} divalidasi untuk T{transaction_id}")

    def end_transaction(self, transaction_id: int) -> None:
        """Mengakhiri transaksi."""
        transaction = self.transaction_table.get(transaction_id)
        if not transaction:
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")
        
        transaction.status = TransactionStatus.TERMINATED
        print(f"Transaksi {transaction_id} status menjadi TERMINATED.")

        try:
            self.lock_manager.release_locks(transaction_id)
        except Exception as e:
            return Response(False, f"Gagal melepaskan kunci untuk T{transaction_id}: {e}")

        try:
            self.transaction_table.pop(transaction_id, None)
        except Exception as e:
            return Response(False, f"Gagal menghapus transaksi {transaction_id}: {e}")

        return Response(True, f"Transaksi {transaction_id} berakhir (status={transaction.status.name}).")
            
    def commit_transaction(self, transaction_id: int) -> None:
        """Melakukan commit terhadap transaksi (write data ke log / storage)."""
        transaction = self.transaction_table.get(transaction_id)
        if transaction:
            transaction.status = TransactionStatus.COMMITTED
            print(f"Melakukan commit transaksi {transaction_id}")
            self.end_transaction(transaction_id)
            return Response(True, f"Transaksi {transaction_id} berhasil di-commit.")
        else:
            print(f"Commit gagal. Transaksi {transaction_id} tidak ditemukan.")
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")
            
    def abort_transaction(self, transaction_id: int) -> None:
        """Membatalkan transaksi dan melakukan rollback (abort)."""
        transaction = self.transaction_table.get(transaction_id)
        if transaction:
            transaction.status = TransactionStatus.ABORTED
            print(f"Membatalkan transaksi {transaction_id}")
            # rollback
            self.end_transaction(transaction_id)
            return Response(True, f"Transaksi {transaction_id} berhasil di-abort.")
        else:
            print(f"Abort gagal. Transaksi {transaction_id} tidak ditemukan.")
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")