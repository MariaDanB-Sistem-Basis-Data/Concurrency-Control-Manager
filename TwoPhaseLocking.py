import random
import time

from ConcurrencyMethod import ConcurrencyMethod
from ccm_helper.Operation import Operation
from ccm_model.Transaction import Transaction
from ccm_model.Response import Response
from ccm_model.Enums import Action, TransactionStatus
from ccm_model.DeadlockDetector import DeadlockDetector
from ccm_model.LockManager import LockManager
from ccm_model.TransactionManager import TransactionManager
from ccm_helper.Row import Row


class TwoPhaseLocking(ConcurrencyMethod):
    def __init__(self):
        self.transaction_manager: TransactionManager = None
        self.lock_table: dict[int, Transaction] = {}
        self.deadlock_detector: DeadlockDetector = DeadlockDetector()
        self.lock_manager: LockManager = LockManager()
        self._next_tid = 1
        
    def set_transaction_manager(self, transaction_manager: TransactionManager) -> None:
        self.transaction_manager = transaction_manager

    def log_object(self, object: Row, transaction_id: int) -> None:
        """Mencatat objek (Row) yang diakses oleh transaksi."""
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
             print(f"ERROR: Transaksi {transaction_id} tidak ditemukan untuk logging.")
             return
             
        transaction.write_set.append(object.resource_key)
        
        print(f"[LOG] {object.resource_key} dicatat ke Write Set T{transaction_id}.")

    def validate_object(self, obj: Row, transaction_id: int, action: Action) -> Response:
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")

        resource_id = obj.resource_key

        op_type = "R" if action == Action.READ else "W"
        operation = Operation(transaction_id, op_type, resource_id)

        success = self.lock_manager.request_lock(operation)

        # masukkin ke set untuk write atau read
        if success:
            if action == Action.READ:
                transaction.read_set.append(resource_id)
            else:
                transaction.write_set.append(resource_id)

            print(f"[2PL] LOCK diberikan {op_type} {resource_id} untuk T{transaction_id}")
            return Response(True, f"{action.name} pada {resource_id} diizinkan.")

        holder = None
        for res in self.lock_manager.resources.values():
            if res.resourceName == resource_id and res.lockedBy:
                holder = next(iter(res.lockedBy))
                break

        if holder is None:
            return Response(False, f"Lock gagal untuk {resource_id}")

        self.deadlock_detector.add_wait_edge(transaction_id, holder)

        deadlock, cycle = self.deadlock_detector.check_deadlock()
        
        # kalau deadlock pilih victim terus abort
        if deadlock:
            flat = []
            for x in cycle:
                if isinstance(x, list):
                    flat.extend(x)
                else:
                    flat.append(x)

            victim = flat[-1] 
            print(f"[DEADLOCK] Victim = T{victim}")

            self.transaction_manager.abort_transaction(victim)
            self.lock_manager.release_locks(victim)

            return Response(False, f"Deadlock terdeteksi. T{victim} di-abort.")

        print(f"[WAIT] T{transaction_id} menunggu lock {resource_id} milik T{holder}")
        return Response(False, f"T{transaction_id} harus menunggu {resource_id}.")

    def end_transaction(self, transaction_id: int) -> None:
        """Mengakhiri transaksi."""
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")
        
        self.transaction_manager.terminate_transaction(transaction_id)
        print(f"Transaksi {transaction_id} status menjadi TERMINATED.")

        try:
            self.lock_manager.release_locks(transaction_id)
        except Exception as e:
            return Response(False, f"Gagal melepaskan kunci untuk T{transaction_id}: {e}")

        try:
            self.transaction_manager.remove_transaction(transaction_id)
        except Exception as e:
            return Response(False, f"Gagal menghapus transaksi {transaction_id}: {e}")

        return Response(True, f"Transaksi {transaction_id} berakhir (status={transaction.status.name}).")
            
