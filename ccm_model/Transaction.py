from dataclasses import dataclass, field
from datetime import datetime
from typing import Set

from ccm_model.Enums import TransactionState


@dataclass
class Transaction:
    transaction_id: int
    status: TransactionState
    start_time: datetime = None
    last_access_time: datetime = None
    
    def get_transaction_id(self) -> int:
        return self.transaction_id
    
    def get_state(self):
        return self.status
    
    def get_start_time(self):
        return self.start_time
    
    def get_last_access_time(self):
        return self.last_access_time
    
    def is_active(self) -> bool:
        return self.status == TransactionState.ACTIVE
    
    def is_committed(self) -> bool:
        return self.status == TransactionState.COMMITTED
    
    def is_aborted(self) -> bool:
        return self.status == TransactionState.ABORTED
    
    def can_be_aborted(self) -> bool:
        return self.status in {TransactionState.ACTIVE, TransactionState.PARTIALLY_COMMITTED}
    