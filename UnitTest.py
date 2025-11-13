from ccm_tests.BeginTransactionTest import run_begin_transaction_tests
from ccm_tests.LockManagerTest import run_lock_manager_tests
from ccm_tests.LogObjectTest import run_log_object_tests
    
if __name__ == '__main__':
	run_lock_manager_tests()
	run_log_object_tests()
	run_begin_transaction_tests()