class Config(object):
    batch_account_name = ''
    batch_account_key = ''
    batch_account_url = ''
    storage_account_name = ''
    storage_account_key = ''
    # Blob container names must not contain capital letters (?!?)
    script_container_name = 'primecontainerscript'
    output_container_name = 'primecontaineroutput'
    pool_id = 'PrimeNumbersPool'
    dedicated_node_count = 0
    low_priority_node_count = 5
    vm_size = 'STANDARD_A1_v2'
    job_id = 'PrimeNumbersJob'
