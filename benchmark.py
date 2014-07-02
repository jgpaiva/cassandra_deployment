def set_pars():
    from environment import cassandra_settings
    cassandra_settings.run_original = False
    #cassandra_settings.recordcount = 1
    cassandra_settings.recordcount = 1000000
    cassandra_settings.timeout = 60*40 # 40mins
    cassandra_settings.save_ops = True
    cassandra_settings.debug_logs = False
    cassandra_settings.write_consistency = 'ALL'
    cassandra_settings.readproportion = '0.99999'
    cassandra_settings.updateproportion = '0.00001'
    cassandra_settings.sleep_time = 0
    cassandra_settings.ycsb_nodes = 4

def configs():
    from environment import cassandra_settings as settings

    yield True # do prepare first?

    #for sleep_time, divisor in [(0,1000000),(1,100000),(10,1000)]:
    for skew in ['1.5','2']:
        for sleep_time, operations in [(10,100000)]:
            for threads in [2000, 8000, 16000]:
                settings.operationcount = operations
                settings.threads = threads
                settings.sleep_time = sleep_time
                settings.skew = skew

                settings.replication_factor = 2
                settings.large_replication_degree = 2
                settings.max_items_for_large_replication_degree = 0
                yield False

                settings.replication_factor = 8
                settings.large_replication_degree = 8
                settings.max_items_for_large_replication_degree = 0
                yield False

                settings.replication_factor = 1
                settings.large_replication_degree = 8
                settings.max_items_for_large_replication_degree = 100
                yield False

                settings.replication_factor = 1
                settings.large_replication_degree = 8
                settings.max_items_for_large_replication_degree = 1000
                yield False

                settings.replication_factor = 1
                settings.large_replication_degree = 8
                settings.max_items_for_large_replication_degree = 3
                yield False
