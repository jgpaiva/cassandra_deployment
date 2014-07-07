def set_pars():
    from environment import cassandra_settings as settings
    settings.run_original = False
    #settings.recordcount = 1
    settings.recordcount = 1000000
    settings.timeout = 60*40 # 40mins
    settings.save_ops = False
    settings.debug_logs = True
    settings.write_consistency = 'ALL'
    settings.readproportion = '0.99999'
    settings.updateproportion = '0.00001'
    settings.sleep_time = 0
    settings.ycsb_nodes = 4

def configs():
    from environment import cassandra_settings as settings
    from environment import init

    yield True # do prepare first?

    #for sleep_time, divisor in [(0,1000000),(1,100000),(10,1000)]:
    for ignore in reversed(range(3)):
        for skew in ['2']:#,'1.5']:
#        for commit in ["fb7eaf787135","1c7c4c35017dfe","0bac68c8aae8d3a4f43",
#                       "04b615fe9477ff5854","edaf91392344883f","4c9eef0b4ed68d9ef",
#                       "c5f720432f208eab","7f42a32dea8d7b16","0eb0b54ea5fff5b0e"]:
            for sleep_time, operations in [(0,200000)]:
                for threads in [2000, 8000]:
                    settings.operationcount = operations
                    settings.threads = threads
                    settings.sleep_time = sleep_time
                    settings.skew = skew
                    settings.ignore = ignore
                    init()
                    set_pars()

                    settings.replication_factor = 1
                    settings.large_replication_degree = 8
                    settings.max_items_for_large_replication_degree = 1
                    yield False

                    settings.replication_factor = 1
                    settings.large_replication_degree = 8
                    settings.max_items_for_large_replication_degree = 10
                    yield False

                    settings.replication_factor = 1
                    settings.large_replication_degree = 2
                    settings.max_items_for_large_replication_degree = 1
                    yield False

                    settings.replication_factor = 1
                    settings.large_replication_degree = 2
                    settings.max_items_for_large_replication_degree = 10
                    yield False

                    settings.replication_factor = 1
                    settings.large_replication_degree = 1
                    settings.max_items_for_large_replication_degree = 0
                    yield False

                    settings.replication_factor = 2
                    settings.large_replication_degree = 2
                    settings.max_items_for_large_replication_degree = 0
                    yield False

                    settings.replication_factor = 8
                    settings.large_replication_degree = 8
                    settings.max_items_for_large_replication_degree = 0
                    yield False
