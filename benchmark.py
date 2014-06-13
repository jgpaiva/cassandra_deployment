def set_pars():
    from environment import cassandra_settings
    cassandra_settings.run_original = False
    cassandra_settings.threads = 200
    #cassandra_settings.operationcount = 25000000
    #cassandra_settings.recordcount = 1000000
    cassandra_settings.operationcount = 5000000
    cassandra_settings.recordcount = 1000000
    cassandra_settings.timeout = 60*40 # 40mins
    cassandra_settings.data_placement_rounds_duration = 2000 #2 sec
    cassandra_settings.save_ops = True
    cassandra_settings.debug_logs = False


def configs():
    from environment import cassandra_settings as settings
    for i in range(3):

        settings.replication_factor = 1
        settings.large_replication_degree = 2
        settings.max_items_for_large_replication_degree = 40
        yield i==0 #do prepare first

        settings.replication_factor = 1
        settings.large_replication_degree = 1
        settings.max_items_for_large_replication_degree = 0
        yield False #avoid prepare first

        settings.replication_factor = 2
        settings.large_replication_degree = 2
        settings.max_items_for_large_replication_degree = 0
        yield False #avoid prepare first

        settings.replication_factor = 2
        settings.large_replication_degree = 4
        settings.max_items_for_large_replication_degree = 40
        yield False #avoid prepare first

        settings.replication_factor = 2
        settings.large_replication_degree = 4
        settings.max_items_for_large_replication_degree = 5
        yield False #avoid prepare first

        settings.replication_factor = 3
        settings.large_replication_degree = 3
        settings.max_items_for_large_replication_degree = 0
        yield False #avoid prepare first

        settings.replication_factor = 3
        settings.large_replication_degree = 6
        settings.max_items_for_large_replication_degree = 40
        yield False #avoid prepare first

        settings.replication_factor = 4
        settings.large_replication_degree = 4
        settings.max_items_for_large_replication_degree = 0
        yield False #avoid prepare first

        settings.replication_factor = 1
        settings.large_replication_degree = 4
        settings.max_items_for_large_replication_degree = 40
        yield False #avoid prepare first

        settings.replication_factor = 2
        settings.large_replication_degree = 8
        settings.max_items_for_large_replication_degree = 40
        yield False #avoid prepare first

        settings.replication_factor = 2
        settings.large_replication_degree = 16
        settings.max_items_for_large_replication_degree = 40
        yield False #avoid prepare first

        settings.replication_factor = 2
        settings.large_replication_degree = 8
        settings.max_items_for_large_replication_degree = 5
        yield False #avoid prepare first

        settings.replication_factor = 2
        settings.large_replication_degree = 16
        settings.max_items_for_large_replication_degree = 5
        yield False #avoid prepare first

        settings.replication_factor = 8
        settings.large_replication_degree = 8
        settings.max_items_for_large_replication_degree = 0
        yield False #avoid prepare first

#    settings.run_original = True
#    f.prepare()
#    for i in range(3):
#        settings.replication_factor = 1
#        f.benchmark_round()
#
#        settings.replication_factor = 2
#        f.benchmark_round()
#
#        settings.replication_factor = 3
#        f.benchmark_round()
#
#        settings.replication_factor = 4
#        f.benchmark_round()
#
#        settings.replication_factor = 8
#        f.benchmark_round()
