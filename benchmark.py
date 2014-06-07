def set_pars():
    from environment import cassandra_settings
    cassandra_settings.threads = 500
    cassandra_settings.operationcount = 250000
    cassandra_settings.recordcount = 100000
    cassandra_settings.timeout = 60*25


def benchmark():
    from environment import cassandra_settings as settings
    import fabfile as f
    for i in range(3):
        settings.replication_factor = 2
        settings.large_replication_degree = 4
        settings.max_items_for_large_replication_degree = 0
        f.benchmark_round()

        settings.replication_factor = 2
        settings.large_replication_degree = 4
        settings.max_items_for_large_replication_degree = 5
        f.benchmark_round()

        settings.replication_factor = 2
        settings.large_replication_degree = 4
        settings.max_items_for_large_replication_degree = 10
        f.benchmark_round()

        settings.replication_factor = 2
        settings.large_replication_degree = 4
        settings.max_items_for_large_replication_degree = 40
        f.benchmark_round()

        settings.replication_factor = 2
        settings.large_replication_degree = 4
        settings.max_items_for_large_replication_degree = 200
        f.benchmark_round()

        settings.replication_factor = 2
        settings.large_replication_degree = 2
        settings.max_items_for_large_replication_degree = 20
        f.benchmark_round()

        settings.replication_factor = 2
        settings.large_replication_degree = 3
        settings.max_items_for_large_replication_degree = 20
        f.benchmark_round()

        settings.replication_factor = 2
        settings.large_replication_degree = 5
        settings.max_items_for_large_replication_degree = 20
        f.benchmark_round()

        settings.replication_factor = 2
        settings.large_replication_degree = 7
        settings.max_items_for_large_replication_degree = 20
        f.benchmark_round()

        settings.replication_factor = 3
        settings.large_replication_degree = 3
        settings.max_items_for_large_replication_degree = 0
        f.benchmark_round()

        settings.replication_factor = 4
        settings.large_replication_degree = 4
        settings.max_items_for_large_replication_degree = 0
        f.benchmark_round()
