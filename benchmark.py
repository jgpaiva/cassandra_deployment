import fabfile as f
from environment import cassandra_settings

def benchmark():
    f.prepare()

    cassandra_settings.threads = 1500
    cassandra_settings.operationcount = 60000000
    cassandra_settings.replication_factor = 4
    cassandra_settings.large_replication_degree = 4
    cassandra_settings.max_items_for_large_replication_degree = 10

    f.benchmark_round()

    cassandra_settings.max_items_for_large_replication_degree = 0
    f.benchmark_round()

    cassandra_settings.large_replication_degree = 8
    f.benchmark_round()

    cassandra_settings.large_replication_degree = 20
    f.benchmark_round()

    #cassandra_settings.large_replication_degree = 12
    #cassandra_settings.max_items_for_large_replication_degree = 20
    #f.benchmark_round()

    #cassandra_settings.max_items_for_large_replication_degree = 0
    #f.benchmark_round()
