def set_pars():
    from environment import cassandra_settings as settings
    settings.recordcount = 1
    #settings.recordcount = 1000000
    settings.timeout = 60*40 # 40mins
    settings.readproportion = '0.99999'
    settings.updateproportion = '0.00001'
    settings.ycsb_nodes = 4
    settings.cassandra_commit = '9a6298e296915113fcbf9d60dd8a997ace0e077d'
    settings.operations = 200000
    settings.threads = 2000

def configs():
    from environment import cassandra_settings as settings
    from environment import init

    yield False # do prepare first?

    #for sleep_time, divisor in [(0,1000000),(1,100000),(10,1000)]:
    for ignore in xrange(0,8,2):
        for skew in ['2']:#,'1.5']:
                settings.skew = skew
                settings.ignore = ignore
                init()
                set_pars()

                #settings.replication_factor = 1
                #settings.large_replication_degree = 8
                #settings.max_items_for_large_replication_degree = 1
                #yield False

                #settings.replication_factor = 1
                #settings.large_replication_degree = 8
                #settings.max_items_for_large_replication_degree = 10
                #yield False

                #settings.replication_factor = 1
                #settings.large_replication_degree = 2
                #settings.max_items_for_large_replication_degree = 1
                #yield False

                #settings.replication_factor = 1
                #settings.large_replication_degree = 2
                #settings.max_items_for_large_replication_degree = 10
                #yield False

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
