CODE_DIR = "/home/jgpaiva/cassandra"
YCSB_CODE_DIR = "/home/jgpaiva/YCSB"
BASE_GIT_DIR = "/home/jgpaiva/nas/autoreplicator/"
LOG_FOLDER = "/var/log/cassandra/"
LOG_FILE = "/var/log/cassandra/system.log"
SERVER_URL = "cloudtm.ist.utl.pt"
CASSANDRA_VAR = "/var/lib/cassandra/"
SAVE_OPS_PAR = "SaveAllReadsAndWrites"
IGNORE_NON_LOCAL_PAR = "IgnoreNonLocal"
SELECT_RANDOM_NODE_PAR = "SelectRandomNode"
MAX_ITEMS_FOR_LARGE_REPL_PAR = "MaxItemsForLargeReplicationDegree"
SLEEP_TIME_PAR = "SleepTime"
JMX_BEAN = "org.apache.cassandra.db:type=StorageProxy"
JMX_TERM_JAR = "jmxterm-1.0-alpha-4-uber.jar"
JMX_PORT = 7199
MAX_RETRIES = 3
SLAVES_FILE = 'slaves'
YCSB_RUN_OUT_FILE = "/tmp/run.out"
YCSB_RUN_ERR_FILE = "/tmp/run.err"
YCSB_LOAD_OUT_FILE = "/tmp/load.out"
YCSB_LOAD_ERR_FILE = "/tmp/load.err"


class DecentRepr(type):

    def __repr__(self):
        settings = (var for var in vars(self) if not var.startswith('_'))
        return self.__name__ + "{" + ", ".join((i + ":" + str(getattr(self, i)) for i in settings)) + "}"


class cassandra_settings(object):
    __metaclass__ = DecentRepr

    run_original = False
    max_items_for_large_replication_degree = 20
    replication_factor = 2
    large_replication_degree = 4
    ignore_non_local = False
    threads = 30
    save_ops = False
    select_random_node = False
    operationcount = 400000
    sleep_time = 0
    run_ycsb_on_single_node = True
