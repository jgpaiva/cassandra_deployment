from fabric.api import warn_only
from fabric.api import run
from fabric.api import abort
from fabric.api import env
from fabric.api import sudo

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
    save_repl_set = True
    select_random_node = False
    operationcount = 400000
    sleep_time = 0

def run_with_retry(command):
    with warn_only():
        for i in range(MAX_RETRIES):
            res = run(command)
            if not res.failed:
                break
            print "WARNING: command '{0}' failed {1} times at node".format(command,i+1,env.host_string)
        else:
            abort("FATAL: command '{0}' failed {1} times".format(command,MAX_RETRIES))

def sudo_with_retry(command):
    with warn_only():
        for i in range(MAX_RETRIES):
            res = sudo(command)
            if not res.failed:
                break
            print "WARNING: command '{0}' failed {1} times at node".format(command,i+1,env.host_string)
        else:
            abort("FATAL: command '{0}' failed {1} times".format(command,MAX_RETRIES))
