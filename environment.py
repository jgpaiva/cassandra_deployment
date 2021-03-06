import re
from fabric.api import env

CODE_DIR = "/home/jgpaiva/cassandra"
YCSB_CODE_DIR = "/home/jgpaiva/YCSB"
BASE_GIT_DIR = "/home/jgpaiva/nas/autoreplicator/"
LOG_FOLDER = CODE_DIR + "/logs"
LOG_FILE = LOG_FOLDER + "/system.log"
SERVER_URL = "cloudtm.ist.utl.pt"
CASSANDRA_VAR = "/var/lib/cassandra/"
CASSANDRA_DATA = CODE_DIR + "/data"
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
YCSB_WRITE_PROPERTY = "cassandra.writeconsistencylevel"
DSTAT_SERVER = "/tmp/dstat_server.csv"
DSTAT_YCSB = "/tmp/dstat_ycsb.csv"
YCSB_SKEW_PROPERTY = "zipfian_constant"

_pattern = r"""
     [-+]? # optional sign
     (?:
         (?: \d* \. \d+ ) # .1 .12 .123 etc 9.1 etc 98.1 etc
         |
         (?: \d+ \.? ) # 1. 12. 123. etc 1 12 123 etc
     )
     # followed by optional exponent part if desired
     (?: [Ee] [+-]? \d+ ) ?
     """
numeric_const_pattern = re.compile(_pattern, re.VERBOSE)


def init():
    with open(SLAVES_FILE, 'r') as f:
        env.hosts = [line[:-1] for line in f]

    env.roledefs['all'] = env.hosts
    env.roledefs['master'] = [env.hosts[-1]]
    if cassandra_settings.ycsb_nodes is None:
        cassandra_settings.ycsb_nodes = len(env.hosts) / 2
    env.roledefs['ycsbnodes'] = env.hosts[:cassandra_settings.ycsb_nodes]
    env.hosts = env.hosts[cassandra_settings.ycsb_nodes +
                          cassandra_settings.ignore:]
    cassandra_settings.processing_nodes = len(env.hosts)


class mydict(dict):
    __slots__ = ('_frozen',)

    def __init__(self,*args,**kargs):
        super(mydict,self).__init__(*args,**kargs)
        self._frozen = False

    def __setitem__(self, name, value):
        if self._frozen and not dict.__contains__(self, name):
            raise KeyError("Cannot set key %s" % name)
        else:
            dict.__setitem__(self, name, value)

    def freeze(self):
        self._frozen = True

    def unfreeze(self):
        self._frozen = False

    def _tuple(self):
        return tuple(sorted(self.iteritems()))

    def __hash__(self):
        return hash(self._tuple())

    def __iter__(self):
        yield self

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        try:
            object.__setattr__(self, name, value)
        except AttributeError as e:
            if name in self:
                self[name] = value
            else:
                raise e

cassandra_settings = mydict()
_c = cassandra_settings

_c['max_items_for_large_replication_degree'] = 20
_c['replication_factor'] = 2
_c['large_replication_degree'] = 4
_c['ignore_non_local'] = False
_c['threads'] = 30
_c['select_random_node'] = False
_c['operationcount'] = 400000
_c['recordcount'] = 1000000
_c['sleep_time'] = 0
_c['save_ops'] = False
_c['debug_logs'] = False
_c['data_placement_rounds_duration'] = 2000
_c['timeout'] = 60*60
_c['processing_nodes'] = -1
_c['ycsb_nodes'] = None
_c['write_consistency'] = 'ALL'
_c['readproportion'] = '0.95'
_c['updateproportion'] = '0.05'
_c['skew'] = '0.99'
_c['cassandra_commit'] = None
_c['ignore'] = 0

_c.freeze()

env.colorize_errors = True
