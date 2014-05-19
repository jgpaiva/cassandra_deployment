CODE_DIR = "/home/jgpaiva/cassandra"
YCSB_CODE_DIR = "/home/jgpaiva/YCSB"
BASE_GIT_DIR = "/home/jgpaiva/nas/autoreplicator/"
LOG_FILE = "/var/log/cassandra/system.log"
SERVER_URL = "cloudtm.ist.utl.pt"
CASSANDRA_VAR = "/var/lib/cassandra/"

class DecentRepr(type):
      def __repr__(self):
          settings =  (var for var in vars(self) if not var.startswith('_'))
          return self.__name__ + "{" + ", ".join((i + ":" + str(getattr(self,i)) for i in settings)) + "}"


class cassandra_settings(object):
    __metaclass__ = DecentRepr

    run_original = False
    max_items_for_large_replication_degree = 20
    replication_factor = 2
    large_replication_degree = 4
    threads = 30
