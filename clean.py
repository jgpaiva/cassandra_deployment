from __future__ import with_statement
from fabric.api import run
from fabric.api import parallel
from fabric.api import sudo
from fabric.api import quiet
from fabric.api import cd
from environment import LOG_FOLDER
from environment import CASSANDRA_VAR
from environment import CASSANDRA_DATA
from environment import CODE_DIR
from environment import YCSB_CODE_DIR
from environment import YCSB_RUN_OUT_FILE
from environment import YCSB_RUN_ERR_FILE
from environment import YCSB_LOAD_OUT_FILE
from environment import YCSB_LOAD_ERR_FILE
from environment import DSTAT_SERVER
from environment import DSTAT_YCSB


@parallel
def kill():
    '''kill all java processes'''
    with quiet():
        sudo("killall -9 -q java")
        run("killall -9 -q java")
        sudo("pkill -f dstat")

@parallel
def cleankill():
    '''kill all java processes'''
    with quiet():
        sudo("killall -q java")
        run("killall -q java")


@parallel
def logs():
    '''delete cassandra logs'''
    with quiet():
        sudo("rm -f {0}/*".format(LOG_FOLDER))
        for i in [YCSB_RUN_OUT_FILE, YCSB_RUN_ERR_FILE,
                  YCSB_LOAD_OUT_FILE, YCSB_LOAD_ERR_FILE]:
            sudo("rm {0}".format(i))
        sudo("rm -f {0} {1}".format(DSTAT_SERVER,DSTAT_YCSB))


@parallel
def state():
    '''delete all cassandra persistent state'''
    with quiet():
        sudo("rm -rf {0}".format(CASSANDRA_VAR))
        sudo("rm -rf {0}".format(CASSANDRA_DATA))
        with cd(CODE_DIR):
            sudo("rm -rf *.hprof")
        with cd(YCSB_CODE_DIR):
            sudo("rm -rf *.hprof")


@parallel
def git():
    for repo in [CODE_DIR, YCSB_CODE_DIR]:
        sudo("rm -rf {0}".format(repo))


@parallel
def nodes():
    with cd("~"):
        sudo("rm -rf *")
    with cd("/tmp/"):
        sudo("rm -rf *")
