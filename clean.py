from __future__ import with_statement
from fabric.api import run
from fabric.api import parallel
from fabric.api import sudo
from fabric.api import quiet
from environment import LOG_FILE,CASSANDRA_VAR,CODE_DIR,YCSB_CODE_DIR

@parallel
def killall():
    '''kill all java processes'''
    with quiet():
        sudo("killall -q java")

@parallel
def clear_logs():
    '''delete cassandra logs'''
    with quiet():
        sudo("rm -f {0}".format(LOG_FILE))

@parallel
def clear_state():
    '''delete all cassandra persistent state'''
    with quiet():
        sudo("rm -rf {0}".format(CASSANDRA_VAR))

@parallel
def delete_git():
    for repo in [CODE_DIR,YCSB_CODE_DIR]:
        run("rm -rf {0}".format(repo))