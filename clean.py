from __future__ import with_statement
from fabric.api import run
from fabric.api import parallel
from fabric.api import sudo
from fabric.api import quiet
from fabric.api import cd
from environment import LOG_FOLDER
from environment import CASSANDRA_VAR
from environment import CODE_DIR
from environment import YCSB_CODE_DIR
from environment import YCSB_RUN_OUT_FILE
from environment import YCSB_RUN_ERR_FILE
from environment import YCSB_LOAD_OUT_FILE
from environment import YCSB_LOAD_ERR_FILE


@parallel
def killall():
    '''kill all java processes'''
    with quiet():
        sudo("killall -q java")
    with quiet():
        run("killall -q java")


@parallel
def clear_logs():
    '''delete cassandra logs'''
    with quiet():
        sudo("rm -f {0}/*".format(LOG_FOLDER))
    with quiet():
        for i in [YCSB_RUN_OUT_FILE, YCSB_RUN_ERR_FILE,
                  YCSB_LOAD_OUT_FILE, YCSB_LOAD_ERR_FILE]:
            sudo("rm {}", i)


@parallel
def clear_state():
    '''delete all cassandra persistent state'''
    with quiet():
        sudo("rm -rf {0}".format(CASSANDRA_VAR))
        with cd(CODE_DIR):
            sudo("rm -rf *.hprof")
        with cd(YCSB_CODE_DIR):
            sudo("rm -rf *.hprof")


@parallel
def delete_git():
    for repo in [CODE_DIR, YCSB_CODE_DIR]:
        run("rm -rf {0}".format(repo))


@parallel
def clean_nodes():
    run("rm -rf *")
