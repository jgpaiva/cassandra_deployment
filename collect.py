from __future__ import with_statement
from fabric.api import run
from fabric.api import cd
from fabric.api import parallel
from fabric.api import env
from fabric.api import roles
from fabric.api import execute
from fabric.api import task
from fabric.api import get
from fabric.api import local
from fabric.api import hide
from fabric.api import warn_only

from environment import YCSB_RUN_OUT_FILE
from environment import YCSB_RUN_ERR_FILE
from environment import YCSB_LOAD_OUT_FILE
from environment import YCSB_LOAD_ERR_FILE
from environment import DSTAT_SERVER
from environment import DSTAT_YCSB

from os import path

from environment import CODE_DIR, YCSB_CODE_DIR, LOG_FILE
from environment import cassandra_settings

import jmx

from datetime import datetime as dt

from pprint import pprint


@task
@roles('master')
def collect():
    res_dir = "results." + str(dt.now().strftime('%Y%m%d.%H%M%S'))
    execute(collect_results_from_nodes, res_dir)
    out_file = path.join(res_dir, 'settings.out')
    with open(out_file, 'w') as f:
        pprint(cassandra_settings,f)

@parallel
def collect_results_from_nodes(res_dir):
    node_dir = path.join(res_dir, env.host_string)
    local("mkdir -p {node_dir}".format(**locals()))

    with warn_only():
        with hide('everything'):
            for i in [YCSB_RUN_OUT_FILE, YCSB_RUN_ERR_FILE,
                    YCSB_LOAD_OUT_FILE, YCSB_LOAD_ERR_FILE]:
                print("getting {0} for node {1}".format(i,env.host_string))
                get(i, node_dir)
            print("getting {0} for node {1}".format('conf',env.host_string))
            get(path.join(CODE_DIR, 'conf'), node_dir)
            print("getting {0} for node {1}".format('log file',env.host_string))
            get(LOG_FILE, node_dir)

            git_status = {}
            for folder in [CODE_DIR, YCSB_CODE_DIR]:
                with cd(folder):
                    out = run("git log -n 1", pty=False)
                    git_status[folder] = out
            with open(path.join(node_dir, "git_status.out"), 'w') as f:
                print("getting {0} for node {1}".format('git',env.host_string))
                f.write(str(git_status))
                f.write('\n')

            for parameter in ['LargeReplSet', 'MyLargeReplSet', 'AllReads', 'AllWrites']:
                with open(path.join(node_dir, parameter + ".log"), 'a') as f:
                    print("getting {0} for node {1}".format(parameter,env.host_string))
                    f.writelines(jmx.get_value(parameter) or [])
            print("getting {0} for node {1}".format('dstat',env.host_string))
            get(DSTAT_SERVER,node_dir)
            get(DSTAT_YCSB,node_dir)
