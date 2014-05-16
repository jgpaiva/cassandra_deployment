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
from os import path
from environment import CODE_DIR,YCSB_CODE_DIR,LOG_FILE

from datetime import datetime as dt
from environment import cassandra_settings


@task
@roles('master')
def collect_results():
    res_dir = "results." + str(dt.now().strftime('%Y%m%d.%H%M%S'))
    execute(collect_results_from_nodes,res_dir)
    write_settings(res_dir)

def write_settings(res_dir):
    out_file = path.join(res_dir,'settings')
    with open(out_file,'w') as f:
        f.write(str(cassandra_settings) + "\n")

@parallel
def collect_results_from_nodes(res_dir):
    node_dir = path.join(res_dir,env.host_string)
    local("mkdir -p {node_dir}".format(**locals()))
    get("/tmp/run.out",node_dir)
    get("/tmp/load.out",node_dir)
    get(path.join(CODE_DIR,'conf'),node_dir)
    get(LOG_FILE,node_dir)
    git_status = {}
    for folder in [CODE_DIR,YCSB_CODE_DIR]:
        with hide('stdout'):
            with cd(folder):
                out = run("git log -n 1",pty=False)
                git_status[folder] = out
    with open(path.join(node_dir,"git_status"),'w') as f:
        f.write(str(git_status))


