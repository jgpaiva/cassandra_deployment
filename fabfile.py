from __future__ import with_statement
from fabric.api import run
from fabric.api import cd
from fabric.api import parallel
from fabric.api import sudo
from fabric.api import env
from fabric.api import settings
from fabric.api import roles
from fabric.api import execute
from os import path

import time

with open('slaves','r') as f:
    env.hosts = [line[:-1] for line in f]

env.roledefs['master'] = [env.hosts[0]]

CODE_DIR = "~/cassandra"
PID_FILE = "/tmp/cassandra.pid"
BASE_DIR = "/home/jgpaiva/nas/autoreplicator/"

@parallel
def killall():
    '''kill all java processes'''
    with settings(warn_only=True):
        sudo("killall -q java || true")

@parallel
def get_code():
    '''update git'''
    with cd(CODE_DIR):
        run("git pull origin")

@parallel
def compile_code():
    '''clean and compile cassandra code'''
    with cd(CODE_DIR):
        run("ant -q clean > /dev/null")
        run("ant -q build > /dev/null")

@parallel
def clear_logs():
    '''delete cassandra logs'''
    sudo("rm -f /var/log/cassandra/system.log")

def start():
    '''start cassandra in all nodes in order'''
    time.sleep(10)
    with cd(CODE_DIR):
        sudo("screen -d -m /home/jgpaiva/cassandra/bin/cassandra -f",pty=False)

@parallel
def clean():
    '''delete all cassandra persistent state'''
    with cd(CODE_DIR):
        sudo("rm -rf /var/lib/cassandra/")

@roles('master')
def setup_cassandra():
    '''get cassandra ready to run from zero'''
    execute(config_git)
    execute(config)
    execute(prepare)
    execute(clean)
    execute(start)
    time.sleep(60)
    execute(setup_ycsb)

@parallel
def config_git():
    '''setup git on hosts'''
    for repo,branch in [('cassandra','cassandra-2.1'),('YCSB','master')]:
        run("git init {0}".format(repo))
        with cd(repo):
            run("git remote add origin ssh://cloudtm.ist.utl.pt{0} ".format(path.join(BASE_DIR,repo)))
            run("git fetch --all")
            run("git checkout {0}".format(branch))

@parallel
def delete_git():
    run("rm -rf cassandra")
    run("rm -rf YCSB")

@parallel
def config():
    '''setup cassandra configuration parameters'''
    with cd(path.join(CODE_DIR,'conf')):
        run('''sed -i 's/seeds:.*/seeds: "{0}"/' cassandra.yaml'''.format(env.roledefs['master'][0]))
        run('''sed -i "s/^listen_address:.*/listen_address: {0}/" cassandra.yaml'''.format(env.host_string))
        run('''sed -i "s/^rpc_address:.*/rpc_address: /" cassandra.yaml''')

@roles('master')
def setup_ycsb():
    '''setup cassandra tables for ycsb'''
    with cd(path.join(CODE_DIR,'bin')):
        run('''./cassandra-cli --host {0}'''.format(env.roledefs['master'][0]) +
r'''<<EOF
create keyspace usertable with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = [{replication_factor:2}];
use usertable;
create column family data with column_type = 'Standard' and comparator = 'UTF8Type';
exit;
EOF
''')

@roles('master')
def prepare():
    '''boot cassandra from its persistent state'''
    execute(killall)
    execute(clear_logs)
    execute(get_code)
    execute(compile_code)
