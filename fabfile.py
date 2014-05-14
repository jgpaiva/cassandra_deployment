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

from time import sleep

env.hosts=["172.31.0.134","172.31.0.143","172.31.0.144"]

env.roledefs['master'] = [env.hosts[0]]

CODE_DIR = "~/cassandra"
PID_FILE = "/tmp/cassandra.pid"

@parallel
def killall():
    with settings(warn_only=True):
        sudo("killall java")

@parallel
def get_code():
    with cd(CODE_DIR):
        run("git pull")

@parallel
def compile_code():
    with cd(CODE_DIR):
        run("ant clean")
        run("ant build")

@parallel
def clear_logs():
    with settings(warn_only=True):
        sudo("rm /var/log/cassandra/system.log")

def start_cassandra():
    sleep(5)
    with cd(CODE_DIR):
        sudo("screen -d -m /home/jgpaiva/cassandra/bin/cassandra -f")

def clean_cassandra():
    with cd(CODE_DIR):
        sudo("rm -rf /var/lib/cassandra/")

def setup_cassandra():
    execute(config_cassandra)
    execute(prepare)
    execute(start_cassandra)
    execute(setup_ycsb)

def config_cassandra():
    with cd(path.join(CODE_DIR,'conf')):
        run('''sed -i 's/seeds:.*/seeds: "{0}"/' cassandra.yaml'''.format(env.roledefs['master'][0]))
        run('''sed -i "s/^listen_address:.*/listen_address: {0}/" cassandra.yaml'''.format(env.host_string))
        run('''sed -i "s/^rpc_address:.*/rpc_address: /" cassandra.yaml''')

@roles('master')
def setup_ycsb():
    with cd(path.join(CODE_DIR,'bin')):
        run('''./cassandra-cli --host {0}'''.format(env.roledefs['master'][0]) +
r'''<<EOF
create keyspace usertable with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = [{replication_factor:2}];
use usertable;
create column family data with column_type = 'Standard' and comparator = 'UTF8Type';
exit;
EOF
''')

@parallel
def prepare():
    execute(killall)
    execute(clear_logs)
    execute(get_code)
    execute(compile_code)
