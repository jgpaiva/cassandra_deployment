from __future__ import with_statement
from fabric.api import run
from fabric.api import cd
from fabric.api import parallel
from fabric.api import sudo
from fabric.api import env
from fabric.api import settings
from fabric.api import roles
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

@roles('master')
def setup_cassandra():
    with cd(path.join(CODE_DIR,'bin')):
        run('''./cassandra-cli --host {0}'''.format(env.hosts[0]) + r'''<<EOF
create keyspace usertable with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = [{replication_factor:2}];
use usertable;
create column family data with column_type = 'Standard' and comparator = 'UTF8Type';
exit;
EOF
''')

def clean_cassandra():
    with cd(CODE_DIR):
        sudo("rm -rf /var/lib/cassandra/")

@parallel
def prepare():
    killall()
    clear_logs()
    get_code()
    compile_code()
