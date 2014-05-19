from __future__ import with_statement
from fabric.api import run
from fabric.api import cd
from fabric.api import parallel
from fabric.api import sudo
from fabric.api import env
from fabric.api import roles
from fabric.api import execute
from fabric.api import task
from fabric.api import hide
from fabric.api import serial
from os import path
from environment import CODE_DIR,YCSB_CODE_DIR
from environment import cassandra_settings

from clean import killall, clear_logs, clear_state
from collect import collect_results
from handle_git import get_code, compile_code, config_git, compile_ycsb

import time
from datetime import datetime as dt

with open('slaves','r') as f:
    env.hosts = [line[:-1] for line in f]

env.roledefs['master'] = [env.hosts[0]]

@parallel
def config():
    '''setup cassandra configuration parameters'''
    print 'setting up config at {0}'.format(env.host_string)
    with hide('running'):
        with cd(path.join(CODE_DIR,'conf')):
            run('''sed -i 's/seeds:.*/seeds: "{0}"/' cassandra.yaml'''.format(env.roledefs['master'][0]))
            run('''sed -i "s/^listen_address:.*/listen_address: {0}/" cassandra.yaml'''.format(env.host_string))
            run('''sed -i "s/^rpc_address:.*/rpc_address: /" cassandra.yaml''')
            run('''sed -i "s/^max_items_for_large_replication_degree:.*/max_items_for_large_replication_degree: {0}/" cassandra.yaml'''.format(cassandra_settings.max_items_for_large_replication_degree))
            run('''sed -i "s/^large_replication_degree:.*/large_replication_degree: {0}/" cassandra.yaml'''.format(cassandra_settings.large_replication_degree))

@roles('master')
def setup_ycsb():
    '''setup cassandra tables for ycsb'''
    with cd(path.join(CODE_DIR,'bin')):
        run('''./cassandra-cli --host {0}'''.format(env.roledefs['master'][0]) +
r'''<<EOF
create keyspace usertable with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = [{replication_factor: ''' + str(cassandra_settings.replication_factor) + '''}];
use usertable;
create column family data with column_type = 'Standard' and comparator = 'UTF8Type';
exit;
EOF
''')

@serial
def start():
    '''start cassandra in all nodes in order'''
    time.sleep(10)
    with cd(CODE_DIR):
        cassandra_bin = path.join(CODE_DIR,"bin","cassandra")
        sudo("screen -d -m {0} -f".format(cassandra_bin),pty=False)

@parallel
def do_ycsb(arg):
    with cd(YCSB_CODE_DIR):
        run('./bin/ycsb {1} cassandra-10 -threads {2} -p hosts={0} -P workloads/workloadb > /tmp/{1}.out'.format(env.host_string,arg,cassandra_settings.threads))

@task
@roles('master')
def setup_cassandra():
    '''get cassandra ready to run from zero'''
    execute(config_git)

    execute(clear_state)
    execute(clear_logs)
    execute(killall)

    execute(get_code)
    execute(config)
    execute(compile_code)
    execute(compile_ycsb)

    execute(start)
    time.sleep(30)
    execute(setup_ycsb)
    time.sleep(30)

def print_time():
    print str(dt.now().strftime('%Y-%m-%d %H:%M.%S'))

def benchmark_round():
    print_time()
    execute(clear_state)
    execute(clear_logs)
    execute(killall)

    execute(config)

    print_time()
    execute(start)
    time.sleep(30)
    execute(setup_ycsb)
    time.sleep(30)

    print_time()
    execute(do_ycsb,'load')
    execute(killall)

    print_time()
    execute(start)
    time.sleep(30)
    execute(do_ycsb,'run')

    print_time()
    execute(collect_results)
    execute(killall)
    print_time()

def prepare():
    execute(get_code)
    execute(config)
    execute(compile_code)

@task
@roles('master')
def benchmark():
    prepare()

    cassandra_settings.run_original = True
    cassandra_settings.max_items_for_large_replication_degree = 0
    cassandra_settings.replication_factor = 2
    benchmark_round()
