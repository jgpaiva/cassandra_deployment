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
from fabric.api import put
from fabric.api import warn_only

from os import path
import time
from datetime import datetime as dt

from environment import CODE_DIR
from environment import YCSB_CODE_DIR
from environment import SAVE_OPS_PAR
from environment import IGNORE_NON_LOCAL_PAR
from environment import MAX_ITEMS_FOR_LARGE_REPL_PAR
from environment import SLAVES_FILE
from environment import cassandra_settings

from clean import killall, clear_logs, clear_state, clean_nodes
from collect import collect_results
from handle_git import get_code, compile_code, config_git, compile_ycsb
import jmx

with open(SLAVES_FILE, 'r') as f:
    env.hosts = [line[:-1] for line in f]

env.roledefs['master'] = [env.hosts[0]]

def print_time():
    print "TIME: " + str(dt.now().strftime('%Y-%m-%d %H:%M.%S'))

old_execute = execute
def execute_with_time(*args,**kargs):
    print_time()
    old_execute(*args,**kargs)
execute = execute_with_time


@parallel
def config():
    '''setup cassandra configuration parameters'''
    print 'setting up config at {0}'.format(env.host_string)
    with hide('running'):
        with cd(path.join(CODE_DIR, 'conf')):
            run('''sed -i 's/seeds:.*/seeds: "{0}"/' cassandra.yaml'''.format(
                ", ".join(env.hosts)))
            run('''sed -i "s/^listen_address:.*/listen_address: {0}/" cassandra.yaml'''.format(env.host_string))
            run('''sed -i "s/^rpc_address:.*/rpc_address: /" cassandra.yaml''')
            run('''sed -i "s/^max_items_for_large_replication_degree:.*/max_items_for_large_replication_degree: {0}/" cassandra.yaml'''.format(
                cassandra_settings.max_items_for_large_replication_degree))
            run('''sed -i "s/^large_replication_degree:.*/large_replication_degree: {0}/" cassandra.yaml'''.format(
                cassandra_settings.large_replication_degree))
            run('''sed -i "s/^ignore_non_local:.*/ignore_non_local: {0}/" cassandra.yaml'''.format(
                cassandra_settings.ignore_non_local))
            run('''sed -i "s/^select_random_node:.*/select_random_node: {0}/" cassandra.yaml'''.format(
                cassandra_settings.select_random_node))
        with cd(path.join(YCSB_CODE_DIR, 'workloads')):
            run('''sed -i "s/^operationcount=.*/operationcount= {0}/" workloadb'''.format(
                cassandra_settings.operationcount))


@roles('master')
def setup_ycsb():
    '''setup cassandra tables for ycsb'''
    with cd(path.join(CODE_DIR, 'bin')):
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
    with cd(CODE_DIR):
        cassandra_bin = path.join(CODE_DIR, "bin", "cassandra")
        sudo("screen -d -m {0} -f".format(cassandra_bin), pty=False)
    time.sleep(10)
    sudo("pgrep -f 'java.*cassandra'")


@parallel
def do_ycsb(arg):
    if arg == 'load':
        threads = 64
    else:
        threads = cassandra_settings.threads
    with cd(YCSB_CODE_DIR):
        run('./bin/ycsb {1} cassandra-10 -threads {2} -p hosts={0} -P workloads/workloadb -s > /tmp/{1}.out 2> /tmp/{1}.err'.format(env.host_string,
            arg, threads))


@task
@roles('master')
def setup_environment():
    '''get cassandra ready to run from zero'''
    execute(clean_nodes)
    execute(config_git)

    execute(clear_state)
    execute(clear_logs)
    execute(killall)

    execute(get_code)
    execute(config)
    execute(compile_all)


@task
@roles('master')
def compile_all():
    execute(compile_code)
    execute(compile_ycsb)
    execute(upload_libs)


@parallel
def empty_and_config_nodes():
    killall()
    clear_state()
    clear_logs()
    config()

@parallel
def prepare_load():
    set_bool_par(SAVE_OPS_PAR, False)
    set_bool_par(IGNORE_NON_LOCAL_PAR, False)
    jmx.set(MAX_ITEMS_FOR_LARGE_REPL_PAR, 0)

@parallel
def prepare_run():
    set_bool_par(SAVE_OPS_PAR, cassandra_settings.save_ops)
    set_bool_par(IGNORE_NON_LOCAL_PAR,
            cassandra_settings.ignore_non_local)
    jmx.set(MAX_ITEMS_FOR_LARGE_REPL_PAR,
            cassandra_settings.max_items_for_large_replication_degree)

def benchmark_round():
    execute(empty_and_config_nodes)

    execute(start)
    time.sleep(30)
    execute(setup_ycsb)
    time.sleep(30)

    execute(prepare_load)
    execute(do_ycsb, 'load')
    execute(killall)

    execute(start)
    time.sleep(30)

    execute(prepare_run)
    execute(do_ycsb, 'run')

    time.sleep(10)
    execute(collect_results)
    execute(killall)


@parallel
def upload_libs():
    put(local_path='lib/*', remote_path='~/')


def prepare():
    execute(upload_libs)
    execute(get_code)
    execute(config)
    execute(compile_code)
    execute(compile_ycsb)


@parallel
def set_bool_par(par, val):
    jmx.set(par, 'true' if val else 'false')


@task
def fork(cmd):
    with warn_only():
        sudo(cmd)


@task
@roles('master')
def boot_cassandra():
    prepare()
    execute(clear_state)
    execute(clear_logs)
    execute(killall)

    execute(config)

    execute(start)
    time.sleep(30)
    execute(setup_ycsb)
    time.sleep(30)


@task
@roles('master')
def benchmark():
    prepare()

    cassandra_settings.ignore_non_local = True
    cassandra_settings.threads = 200

    benchmark_round()

    cassandra_settings.max_items_for_large_replication_degree = 0
    benchmark_round()

    cassandra_settings.large_replication_degree = 12
    cassandra_settings.max_items_for_large_replication_degree = 20
    benchmark_round()
