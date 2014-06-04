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
from fabric.api import quiet
from fabric.api import abort

from os import path

import time

from datetime import datetime as dt

from environment import CODE_DIR
from environment import YCSB_CODE_DIR
from environment import SAVE_OPS_PAR
from environment import IGNORE_NON_LOCAL_PAR
from environment import MAX_ITEMS_FOR_LARGE_REPL_PAR
from environment import SLAVES_FILE
from environment import MAX_RETRIES
from environment import SLEEP_TIME_PAR
from environment import cassandra_settings
from environment import YCSB_RUN_OUT_FILE
from environment import YCSB_RUN_ERR_FILE
from environment import YCSB_LOAD_OUT_FILE
from environment import YCSB_LOAD_ERR_FILE

from clean import killall
from clean import clear_logs
from clean import clear_state
from clean import clean_nodes

from collect import collect_results

from handle_git import get_code
from handle_git import compile_code
from handle_git import config_git
from handle_git import compile_ycsb

import jmx

with open(SLAVES_FILE, 'r') as f:
    env.hosts = [line[:-1] for line in f]

env.roledefs['master'] = [env.hosts[0]]
if cassandra_settings.run_ycsb_on_single_node:
    env.hosts = env.hosts[1:]


def print_time():
    print "TIME: " + str(dt.now().strftime('%Y-%m-%d %H:%M.%S'))

old_execute = execute


def execute_with_time(*args, **kargs):
    print_time()
    old_execute(*args, **kargs)
execute = execute_with_time


@parallel
def config():
    '''setup cassandra configuration parameters'''
    print 'setting up config at {0}'.format(env.host_string)
    with hide('running'):
        with cd(path.join(CODE_DIR, 'conf')):
            options_file = 'cassandra.yaml'
            run('''sed -i 's/seeds:.*/seeds: "{0}"/' cassandra.yaml'''.format(
                ", ".join(env.hosts)))
            set_option('listen_address', env.host_string, options_file)
            set_option('rpc_address', "", options_file)
            set_option('max_items_for_large_replication_degree',
                       cassandra_settings.max_items_for_large_replication_degree, options_file)
            set_option('large_replication_degree',
                       cassandra_settings.large_replication_degree, options_file)
            set_option('ignore_non_local',
                       cassandra_settings.ignore_non_local, options_file)
            set_option('select_random_node',
                       cassandra_settings.select_random_node, options_file)
        with cd(path.join(YCSB_CODE_DIR, 'workloads')):
            set_option('operationcount',
                       cassandra_settings.operationcount, 'workloadb')


def set_option(option, value, file):
    option_string = 'sed -i "s/^{option}:.*/{option}: {value}/" {file}'.format(
        **locals())
    run(option_string)


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
        for i in range(MAX_RETRIES):
            cassandra_bin = path.join(CODE_DIR, "bin", "cassandra")
            sudo("screen -d -m {0} -f".format(cassandra_bin), pty=False)
            with quiet():
                time.sleep(10)
                res = sudo("pgrep -f 'java.*c[a]ssandra'")
                if not res.failed:
                    break
                print('Starting cassandra failed at {0}. Retry {1}.'.format(
                    env.host_string, i))
        else:
            abort('starting cassandra failed at node {0}'.format(
                env.host_string))


@task
@parallel
def start_check():
    sudo("pgrep -f 'java.*c[a]ssandra'")


def do_ycsb(operation):
    if cassandra_settings.run_ycsb_on_single_node:
        execute(do_centralized_ycsb, operation)
    else:
        execute(do_parallel_ycsb, operation)


@parallel
def do_parallel_ycsb(arg):
    hosts = env.hosts
    if arg == 'load':
        threads = 64
        out_file = YCSB_LOAD_OUT_FILE
        err_file = YCSB_LOAD_ERR_FILE
    else:
        threads = cassandra_settings.threads
        out_file = YCSB_RUN_OUT_FILE
        err_file = YCSB_RUN_ERR_FILE
    with cd(YCSB_CODE_DIR):
        sudo('./bin/ycsb {arg} cassandra-10 -threads {threads} -p hosts={hosts}'
             ' -P workloads/workloadb -s > {out_file} 2> {err_file}'.format(
                 **locals()))


@roles('master')
def do_centralized_ycsb(arg):
    do_parallel_ycsb(arg)


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
    jmx.set(MAX_ITEMS_FOR_LARGE_REPL_PAR, 0)
    jmx.set(SLEEP_TIME_PAR, 0)


@parallel
def prepare_run():
    set_bool_par(SAVE_OPS_PAR, cassandra_settings.save_ops)
    set_bool_par(IGNORE_NON_LOCAL_PAR,
                 cassandra_settings.ignore_non_local)
    jmx.set(MAX_ITEMS_FOR_LARGE_REPL_PAR,
            cassandra_settings.max_items_for_large_replication_degree)
    jmx.set(SLEEP_TIME_PAR, cassandra_settings.sleep_time)


def benchmark_round():
    execute(empty_and_config_nodes)

    execute(start)
    execute(start_check)
    time.sleep(30)
    execute(setup_ycsb)
    time.sleep(30)

    execute(prepare_load)
    do_ycsb('load')
    execute(killall)

    execute(start)
    execute(start_check)
    time.sleep(30)

    execute(prepare_run)
    do_ycsb('run')

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
    execute(start_check)
    time.sleep(30)
    execute(setup_ycsb)
    time.sleep(30)


@task
@roles('master')
def benchmark():
    prepare()

    cassandra_settings.save_ops = True
    cassandra_settings.threads = 1000
    cassandra_settings.sleep_time = 0
    cassandra_settings.operationcount = 10000000

    benchmark_round()

    cassandra_settings.max_items_for_large_replication_degree = 0
    benchmark_round()

    cassandra_settings.large_replication_degree = 12
    cassandra_settings.max_items_for_large_replication_degree = 20
    benchmark_round()
