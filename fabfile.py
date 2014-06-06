from __future__ import with_statement
from fabric.api import run
from fabric.api import cd
from fabric.api import parallel
from fabric.api import sudo
from fabric.api import env
from fabric.api import roles
from fabric.api import execute as plain_execute
from fabric.api import task
from fabric.api import hide
from fabric.api import serial
from fabric.api import put
from fabric.api import warn_only
from fabric.api import quiet
from fabric.api import abort

from os import path

from contextlib import contextmanager

import time

from datetime import datetime as dt

import environment
from environment import CODE_DIR
from environment import YCSB_CODE_DIR
from environment import SAVE_OPS_PAR
from environment import IGNORE_NON_LOCAL_PAR
from environment import MAX_ITEMS_FOR_LARGE_REPL_PAR
from environment import MAX_RETRIES
from environment import SLEEP_TIME_PAR
from environment import cassandra_settings
from environment import YCSB_RUN_OUT_FILE
from environment import YCSB_RUN_ERR_FILE
from environment import YCSB_LOAD_OUT_FILE
from environment import YCSB_LOAD_ERR_FILE

import clean

import collect

import git

import jmx

import benchmark as bench

bench.set_pars()
environment.init()

def print_time():
    print "TIME: " + str(dt.now().strftime('%Y-%m-%d %H:%M.%S'))


def execute(*args, **kargs):
    print_time()
    plain_execute(*args, **kargs)


@parallel
def config():
    '''setup cassandra configuration parameters'''
    print 'setting up config at {0}'.format(env.host_string)
    with hide('running'):
        with cd(path.join(CODE_DIR, 'conf')):
            options_file = 'cassandra.yaml'
            run('''sed -i 's/seeds:.*/seeds: "{0}"/' cassandra.yaml'''.format(
                ", ".join(env.hosts)))
            set_option('listen_address:', env.host_string, options_file)
            set_option('rpc_address:', "", options_file)
            set_option('max_items_for_large_replication_degree:',
                       cassandra_settings.max_items_for_large_replication_degree, options_file)
            set_option('large_replication_degree:',
                       cassandra_settings.large_replication_degree, options_file)
            set_option('ignore_non_local:',
                       cassandra_settings.ignore_non_local, options_file)
            set_option('select_random_node:',
                       cassandra_settings.select_random_node, options_file)
        with cd(path.join(YCSB_CODE_DIR, 'workloads')):
            set_option('operationcount=',
                       cassandra_settings.operationcount, 'workloadb')
            set_option('recordcount=',
                       cassandra_settings.recordcount, 'workloadb')

def set_option(option, value, file):
    option_string = 'sed -i "s/^{option}.*/{option} {value}/" {file}'.format(
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
            time.sleep(2)
            with quiet():
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
    with hide('everything'):
        sudo("pgrep -f 'java.*c[a]ssandra'")

@parallel
@roles('ycsbnodes')
def do_ycsb(operation):
    hosts = ",".join(env.hosts)
    if operation == 'load':
        threads = 200
        out_file = YCSB_LOAD_OUT_FILE
        err_file = YCSB_LOAD_ERR_FILE
    else:
        threads = cassandra_settings.threads
        out_file = YCSB_RUN_OUT_FILE
        err_file = YCSB_RUN_ERR_FILE
    with cd(YCSB_CODE_DIR):
        sudo('./bin/ycsb {operation} cassandra-10 -threads {threads} -p hosts={hosts}'
             ' -P workloads/workloadb -s > {out_file} 2> {err_file}'.format(
                 **locals()))


@task
@roles('master')
def setup_environment():
    '''get cassandra ready to run from clean state'''
    with set_nodes(env.hosts + env.roledefs['ycsbnodes']):
        execute(clean.kill)
        execute(clean.nodes)
        execute(git.configure)

        execute(clean.state)
        execute(clean.logs)


@parallel
def empty_and_config_nodes():
    clean.kill()
    clean.state()
    clean.logs()
    config()


@parallel
def prepare_load():
    with hide('running'):
        jmx.set_bool_value(SAVE_OPS_PAR, False)
        jmx.set_bool_value(IGNORE_NON_LOCAL_PAR, False)
        jmx.set_value(MAX_ITEMS_FOR_LARGE_REPL_PAR, 0)
        jmx.set_value(MAX_ITEMS_FOR_LARGE_REPL_PAR, 0)
        jmx.set_value(SLEEP_TIME_PAR, 0)


@parallel
def prepare_run():
    with hide('running'):
        jmx.set_bool_value(SAVE_OPS_PAR, cassandra_settings.save_ops)
        jmx.set_bool_value(IGNORE_NON_LOCAL_PAR,
                           cassandra_settings.ignore_non_local)
        jmx.set_value(MAX_ITEMS_FOR_LARGE_REPL_PAR,
                      cassandra_settings.max_items_for_large_replication_degree)
        jmx.set_value(SLEEP_TIME_PAR, cassandra_settings.sleep_time)


def benchmark_round():
    with set_nodes(env.hosts + env.roledefs['ycsbnodes']):
        execute(empty_and_config_nodes)

    execute(start)
    time.sleep(30)
    execute(start_check)
    execute(setup_ycsb)
    time.sleep(10)

    execute(prepare_load)
    execute(do_ycsb,'load')
    with set_nodes(env.hosts + env.roledefs['ycsbnodes']):
        execute(clean.kill)

    time.sleep(5)
    execute(start)
    time.sleep(30)

    execute(start_check)
    execute(prepare_run)
    execute(do_ycsb,'run')

    time.sleep(10)
    with set_nodes(env.hosts + env.roledefs['ycsbnodes']):
        execute(collect.collect)
        execute(clean.kill)


@contextmanager
def set_nodes(newhosts):
    tmp = env.hosts
    env.hosts = list(set(newhosts))
    yield
    env.hosts = tmp


@parallel
def upload_libs():
    put(local_path='lib/*', remote_path='~/')


def prepare():
    execute(upload_libs)
    execute(git.get_code)
    execute(config)
    execute(git.compile_code)
    execute(git.compile_ycsb)


@task
def fork(cmd):
    with warn_only():
        sudo(cmd)


@task
@roles('master')
def boot_cassandra():
    prepare()
    execute(clean.state)
    execute(clean.logs)
    execute(clean.kill)

    execute(config)

    execute(start)
    time.sleep(30)
    execute(setup_ycsb)
    time.sleep(30)


@task
@roles('master')
def benchmark():
    with set_nodes(env.hosts + env.roledefs['ycsbnodes']):
        prepare()
    bench.benchmark()
