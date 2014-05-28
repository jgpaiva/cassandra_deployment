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

from os import path
import time
from datetime import datetime as dt

from environment import CODE_DIR, YCSB_CODE_DIR, SAVE_OPS_PAR, IGNORE_NON_LOCAL_PAR
from environment import cassandra_settings

from clean import killall, clear_logs, clear_state, clean_nodes
from collect import collect_results
from handle_git import get_code, compile_code, config_git, compile_ycsb
import jmx

with open('slaves', 'r') as f:
    env.hosts = [line[:-1] for line in f]

env.roledefs['master'] = [env.hosts[0]]


@parallel
def config():
    '''setup cassandra configuration parameters'''
    print 'setting up config at {0}'.format(env.host_string)
    with hide('running'):
        with cd(path.join(CODE_DIR, 'conf')):
            run('''sed -i 's/seeds:.*/seeds: "{0}"/' cassandra.yaml'''.format(
                env.roledefs['master'][0]))
            run('''sed -i "s/^listen_address:.*/listen_address: {0}/" cassandra.yaml'''.format(env.host_string))
            run('''sed -i "s/^rpc_address:.*/rpc_address: /" cassandra.yaml''')
            run('''sed -i "s/^max_items_for_large_replication_degree:.*/max_items_for_large_replication_degree: {0}/" cassandra.yaml'''.format(
                cassandra_settings.max_items_for_large_replication_degree))
            run('''sed -i "s/^large_replication_degree:.*/large_replication_degree: {0}/" cassandra.yaml'''.format(
                cassandra_settings.large_replication_degree))
            run('''sed -i "s/^ignore_non_local:.*/ignore_non_local: {0}/" cassandra.yaml'''.format(
                cassandra_settings.ignore_non_local))


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
    time.sleep(10)
    with cd(CODE_DIR):
        cassandra_bin = path.join(CODE_DIR, "bin", "cassandra")
        sudo("screen -d -m {0} -f".format(cassandra_bin), pty=False)


@parallel
def do_ycsb(arg):
    with cd(YCSB_CODE_DIR):
        run('./bin/ycsb {1} cassandra-10 -threads {2} -p hosts={0} -P workloads/workloadb > /tmp/{1}.out'.format(env.host_string,
            arg, cassandra_settings.threads))


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

    execute(set_ignore_non_local,False)
    print_time()
    execute(do_ycsb, 'load')
    execute(killall)

    execute(set_ignore_non_local,cassandra_settings.ignore_non_local)
    print_time()
    execute(start)
    time.sleep(30)

    execute(set_save_ops)
    execute(do_ycsb, 'run')

    print_time()
    execute(collect_results)
    execute(killall)
    print_time()


@parallel
def upload_libs():
    put(local_path='lib/*', remote_path='~/')


def prepare():
    execute(upload_libs)
    execute(get_code)
    execute(config)
    execute(compile_code)


@parallel
def set_save_ops():
    jmx.set(SAVE_OPS_PAR, 'true' if cassandra_settings.save_ops else 'false')

@parallel
def set_ignore_non_local(val):
    jmx.set(IGNORE_NON_LOCAL_PAR, 'true' if val else 'false')

@task
def fork(cmd):
    run(cmd)

@task
@roles('master')
def boot_cassandra():
    prepare()
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


@task
@roles('master')
def benchmark():
    prepare()

    cassandra_settings.save_ops = True
    cassandra_settings.save_repl_set = True
    benchmark_round()

    cassandra_settings.save_ops = True
    cassandra_settings.save_repl_set = True
    cassandra_settings.max_items_for_large_replication_degree = 0
    benchmark_round()

    cassandra_settings.save_ops = True
    cassandra_settings.save_repl_set = True
    cassandra_settings.large_replication_degree = 12
    cassandra_settings.max_items_for_large_replication_degree = 20
    benchmark_round()
