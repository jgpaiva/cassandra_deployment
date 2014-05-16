from __future__ import with_statement
from fabric.api import run
from fabric.api import cd
from fabric.api import parallel
from fabric.api import sudo
from fabric.api import env
from fabric.api import settings
from fabric.api import roles
from fabric.api import execute
from fabric.api import task
from fabric.api import get
from fabric.api import local
from fabric.api import hide
from fabric.api import serial
from os import path

import time
from datetime import datetime as dt

with open('slaves','r') as f:
    env.hosts = [line[:-1] for line in f]

env.roledefs['master'] = [env.hosts[0]]

CODE_DIR = "~/cassandra"
YCSB_CODE_DIR = "~/YCSB"
PID_FILE = "/tmp/cassandra.pid"
BASE_GIT_DIR = "/home/jgpaiva/nas/autoreplicator/"
LOG_FILE = "/var/log/cassandra/system.log"
SERVER_URL = "cloudtm.ist.utl.pt"

class DecentRepr(type):
      def __repr__(self):
          settings =  (var for var in vars(self) if not var.startswith('_'))
          return self.__name__ + "{" + ", ".join((i + ":" + str(getattr(self,i)) for i in settings)) + "}"

@parallel
def killall():
    '''kill all java processes'''
    with settings(warn_only=True):
        sudo("killall -q java || true")

@parallel
def get_code():
    '''update git'''
    with hide('stdout'):
        with cd(CODE_DIR):
            run("git reset --hard")
            if cassandra_settings.run_original:
                run("git checkout -q 11827f0d7e0d50565f276a7aefe9a88873529ba7")
            else:
                run("git checkout -q cassandra-2.1")
                run("git pull -q origin")
        with cd(YCSB_CODE_DIR):
            run("git checkout -q master")
            run("git pull -q origin")

@parallel
def compile_code():
    '''clean and compile cassandra code'''
    with cd(CODE_DIR):
        run("ant -q clean > /dev/null")
        run("ant -q build > /dev/null")

@parallel
def clear_logs():
    '''delete cassandra logs'''
    sudo("rm -f {0}".format(LOG_FILE))

@serial
def start():
    '''start cassandra in all nodes in order'''
    time.sleep(10)
    with cd(CODE_DIR):
        sudo("screen -d -m /home/jgpaiva/cassandra/bin/cassandra -f",pty=False)

@parallel
def clean():
    '''delete all cassandra persistent state'''
    killall()
    sudo("rm -rf /var/lib/cassandra/")

@parallel
def config_git():
    '''setup git on hosts'''
    run('git config --global user.email "you@example.com"')
    run('git config --global user.name "Your Name"')
    for repo,branch in [('cassandra','cassandra-2.1'),('YCSB','master')]:
        repo_dir = "ssh://{0}{1}".format(SERVER_URL,path.join(BASE_GIT_DIR,repo))
        run("git clone {0}".format(repo_dir))
        with cd(repo):
            run("git checkout {0}".format(branch))

@parallel
def delete_git():
    for repo in [CODE_DIR,YCSB_CODE_DIR]:
        run("rm -rf {0}".format(repo))

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

@parallel
def compile_ycsb():
    with cd('YCSB'):
        run('cd core && mvn clean install && cd ../cassandra/ && mvn clean install')

@parallel
def run_ycsb():
    with cd('YCSB'):
        run('./bin/ycsb run cassandra-10 -p hosts={0} -P workloads/workloadb > /tmp/run.out'.format(env.host_string))

@parallel
def load_ycsb():
    with cd('YCSB'):
        run('./bin/ycsb load cassandra-10 -p hosts={0} -P workloads/workloadb > /tmp/load.out'.format(env.host_string))

@task
@roles('master')
def prepare():
    '''boot cassandra from its persistent state'''
    execute(killall)
    execute(clear_logs)
    execute(get_code)
    execute(config)
    execute(compile_code)

@task
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

@task
@roles('master')
def collect_results():
    res_dir = "results." + str(dt.now().strftime('%Y%m%d.%H%M%S'))
    execute(collect_results_from_nodes,res_dir)
    write_settings(res_dir)

def write_settings(res_dir):
    out_file = path.join(res_dir,'settings')
    local('echo "{0}" > {1}'.format(cassandra_settings,out_file))

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
    local('echo "{0}" > {1}'.format(git_status,path.join(node_dir,"git_status")))


def benchmark_round():
    execute(clean)
    execute(config)
    execute(prepare)
    execute(start)
    time.sleep(30)
    execute(setup_ycsb)
    time.sleep(30)
    execute(load_ycsb)
    execute(prepare)
    execute(start)
    time.sleep(30)
    execute(run_ycsb)
    execute(collect_results)
    execute(killall)

class cassandra_settings(object):
    __metaclass__ = DecentRepr

    run_original = False
    max_items_for_large_replication_degree = 20
    replication_factor = 2
    large_replication_degree = 4
print "running with settings: {0}".format(cassandra_settings)

@task
@roles('master')
def benchmark():
    cassandra_settings.large_replication_degree = 3
    benchmark_round()
    cassandra_settings.large_replication_degree = 4
    benchmark_round()
    cassandra_settings.large_replication_degree = 5
    benchmark_round()
    cassandra_settings.max_items_for_large_replication_degree = 0
    benchmark_round()
    cassandra_settings.run_original = True
    benchmark_round()
