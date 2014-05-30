from __future__ import with_statement
from fabric.api import run
from fabric.api import cd
from fabric.api import parallel
from fabric.api import hide
from fabric.api import quiet
from fabric.api import abort
from fabric.api import env

from environment import CODE_DIR, YCSB_CODE_DIR, BASE_GIT_DIR, SERVER_URL, MAX_COMPILE_RETRIES
from environment import cassandra_settings

from os import path


@parallel
def get_code():
    '''update git'''
    with hide('stdout'):
        with cd(CODE_DIR):
            run("git reset --hard")
            run("git fetch -q origin")
            if cassandra_settings.run_original:
                run("git checkout -q cassandra-2.1")
                run("git pull -q origin cassandra-2.1")
            else:
                run("git checkout -q autoreplicator")
                run("git pull -q origin autoreplicator")
        with cd(YCSB_CODE_DIR):
            run("git checkout -q master")
            run("git pull -q origin")


@parallel
def compile_code():
    '''clean and compile cassandra code'''
    with cd(CODE_DIR):
        run("ant -q clean > /dev/null")
        with quiet():
            all_res = []
            for i in range(MAX_COMPILE_RETRIES):
                res = run("ant")
                if not res.failed:
                    break
                all_res.append(res)
                print('Compile failed at {0}. Retry {1}.'.format(
                    env.host_string, i))
            else:
                abort('command failed {0} times at node {1}:\n{2}'.format(
                    MAX_COMPILE_RETRIES, env.host_string, res))


@parallel
def compile_ycsb():
    with cd(path.join(YCSB_CODE_DIR,'core')):
        run('mvn -q -Dmaven.test.skip=true clean install')
    with cd(path.join(YCSB_CODE_DIR,'cassandra')):
        run('mvn -q -Dmaven.test.skip=true clean install')


@parallel
def config_git():
    '''setup git on hosts'''
    run('git config --global user.email "you@example.com"')
    run('git config --global user.name "Your Name"')
    for repo, branch in [('cassandra', 'cassandra-2.1'), ('YCSB', 'master')]:
        repo_dir = "ssh://{0}{1}".format(SERVER_URL, path.join(BASE_GIT_DIR, repo))
        run("git clone {0}".format(repo_dir))
        with cd(repo):
            run("git checkout {0}".format(branch))
