from __future__ import with_statement
from fabric.api import run
from fabric.api import cd
from fabric.api import parallel
from fabric.api import hide
from fabric.api import warn_only

from environment import CODE_DIR
from environment import YCSB_CODE_DIR
from environment import BASE_GIT_DIR
from environment import SERVER_URL
from environment import MAX_RETRIES
from environment import cassandra_settings

from utils import run_with_retry

from os import path


@parallel
def get_code():
    '''update git'''
    with hide('stdout'):
        with cd(CODE_DIR):
            run("git reset --hard")
            with warn_only():
                for i in range(MAX_RETRIES):
                    res = run("git fetch -q origin")
                    if not res.failed:
                        break
            if cassandra_settings.run_original:
                run("git checkout -q cassandra-2.1")
                run_with_retry("git pull -q origin cassandra-2.1")
            else:
                run("git checkout -q autoreplicator")
                run_with_retry("git pull -q origin autoreplicator")
        with cd(YCSB_CODE_DIR):
            run("git checkout -q master")
            run_with_retry("git pull -q origin")


@parallel
def compile_code():
    '''clean and compile cassandra code'''
    with cd(CODE_DIR):
        run("ant -q clean > /dev/null")
        run_with_retry("ant")

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
        run_with_retry("git clone {0}".format(repo_dir))
        with cd(repo):
            run("git checkout {0}".format(branch))
