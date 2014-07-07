from __future__ import with_statement
from fabric.api import run
from fabric.api import cd
from fabric.api import parallel
from fabric.api import hide
from fabric.api import task

from environment import CODE_DIR
from environment import YCSB_CODE_DIR
from environment import BASE_GIT_DIR
from environment import SERVER_URL
from environment import cassandra_settings

from utils import run_with_retry

from os import path


@parallel
def get_code():
    '''update git'''
    with hide('stdout'):
        if cassandra_settings.run_original:
            branch = 'origin/cassandra-2.1'
        else:
            branch = cassandra_settings.cassandra_commit or 'origin/autoreplicator2'
        for directory, branch in [(CODE_DIR, branch), (YCSB_CODE_DIR, 'origin/master')]:
            _get_code(branch, directory)


def _get_code(branch, working_dir):
    with cd(working_dir):
        run("git reset --hard")
        run("git checkout .")
        run_with_retry("git fetch -q -a")
        run("git checkout -q {branch}".format(**locals()))


@parallel
def compile_code():
    '''clean and compile cassandra code'''
    with cd(CODE_DIR):
        with hide('stdout'):
            run("ant -q clean > /dev/null")
            run_with_retry("ant -q")


@task
@parallel
def compile_ycsb():
    with hide('stdout'):
        with cd(path.join(YCSB_CODE_DIR, 'core')):
            run('mvn -q -Dmaven.test.skip=true clean install')
        with cd(path.join(YCSB_CODE_DIR, 'cassandra')):
            run('mvn -q -Dmaven.test.skip=true clean install')


@parallel
def configure():
    '''setup git on hosts'''
    run('git config --global user.email "fake@mail.com"')
    run('git config --global user.name "Fake Name"')
    for repo, branch in [('cassandra', 'cassandra-2.1'), ('YCSB', 'master')]:
        repo_url = "ssh://" + SERVER_URL + path.join(BASE_GIT_DIR, repo)
        run_with_retry("git clone {repo_url}".format(**locals()))
        with cd(repo):
            run("git checkout {branch}".format(**locals()))
