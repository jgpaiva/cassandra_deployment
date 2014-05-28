from __future__ import with_statement
from fabric.api import run
from fabric.api import cd
from fabric.api import parallel
from fabric.api import hide
from os import path
from environment import CODE_DIR, YCSB_CODE_DIR, BASE_GIT_DIR, SERVER_URL
from environment import cassandra_settings


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
        run("ant -q build")


@parallel
def compile_ycsb():
    with cd(path.join(YCSB_CODE_DIR,'core')):
        run('mvn clean install')
    with cd(path.join(YCSB_CODE_DIR,'cassandra')):
        run('mvn clean install')


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
