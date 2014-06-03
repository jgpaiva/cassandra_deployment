from fabric.api import warn_only
from fabric.api import run
from fabric.api import abort
from fabric.api import env
from fabric.api import sudo
from environment import MAX_RETRIES

def run_with_retry(command):
    with warn_only():
        for i in range(MAX_RETRIES):
            res = run(command)
            if not res.failed:
                break
            print "WARNING: command '{0}' failed {1} times at node".format(command,i+1,env.host_string)
        else:
            abort("FATAL: command '{0}' failed {1} times".format(command,MAX_RETRIES))

def sudo_with_retry(command):
    with warn_only():
        for i in range(MAX_RETRIES):
            res = sudo(command)
            if not res.failed:
                break
            print "WARNING: command '{0}' failed {1} times at node".format(command,i+1,env.host_string)
        else:
            abort("FATAL: command '{0}' failed {1} times".format(command,MAX_RETRIES))
