from fabric.api import warn_only
from fabric.api import run
from fabric.api import abort
from fabric.api import env
from fabric.api import sudo
from environment import MAX_RETRIES


def run_with_retry(command):
    return _operation_with_retry(run, command)


def sudo_with_retry(command):
    return _operation_with_retry(sudo, command)


def _operation_with_retry(operation, command):
    retries = MAX_RETRIES
    node = env.host_string
    with warn_only():
        for i in range(retries):
            res = operation(command)
            if not res.failed:
                return res
            print "WARNING: command '{command}' failed {0} times at node {node}".format(
                i + 1, **locals())
        else:
            abort(
                "FATAL: command '{command}' failed {retries} times".format(
                    **locals()))
