from fabric.api import warn_only
from fabric.api import run
from fabric.utils import error
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
    for i in range(retries):
        with warn_only():
            res = operation(command)
            if not res.failed:
                return res
            error("Command '{command}' failed {0} times at node {node}".format(
                i + 1, **locals()))
    error("Command '{command}' failed {retries} times. Aborted".format(
            **locals()))
