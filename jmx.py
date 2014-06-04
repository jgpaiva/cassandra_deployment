from fabric.api import task
from fabric.api import roles
from fabric.api import parallel
from environment import JMX_BEAN, JMX_TERM_JAR, JMX_PORT  # NOQA

from utils import sudo_with_retry


@task
@roles('master')
def get_value(attribute):
    return sudo_with_retry('echo "get -s -b {JMX_BEAN} {0}" |'
                           ' java -jar {JMX_TERM_JAR} -l localhost:{JMX_PORT}'
                           ' -v silent -n'.format(attribute, **globals()))


@task
@parallel
def set_value(attribute, val):
    sudo_with_retry('echo "set -b {JMX_BEAN} {0} {1}" |'
                    ' java -jar {JMX_TERM_JAR} -l localhost:{JMX_PORT} '
                    ' -v silent -n'.format(attribute, val, **globals()))

@task
@parallel
def set_bool_value(par, val):
    set_value(par, 'true' if val else 'false')


