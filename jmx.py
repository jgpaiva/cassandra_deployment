from fabric.api import sudo
from fabric.api import task
from fabric.api import roles
from environment import JMX_BEAN, JMX_TERM_JAR, JMX_PORT  # NOQA


@task
@roles('master')
def get(attribute):
    return sudo("""echo "get -s -b {JMX_BEAN} {0}" | java -jar {JMX_TERM_JAR} -l localhost:{JMX_PORT} -v silent -n""".format(attribute, **globals()))


@task
@roles('master')
def set(attribute, val):
    sudo("""echo "set -b {JMX_BEAN} {0} {1}" | java -jar {JMX_TERM_JAR} -l localhost:{JMX_PORT} -v silent -n""".format(attribute, val, **globals()))
