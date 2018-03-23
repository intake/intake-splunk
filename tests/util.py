import os
import shlex
import subprocess
from time import sleep
import requests

here = os.path.dirname(__file__)


def start_splunk():
    """Bring up a container running ES.

    Waits until REST API is live and responsive.
    """
    print('Starting Splunk server...')

    cmd = shlex.split('docker create -p 8000:8000 -p 8089:8089 -p 9997:9997'
                      ' -p 514:514 --name intake-splunk  dmaxwell/splunk')
    cid = subprocess.check_output(cmd).decode()[:-1]
    fn = os.path.join(here, 'server.conf')
    subprocess.check_call(shlex.split(
        "docker cp {} "
        "intake-splunk:/opt/splunk/etc/system/default/server.conf".format(fn)
    ))
    subprocess.check_call(shlex.split("docker start intake-splunk"))

    timeout = 10
    while True:
        try:
            r = requests.get('http://localhost:8000')
            if r.ok:
                break
        except requests.ConnectionError:
            timeout -= 0.1
            sleep(0.1)
            assert timeout > 0, "Timeout waiting from Splunk"
    return cid


def stop_docker(name='intake-splunk', cid=None, let_fail=False):
    """Stop docker container with given name tag

    Parameters
    ----------
    name: str
        name field which has been attached to the container we wish to remove
    cid: str
        container ID, if known
    let_fail: bool
        whether to raise an exception if the underlying commands return an
        error.
    """
    try:
        if cid is None:
            print('Finding %s ...' % name)
            cmd = shlex.split('docker ps -q --filter "name=%s"' % name)
            cid = subprocess.check_output(cmd).strip().decode()
        if cid:
            print('Stopping %s ...' % cid)
            subprocess.call(['docker', 'rm', '-f', cid])
    except subprocess.CalledProcessError as e:
        print(e)
        if not let_fail:
            raise
