import os

import pytest
import pandas as pd

from intake_splunk.core import SplunkSource
from .util import start_splunk, stop_docker

CONNECT = {'host': 'localhost', 'port': 9200}
TEST_DATA_DIR = os.path.dirname(__file__)
TEST_DATA = 'sample1.csv'
df = pd.read_csv(os.path.join(TEST_DATA_DIR, TEST_DATA))


@pytest.fixture(scope='module')
def engine():
    """Start docker container for ES and cleanup connection afterward."""
    stop_docker('intake-splunk', let_fail=True)
    cid = None
    try:
        cid = start_splunk()

        yield 'https://localhost:8089'
    finally:
        stop_docker(cid=cid)


def test_basic(engine):
    # should always return something
    test_query = 'index=_internal'
    c = SplunkSource(test_query, engine, ('admin', 'changeme'), 100)
    d = c.to_dask()
    assert len(d.compute())
