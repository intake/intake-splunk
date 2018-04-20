import pytest

import intake
from intake_splunk.core import SplunkSource
from .util import start_splunk, stop_docker


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


def test_open(engine):
    # should always return something
    test_query = 'index=_internal'
    c = intake.open_splunk(test_query, engine, ('admin', 'changeme'), 100)
    d = c.to_dask()
    assert len(d.compute())
