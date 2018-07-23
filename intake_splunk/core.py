
import base64
import io
import requests
import pandas as pd
import time
import warnings
from intake.source import base

# because Splunk connections are against a self-signed cert, all connections
# would raise a warning
from . import __version__
warnings.filterwarnings('ignore', module='urllib3.connectionpool')


class SplunkSource(base.DataSource):
    """Execute a query on Splunk

    Parameters
    ----------
    query : str
        String to pass to Splunk for execution. If it does not start with "|"
        or "search", "search" will be prepended.
    url : str
        Endpoint on which to reach splunk, including protocol and port.
    auth : (str, str) or str
        Username/password to authenticate by.
    chunksize : int

    """
    container = 'dataframe'
    version = __version__
    name = 'splunk'
    partition_access = True

    def __init__(self, query, url, auth, chunksize=5000,
                 metadata=None):
        self.url = url
        self.auth = auth
        self.query = query
        self.chunksize = chunksize
        self._df = None
        super(SplunkSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        if self._df is None:
            # this waits until query is ready, but Splunk has results_preview
            # end-point which can be fetched while query is running
            self.splunk = SplunkConnect(self.url)
            if isinstance(self.auth, (tuple, list)):
                self.splunk.auth(*self.auth)
            else:
                self.splunk.auth_head(key=self.auth)
            self._df = self.splunk.read_dask(self.query, self.chunksize)
        self.npartitions = self._df.npartitions
        return base.Schema(datashape=None,
                           dtype=self._df,
                           shape=(None, len(self._df.columns)),
                           npartitions=self.npartitions,
                           extra_metadata={})

    def _get_partition(self, i):
        return self._df.get_partition(i).compute()

    def to_dask(self):
        self.discover()
        return self._df


class SplunkConnect:

    """
    Talk to Splunk over REST, download data to dataframes

    If there is no pre-fetched auth key, must call ``.auth(user, pw)`` to
    establish credentials, or ``.auth_head(user=user, pw=pw)`` to use simple
    auth on all calls.

    Main user methods: read_pandas, read_pandas_iter, read_dask

    Parameters
    ----------
    base_url: str
        Address to contact Splunk on, e.g., ``https://localhost:8089``
    key: str
        Auth key, if known
    """

    POLL_TIME = 1  # seconds to sleep between successive polls
    TIMEOUT = 600  # maximum seconds to wait for query to finish

    def __init__(self, base_url, key=None):
        self.url = base_url
        self.key = key
        self.head = None
        if key:
            self.auth_head(key)

    def auth(self, user, pw):
        """
        Login to splunk and get a session key
        """
        url = self.url + '/services/auth/login?output_mode=json'
        r = requests.post(url, verify=False, data={'username': user,
                                                   'password': pw})
        self.key = r.json()['sessionKey']
        self.auth_head(self.key)

    def auth_head(self, key=None, user=None, pw=None):
        """
        Make header either by session key or by user/pass
        """
        if key:
            self.head = {'Authorization': 'Splunk %s' % key}
        elif user is None and pw is None:
            raise ValueError('Must supply key or user/password')
        else:
            code = "%s:%s" % (user, pw)
            self.head = {'Authorization': 'Basic %s' % base64.b64encode(
                         code.encode()).decode()}

    @staticmethod
    def _sanitize_query(q):
        """
        Ensure that all queries are actually valid searches
        """
        q = q.strip()
        if not q.startswith('search') and not q.startswith('|'):
            return "search " + q
        return q

    def list_saved_searches(self):
        """
        Get saved search names/definitions as a dict
        """
        r = requests.get(
            self.url + '/services/saved/searches?output_mode=json',
            headers=self.head, verify=False)
        out = r.json()['entry']
        return {o['name']: o['content']['search'] for o in out}

    def start_query(self, q):
        """
        Initiate a query as a job
        """
        q = self._sanitize_query(q)
        # opportunity to pass extra args here, especially job timeout
        # http://docs.splunk.com/Documentation/Splunk/6.2.6/RESTREF/RESTsearch#POST_search.2Fjobs_method_detail
        r = requests.post(self.url + '/services/search/jobs?output_mode=json',
                          verify=False, data={'search': q},
                          headers=self.head)
        return r.json()['sid']

    def poll_query(self, sid):
        """
        Check the status of a job
        """
        path = '/services/search/jobs/{}?output_mode=json'.format(sid)
        r = requests.get(self.url + path, verify=False, headers=self.head)
        out = r.json()['entry'][0]['content']
        # why not pass all job details?
        return out['isDone'], out.get('resultCount', 0)

    def wait_poll(self, sid):
        # instead of polling, job could be started in exec_mode="blocking"
        time0 = time.time()
        while True:
            done, count = self.poll_query(sid)
            if done:
                return done, count
            if time.time() - time0 > self.TIMEOUT:
                raise RuntimeError("Timeout waiting for Splunk "
                                   "to finish query")
            time.sleep(self.POLL_TIME)

    def get_query_result(self, sid, offset=0, count=0):
        """
        Fetch query output (as CSV)
        """
        # could potentially be streaming download
        path = ('/services/search/jobs/{}/results/?output_mode=csv'
                '&offset={}&count={}').format(sid, offset, count)
        r = requests.get(self.url + path,  verify=False, headers=self.head)
        return r.content

    def get_dataframe(self, sid, offset=0, count=0, **kwargs):
        """
        Read a chunk from completed query, return a pandas dataframe

        Parameters
        ----------
        sid: str
            The job's ID
        offset: int
            Starting row
        count: int
            Number of rows to fetch
        kwargs: passed to pd.read_csv
        """
        # Since we know the count, could pre-allocate df and set values in
        # chunks while streaming the download
        txt = self.get_query_result(sid, offset, count)
        return pd.read_csv(io.BytesIO(txt), **kwargs)

    def read_pandas(self, q, **kwargs):
        """
        Start query, wait for completion and download data as a dataframe

        Parameters
        ----------
        q: str
            Valid Splunk query
        kwargs: passed to pd.read_csv
        """
        sid = self.start_query(q)
        self.wait_poll(sid)
        return self.get_dataframe(sid, **kwargs)

    def read_pandas_iter(self, q, chunksize, **kwargs):
        """
        Start query, wait for completion and make an iterator of dataframes

        Parameters
        ----------
        q: str
            Valid Splunk query
        chunksize: int
            Number of rows in each dataframe
        kwargs: passed to pd.read_csv
        """
        sid = self.start_query(q)
        done, count = self.wait_poll(sid)
        for i in range(0, count, chunksize):
            yield self.get_dataframe(sid, offset=i, count=chunksize, **kwargs)

    def read_dask(self, q, chunksize, **kwargs):
        """
        Start query, wait for completion, return lazy dask dataframe.

        This does download the first 20 rows in this thread, to infer dtypes.

        Parameters
        ----------
        q: str
            Valid Splunk query
        chunksize: int
            Number of rows in each dataframe
        kwargs: passed to pd.read_csv
        """
        from dask import delayed
        import dask.dataframe as dd
        sid = self.start_query(q)
        done, count = self.wait_poll(sid)
        meta = self.get_dataframe(sid, count=20)[:0]
        parts = [delayed(self.get_dataframe)(sid, offset=i, count=chunksize,
                                             **kwargs)
                 for i in range(0, count, chunksize)]
        return dd.from_delayed(parts, meta=meta)
