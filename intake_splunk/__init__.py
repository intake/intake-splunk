from intake.source import base
from . import core
__version__ = '0.0.1'


class Plugin(base.Plugin):
    """Plugin for Splunk reader"""

    def __init__(self):
        super(Plugin, self).__init__(name='splunk',
                                     version=__version__,
                                     container='dataframe',
                                     partition_access=True)

    def open(self, query, url, auth, chunksize=5000, **kwargs):
        """
        Create SplunkSource instance

        Parameters
        ----------
        query, url, auth, chunksize
            See ``SplunkSource``.
        """
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        query = source_kwargs.pop('query', "")
        url = source_kwargs.pop('url', "")
        auth = source_kwargs.pop('auth', tuple())
        return SplunkSource(query, url, auth,
                            metadata=base_kwargs['metadata'],
                            **source_kwargs)


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
    container = 'python'

    def __init__(self, query, url, auth, chunksize=5000,
                 metadata=None):
        self.splunk = core.SplunkConnect(url)
        if isinstance(auth, (tuple, list)):
            self.splunk.auth(*auth)
        else:
            self.splunk.auth_head(key=auth)
        self.query = query
        self.chunksize = chunksize
        self._df = None
        super(SplunkSource, self).__init__(container=self.container,
                                           metadata=metadata)

    def _get_schema(self):
        if self._df is None:
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