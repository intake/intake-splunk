from intake.source import base
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


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
        from intake_splunk.core import SplunkSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        query = source_kwargs.pop('query', "")
        url = source_kwargs.pop('url', "")
        auth = source_kwargs.pop('auth', tuple())
        return SplunkSource(query, url, auth,
                            metadata=base_kwargs['metadata'],
                            **source_kwargs)
