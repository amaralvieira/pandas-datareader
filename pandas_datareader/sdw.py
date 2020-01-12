from __future__ import unicode_literals

import pandas as pd

from pandas_datareader.base import _BaseReader
from pandas_datareader.compat import string_types
from pandas_datareader.io.sdmx import _read_sdmx_dsd, read_sdmx


class SDWReader(_BaseReader):
    """Get data for the given name from SDW."""

    _URL = "https://sdw-wsrest.ecb.europa.eu/service"
    
    @property
    def url(self):
        """API URL"""
        if not isinstance(self.symbols, string_types):
            raise ValueError("data name must be string")
        dataset, keys = self.symbols.split('.', 1)
        url = "{0}/data/{1}/{2}?startperiod={3}&endperiod={4}"
        return url.format(self._URL, dataset, keys, self.start.year, self.end.year)

    @property
    def dsd_url(self):
        """API DSD URL"""
        if not isinstance(self.symbols, string_types):
            raise ValueError("data name must be string")
        dataset, keys = self.symbols.split('.', 1)
        url = "{0}/datastructure/ECB/ECB_{1}1?references=codelist"
        return url.format(self._URL, dataset)

    def _read_one_data(self, url, params):
        resp_dsd = self._get_response(self.dsd_url)
        dsd = _read_sdmx_dsd(resp_dsd.content)

        resp = self._get_response(url)
        data = read_sdmx(resp.content, dsd=dsd)

        try:
            data.index = pd.to_datetime(data.index)
            data = data.sort_index()
        except ValueError:
            pass

        try:
            data = data.truncate(self.start, self.end)
        except TypeError:
            pass

        return data