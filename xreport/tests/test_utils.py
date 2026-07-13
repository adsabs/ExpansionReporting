import os
import sys
import unittest
import httpretty
import json
import urllib.request, urllib.parse, urllib.error
from xreport.utils import _group
from xreport.utils import _make_dict
from xreport.utils import _get_citations
from xreport.utils import _get_usage
from xreport.utils import _get_facet_data
from xreport.utils import _get_records

class TestMethods(unittest.TestCase):

    '''Check if methods return expected results'''
    def setUp(self):
        from xreport.compat import load_config
        self.proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../../'))
        self.config = load_config(proj_home=self.proj_home)
    
    def test_group(self):
            '''Test if the grouping method behaves properly'''
            results = ['1', 2, '3', 4]
            expected = [('1',2),('3',4)]
            self.assertEqual(list(_group(results, 2)), expected)
            
    def test_make_dict(self):
        '''Test making a dictionary from a list of tuples'''
        results = [('1',2),('3',4)]
        expected = {1:2, 3:4}
        self.assertEqual(_make_dict(results), expected)
    
    @httpretty.activate
    def test_get_citations(self):
        # Get the mock data
        datafile = '{0}/xreport/tests/data/PivotDataYearCitCount.json'.format(self.proj_home)
        with open(datafile) as mdata:
            mockdata = json.load(mdata)
        # The URL to mock
        query_url = "{}/search/query".format(self.config['ADS_API_URL'])
        # Register the URL and mock data
        httpretty.register_uri(
                    httpretty.GET, 
                    query_url,
                    content_type='application/json',
                    status=200,
                    body=json.dumps(mockdata))
        # Do the query
        q = "star"
        expected = 14338828
        self.assertEqual( _get_citations(self.config, q), expected)

    @httpretty.activate
    def test_get_citations_500(self):
        # Get the mock data
        datafile = '{0}/xreport/tests/data/PivotDataYearCitCount.json'.format(self.proj_home)
        with open(datafile) as mdata:
            mockdata = json.load(mdata)
        # The URL to mock
        query_url = "{}/search/query".format(self.config['ADS_API_URL'])
        # Register the URL and mock data
        httpretty.register_uri(
                    httpretty.GET, 
                    query_url,
                    content_type='application/json',
                    status=500,
                    body=json.dumps(mockdata))
        # Do the query
        q = "star"
        expected = "Search API request with error code '500'"
        try:
            results = _get_citations(self.config, q)
        except Exception as err:
            self.assertEqual(str(err), expected)

    @httpretty.activate
    def test_get_citations_invalid_JSON_response(self):
        # The URL to mock
        query_url = "{}/search/query".format(self.config['ADS_API_URL'])
        # Register the URL and mock data
        httpretty.register_uri(
                    httpretty.GET, 
                    query_url,
                    content_type='application/json',
                    status=200,
                    body="foo")
        # Do the query
        q = "star"
        expected = "No JSON object could be decoded from Search API"
        try:
            results = _get_citations(self.config, q)
        except Exception as err:
            self.assertEqual(str(err), expected)

    @httpretty.activate
    def test_get_facet_data(self):
        # Get the mock data for testing year counts
        datafile = '{0}/xreport/tests/data/FacetDataYearCount.json'.format(self.proj_home)
        with open(datafile) as mdata:
            mockdata = json.load(mdata)
        # The URL to mock
        query_url = "{}/search/query".format(self.config['ADS_API_URL'])
        # Register the URL and mock data
        httpretty.register_uri(
                    httpretty.GET, 
                    query_url,
                    content_type='application/json',
                    status=200,
                    body=json.dumps(mockdata))
        # Do the query
        q = "star"
        expected = {2012: 3118, 2015: 3055, 2016: 3038, 2017: 3104, 2019: 3190, 2020: 3079}
        self.assertEqual(_get_facet_data(self.config, q, 'year'), expected)
        # Get the mock data for testing year counts
        datafile = '{0}/xreport/tests/data/FacetDataVolumeCount.json'.format(self.proj_home)
        with open(datafile) as mdata:
            mockdata = json.load(mdata)
        # The URL to mock
        query_url = "{}/search/query".format(self.config['ADS_API_URL'])
        # Register the URL and mock data
        httpretty.register_uri(
                    httpretty.GET, 
                    query_url,
                    content_type='application/json',
                    status=200,
                    body=json.dumps(mockdata))
        # Do the query
        q = "star"
        expected = {904: 201, 900: 196, 889: 189, 897: 186, 891: 182, 905: 129}
        self.assertEqual(_get_facet_data(self.config, q, 'volume'), expected)

    @httpretty.activate
    def test_get_records(self):
        # Get the mock data for testing year counts
        datafile = '{0}/xreport/tests/data/SolrResponse.json'.format(self.proj_home)
        with open(datafile) as mdata:
            mockdata = json.load(mdata)
        # The URL to mock
        query_url = "{}/search/query".format(self.config['ADS_API_URL'])
        # Register the URL and mock data
        httpretty.register_uri(
                    httpretty.GET, 
                    query_url,
                    content_type='application/json',
                    status=200,
                    body=json.dumps(mockdata))
        # Do the query
        q = "star"
        expected = {'author_norm': ['Kobayashi, C', 'Karakas, A', 'Lugaro, M'], 'bibcode': '2020ApJ...900..179K', 
                    'citation_count': 152, 'title': ['The Origin of Elements from Carbon to Uranium']}
        self.assertEqual(_get_records(self.config, q, 'bibcode')[0], expected)

    def test_get_usage(self):
        '''Test getting usage data'''
        self.config['CLASSIC_USAGE_INDEX'] = {
            'reads':'{0}/xreport/tests/data/reads.links'.format(self.proj_home),
            'downloads':'{0}/xreport/tests/data/downloads.links'.format(self.proj_home)
        }
        # Expected reads and downloads data for a set of bibcodes
        bibcodes = ['2022ApJ...924...44A','2022A&A...660A..44K','2022MNRAS.509...44W']
        expected_bibcodes_reads = (449, 288)
        expected_bibcodes_downloads = (242, 167)
        # Expected reads and downloads data for a set of journals
        journals = ['ApJ','MNRAS','A&A']
        expected_journals_reads = (975, 483)
        expected_journals_downloads = (462, 218)
        # Get the data and compare with expected values
        self.assertEqual(_get_usage(self.config, bibcodes=bibcodes), expected_bibcodes_reads)
        self.assertEqual(_get_usage(self.config, bibcodes=bibcodes, udata='downloads'), expected_bibcodes_downloads)
        self.assertEqual(_get_usage(self.config, jrnls=journals), expected_journals_reads)
        self.assertEqual(_get_usage(self.config, jrnls=journals, udata='downloads'), expected_journals_downloads)

if __name__ == '__main__':
    unittest.main()