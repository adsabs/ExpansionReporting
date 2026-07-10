import os
import sys
import glob
import unittest
import mock
from datetime import datetime
import httpretty
import json
import urllib.request, urllib.parse, urllib.error
import pandas as pd

from xreport.reports import Report
from xreport.reports import FullTextReport
from xreport.reports import ReferenceMatchingReport
from xreport.reports import ReferenceCoverageReport
from xreport.reports import SummaryReport

class TestMethods(unittest.TestCase):

    '''Check if methods return expected results'''
    def setUp(self):
        from adsputils import load_config
        self.proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../../'))
        self.config = load_config(proj_home=self.proj_home)

    @httpretty.activate
    def test_reports(self):
        # The mock for the ADS API request. We need to send back different data, depending on
        # which facet query is requested. Since the same API query is used (with just different
        # parameters in the header), we need a callback function to determine the appropriate data
        def request_callback(request, uri, response_headers):
            content_type = request.headers.get('Content-Type')
            if uri.find('facet.field=volume') > -1:
                # The query was for a facet query by volume
                datafile = '{0}/xreport/tests/data/FacetDataVolumeCount.json'.format(self.proj_home)
            elif uri.find('facet.field=year') > -1:
                # The query was for a facet query by year
                datafile = '{0}/xreport/tests/data/FacetDataYearCount.json'.format(self.proj_home)
            else:
                # The query must have been a pivot query
                datafile = '{0}/xreport/tests/data/PivotDataYearCitCount.json'.format(self.proj_home)
            with open(datafile) as mdata:
                mockdata = json.load(mdata)

            return [200, response_headers, json.dumps(mockdata)]
        # The URL to be mocked
        query_url = "{}/search/query".format(self.config['ADS_API_URL'])
        # Register the query URL
        httpretty.register_uri(
                    httpretty.GET, 
                    query_url,
                    content_type='application/json',
                    status=200,
                    body=request_callback)

        ######## TEST OF THE GENERIC REPORT CLASS ###############################

        # Instantiate the generic Report class
        # Update the default config with the location of mock data
        config = {
            'CLASSIC_FULLTEXT_INDEX':'{0}/xreport/tests/data/fulltext.links'.format(self.proj_home)
        }
        r = Report(config=config)
        r.config['OUTPUT_DIRECTORY'] = '{0}/xreport/tests/data'.format(self.proj_home)
        # For testing we work with just a minimal journal set
        r.config['JOURNALS']['AST'] = ['ApJ..','MNRAS']
        # Confirm that the date string in the class is today's date
        self.assertEqual(r.dstring, datetime.today().strftime('%Y%m%d'))
        # We expect the following keys in the configuration dictionary
        expected_config_keys = ['PROJ_HOME', 'ADS_API_TOKEN', 'ADS_API_URL', 'CLASSIC_FULLTEXT_INDEX',
                                'CLASSIC_USAGE_INDEX', 'COLLECTIONS', 'COLLECTION_FILTERS', 'CONTENT_QUERIES', 'FORMATS', 
                                'JOURNALS', 'LOGGING_LEVEL', 'LOG_STDOUT', 'OUTPUT_DIRECTORY', 'SKIP_USAGE', 'SOURCES', 
                                'SUBJECTS', 'YEAR_IS_VOL']
        self.assertListEqual(list(r.config.keys()), expected_config_keys)
        # The make_report method for the generic Report should contain just basic journal data
        r.make_report('AST', 'SUMMARY')
        statsdata_expected = {'ApJ..': {'pubdata': {904: 201, 900: 196, 889: 189, 897: 186, 891: 182, 905: 129}, 
                                        'startyear': 2012, 'lastyear': 2020, 'startvol': 889, 'lastvol': 905, 
                                        'general': {}, 'arxiv': {}, 'publisher': {}, 'crossref': {}}, 
                              'MNRAS': {'pubdata': {904: 201, 900: 196, 889: 189, 897: 186, 891: 182, 905: 129}, 
                                        'startyear': 2012, 'lastyear': 2020, 'startvol': 889, 'lastvol': 905, 
                                        'general': {}, 'arxiv': {}, 'publisher': {}, 'crossref': {}}}
        self.assertDictEqual(r.statsdata, statsdata_expected)
        # If we specify a non-existing collection, the make_report method should complain
        error = False
        try:
            r.make_report('FOO', 'SUMMARY')
        except Exception as err:
            error = True
        self.assertTrue(error)
        # The summary data in the generic Report just contain just initialization values
        summarydata_expected = {'nrecs': 0, 'ftrecs': 0, 'refrecs': 0, 'oarecs': 0, 'dlrecs': 0, 'citnum': 0, 
                     'recent_citnum': 0, 'reads': 'NA', 'recent_reads': 'NA', 'downloads': 'NA', 'recent_downloads': 'NA'}
        for v in r.summarydata.values():
            self.assertDictEqual(v, summarydata_expected)
        #
        r.save_report('AST', 'NASA', 'FULLTEXT')
        outdir = "{0}/{1}".format(r.config['OUTPUT_DIRECTORY'], 'NASA')
        output_file = "{0}/{1}_{2}_{3}.xlsx".format(outdir, 'fulltext', 'AST', r.dstring)
        self.assertTrue(os.path.exists(output_file))
        # Confirm that we can read the file back into Pandas
        success = True
        try:
            df = pd.read_excel(output_file, index_col=0)
        except:
            success = False
        self.assertTrue(success)
        # clean up the directory/file we created
        os.remove(output_file)
        os.rmdir(outdir)
        self.assertFalse(os.path.exists(output_file))
        # The CURATORS report type should generate two output files
        r.save_report('AST', 'CURATORS', 'FULLTEXT')
        outdir = "{0}/{1}".format(r.config['OUTPUT_DIRECTORY'], 'CURATORS')
        out_files = glob.glob("{0}/*.xlsx".format(outdir))
        # Two Excel files should have been created
        self.assertEqual(len(out_files), 2)
        # Confirm that each file can be read back into Pandas
        for output_file in out_files:
            success = True
            try:
                df = pd.read_excel(output_file, index_col=0)
            except:
                success = False
            self.assertTrue(success)
            os.remove(output_file)
        os.rmdir(outdir)
        # Test highlight function
        values = [95, 55, 65, 75]
        colors = [r._highlight_cells(v) for v in values]
        expected_colors = ['background-color: #6aa84f', 'background-color: #f4cccc', 'background-color: #ffe599', 'background-color: #cfe2f3']
        self.assertListEqual(colors, expected_colors)

        ######## TEST OF THE FULL TEXT REPORT CLASS ###############################

        # Now instantiate the Fulltext Report
        ftr = FullTextReport(config=config)
        try:
            data = ftr.ft_index.query("bibstem=='ApJ..' and source=='arxiv'")
        except:
            data = []
        # There are two entries that match the above criteria in the mock data
        self.assertEqual(len(data), 2)
        # Test make_report using this index
        ftr.config['JOURNALS']['AST'] = ['ApJ..']
        try:
            ftr.make_report("AST", "CURATORS")
        except:
            pass
        # compare results with expected values from mock data
        expected = {889: 5.8, 891: 6.0, 897: 5.9, 900: 5.6, 904: 5.5, 905: 8.5}
        self.assertDictEqual(ftr.statsdata['ApJ..']['publisher'], expected)
        # in this case the 'general' attribute (used for the "NASA" reporting) should be empty
        self.assertDictEqual(ftr.statsdata['ApJ..']['general'], {})
        # Now run make_report that will use facet query
        try:
            ftr.make_report("AST", "NASA")
        except:
            pass
        expected = {889: 100.0, 891: 100.0, 897: 100.0, 900: 100.0, 904: 100.0, 905: 100.0}
        self.assertDictEqual(ftr.statsdata['ApJ..']['general'], expected)

        ######## TEST OF THE REFERENCES REPORT CLASS ###############################

        rmr = ReferenceMatchingReport()
        rmr.config['JOURNALS']['AST'] = ['ApJ..']
        # Create the report using mock data
        rmr.make_report('AST', 'REFERENCES')
        self.assertEqual(rmr.statsdata['ApJ..']['publisher'][900], 96.7)

        ######## TEST OF THE REFERENCE COVERAGE REPORT CLASS ###############################

        rcr = ReferenceCoverageReport()
        # Set the minimum state needed to test _get_refcoverage_data directly
        rcr.use_year = True
        rcr.journals = ['ApJ', 'MNRAS']
        rcr.statsdata = {
            'ApJ': {
                'pubdata': {2012: 189, 2015: 186, 2020: 196},
                'general': {}, 'arxiv': {}, 'publisher': {}, 'crossref': {}
            },
            'MNRAS': {
                'pubdata': {2020: 196},
                'general': {}, 'arxiv': {}, 'publisher': {}, 'crossref': {}
            }
        }
        rcr.config['ADS_REFERENCE_DATA'] = '{0}/xreport/tests/data'.format(self.proj_home)
        rcr.config['ADS_REFCOVERAGE_STATS_YEAR'] = 'refcoverage_aggr_by_year.tsv'
        rcr._get_refcoverage_data()
        # Fraction for ApJ 2020: 190 / 196
        expected_frac_apj_2020 = round(190 / 196, 4)
        self.assertAlmostEqual(rcr.statsdata['ApJ']['general']['2020'], expected_frac_apj_2020, places=3)
        # Fraction for ApJ 2012: 170 / 189
        expected_frac_apj_2012 = round(170 / 189, 4)
        self.assertAlmostEqual(rcr.statsdata['ApJ']['general']['2012'], expected_frac_apj_2012, places=3)
        # Fraction for MNRAS 2020: 185 / 196
        expected_frac_mnras_2020 = round(185 / 196, 4)
        self.assertAlmostEqual(rcr.statsdata['MNRAS']['general']['2020'], expected_frac_mnras_2020, places=3)

        ######## TEST OF THE SUMMARY REPORT CLASS ###############################
        sr = SummaryReport()
        # Limit the context for the summary report
        sr.config['CONTENT_QUERIES'] = {'AST':'{0} entdate:[NOW-365DAYS TO *]'}
        sr.config['COLLECTIONS'] = ['AST']
        sr.config['JOURNALS']['AST'] = ['ApJ..']
        sr.config['CLASSIC_USAGE_INDEX'] = {
            'reads':'{0}/xreport/tests/data/reads.links'.format(self.proj_home),
            'downloads':'{0}/xreport/tests/data/downloads.links'.format(self.proj_home)
        }
        # Generate the summary report
        sr.make_report("AST", "NASA")
        expected_summary = {'AST': {'nrecs': 18584, 'ftrecs': 18584, 'refrecs': 18584, 'oarecs': 18584, 
                                    'dlrecs': 18584, 'citnum': 14338828, 'recent_citnum': 0, 'reads': 681, 
                                    'recent_reads': 271, 'downloads': 322, 'recent_downloads': 149}, 
                            'AST recent sample': {'nrecs': 18584, 'ftrecs': 18584, 'refrecs': 18584, 'oarecs': 18584, 
                                    'dlrecs': 18584, 'citnum': 14338828, 'recent_citnum': 0, 'reads': 'NA', 
                                    'recent_reads': 'NA', 'downloads': 'NA', 'recent_downloads': 'NA'}}
        self.assertDictEqual(sr.summarydata, expected_summary)
