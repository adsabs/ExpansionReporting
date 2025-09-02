from __future__ import absolute_import, unicode_literals
import os
import sys
from builtins import str
import xreport.app as app_module
from xreport.reports import FullTextReport
from xreport.reports import ReferenceMatchingReport
from xreport.reports import MetaDataReport
from xreport.reports import SummaryReport
# ============================= INITIALIZATION ==================================== #

from adsputils import setup_logging, load_config

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
config = load_config(proj_home=proj_home)
app = app_module.xreport('ads-expansion-reporting', proj_home=proj_home, local_config=globals().get('local_config', {}))
logger = app.logger
# ============================= FUNCTIONS ========================================= #
def create_report(**args):
    # What is the report format
    report_format = args['format']
    # For which collection are we generating the report
    collection = args['collection']
    # What report needs to be created
    subject = args['subject']
    # Do we report by volume or year?
    use_year = args['use_year']
    #
    if subject in ['FULLTEXT', 'ALL']:
        # Initialize the class for full text reporting
        ftreport = FullTextReport()
        # Set the reporting type
        ftreport.use_year = use_year
        # The first step consists of retrieving and preparing the data to generate the report
        ftreport.make_report(collection, report_format)
        try:
            ftreport.make_report(collection, report_format)
        except Exception as err:
            msg = "Error making full text report for collection '{0}' in format '{1}': {2}".format(collection, report_format, err)
            logger.error(msg)
        # Write the report to file
        try:
            if report_format == 'MISSING':
                ftreport.save_missing(collection, report_format, subject)
            else:
                ftreport.save_report(collection, report_format, subject)
        except Exception as err:
            msg = "Error saving full text report for collection '{0}' in format '{1}': {2}".format(collection, report_format, err)
            logger.error(msg)
    if subject in ['REFERENCES', 'ALL']:
        # Initialize the class for reference matching reporting
        rmreport = ReferenceMatchingReport()
        # Set the reporting type
        rmreport.use_year = use_year
        try:
            rmreport.make_report(collection, 'general')
        except Exception as err:
            msg = "Error making reference matching report for collection '{0}' in format '{1}': {2}".format(collection, report_format, err)
            logger.error(msg)
        # Write the report to file
        try:
            rmreport.save_report(collection, 'general', subject)
        except Exception as err:
            msg = "Error saving reference matching report for collection '{0}' in format '{1}': {2}".format(collection, report_format, err)
            logger.error(msg)
    if subject in ['METADATA', 'ALL']:
        # Initialize the class for metadata reporting
        mreport = MetaDataReport()
        # Set the reporing type
        mreport.use_year = use_year
        try:
            mreport.make_report(collection, report_format)
        except Exception as err:
            msg = "Error making metadata report for collection '{0}' in format '{1}': {2}".format(collection, report_format, err)
            logger.error(msg)
        # Write the report to file
        try:
            mreport.save_report(collection, report_format, subject)
        except Exception as err:
            msg = "Error saving metadata report for collection '{0}' in format '{1}': {2}".format(collection, report_format, err)
            logger.error(msg)
    if subject == 'SUMMARY':
        # Create a summarizing report
        summary = SummaryReport()
        # Set the reporting type
        summary.use_year = use_year
        try:
            summary.make_report(collection, report_format)
        except Exception as err:
            msg = "Error making summary report for collection '{0}' in format '{1}': {2}".format(collection, report_format, err)
            logger.error(msg)
        try:
            summary.save_report(collection, report_format, subject)
        except Exception as err:
            msg = "Error saving summary report for collection '{0}' in format '{1}': {2}".format(collection, report_format, err)
            logger.error(msg)
        
        
