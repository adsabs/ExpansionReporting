import pandas as pd
import os
import sys
import glob
from xreport.utils import _get_facet_data
from xreport.utils import _get_citations
from xreport.utils import _get_usage
from xreport.utils import _get_records
from xreport.utils import _get_journal_coverage
from xreport.utils import _string2list
from datetime import datetime
from datetime import date
from operator import itemgetter

class Report(object):
    """

    """
    def __init__(self, config={}):
        """
        Initializes the class
        """
        # ============================= INITIALIZATION ==================================== #
        from adsputils import setup_logging, load_config
        proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
        self.config = load_config(proj_home=proj_home)
        if config:
            self.config = {**self.config, **config}
        self.logger = setup_logging(__name__, proj_home=proj_home,
                                level=self.config.get('LOGGING_LEVEL', 'INFO'),
                                attach_stdout=self.config.get('LOG_STDOUT', False))
        # The names of output files will have a date string in them
        self.dstring = datetime.today().strftime('%Y%m%d')
        now = datetime.now()
        self.current_year = now.year
    # ============================= MAIN FUNCTIONALITY ================================ #
    def make_report(self, collection, report_type):
        """
        Given a list journal and a report type, generate the report data
    
        param: collection: collection of publications to create report for
        param: report_type: specification of report type
        """
        # Which journals (i.e. bibstems) make up the collection under consideration
        try:
            self.journals = self.config['JOURNALS'][collection]
        except Exception as err:
            msg = "Unable to find journals for collection: {} (Exception: {})".format(collection, err)
            self.logger.error(msg)
            raise
        # Get a map from bibstem to publisher
        self._get_publishers()
        # Initialize statistics and publisher data structure
        self.statsdata = {}
        self.publisher = {}
        for journal in self.journals:
            self.statsdata[journal] = {
                'pubdata':{},
                'startyear':0,
                'lastyear':0,
                'startvol':0,
                'lastvol':0,
                'general':{},
                'arxiv':{},
                'publisher':{},
                'crossref':{}
            }
            self.publisher[journal] = self.stem2publisher.get(journal,'NA')
        # Initialize summary data structure
        self.summarydata = {}
        for collection in self.config['COLLECTIONS']:
            self.summarydata[collection] = {
                'nrecs':0, # number of records
                'ftrecs':0, # number of records with full text
                'refrecs':0, # number of refereed records
                'oarecs':0, # number Open Access records
                'dlrecs':0, # number of records with data link(s)
                'citnum':0, # total number of citations
                'recent_citnum':0, # total number of recent citations
                'reads':'NA', # total number of reads
                'recent_reads':'NA', # total number of recent reads
                'downloads':'NA', # total number of downloads
                'recent_downloads':'NA', # total number of recent downloads
            }
        for collection in self.config['CONTENT_QUERIES'].keys():
            self.summarydata["{0} recent sample".format(collection)] = {
                'nrecs':0, # number of records
                'ftrecs':0, # number of records with full text
                'refrecs':0, # number of refereed records
                'oarecs':0, # number Open Access records
                'dlrecs':0, # number of records with data link(s)
                'citnum':0, # total number of citations
                'recent_citnum':0, # total number of recent citations
                'reads':'NA', # total number of reads
                'recent_reads':'NA', # total number of recent reads
                'downloads':'NA', # total number of downloads
                'recent_downloads':'NA', # total number of recent downloads
            }
        # Initialize details data structure (reporting of missing publications)
        self.missing = {}
        for journal in self.journals:
            self.missing[journal] = []
        # Update statistics data structure with general publication information
        self._get_publication_data()
        # Record all journals/volumes for which full text, references or metadata coverage
        # needs to be skipped
        self._get_skip_volumes()

    def save_report(self, collection, report_type, subject):
        """
        Save the data created in the make_report method in Excel format
        
        param: collection: collection of publications to create report for
        param: report_type: specification of report type
        param: subject: specification of type data to create report for
        """
        # Where will the report(s) be written to
        outdir = "{0}/{1}".format(self.config['OUTPUT_DIRECTORY'], report_type)
        # Make sure the directory exists
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        # See if we can get a WoS subject for collection (if the collection is a "topic")
        # We will keep the collection name if no such mapping is found
        ## fname = config.get('ASJC2WOS').get(collection, collection)
        fname = collection
        # Transform the data generated in the make_report method:
        # generate a data structure so that we can create a Pandas frame
        header = []
        # Add header rows
        header.append(['jrnl ->'] + [j for j in self.journals])
        header.append(['publisher ->'] + [self.publisher[j] for j in self.journals])
        header.append(['start year ->'] + [str(self.statsdata[j]['startyear']) for j in self.journals])
        header.append(['last year ->'] + [str(self.statsdata[j]['lastyear']) for j in self.journals])
        header.append(['start vol ->'] + [str(self.statsdata[j]['startvol']) for j in self.journals])
        header.append(['last vol ->'] + [str(self.statsdata[j]['lastvol']) for j in self.journals])
        #
        try:
            maxvol = max([int(e['lastvol']) for e in self.statsdata.values()])
        except:
            maxvol = 1
        if report_type in ['NASA','general']:
            outputdata = []
            outputdata += header
            # Generate the name of the output file, including full path
            if self.use_year:
                output_file = "{0}/{1}_{2}_{3}.year.xlsx".format(outdir, subject.lower(), fname.replace(' ','_'), self.dstring)
            else:
                output_file = "{0}/{1}_{2}_{3}.volume.xlsx".format(outdir, subject.lower(), fname.replace(' ','_'), self.dstring)
            # Statistics are reported per volume or year for each journal in the collection
            if self.use_year:
                data_keys = range(self.use_year, self.current_year+1)
            else:
                data_keys = range(1, maxvol+1)
            for data_key in data_keys:
                row = [str(data_key)]
                for jrnl in self.journals:
                    if str(data_key) in self.statsdata[jrnl]['general']:
                        perc = round(100*self.statsdata[jrnl]['general'][str(data_key)],1)
                        row.append(perc)
                    else:
                        row.append("")
                outputdata.append(row)
            output_frame = pd.DataFrame(outputdata)
            # Results are written to an Excel file with conditional formatting and first row and column frozen
            output_frame.style.applymap(self._highlight_cells).to_excel(output_file, engine='openpyxl', index=False, header=False, freeze_panes=(1,1))
        else:
            # For internal reporting we generate two reports, corresponding with 
            # the sources associated with the type data in the report
            # For each of these sources the processing is the same as above
            for source in self.config['SOURCES'][subject]:
                outputdata = []
                outputdata += header
                if self.use_year:
                    output_file = "{0}/{1}_{2}_{3}_{4}.year.xlsx".format(outdir, subject.lower(), source, fname.replace(' ','_'), self.dstring)
                else:
                    output_file = "{0}/{1}_{2}_{3}_{4}.volume.xlsx".format(outdir, subject.lower(), source, fname.replace(' ','_'), self.dstring)
                if self.use_year:
                    data_keys = range(self.use_year, self.current_year+1)
                else:
                    data_keys = range(1, maxvol+1)
                for data_key in data_keys:
                    row = [str(data_key)] + [self.statsdata[j][source].get(str(data_key),"") for j in self.journals]
                    outputdata.append(row)
                if outputdata:
                    output_frame = pd.DataFrame(outputdata)
                    output_frame.style.applymap(self._highlight_cells).to_excel(output_file, engine='openpyxl', index=False, header=False, freeze_panes=(1,1))
    #
    def save_missing(self, collection, report_type, subject):
        """
        Save publication data for publications that are missing for a specific collection and subject

        param: collection: collection of publications to create report for
        param: report_type: specification of report type
        param: subject: specification of type data to create report for
        """
        # Where will the report(s) be written to
        outdir = "{0}/{1}/{2}/{3}".format(self.config['OUTPUT_DIRECTORY'], report_type, subject, collection)
        # Make sure the directory exists
        if not os.path.exists(outdir):
            os.makedirs(outdir, exist_ok=True)
        # Transform the data generated in the make_report method:
        # generate a data structure so that we can create a Pandas frame
        header = []
        # Add header rows
        header.append(['bibcode','DOI','volume','issue','first author','title'])
        for journal in self.journals:
            if len((self.missing[journal])) == 0:
                continue
            # Generate the name of the output file, including full path
            output_file = "{0}/{1}_{2}_{3}.xlsx".format(outdir, subject.lower(), journal.replace('.','').strip(), self.dstring)
            outputdata = []
            outputdata += header
            try:
                entries = sorted(self.missing[journal], key=lambda x: int(itemgetter('volume')(x)))
            except:
                entries = self.missing[journal]
            for entry in entries:
                row = []
                row.append(entry.get('bibcode','NA'))
                row.append(entry.get('doi',['NA'])[0])
                row.append(entry.get('volume','NA'))
                row.append(entry.get('issue','NA'))
                row.append(entry.get('first_author_norm','NA'))
                row.append(entry.get('title',['NA'])[0])
                outputdata.append(row)
            if outputdata:
                output_frame = pd.DataFrame(outputdata)
                output_frame.to_excel(output_file, engine='openpyxl', index=False, header=False)

    def _get_publishers(self):
        """
        For a set of publishers, get their associated publisher
        """
        self.stem2publisher = {}
        with open(self.config['ADS_PUBLISHER_DATA']) as fh:
            for line in fh:
                try:
                    bibstem, pname = line.strip().split('\t')
                except:
                    continue
                self.stem2publisher[bibstem.replace('.','')] = pname

    def _get_publication_data(self):
        """
        For a set of journals, get some basic publication data
        
        """
        for journal in self.journals:
            # First get the number of records per volume
            query = 'bibstem:"{0}" doctype:(article OR inproceedings)'.format(journal)
            # Get the data using a facet query
            art_dict = _get_facet_data(self.config, query, 'volume')
            # Also, get the number of records per year
            year_dict = _get_facet_data(self.config, query, 'year')
            # Update journal statistics
            # The first and most recent publication years
            try:
                self.statsdata[journal]['lastyear'] = max(year_dict.keys())
                self.statsdata[journal]['startyear'] = min(year_dict.keys())
            except:
                continue
            # The first and most recent volumes
            try:
                self.statsdata[journal]['lastvol'] = max(art_dict.keys())
                self.statsdata[journal]['startvol'] = min(art_dict.keys())
            except:
                continue
            # The number of publications per volume or year, to be used later
            # for normalization
            if self.use_year:
                self.statsdata[journal]['pubdata'] = year_dict
            else:
                self.statsdata[journal]['pubdata'] = art_dict
    #
    def _get_skip_volumes(self):
        """
        For full text, references and metadata, check the config for
        volumes that need to be skipped in coverage reporting
        """
        # Determine all volumes for which we need to skip full text coverage reporting
        self.skip_fulltext = {}
        try:
            no_fulltext = self.config['NO_FULLTEXT']
            for jrnl in no_fulltext.keys():
                self.skip_fulltext[jrnl] = _string2list(no_fulltext.get(jrnl,'0'))
        except:
            pass
        # Determine all volumes for which we need to skip reference match reporting
        self.skip_references = {}
        try:
            no_fulltext = self.config['NO_REFERENCES']
            for jrnl in no_fulltext.keys():
                self.skip_fulltext[jrnl] = _string2list(no_fulltext.get(jrnl,'0'))
        except:
            pass
        # Determine all volumes for which we need to skip metadata coverage reporting
        self.skip_metadata = {}
        try:
            no_fulltext = self.config['NO_METADATA']
            for jrnl in no_fulltext.keys():
                self.skip_fulltext[jrnl] = _string2list(no_fulltext.get(jrnl,'0'))
        except:
            pass
        
    def _highlight_cells(self, val):
        """
        Mapping function for use in Pandas to apply conditional cell coloring
        when writing data to Excel
        """
        try:
            if val >= 90:
                color = '#6aa84f'
            elif val <= 60:
                color = '#f4cccc'
            elif val > 60 and val <=70:
                color = '#ffe599'
            else:
                color = '#cfe2f3'
        except:
            color = '#ffffff'
        return 'background-color: {}'.format(color)

class FullTextReport(Report):
    """
    Main engine for gathering and processing data to create
    the full text coverage report 
    """
    def __init__(self, config={}):
        """
        Initializes the class
        """
        super(FullTextReport, self).__init__(config=config)

    def make_report(self, collection, report_type):
        """
        param: collection: collection of publications to create report for
        param: report_type: specification of report type
        """
        super(FullTextReport, self).make_report(collection, report_type)
        # ============================= AUGMENTATION of parent method ================================ #
        # Different report types result in different reports. Specifically, for full text,
        # for external reporting only the fact that there is full text is reported.
        if report_type == "general":
            self._get_fulltext_data_general()
        elif report_type == "curators":
            # First generate a full tex index
            self._get_fulltext_index()
            self._get_fulltext_data_classic('publisher')
            self._get_fulltext_data_classic('arxiv')
        else:
            self._get_missing_publications()

    def save_report(self, collection, report_type, subject):
        """
        Save the data created in the make_report method in Excel format
    
        param: collection: collection of publications to create report for
        param: report_type: specification of report type
        param: subject: specification of type data to create report for
        """
        super(FullTextReport, self).save_report(collection, report_type, subject)

    def _get_fulltext_index(self):
        """
        Initializes the class and prepares a (temporary) lookup facility for
        curators reporting. This lookup facility will be replaced by an API
        query eventually
        """
        fulltext_links = self.config.get("CLASSIC_FULLTEXT_INDEX")
        # Compile a list of journals to generate the lookup facility for
        include = [element for sublist in self.config.get("JOURNALS").values() for element in sublist]
        # This variable will hold the data to generate the Pandas frame
        data = []
        # Name of the coumn in the Pandas data frame that stores the "key"
        if self.use_year:
            key_column = 'year'
        else:
            key_column = 'volume'
        # Gather all required data. The Pandas data frame will allow the following query:
        # provide all full text sources for a given journal and volume combination, from which will
        # follow how many records have full text from arXiv and how many from the publisher (which
        # are the numbers we are after)
        with open(fulltext_links) as fh:
            for line in fh:
                bibcode, ftfile, source = line.strip().split('\t')
                bibstem = bibcode[4:9]
                if bibstem not in include:
                    continue
                # If we report per journal volume, we do not want tmp bibcodes
                if bibcode[9:13].replace('.','') == 'tmp' and not self.use_year:
                    continue
                # Whether we report by year or volume, we use the same variable
                try:
                    if self.use_year:
                        data_key = int(bibcode[0:4])
                    else:
                        data_key  = int(bibcode[9:13].replace('.',''))
                except:
                    self.logger.info("Processing Classic fulltext index. Cannot get year or volume for: {0}. Skipping...".format(bibcode))
                    continue
                if bibstem in self.config.get("YEAR_IS_VOL") and not self.use_year:
                    data_key = int(bibcode[0:4])
                letter  = bibcode[13]
                if bibstem == 'ApJ..' and letter == 'L':
                    bibstem = 'ApJL'
                data.append([bibstem, data_key, source.lower()])
        # The lookup facility is a Pandas dataframe
        self.ft_index = pd.DataFrame(data, columns=['bibstem',key_column,'source'])
        
    def _get_fulltext_data_general(self):
        """
        For a set of journals, get full text data (the number of records with full text per volume)

        """
        # Determine if certain volumes need to be skipped:
        for journal in self.journals:
            # The ADS query to retrieve all records with full text for a given journal
            # Filters:
            # has:body --> get all records with full text indexed
            # doctype:(article OR inproceedings) --> remove all records indexed as non-articles
            # author_count:[1 TO *] --> not a good idea (because some historical publications don't have an author)
            # entdate:[* TO NOW-40DAYS] --> not a good idea in case records get re-indexed
            query = 'bibstem:"{0}" has:body doctype:(article OR inproceedings)'.format(journal)
            # The query populates a dictionary keyed on volume number, listing the number of records per volume
            if self.use_year:
                full_dict = _get_facet_data(self.config, query, 'year')
            else:
                full_dict = _get_facet_data(self.config, query, 'volume')
            # Coverage data is stored in a dictionary
            cov_dict = {}
            # Collect volumes to be skipped, if any
            try:
                skip = self.skip_fulltext[journal]
            except:
                skip = []
            for data_key in sorted(self.statsdata[journal]['pubdata'].keys()):
                if data_key in skip:
                    continue
                try:
                    frac = float(full_dict[data_key])/float(self.statsdata[journal]['pubdata'][data_key])
                except:
                    frac = 0.0
                if journal in self.config.get("YEAR_IS_VOL") and not self.use_year:
                    data_key = data_key - self.config.get("YEAR_IS_VOL")[journal] + 1
                cov_dict[str(data_key)] = frac
            # Update the global statistics data structure
            self.statsdata[journal]['general'] = cov_dict

    def _get_fulltext_data_classic(self, ft_source):
        """
        For a set of journals, get full text data from Classic
        Note: this method will be replaced by API calls once Solr has been updated
        
        param: source: source of fulltext
        """
        self.journals = ['ApJ','A&A','MNRAS']
        for journal in self.journals:
            # Coverage data is stored in a dictionary
            cov_dict = {}
            # Collect volumes to be skipped, if any
            try:
                skip = self.skip_fulltext[journal]
            except:
                skip = []
            for data_key in sorted(self.statsdata[journal]['pubdata'].keys()):
                if data_key in skip:
                    continue
                # For each volume of the journals in the collection we query the Pandas dataframe to retrieve the sources of full text
                if ft_source == 'arxiv':
                    # How many records are there with full text from arXiv?
                    if self.use_year:
                        data = self.ft_index.query("bibstem=='{0}' and year=={1} and source=='arxiv'".format(journal, data_key))
                    else:
                        data = self.ft_index.query("bibstem=='{0}' and volume=={1} and source=='arxiv'".format(journal, data_key))
                else:
                    if self.use_year:
                        data = self.ft_index.query("bibstem=='{0}' and year=={1} and source!='arxiv'".format(journal, data_key))
                    else:
                        data = self.ft_index.query("bibstem=='{0}' and volume=={1} and source!='arxiv'".format(journal, data_key))
                try:
                    sources = data['source'].tolist()
                except Exception as err:
                    self.logger.error('Source lookup in Classic index blew up for journal {0}, year/volume {1}: {2}'.format(journal, data_key, err))
                    sources = []
                try:
                    frac = float(len(sources))/float(self.statsdata[journal]['pubdata'][data_key])
                except:
                    frac = 0.0
                if journal in self.config.get("YEAR_IS_VOL") and not self.use_year:
                    data_key = data_key - self.config.get("YEAR_IS_VOL")[journal] + 1
                cov_dict[str(data_key)] = round(100*frac,1)
            self.statsdata[journal][ft_source] = cov_dict

    def _get_missing_publications(self):
        """
        For a set of journals, find the publications without fulltext
        """
        for journal in self.journals:
            # The ADS query to retrieve all records without full text for a given journal
            query = 'bibstem:"{0}"  -has:body doctype:(article OR inproceedings)'.format(journal)
            missing_pubs = _get_records(self.config, query, 'bibcode,doi,title,first_author_norm,volume,issue')
            self.missing[journal] = missing_pubs

class ReferenceMatchingReport(Report):
    """
    Main engine for gathering and processing data to create
    the reference matching report. This report depends fully
	on data in the ADS back office, because it currently is not
	available anywhere else. At some point we may have a database
	containing all the raw reference data; then the time has come
	to revisit this reporting module.
    """
    def __init__(self, config={}):
        """
        Initializes the class
        """
        super(ReferenceMatchingReport, self).__init__(config=config)
        #
    def make_report(self, collection, report_type):
        """
        param: collection: collection of publications to create report for
        param: report_type: specification of report type
        """
        super(ReferenceMatchingReport, self).make_report(collection, report_type)
        # ============================= AUGMENTATION of parent method ================================ #
        # Gather reference data necessary for the report
        self._get_reference_data()
        # Generate the matching statistics
        self._get_reference_stats()

    def save_report(self, collection, report_type, subject):
        """
        Save the data created in the make_report method in Excel format
    
        param: collection: collection to create report for
        param: report_type: specification of report type
        param: subject: specification of type data to create report for
        """
        super(ReferenceMatchingReport, self).save_report(collection, report_type, subject)

    def _get_reference_data(self):
        """
        Retrieve the data required to generate the matching statistics
        """
        if self.use_year:
            # We are reporting by year, so retrieve reference data aggregated by year
            reference_data_file = "{0}/{1}".format(self.config["ADS_REFERENCE_DATA"],self.config["ADS_REFERENCE_STATS_YEAR"])
        else:
            # We are reporting by volume, so retrieve reference data aggregated by volume
            reference_data_file = "{0}/{1}".format(self.config["ADS_REFERENCE_DATA"],self.config["ADS_REFERENCE_STATS_VOLUME"])
        # Read reference data into a Pandas data frame
        self.reference_stats = pd.read_csv(reference_data_file, sep='\t')

    def _get_reference_stats(self):
        """
        For a set of journals, get reference matching statistics
        """
        if self.use_year:
            key_column = 'year'
        else:
            key_column = 'volume'
        for journal in self.journals:
            # The data keys are either volumes or years, depending on our choice
            data_keys = sorted(self.statsdata[journal]['pubdata'].keys())
            results = self.reference_stats[self.reference_stats['bibstem'] == journal]
            res_dict = dict(zip(results[key_column], results['fraction']))
            self.statsdata[journal]['general'] = {str(key): value for key, value in res_dict.items()}

class MetaDataReport(Report):
    """
    Create metadata completeness report 
    """
    def __init__(self, config={}):
        """
        Initializes the class
        """
        super(MetaDataReport, self).__init__(config=config)

    def make_report(self, collection, report_type):
        """
        param: collection: collection of publications to create report for
        param: report_type: specification of report type
        """
        super(MetaDataReport, self).make_report(collection, report_type)
        # ============================= AUGMENTATION of parent method ================================ #
        self._get_metadata_data()

    def save_report(self, collection, report_type, subject):
        """
        Save the data created in the make_report method in Excel format
    
        param: collection: collection of publications to create report for
        param: report_type: specification of report type
        param: subject: specification of type data to create report for
        """
        super(MetaDataReport, self).save_report(collection, report_type, subject)

    def _get_metadata_data(self):
        """
        For a set of journals, get coverage data from the Journals Database

        """
        # Determine if certain volumes need to be skipped:
        for journal in self.journals:
            # The query populates a dictionary keyed on volume number, listing the number of records per volume
            letter = False
            if journal == 'ApJL':
                cov_data = _get_journal_coverage(self.config, 'ApJ')
                letter = True
            else:
                cov_data = _get_journal_coverage(self.config, journal)
            cov_dict = {}
            for entry in cov_data:
                if self.use_year:
                    try:
                        year = int(entry['year'])
                    except:
                        self.logger.warning('Found strange year while getting metadata stats for {}: {}'.format(journal,entry['year']))
                        year = entry['year']
                    
                    compl_data = [(v['ADS_records'], v['Crossref_records'], v['volume']) for v in entry['volumes']]
                    
                    if letter:
                        compl_data = [e for e in compl_data if e[2].endswith('L')]
                    else:
                        compl_data = [e for e in compl_data if not e[2].endswith('L')]
                    
                    try:
                        frac = float(sum([e[0] for e in compl_data]))/float(sum([e[1] for e in compl_data]))
                    except:
                        frac = 0.0
                    
                    cov_dict[str(year)] = frac
                else:
                    for v in entry['volumes']:
                        volno = v['volume']
                        if letter and volno.endswith('L'):
                            try:
                                cov_dict[volno.replace('L','')] = float(v['ADS_records'])/float(v['Crossref_records'])
                            except:
                                cov_dict[volno.replace('L','')] = 0.0
                        else:
                            try:
                                cov_dict[volno] = float(v['ADS_records'])/float(v['Crossref_records'])
                            except:
                                cov_dict[volno] = 0.0
            # Coverage data is stored in a dictionary
            self.statsdata[journal]['general'] = cov_dict

class SummaryReport(Report):
    """
    Create summary report for a specific target audience
    """
    def __init__(self, config={}):
        """
        Initializes the class
        """
        super(SummaryReport, self).__init__(config=config)

    def make_report(self, collection, report_type):
        """
        param: collection: collection of publications to create report for
        param: report_type: specification of report type
        param: subject: specification of type data to create report for
        """
        super(SummaryReport, self).make_report(collection, report_type)
        # ============================= AUGMENTATION of parent method ================================ #
        self._get_summary_stats(report_type)

    def save_report(self, collection, report_type, subject):
        """
        Save the data created in the make_report method in Excel format
        
        param: collection: collection of publications to create report for
        param: report_type: specification of report type
        param: subject: specification of type data to create report for
        """
        # Where will the report(s) be written to
        outdir = "{0}/{1}".format(self.config['OUTPUT_DIRECTORY'], report_type)
        # Make sure the directory exists
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        # Transform the data generated in the make_report method:
        # generate a data structure so that we can create a Pandas frame
        header = []
        # Add header rows
        header.append(['collection'] + [m for m in self.summarydata['PS'].keys()])
        #
        if report_type == 'NASA':
            outputdata = []
            outputdata += header
            # Generate the name of the output file, including full path
            output_file = "{0}/{1}_{2}.xlsx".format(outdir, subject.lower(), self.dstring)
            # Statistics are reported per volume for each journal in the collection
            for collection in self.summarydata.keys() :
                row = [collection] + [v for v in self.summarydata[collection].values()]
                outputdata.append(row)
            # Add the footer explaining the meaning of the columns and rows
            outputdata.append([''])
            # Columns
            outputdata.append(['Columns'])
            for colname, colmeaning in self.config['SUMMARY_COLUMNS'].items():
                outputdata.append([colname, colmeaning])
            outputdata.append(['Rowns'])
            # Rows
            for rowname, rowmeaning in self.config['SUMMARY_ROWS'].items():
                outputdata.append([rowname, rowmeaning])
            output_frame = pd.DataFrame(outputdata)
            # Results are written to an Excel file with conditional formatting and first row and column frozen
            output_frame.style.to_excel(output_file, engine='openpyxl', index=False, header=False, freeze_panes=(1,1))

    def _get_summary_stats(self, report_type):
        """
        For a set of journals, get some basic publication data
        
        param: report_type: specification of report type
        """
        today = date.today()
        for collection in self.config['COLLECTIONS']:
            if collection == 'CORE':
                continue
            # Retrieve the journals for the collection being processed
            journals = self.config['JOURNALS'][collection]
            # Construct the query to retrieve all records for these journals
            query = 'bibstem:({0}) doctype:(article OR inproceedings)'.format(" OR ".join(journals))
            # Do we have an special filter for this collection?
            cfilter = self.config['COLLECTION_FILTERS'].get(collection, None)
            if cfilter:
                query += " {0}".format(cfilter)
            # Get the total number of citations (via pivot query)
            citnum = _get_citations(self.config, query)
            self.summarydata[collection]['citnum'] = citnum
            # Get the number of recent citations (i.e. current year) via facet query
            q = 'citations({0}) year:{1}'.format(query, today.year)
            results = _get_facet_data(self.config, q, 'year')
            self.summarydata[collection]['recent_citnum'] = results.get(today.year,0)
            # Get usage numbers (via Classic index files), first reads, then downloads
            if collection not in self.config['SKIP_USAGE']:
                reads, recent_reads = _get_usage(self.config, jrnls=journals)
                self.summarydata[collection]['reads'] = reads
                self.summarydata[collection]['recent_reads'] = recent_reads
                downl, recent_downl = _get_usage(self.config, jrnls=journals, udata='downloads')
                self.summarydata[collection]['downloads'] = downl
                self.summarydata[collection]['recent_downloads'] = recent_downl
            # Get the total number of records via facet query on publication year
            results = _get_facet_data(self.config, query, 'year')
            self.summarydata[collection]['nrecs'] = sum(results.values())
            # How many of these records have full text associated with them
            query = 'bibstem:({0}) fulltext_mtime:["1000-01-01t00:00:00.000Z" TO *] doctype:(article OR inproceedings)'.format(" OR ".join(journals))
            if cfilter:
                query += " {0}".format(cfilter)
            results = _get_facet_data(self.config, query, 'year')
            self.summarydata[collection]['ftrecs'] = sum(results.values())
            # How many of these records are Open Access
            query = 'bibstem:({0}) property:openaccess doctype:(article OR inproceedings)'.format(" OR ".join(journals))
            if cfilter:
                query += " {0}".format(cfilter)
            results = _get_facet_data(self.config, query, 'year')
            self.summarydata[collection]['oarecs'] = sum(results.values())
            # How many of these records have at least one data link
            query = 'bibstem:({0}) property:data doctype:(article OR inproceedings)'.format(" OR ".join(journals))
            if cfilter:
                query += " {0}".format(cfilter)
            results = _get_facet_data(self.config, query, 'year')
            self.summarydata[collection]['dlrecs'] = sum(results.values())
            # How many of these records are refereed
            query = 'bibstem:({0}) property:refereed doctype:(article OR inproceedings)'.format(" OR ".join(journals))
            if cfilter:
                query += " {0}".format(cfilter)
            results = _get_facet_data(self.config, query, 'year')
            self.summarydata[collection]['refrecs'] = sum(results.values())
        for collection in self.config['CONTENT_QUERIES'].keys():
            # Do the same as above for "content queries". These queries are supposed to retrieve
            # sets of recent records representative for each collection, but going beyond just the
            # journals
            journals = self.config['JOURNALS'][collection]
            label = "{0} recent sample".format(collection)
            jq = 'bibstem:({0}) doctype:(article OR inproceedings)'.format(" OR ".join(journals))
            # Do we have an special filter for this collection?
            cfilter = self.config['COLLECTION_FILTERS'].get(collection, None)
            if cfilter:
                jq += " {0}".format(cfilter)
            cq = "({0} OR references({1}) OR citations({2}))".format(jq, jq, jq)
            query = self.config['CONTENT_QUERIES'][collection].format(cq)
            # Get the citation numbers
            citnum = _get_citations(self.config, query)
            self.summarydata[label]['citnum'] = citnum
            q = "citations({0}) year:{1}".format(query, today.year)
            results = _get_facet_data(self.config, q, 'year')
            self.summarydata[label]['recent_citnum'] = results.get(today.year,0)
            # Get usage numbers
            # Currently there is no efficient way to retrieve usage data for large sets
            # of individual records
            self.summarydata[label]['reads'] = "NA"
            self.summarydata[label]['recent_reads'] = "NA"
            self.summarydata[label]['downloads'] = "NA"
            self.summarydata[label]['recent_downloads'] = "NA"
            # Get the total number of records via facet query on publication year
            results = _get_facet_data(self.config, query, 'year')
            self.summarydata[label]['nrecs'] = sum(results.values())
            # How many of these records have full text associated with them
            q = '{0} fulltext_mtime:["1000-01-01t00:00:00.000Z" TO *] doctype:(article OR inproceedings)'.format(query)
            results = _get_facet_data(self.config, q, 'year')
            self.summarydata[label]['ftrecs'] = sum(results.values())
            # How many of these records are Open Access
            q = '{0} property:openaccess doctype:(article OR inproceedings)'.format(query)
            results = _get_facet_data(self.config, q, 'year')
            self.summarydata[label]['oarecs'] = sum(results.values())
            # How many of these records have at least one data link
            q = '{0} property:data doctype:(article OR inproceedings)'.format(query)
            results = _get_facet_data(self.config, q, 'year')
            self.summarydata[label]['dlrecs'] = sum(results.values())
            # How many of these records are refereed
            q = '{0} property:refereed doctype:(article OR inproceedings)'.format(query)
            results = _get_facet_data(self.config, q, 'year')
            self.summarydata[label]['refrecs'] = sum(results.values())
