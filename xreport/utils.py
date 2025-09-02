import re
import os
import sys
import urllib.request, urllib.parse, urllib.error
import requests
import math
from datetime import date
# ============================= INITIALIZATION ==================================== #

from adsputils import setup_logging, load_config

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
config = load_config(proj_home=proj_home)
logger = setup_logging(__name__, proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))
# =============================== HELPER FUNCTIONS ================================ #
def _group(lst, n):
    """
    Transform a list of values into a list of tuples of length n
    
    param: lst: the input list
    param: n: tuple length
    """
    for i in range(0, len(lst), n):
        val = lst[i:i+n]
        if len(val) == n:
            yield tuple(val)

def _string2list(numstr):
    """
    Convert a string of numbers into a list/range
    Example: '1,2,5-7,10' --> [1, 2, 5, 6, 7, 10]
    
    param: str: the input string
    """
    result = []
    for part in numstr.split(','):
        if '-' in part:
            a, b = part.split('-')
            a, b = int(a), int(b)
            result.extend(range(a, b + 1))
        else:
            a = int(part)
            result.append(a)
    return result

def _make_dict(tup, key_is_int=True):
    """
    Turn list of tuples into a dictionary
    
    param: tup: list of tuples
    param: key_is_int: keys of dictionary will be integers if true
    """
    newtup = tup
    if key_is_int:
        newtup = [(int(re.sub("[^0-9]", "", e[0])), e[1]) for e in tup]        
    return dict(newtup)

def _do_query(conf, params, endpoint='search/query'):
    """
    Send of a query to the ADS API (essentially, any API defined by config values)
    
    param: conf: dictionary with configuration values
    param: params: idctionary with query parameters
    """
    headers = {}
    headers["Authorization"] = "Bearer {}".format(conf['ADS_API_TOKEN'])
    headers["Accept"] = "application/json"
    if isinstance(params, str):
        url = "{}/{}/{}".format(conf['ADS_API_URL'], endpoint, params)
    else:
        url = "{}/{}?{}".format(conf['ADS_API_URL'], endpoint, urllib.parse.urlencode(params))
    r_json = {}
    try:
        r = requests.get(url, headers=headers)
    except Exception as err:
        logger.error("Search API request failed: {}: {}".format(err, url))
        raise
    if not r.ok:
        msg = "Search API request with error code '{}': {}".format(r.status_code, url)
        logger.error(msg)
        raise Exception(msg)
    else:
        try:
            r_json = r.json()
        except ValueError:
            msg = "No JSON object could be decoded from Search API"
            logger.error(msg)
            raise Exception(msg)
        else:
            return r_json
    return r_json

# =============================== DATA RETRIEVAL FUNCTIONS ==================== #

def _get_citations(conf, query_string):
    """
    Get citation counts via ADS API pivot query
    
    param: conf: dictionary with configuration values
    param: query_string: the query string to execute pivot query on
    """
    # creating the date object of today's date
    todays_date = date.today()
    params = {
            'facet': 'true',
            'facet.limit': 2000,
            'facet.minCount': '1',
            'facet.pivot': 'year,citation_count',
            'q': query_string,
            'sort': 'citation_count desc'
        }
    query_data = _do_query(conf, params)
    pivots_data = query_data['facet_counts']['facet_pivot'].get('year,citation_count')
    data =[item for sublist in [p['pivot'] for p in pivots_data] for item in sublist]
    # The total number of citations is the sum of each citation value times its multiplicity
    total_cites = sum([e['count']*e['value'] for e in data])
    
    return total_cites

def _get_facet_data(conf, query_string, facet):
    """
    Do an ADS API facet query
    
    param: conf: dictionary with configuration values
    param: query_string: the query string to execute pivot query on
    param: facet: the facet to return
    """
    params = {
        'q':query_string,
        'fl': 'id',
        'rows': 1,
        'facet':'on',
        'facet.field': facet,
        'facet.limit': 1000,
        'facet.mincount': 1,
        'facet.offset':0,
        'sort':'date desc'
    }

    data = _do_query(conf, params)
    results = data['facet_counts']['facet_fields'].get(facet)
    # Return a dictionary with facet values and associated frequencies
    res_dict = _make_dict(list(_group(results, 2)))
    if facet == 'volume':
        try:
            filt_dict = {key:value for (key, value) in res_dict.items() if key < 2100}
        except:
            filt_dict = res_dict
        return filt_dict
    else:
        return res_dict

def _get_records(conf, query_string, return_fields):
    """
    Do a general ADS API query
    
    param: conf: dictionary with configuration values
    param: query_string: the query string to execute pivot query on
    param: return_fields: which Solr fields to return
    """
    start = 0
    rows = 1000
    results = []
    params = {
        'q':query_string,
        'fl': return_fields,
        'rows': rows,
        'start': start
    }
    data = _do_query(conf, params)
    try:
        results = data['response']['docs']
    except:
        raise Exception('Solr returned unexpected data!')
    num_documents = int(data['response']['numFound'])
    num_paginates = int(math.ceil((num_documents) / (1.0*rows))) - 1
    start += rows
    for i in range(num_paginates):
        params['start'] = start
        data = _do_query(conf, params)
        try:
            results += data['response']['docs']
        except:
            raise Exception('Solr returned unexpected data!')
        start += rows
    return results

def _get_usage(config, jrnls=[], bibcodes=[], udata='reads'):
    """
    Return usage data from Classic index files for a set of journals of bibcodes
    
    param: conf: dictionary with configuration values
    param: journals: a list of bibstems, if specified
    param: bibcodes: a list of bibcodes, if specified
    param: udata: what type of usage data to return
    """
    total = 0
    recent = 0
    index_file = config.get('CLASSIC_USAGE_INDEX')[udata]
    # Cycle through index file and get usage data for either specific journals or bibcodes
    with open(index_file) as fh:
        for line in fh:
           data = line.strip().split('\t')
           if jrnls and data[0][4:9] not in jrnls:
               continue
           elif bibcodes and data[0] not in bibcodes:
               continue
           total += sum([int(d) for d in data[1:]])
           recent += int(data[-1])
    return total, recent

def _get_journal_coverage(conf, jrnl):
    """
    Get metadata completeness statistics from Journals Database for a given journal
    
    param: conf: dictionary with configuration values
    param: jrnl: a journal abbreviation (bibstem)
    """
    try:
        data = _do_query(conf, jrnl.replace('.',''), endpoint='journals/summary')
        completeness_data = data['summary']['master']['completeness_details']['completeness_by_volume']
    except:
        logger.error('No completeness data found for: {0}'.format(jrnl))
        completeness_data = {}

    return completeness_data
    
