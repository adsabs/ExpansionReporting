from topic_sets import TOPIC_SETS
from topic_sets import ASJC2WOS
# ============================= LOGGING ======================================== #
LOGGING_LEVEL = 'INFO'
LOG_STDOUT = False
# ============================= ADS ============================================ #
ADS_API_TOKEN = "<secret>"
ADS_API_URL = "https://api.adsabs.harvard.edu/v1"
CLASSIC_FULLTEXT_INDEX = "/tmp/all.links"
CLASSIC_USAGE_INDEX = {
    'reads':'/tmp/reads.links',
    'downloads':'/tmp/downloads.links'
}
ADS_REFERENCE_DATA = "/references/stats"
ADS_REFERENCE_STATS_YEAR = "refcounts_aggr_by_year.tsv"
ADS_REFERENCE_STATS_VOLUME = "refcounts_aggr_by_volume.tsv"
ADS_PUBLISHER_DATA = "/config/publisher_bibstem.dat"
# The root of the output location
OUTPUT_DIRECTORY = '/tmp/reports'
# ============================= APPLICATION ==================================== #
# 
# Core collections we are reporting on
CORE_COLLECTIONS = {
    'AST':'Astronomy and Astrophysics',
    'HP':'Heliophysics (Solar Physics, Space Weather and Aeronomy)',
    'PS':'Planetary Science',
    'ES':'Earth Science',
    'OA':'Open Access Journals'
}
# List containing all collection labels
COLLECTIONS = list(TOPIC_SETS.keys()) + list(CORE_COLLECTIONS.keys())
# Report formats supported
FORMATS = ['general', 'curators', 'missing']
# Report types supported
SUBJECTS = ['FULLTEXT', 'REFERENCES', 'SUMMARY', 'METADATA', 'ALL']
TOPIC_SUBJECTS = ['FULLTEXT', 'REFERENCES', 'METADATA']
# Which journals are we reporting on per collection
JOURNALS = {
    'CORE_AST':['A&A','A&ARv','A&AS','AJ','AN','APh','ApJ','ApJL','ApJS','ARA&A','NatAs','PASP'],
    'AST': ['A&A','A&ARv','A&AS','AJ','AN','APh','ApJ','ApJL','ApJS','ARA&A','ARep','AsBio','AstL','CeMDA','FrASS','Galax','GeCoA','IAUS','IJAsB','JCAP','MNRAS','NatAs','PASA','PASJ','PASP','RAA','RNAAS','SCPMA','Univ','ASPC'],
    'PS': ["AREPS","ASTRA","AdSpR","AnGeo","Ap&SS","AsBio","CeMDA","E&PSL","EM&P","GeCoA","IJAsB","Icar","JAtS",
           "JGRA","JGRD","JGRE","M&PS","M&PSA","Metic","NatGe","P&SS","PEPI","RvGeo","SSRv","SoSyR","SoPh",
           "SpWea","PSJ","Moon","SpPol"],
    'HP': ['SoPh','SpWea'],
    'HP_AST': ["ApJ","ApJL","ApJS","AJ","MNRAS","A&A","A&AS"],
    'PS_AST': ["ApJ","ApJL","ApJS","AJ","MNRAS","A&A","A&AS"],
    'ES': ['AREPS', 'AmJS.', 'AmMin', 'ApGC.', 'AsBio', 'BAAPG', 'BCaPG', 'BVol.', 'Borea', 'BuSSA', 'CCM..', 'CG...', 'CaJES', 'CaMin', 'ChGeo', 'ClMin', 'CliPa', 'CoMP.', 'E&PSL', 'EEGeo', 'EJMin', 'EOSTr', 'ESPL.', 'ESRv.', 'Earth', 'EcGeo', 'Eleme', 'EnG', 'EnGeo', 'EngGe', 'ExMG', 'GBioC', 'GEEA', 'GGG', 'GGR', 'GPC..', 'GSAB.', 'GSASP', 'GSLSP', 'Gbio', 'GeCoA', 'Geo..', 'GeoJI', 'GeoM.', 'GeoRL', 'GeoRu', 'Geode', 'Geomo', 'Geop.', 'Geosp', 'GrWat', 'HESS', 'HyPr.', 'IGRv.', 'IJCG', 'IJEaS', 'IJRMM', 'Icar.', 'JCExp', 'JCHyd', 'JEEG', 'JForR', 'JG...', 'JGR..', 'JGRA', 'JGRB', 'JGRC', 'JGRD', 'JGRE', 'JGRF', 'JGRG', 'JGeEd', 'JHyd.', 'JMetG', 'JPal.', 'JPet.', 'JPetG', 'JQS..', 'JSG..', 'JSedR', 'JVGR.', 'JVPal', 'LeaEd', 'Litho', 'Lsphe', 'M&PS.', 'MGeol', 'MatGs', 'Micpl', 'MinDe', 'MinM.', 'NatGe', 'Natur', 'OrGeo', 'PApGe', 'PCM..', 'PEPI.', 'PGeo', 'PPP', 'PalAB', 'PalOc', 'Palai', 'Palgy', 'Paly', 'Pbio', 'PreR.', 'QJEGH', 'QuRes', 'RvGeo', 'RvMG.', 'SSASJ', 'Sci..', 'SedG.', 'Sedim', 'Tecto', 'Tectp', 'WRR..'],
    'OA':['PSJ','RSOS','JApA','NatAs','Univ','Galax','AdAst','OJAp','OAJ']
}
# Add the special collections to the core journal sets
JOURNALS.update(TOPIC_SETS)
# Add Astrophysics, Heliophysics and Planetary Science to topic sets
TOPIC_SETS['Astrophysics'] = JOURNALS['AST']
TOPIC_SETS['Heliophysics'] = JOURNALS['HP']
TOPIC_SETS['Planetary Science'] = JOURNALS['PS']
# For some collection we define filters (to e.g. get the right content from multidisciplinary journals)
COLLECTION_FILTERS = {
    'HP_AST':'keyword:"sun*"',
    'PS_AST':'keyword:("planets*" OR meteor OR asteroid OR "celestial*" OR "solar system") -exoplanet -interstellar'
}
# Filtering to help us get recent content (i.e. entered in the past year)
CONTENT_QUERIES = {
    'HP':'{0} entdate:[NOW-365DAYS TO *]',
    'HP_AST':'{0} entdate:[NOW-365DAYS TO *]',
    'PS':'{0} entdate:[NOW-365DAYS TO *]',
    'PS_AST':'{0} entdate:[NOW-365DAYS TO *]',
    'AST':'{0} entdate:[NOW-365DAYS TO *]'
}
# For specific types of metadata, specify the sources we are considering
SOURCES = {
    'FULLTEXT': ['publisher','arxiv'],
    'REFERENCES': ['publisher', 'crossref']
}
# Column definitions for Summary Report
SUMMARY_COLUMNS = {
    "nrecs":"The current number of records in the collection being reported",
    "ftrecs":"How many of these records have full text associated/indexed with them?",
    "refrecs":"How many of these records are refereed?",
    "oarecs":"How many of these records are Open Access?",
    "dlrecs":"How many of these records have at least one data link?",
    "citnum":"The current total number of citations for these records",
    "recent_citnum":"How many citations have been added during the current year?",
    "reads":"The amount of ADS reads for these records",
    "recent_reads":"How many reads have been added during the current year?",
    "downloads":"The amount of ADS downloads for these records",
    "recent_downloads":"How many downloads have been added during the current year?"
}
# Row definitions for Summary Report
SUMMARY_ROWS = {
    "AST":"Core astronomy collection",
    "HP":"Core heliophysics collection",
    "PS":"Core Planetary Science collection",
    "HP_AST":"Heliophysics content in main astronomy journals (filter: keyword:'sun*')",
    "PS_AST":"Planetary Science content in main astronomy journals (filter: keyword:('planets*' OR meteor OR asteroid OR 'celestial*' OR 'solar system') -exoplanet -interstellar)",
    "AST recent sample":"Core Astronomy collection including references and citations, filtered on entry date (entdate:[NOW-365DAYS TO *])",
    "HP recent sample":"Core Heliophysics collection including references and citations, filtered on entry date (entdate:[NOW-365DAYS TO *])",
    "HP_AST recent sample":"Heliophysics in Main Astronomy collection including references and citations, filtered on entry date (entdate:[NOW-365DAYS TO *])",
    "PS recent sample":"Core Planetary Science collection including references and citations, filtered on entry date (entdate:[NOW-365DAYS TO *])",
    "PS_AST recent sample":"Planetary Science in Main Astronomy collection including references and citations, filtered on entry date (entdate:[NOW-365DAYS TO *])",
}
# For these collections we need to skip the calculation of usage
# (because it would involve retrieving all bibcodes)
SKIP_USAGE = ['HP_AST', 'PS_AST']
# For reports by publication year, this is the default start year
DEFAULT_START_YEAR = 1997
# For these publications (bibstem) the volume is treated as volume. This dictionary lists the start year
YEAR_IS_VOL = {
    'JCAP':2003
}
# Specification of volume ranges where coverage should not be calculated
# Example: for some volumes there will be no full text (the publisher does not have it
#          and the ADS will not digitize)
NO_FULLTEXT = {
    'AnGeo': '1-13',
    'SoSyR': '1-36',
    'SoPh': '72,101',
}

