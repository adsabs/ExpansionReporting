from __future__ import print_function
import argparse
import datetime
import sys
import os

from xreport import tasks

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# ============================= INITIALIZATION ==================================== #

from adsputils import setup_logging, load_config
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)
logger = setup_logging('run.py', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))
                        

# =============================== FUNCTIONS ======================================= #

collmap = {
    'AST':'Astrophysics',
    'PS': 'Planetary Science',
    'HP': 'Heliophysics'
}

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-y', '--year', action='store_true',
                        help='Report by year, rather than volume')
    parser.add_argument('-sy', '--start-year', default=config.get('DEFAULT_START_YEAR', 1997), type=int, dest='start_year',
                        help='Set start year, if reporting by year')
    parser.add_argument('-c', '--collection', default='CORE', dest='collection',
                        help='Collection to create report for (accepted values: AST or PS+HP)')
    parser.add_argument('-f', '--format', default='general', dest='format',
                        help='Format of report')
    parser.add_argument('-s', '--subject', default=None, dest='subject',
                        help='Subject of the report')
    parser.add_argument('-a', '--all', action='store_true',
                        help='Run all reports on a specific subject, if supplied')
    parser.add_argument('-t', '--topics', action='store_true',
                        help='Run all reports on all special topics')
    parser.add_argument('-l', '--list', action='store_true',
                        help='List of all collections')
    args = parser.parse_args()

    # Determine the type of reporting: by volume or by year. If by year, set the start year
    use_year = args.year
    if args.year:
        use_year = args.start_year
    if args.list:
        print('Supported core collections:\nlabel\tname')
        for coll, name in config.get('CORE_COLLECTIONS').items():
            print('{0}\t{1}'.format(coll, name))
        print('\nSupported special collections and disciplines:\nlabel\tname')
        for coll, name in config.get('TOPIC_SETS').items():
            print('{0}\t{1}'.format(coll, name))
        sys.exit()
    if args.collection not in config.get('COLLECTIONS') and not args.all and not args.topics:
        sys.exit('Please specify one of the following values for the collection parameter: {}'.format(config.get('COLLECTIONS')))
    if args.topics and not args.subject:
        sys.exit('To run topic stats, please specify one of the following values for the subject parameter: {}'.format(config.get('TOPIC_SUBJECTS')))
    if args.format not in config.get('FORMATS'):
        sys.exit('Please specify one of the following values for the format parameter: {}'.format(config.get('FORMATS')))
    if not args.subject or args.subject not in config.get('SUBJECTS'):
        sys.exit('Please specify one of the following values for the subject parameter: {}'.format(config.get('SUBJECTS')))
    if args.format.lower() == 'curators' and args.subject.lower() != 'fulltext':
        sys.exit('The "curators" format only supports the "fulltext" report')
    if args.all:
        for coll in config.get('COLLECTIONS'):
            for subject in ['FULLTEXT', 'REFERENCES','METADATA']:
                try:
                    report = tasks.create_report(collection=coll, format=args.format, subject=subject, use_year=use_year)
                except:
                    logger.error('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(subject, args.format, coll, error))
                    sys.exit('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(subject, args.format, coll, error))
    elif args.topics:
        for coll, name in config.get('TOPIC_SETS').items():
            if args.format.lower() != 'curators':
                try:
                    report = tasks.create_report(collection=coll, format="general", subject=args.subject, use_year=use_year)
                except Exception as error:
                    logger.error('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(args.subject, "general", coll, error))
                    sys.exit('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(args.subject, "general", coll, error))
            else:
                 try:
                     report = tasks.create_report(collection=coll, format="CURATORS", subject="FULLTEXT", use_year=use_year)
                 except:
                     logger.error('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(subject, "CURATORS", coll, error))
                     sys.exit('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(subject, "CURATORS", coll, error))
    else:
        try:
            coll = collmap.get(args.collection, args.collection)
            report = tasks.create_report(collection=coll, format=args.format, subject=args.subject, use_year=use_year)
        except Exception as error:
            logger.error('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(args.subject, args.format, args.collection, error))
            sys.exit('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(args.subject, args.format, args.collection, error))
