from __future__ import print_function
import argparse
import datetime
import sys
import os

from xreport import tasks

# ============================= INITIALIZATION ==================================== #

from adsputils import setup_logging, load_config
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)
logger = setup_logging('run.py', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))
                        

# =============================== FUNCTIONS ======================================= #


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--collection', default='CORE', dest='collection',
                        help='Collection to create report for (accepted values: AST or PS+HP)')
    parser.add_argument('-f', '--format', default='NASA', dest='format',
                        help='Format of report')
    parser.add_argument('-s', '--subject', default='ALL', dest='subject',
                        help='Subject of the report')
    parser.add_argument('-a', '--all', action='store_true',
                        help='Run all reports on a specific subject, if supplied')
    parser.add_argument('-t', '--topics', action='store_true',
                        help='Run all reports on all special topics')
    parser.add_argument('-l', '--list', action='store_true',
                        help='List of all collections')
    args = parser.parse_args()

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
    if args.format not in config.get('FORMATS'):
        sys.exit('Please specify one of the following values for the format parameter: {}'.format(config.get('FORMATS')))
    if args.subject not in config.get('SUBJECTS'):
        sys.exit('Please specify one of the following values for the subject parameter: {}'.format(config.get('SUBJECTS')))
    if args.all:
        for coll in config.get('COLLECTIONS'):
            for subject in ['FULLTEXT', 'REFERENCES','METADATA']:
                try:
                    report = tasks.create_report(collection=coll, format=args.format, subject=subject)
                except:
                    logger.error('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(subject, args.format, coll, error))
                    sys.exit('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(subject, args.format, coll, error))
    elif args.topics:
        for coll, name in config.get('TOPIC_SETS').items():
             for subject in ['FULLTEXT', 'REFERENCES','METADATA']:
                 try:
                     report = tasks.create_report(collection=coll, format="NASA", subject=subject)
                 except:
                     logger.error('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(subject, "NASA", coll, error))
                     sys.exit('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(subject, "NASA", coll, error))
                 try:
                     report = tasks.create_report(collection=coll, format="CURATORS", subject=subject)
                 except:
                     logger.error('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(subject, "CURATORS", coll, error))
                     sys.exit('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(subject, "CURATORS", coll, error))
    else:
        try:
            report = tasks.create_report(collection=args.collection, format=args.format, subject=args.subject)
        except Exception as error:
            logger.error('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(args.subject, args.format, args.collection, error))
            sys.exit('Creating "{0}" report for "{1}" on collection "{2}" failed: {3}'.format(args.subject, args.format, args.collection, error))
