import os
import sys
import re
import glob
import pandas as pd

debug = 0

src_base_dir = '/proj/ads/references'
base_dir = '/proj/ads/references'

index_files = glob.glob('{0}/links/*.index'.format(base_dir))
skip_existing_index = False

def get_references_from_file(reference_file, bibcode):
    resol_file = reference_file.replace('sources','resolved') + ".result"
    try:
        fdesc = open(resol_file)
    except IOError:
        # Something is wrong with the reference file for this bibcode, warn but no longer fail
        sys.stderr.write('%s missing reference file %s\n' % (bibcode, resol_file))
        return []

    # First find the start of the reference section.
    for line in fdesc:
        if line.startswith('---<'):
            mat = re.match('<(.*?)>', line[3:])
            if mat:
                citing_bib = mat.group(1)
                if bibcode == citing_bib:
                    break
    else:
        # Bibcode not found.
        return 0

    for index, line in enumerate(fdesc):
        if line.startswith('---<'):
            # Start of a new reference block.
            break
    return index+1

if skip_existing_index:
    index_files = [f for f in index_files if not os.path.exists(f.replace('.index','.counts'))]
# Cycle through counts files. If a counts file does not exist, check if an index file exists
# and if that exists, use that and generate the counts file
for index_file in index_files:
    if os.path.basename(index_file) == 'ISI.index':
        continue
    try:
        index_frame = pd.read_csv(index_file, sep='\t', header=None)
    except Exception as e:
        sys.stderr.write('Unable to read {0}. Skipping... ({1})\n'.format(index_file, e))
        continue
    counts_file = index_file.replace('.index','.counts')
    new_counts_file = "{0}.tmp".format(counts_file)
    counts_file_exists = False
    if os.path.exists(counts_file):
        # Counts file exists, so only look for updates
        if debug:
            sys.stderr.write('Existing counts file: {0}\n'.format(counts_file))
        counts_file_exists = True
        counts_frame = pd.read_csv(counts_file, sep='\t', header=None)
        counts_files = counts_frame.iloc[:, 1]
        new = index_frame[~index_frame.iloc[:, 1].isin(counts_frame.iloc[:, 1])]
        if debug:
            sys.stderr.write('    Number of new entries: {0}\n'.format(len(new)))
        removed = counts_frame[~counts_frame.iloc[:, 1].isin(index_frame.iloc[:, 1])]
        remain = counts_frame[counts_frame.iloc[:, 1].isin(index_frame.iloc[:, 1])]
    if counts_file_exists:
        new_entries = []
        for index,row in new.iterrows():
            refsrc = '{0}/sources/{1}'.format(src_base_dir, row[1])
            try:
                nrefs = get_references_from_file(refsrc, row[0])
            except:
                nrefs = 0
            new_entries.append([row[0], row[1], nrefs])
        new_df = pd.DataFrame(new_entries, columns=remain.columns)
        df_combined = pd.concat([remain, new_df], ignore_index=True)
        result_df = df_combined.sort_values(by=df_combined.columns[0])
        result_df.to_csv(new_counts_file, sep='\t', index=False, header=False)
    else:
        # No existing counts file yet. Create fresh one from index file
        new_entries = []
        for index, row in index_frame.iterrows():
            refsrc = '{0}/sources/{1}'.format(src_base_dir, row[1])
            try:
                nrefs = get_references_from_file(refsrc, row[0])
            except:
                nrefs = 0
            new_entries.append([row[0], row[1], nrefs])
        new_df = pd.DataFrame(new_entries)
        result_df = new_df.sort_values(by=new_df.columns[0])
        result_df.to_csv(new_counts_file, sep='\t', index=False, header=False)
    # Finally, rename tmp counts file to actual counts file
    try:
        # Rename the file
        os.rename(new_counts_file, counts_file)
    except FileNotFoundError:
        sys.stderr.write("Error: The file {0} was not found\n".format(new_counts_file))
    except PermissionError:
        sys.stderr.write("Error: Permission denied. Unable to rename the file {0}\n".format(new_counts_file))
    except Exception as e:
        sys.stderr.write("An unexpected error occurred: {0}\n".forat(e))
