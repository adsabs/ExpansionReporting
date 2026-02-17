import os
import sys
import glob
from collections import defaultdict
import pandas as pd

src_base_dir = '/proj/ads/references'
base_dir = '/proj/ads/references'
res_dir = '/proj/ads/abstracts/stats/references'

reference_counts_file = 'references.counts.tsv'
results_file = "{0}/{1}".format(res_dir, reference_counts_file)

counts_files = glob.glob('{0}/links/*.counts'.format(base_dir))

skip_sources = ['AUTHOR','OTHER','ISI','CONF']

cr_refcounts = defaultdict(list)
pb_refcounts = defaultdict(list)

bibcodes = set()

for counts_file in counts_files:
    with open(counts_file) as fh:
        for line in fh:
            bibcode, source, refcount = line.strip().split('\t')
            bibcodes.add(bibcode)
            if source.split('/')[0] in skip_sources:
                continue
            if source.endswith('.xref.xml'):
                cr_refcounts[bibcode].append(int(refcount))
            else:
                pb_refcounts[bibcode].append(int(refcount))

sys.stderr.write('Found {0} bibcodes\n'.format(len(bibcodes)))

reference_counts = []
for bibc in list(bibcodes):
    try:
        cr_count = max(cr_refcounts[bibc])
    except:
        cr_count = 0
    try:
        pb_count = max(pb_refcounts[bibc])
    except:
        pb_count = 0
    ref_count = max(cr_count, pb_count)
    if ref_count > 0:
        reference_counts.append([bibc, ref_count])
# Put the results into a Pandas data frame
counts_frame = pd.DataFrame(reference_counts)
# Sort the frame by bibcode
result_df = counts_frame.sort_values(by=counts_frame.columns[0])
# Save the data frame to a tsv file, without header
result_df.to_csv(results_file, sep='\t', index=False, header=False)
