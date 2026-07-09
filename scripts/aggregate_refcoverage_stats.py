import os
import sys
import pandas as pd

ref_base = '/proj/ads/abstracts/stats/references'
meta_base = '/proj/ads/abstracts/stats/metadata'

# Input: records with any references (bibcode, max_ref_count from publisher or Crossref)
references_counts_file = '{0}/references.counts.tsv'.format(ref_base)
# Input: total record counts per bibstem/year and bibstem/volume
records_year_file = '{0}/records_agg_year.tsv'.format(meta_base)
records_volume_file = '{0}/records_agg_volume.tsv'.format(meta_base)
# Output files
agg_by_year_res = '{0}/refcoverage_aggr_by_year.tsv'.format(ref_base)
agg_by_volume_res = '{0}/refcoverage_aggr_by_volume.tsv'.format(ref_base)

# Read bibcodes with references (one row per bibcode with refs)
df = pd.read_csv(references_counts_file, sep='\t', header=None, names=['bibcode', 'ref_count'])
# Extract year, bibstem, volume, letter from bibcodes
pattern = r'^(\d{4})(.{5})(.{4})(.)'
df[['year', 'bibstem', 'volume', 'letter']] = df['bibcode'].str.extract(pattern)
# Remove periods from bibstems
df['bibstem'] = df['bibstem'].str.replace('.', '', regex=False)
# Remove periods from volumes
df['volume'] = df['volume'].str.replace('.', '', regex=False)
# Replace ApJ with ApJL for ApJ Letter papers
condition1 = (df['bibstem'] == 'ApJ')
condition2 = (df['letter'] == 'L')
df.loc[condition1 & condition2, 'bibstem'] = 'ApJL'
# Store years as integers
df['year'] = df['year'].astype(int)
# Store volumes as numeric, coerce non-numeric entries (e.g. 'tmp') to NaN
df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
df = df.dropna(subset=['volume'])
df['volume'] = df['volume'].astype(int)

# Count records with refs per bibstem+year and per bibstem+volume
aggr_by_year = df.groupby(['bibstem', 'year']).size().reset_index(name='ref_count')
aggr_by_volume = df.groupby(['bibstem', 'volume']).size().reset_index(name='ref_count')

# Read total record counts from metadata stats
records_year = pd.read_csv(records_year_file, sep='\t')
records_volume = pd.read_csv(records_volume_file, sep='\t')
# Remove periods from bibstems in the metadata stats files
records_year['bibstem'] = records_year['bibstem'].str.replace('.', '', regex=False)
records_volume['bibstem'] = records_volume['bibstem'].str.replace('.', '', regex=False)

# Inner join: only keep bibstem+year/volume entries where references exist
year_merged = pd.merge(records_year, aggr_by_year, on=['bibstem', 'year'])
year_merged.to_csv(agg_by_year_res, sep='\t', index=False)

volume_merged = pd.merge(records_volume, aggr_by_volume, on=['bibstem', 'volume'])
volume_merged.to_csv(agg_by_volume_res, sep='\t', index=False)