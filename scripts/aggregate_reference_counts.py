import os
import sys
import pandas as pd
import numpy as np

ref_base = '/proj/ads/references'
abs_base = '/proj/ads/abstracts/config/links'

# Input files
# columns:
# 1. bibcode
# 2. total raw references
raw_counts = '{0}/stats/references.counts.tsv'.format(ref_base)
# columns:
# 1. bibcode
# 2. total matched references
# 3. total matched refereed references
ads_counts = '{0}/reference/all.counts'.format(abs_base)
# Output files
# Reference counts aggregated by year
agg_by_year_res = '{0}/stats/refcounts_aggr_by_year.tsv'.format(ref_base)
# Reference counts aggregated by volume
agg_by_volume_res = '{0}/stats/refcounts_aggr_by_volume.tsv'.format(ref_base)

# Read the input data
# Raw reference counts
df1 = pd.read_csv(raw_counts, sep='\t', header=None, names=['bibcode','raw_count'])
# Numbers of matched references
df2 = pd.read_csv(ads_counts, sep='\t', header=None, names=['bibcode','ads_count','refereed'])
# Merge the DataFrames on bibcode (first column, index=0)
merged_df = pd.merge(df1, df2, on='bibcode', how='inner')
# Extract year, bibstem, volume and letter from bibcodes
pattern = r'^(\d{4})(.{5})(.{4})(.)'
merged_df[['year','bibstem','volume','letter']] = merged_df['bibcode'].str.extract(pattern)
# Remove periods from bibstems
merged_df['bibstem'] = merged_df['bibstem'].str.replace('.', '', regex=False)
# Remove periods from volumes
merged_df['volume'] = merged_df['volume'].str.replace('.', '', regex=False)
# Trick to replace bibstem ApJ to ApJL for ApJ Letter papers
condition1 = (merged_df['bibstem'] == 'ApJ')
condition2 = (merged_df['letter'] == 'L')
merged_df.loc[condition1 & condition2, 'bibstem'] = 'ApJL'
# Store years as integers
merged_df['year'] = merged_df['year'].astype(int)
# Store volumes as numeric values and coerce non-number values for volume (like 'tmp') to NaN
merged_df['volume'] = pd.to_numeric(merged_df['volume'], errors='coerce')
# Now remove all rows where the volume got NaN as value
merged_df = merged_df.dropna(subset=['volume'])
# Make sure volumes as stored as integers
merged_df['volume'] = merged_df['volume'].astype(int)
# Now we can start aggregating
# First aggregate by bibstem and year and add all raw reference counts for each bibstem/year pair
aggr_by_year_raw = merged_df.groupby(['bibstem','year'])['raw_count'].sum().reset_index()
# Do the same for all ADS counts (i.e. matched references)
aggr_by_year_ads = merged_df.groupby(['bibstem','year'])['ads_count'].sum().reset_index()
# Now we merge these results into one data frame
aggr_by_year = pd.merge(aggr_by_year_raw, aggr_by_year_ads, on=['bibstem','year'], how='inner')
# Append a column that holds the fraction of resolved references to the number of raw references
aggr_by_year['fraction'] = np.where(aggr_by_year['raw_count'] != 0, aggr_by_year['ads_count'] / aggr_by_year['raw_count'], np.nan).round(2)
# Store these results
aggr_by_year.to_csv(agg_by_year_res, sep='\t', index=False)
# Now do the same based on volume
# First aggregate by bibstem and volume and add all raw reference counts for each bibstem/volume pair
aggr_by_volume_raw = merged_df.groupby(['bibstem','volume'])['raw_count'].sum().reset_index()
# Do the same for all ADS counts (i.e. matched references)
aggr_by_volume_ads = merged_df.groupby(['bibstem','volume'])['ads_count'].sum().reset_index()
# Now we merge these results into one data frame
aggr_by_volume = pd.merge(aggr_by_volume_raw, aggr_by_volume_ads, on=['bibstem','volume'], how='inner')
# Append a column that holds the fraction of resolved references to the number of raw references
aggr_by_volume['fraction'] = np.where(aggr_by_volume['raw_count'] != 0, aggr_by_volume['ads_count'] / aggr_by_volume['raw_count'], np.nan).round(2)
# Store these results
aggr_by_volume.to_csv(agg_by_volume_res, sep='\t', index=False)
