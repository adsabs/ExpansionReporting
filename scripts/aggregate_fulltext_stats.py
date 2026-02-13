import os
import sys
import re
import pandas as pd
import numpy as np
## Definition of output files
# Article counts aggragated by bibstem and volume
records_volume_aggr = 'records_agg_volume.tsv'
# Article counts aggregated by bibstem and publication year
records_year_aggr = 'records_agg_year.tsv'
# Articles with fulltext, aggregated by bibstem and volume
agg_by_volume = 'fulltext_volume_aggr.tsv'
# Articles with fulltext, aggregated by bibstem and publication year
agg_by_year = 'fulltext_year_aggr.tsv'
# Bibcodes for all records (first column)
bibdata = '/proj/ads/abstracts/config/bib2accno.dat'
# Bibcodes for all records with fulltext (first column)
fulltext_data = '/proj/ads/abstracts/config/links/fulltext/all.links'
# Read all bibcodes in dataframe
records = pd.read_csv(bibdata, sep='\t', usecols=[0], header=None, names=['bibcode'])
# Add a column containing all bibstems
records['bibstem'] = records['bibcode'].str.extract(r'^\d{4}(.{5})')
# We need to replace the bibstem ApJ with ApJL if the bibcode has an L as 14th character
# Get all entries for ApJ
ApJ = records['bibstem'] == 'ApJ..'
# The 14th character in 'bibcode' is 'L' (index 13)
# The .str.len() check is to prevent errors with shorter strings (should never happen)
#Letter = (records.iloc[:, 0].str.len() >= 14) & (records.iloc[:, 0].str[13] == 'L')
Letter = (records['bibcode'].str.len() >= 14) & (records['bibcode'].str[13] == 'L')
# Combine the conditions
ApJL = ApJ & Letter
records.loc[ApJL, 'bibstem'] = 'ApJL.'
# Add a column with publication years
records['year'] = records['bibcode'].str.extract(r'^(\d{4})')
# Add a column with volume numbers
records['volume'] = records['bibcode'].str.extract(r'^\d{4}.{5}(.{4})')
# Remove all periods in volumes
records['volume'] = records['volume'].str.replace('.', '', regex=False)
# Remove all entries where the volume is not a number
records['volume'] = pd.to_numeric(records['volume'], errors='coerce')
records = records[records['volume'].notna()]
# Make sure volumes are integers
records['volume'] = records['volume'].astype(int)
# Aggregate publications counts by bibstem and volume
rec_grouped_vol_count = records.groupby(['bibstem', 'volume']).size().reset_index(name='record_count')
# Save these counts to a tsv file
rec_grouped_vol_count.to_csv(records_volume_aggr, sep='\t', index=False)
# Aggrgate publication counts by bibstem and year
rec_grouped_year_count = records.groupby(['bibstem', 'year']).size().reset_index(name='record_count')
# Save these counts to a tsv file
rec_grouped_year_count.to_csv(records_year_aggr, sep='\t', index=False)
## Not process fulltext data
# Read all bibcodes of records with fulltext into dataframe
fulltext= pd.read_csv(fulltext_data, sep='\t', usecols=[0], header=None, names=['bibcode'])
# As before, add columns for bibstem, publication year and volume
fulltext['bibstem'] = fulltext['bibcode'].str.extract(r'^\d{4}(.{5})')
# Here as well, replace ApJ with ApJL when applicable
# Get all entries for ApJ
ApJ = fulltext['bibstem'] == 'ApJ..'
# The 14th character in 'bibcode' is 'L' (index 13)
# The .str.len() check is to prevent errors with shorter strings (should never happen)
Letter = (fulltext['bibcode'].str.len() >= 14) & (fulltext['bibcode'].str[13] == 'L')
# Combine the conditions
ApJL = ApJ & Letter
fulltext.loc[ApJL, 'bibstem'] = 'ApJL.'
# Add year and volume columns
fulltext['year'] = fulltext['bibcode'].str.extract(r'^(\d{4})')
fulltext['volume'] = fulltext['bibcode'].str.extract(r'^\d{4}.{5}(.{4})')
# Remove all periods in volumes
fulltext['volume'] = fulltext['volume'].str.replace('.', '', regex=False)
# Remove all entries where the volume is not a number
fulltext['volume'] = pd.to_numeric(fulltext['volume'], errors='coerce')
fulltext = fulltext[fulltext['volume'].notna()]
# Make sure volumes are integers
fulltext['volume'] = fulltext['volume'].astype(int)
# Aggregate publications with fulltext counts by bibstem and volume
ft_grouped_vol_count = fulltext.groupby(['bibstem', 'volume']).size().reset_index(name='ft_count')
# Aggregate publications with fulltext counts by bibstem and publication year
ft_grouped_year_count = fulltext.groupby(['bibstem', 'year']).size().reset_index(name='ft_count')
# Merge the records and fulltext dataframe for volume counts, requiring that bibstem and volume are equal
vol_merged = pd.merge(rec_grouped_vol_count, ft_grouped_vol_count, on=['bibstem', 'volume'])
# Save these results to file
vol_merged.to_csv(agg_by_volume, sep='\t', index=False)
# Merge the records and fulltext dataframe for year counts, requiring that bibstem and year are equal
year_merged = pd.merge(rec_grouped_year_count, ft_grouped_year_count, on=['bibstem', 'year'])
# Save these results to file
year_merged.to_csv(agg_by_year, sep='\t', index=False)
