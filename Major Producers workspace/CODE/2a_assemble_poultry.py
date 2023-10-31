#%% About
'''
This merges all poultry data, performing the same basic steps on each input file:
   - Standardizing country names
   - Adding a prefix to all columns indicating their source

This also performs intermediate processing for some data sets, such as calculating
net imports for import/export data.

The FAO table is used as the base onto which all others are merged.
'''
#%% Base table: FAO
# Animal Production and Producer Prices

# FAOstat contains all countries in scope (European 6, UK, US, China, India, Brazil)
# so use this as the base table. Merge others onto it.

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  fao_chickencombo.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  fao_chickencombo = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'fao_chickencombo.pkl.gz'))
  print('> Data frame loaded.')
datainfo(fao_chickencombo)

# ----------------------------------------------------------------------------
# Create a filter for countries in scope
# ----------------------------------------------------------------------------
countries_fao = list(fao_chickencombo['country'].unique())

# Give United Kingdom a snappier name
rename_fao_countries = {
   'United Kingdom of Great Britain and Northern Ireland':'United Kingdom'
   ,'Russian Federation':'Russia'
}
fao_chickencombo['country'].replace(rename_fao_countries ,inplace=True)

# This list can be any case. Will be converted to uppercase as needed.
fao_countries_inscope = [
   # European 6
   'France'
   ,'Germany'
   ,'Italy'
   ,'Netherlands'
   ,'Poland'
   ,'Spain'

   ,'United Kingdom'
   ,'United States of America'

   ,'Brazil'
   ,'China'
   # Subcategories of China sum to total, so I am dropping them
   # ,'China, Hong Kong SAR'
   # ,'China, Macao SAR'
   # ,'China, Taiwan Province of'
   # ,'China, mainland'

   ,'India'
]
fao_countries_inscope_upcase = [x.upper() for x in fao_countries_inscope]
_fao_chickens_inscope = (fao_chickencombo['country'].str.upper().isin(fao_countries_inscope_upcase))
check_countries_inscope = list(fao_chickencombo.loc[_fao_chickens_inscope ,'country'].unique())

# Add column to data
fao_chickencombo['country_inscope'] = _fao_chickens_inscope

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
# Limit to countries in scope
# Because all tables are left-merged onto this, this limits scope for all tables
fao_chickens_tomerge = fao_chickencombo.loc[_fao_chickens_inscope].copy()

# Create an explicit uppercase version of country
fao_chickens_tomerge['country_upcase'] = fao_chickens_tomerge['country'].str.upper()

# Add prefix to all columns indicating source table
fao_chickens_tomerge = fao_chickens_tomerge.add_prefix('fao_')

datainfo(fao_chickens_tomerge)

#%% Merge Eurostat

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  euro_chickencombo.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  euro_chickencombo = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'euro_chickencombo.pkl.gz'))
  print('> Data frame loaded.')

datainfo(euro_chickencombo)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
euro_chickencombo_tomerge = euro_chickencombo.copy()

# Get country list
countries_euro = list(euro_chickencombo_tomerge['geo'].unique())

# Recode Eurostat codes to country names to match FAO
# For now, only care about countries in scope.
eurostat_country_recode = {
   'FR':'France'
   ,'PL':'Poland'
   ,'ES':'Spain'
   ,'IT':'Italy'
   ,'NL':'Netherlands'
   ,'DE':'Germany'
   ,'UK':'United Kingdom'
}
euro_chickencombo_tomerge['country'] = euro_chickencombo_tomerge['geo'].replace(eurostat_country_recode)
check_eurostat_country_recode = euro_chickencombo_tomerge['country'].unique()

# Create an explicit uppercase version of country for merging
euro_chickencombo_tomerge['country_upcase'] = euro_chickencombo_tomerge['country'].str.upper()

# Drop unnecessary columns
euro_dropcols = ['geo' ,'geo_inscope' ,'geo_eu27']
euro_chickencombo_tomerge = euro_chickencombo_tomerge.drop(columns=euro_dropcols)

# Rename
euro_chickencombo_tomerge = euro_chickencombo_tomerge.rename(columns={'time_period':'year'})

# Add prefix to all columns indicating source table
euro_chickencombo_tomerge = euro_chickencombo_tomerge.add_prefix('euro_')

datainfo(euro_chickencombo_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = 1   # Initialize

# Merge on country and year
chickens_merged = pd.merge(
   left=fao_chickens_tomerge
   ,right=euro_chickencombo_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['euro_country_upcase' ,'euro_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

#%% Merge UK Gov
# UK chicks placed and slaughter, including liveweight

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  uk_broilercombo.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  uk_broilercombo = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'uk_broilercombo.pkl.gz'))
  print('> Data frame loaded.')
datainfo(uk_broilercombo)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
uk_broilercombo_tomerge = uk_broilercombo.copy()

# Add country
uk_broilercombo_tomerge['country'] = 'United Kingdom'
uk_broilercombo_tomerge['country_upcase'] = uk_broilercombo_tomerge['country'].str.upper()

# Add prefix to all columns indicating source table
uk_broilercombo_tomerge = uk_broilercombo_tomerge.add_prefix('ukgov_')

datainfo(uk_broilercombo_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
chickens_merged = pd.merge(
   left=chickens_merged
   ,right=uk_broilercombo_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['ukgov_country_upcase' ,'ukgov_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

#%% Merge USDA

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  usda_broilers_p.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  usda_broilers_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'usda_broilers_p.pkl.gz'))
  print('> Data frame loaded.')
datainfo(usda_broilers_p)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
usda_broilers_tomerge = usda_broilers_p.copy()

usda_broilers_tomerge['country'] = 'United States of America'
usda_broilers_tomerge['country_upcase'] = usda_broilers_tomerge['country'].str.upper()

# Add prefix to all columns indicating source table
usda_broilers_tomerge = usda_broilers_tomerge.add_prefix('usda_')

datainfo(usda_broilers_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
chickens_merged = pd.merge(
   left=chickens_merged
   ,right=usda_broilers_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['usda_country_upcase' ,'usda_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

#%% Merge Eurostat Imports/Exports

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  euro_impexp_p.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  euro_impexp_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'euro_impexp_p.pkl.gz'))
  print('> Data frame loaded.')
datainfo(euro_impexp_p)

try:
  euro_impexp_p_reporter_perspective_checkpartner.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  euro_impexp_p_reporter_perspective_checkpartner = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'euro_impexp_p_reporter_perspective_checkpartner.pkl.gz'))
  print('> Data frame loaded.')
datainfo(euro_impexp_p_reporter_perspective_checkpartner)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
# An approach: use records where our in-scope countries are REPORTERS
# Partner = All countries of the world

# Subset to columns of interest
keep_cols_thisorder = [
   'reporter'
   ,'year'

   # Import and export of chicks
   ,'world_import_live_gallusdom_lte185g_100kg'
   ,'world_import_live_gallusdom_lte185g_head'
   ,'world_import_live_gallusdom_lte185g_value_in_euros'

   ,'world_export_live_gallusdom_lte185g_100kg'
   ,'world_export_live_gallusdom_lte185g_head'
   ,'world_export_live_gallusdom_lte185g_value_in_euros'

   # Import and export of mature birds
   # Do we need exports of mature birds? We have data on slaughter numbers, which will be after mature birds change hands.
   ,'world_import_live_gallusdom_gt185g_100kg'
   ,'world_import_live_gallusdom_gt185g_head'
   ,'world_import_live_gallusdom_gt185g_value_in_euros'

   ,'world_export_live_gallusdom_gt185g_100kg'
   ,'world_export_live_gallusdom_gt185g_head'
   ,'world_export_live_gallusdom_gt185g_value_in_euros'
]
euro_impexp_tomerge = euro_impexp_p_reporter_perspective_checkpartner[keep_cols_thisorder].copy()
datainfo(euro_impexp_tomerge)

# Calculate net imports
euro_impexp_tomerge.eval(
   '''
   net_import_live_gallusdom_lte185g_head = world_import_live_gallusdom_lte185g_head - world_export_live_gallusdom_lte185g_head
   net_import_live_gallusdom_gt185g_head = world_import_live_gallusdom_gt185g_head - world_export_live_gallusdom_gt185g_head
   '''
   ,inplace=True
)

# Recode country
recode_country = {
   "DE":"Germany"
   ,"ES":"Spain"
   ,"FR":"France"
   ,"IT":"Italy"
   ,"NL":"Netherlands"
   ,"PL":"Poland"
   ,"GB":"United Kingdom"
}
euro_impexp_tomerge['country'] = euro_impexp_tomerge['reporter'].replace(recode_country)
euro_impexp_tomerge['country_upcase'] = euro_impexp_tomerge['country'].str.upper()
check_euro_impexp_tomerge_countries = list(euro_impexp_tomerge['country'].unique())

# Add prefix to all columns indicating source table
euro_impexp_tomerge = euro_impexp_tomerge.add_prefix('euimp_')

datainfo(euro_impexp_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
chickens_merged = pd.merge(
   left=chickens_merged
   ,right=euro_impexp_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['euimp_country_upcase' ,'euimp_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

#%% Merge UN Comtrade Imports/Exports

#!!! This data uses HS commodity codes like the UDSA GATS data, but does not go to the same level of detail.
'''
For example, this data contains code 010511 - live fowls of species Gallus Domesticus weighing LTE 185g,
but does not specify how many are breeders vs. fattening birds, which is coded in the last 4 digits of
the 10-digit code:
   "105110040":"live_chickens_lte185g_nonbreeding"
   "105110020":"live_chickens_lte185g_breeding_broiler"
   "105110010":"live_chickens_lte185g_breeding_layer"
'''
# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
uncomtrade_stack_p_nz = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'uncomtrade_stack_p_nz.pkl.gz'))
datainfo(uncomtrade_stack_p_nz)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
uncomtrade_tomerge = uncomtrade_stack_p_nz.copy()

# Calculate net imports
uncomtrade_tomerge.eval(
   '''
   net_import_live_gallusdom_lte185g_head = import_live_chickens_lte185g_alluses_number_of_items - export_live_chickens_lte185g_alluses_number_of_items
   '''
   # Variables for mature birds (>185g) are not present in data
   # net_import_live_gallusdom_gt185g_head = import_live_chickens_gt185g_alluses_number_of_items - export_live_chickens_gt185g_alluses_number_of_items
   ,inplace=True
)

# Recode country
uncomtrade_tomerge_countries = list(uncomtrade_tomerge['reporter'].unique())
# Names for most countries match FAO, except USA
recode_country = {
   'USA':'United States of America'
}
uncomtrade_tomerge['country'] = uncomtrade_tomerge['reporter'].replace(recode_country)
uncomtrade_tomerge['country_upcase'] = uncomtrade_tomerge['country'].str.upper()

# Drop columns related to swine
swinecols = [col for col in list(uncomtrade_tomerge) if 'swine' in col]
uncomtrade_tomerge = uncomtrade_tomerge.drop(columns=swinecols)

# Add prefix to all columns indicating source table
uncomtrade_tomerge = uncomtrade_tomerge.add_prefix('uncom_')

datainfo(uncomtrade_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
chickens_merged = pd.merge(
   left=chickens_merged
   ,right=uncomtrade_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['uncom_country_upcase' ,'uncom_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

#%% Merge USDA PSD Imports/Exports

# PSD data reports import/export of meat, not live animals. We would have to estimate live animals
# by assuming an average weight. Not using for now.

# Also contains meat production, but we are getting that from other sources.

# usda_psd_chickenmeat_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'usda_psd_chickenmeat_p.pkl.gz'))

#%% Merge USDA GATS Imports/Exports
'''
These data show no import/export of mature birds for the US.
Since we have chicks PLACED in the US, we do not need to adjust for import/export of chicks.
Not using this data at this time.
'''
# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
# us_imports_chickens_m_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'us_imports_chickens_m_p.pkl.gz'))
# us_exports_chickens_m_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'us_exports_chickens_m_p.pkl.gz'))

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------

# Calculate net imports

# Add country

# Add prefix to all columns indicating source table

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------

#%% Merge UK Feed Price and Production

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
#  Feed Price
try:
  uk_feedprice.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  uk_feedprice = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'uk_feedprice.pkl.gz'))
  print('> Data frame loaded.')
datainfo(uk_feedprice)

# Feed Production
try:
  uk_feedproduction_selectpoultry_m_p.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  uk_feedproduction_selectpoultry_m_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'uk_feedproduction_selectpoultry_m_p.pkl.gz'))
  print('> Data frame loaded.')
datainfo(uk_feedproduction_selectpoultry_m_p)

uk_feedproduction = uk_feedproduction_selectpoultry_m_p.copy()
del uk_feedproduction_selectpoultry_m_p

# ----------------------------------------------------------------------------
# Combine price and production
# ----------------------------------------------------------------------------
# Get production quarterly, to match price data
## Add Quarter
months_upcase_q1 = ['JAN' ,'FEB' ,'MAR']
months_upcase_q2 = ['APR' ,'MAY' ,'JUN']
months_upcase_q3 = ['JUL' ,'AUG' ,'SEP']
months_upcase_q4 = ['OCT' ,'NOV' ,'DEC']

uk_feedproduction.loc[uk_feedproduction['month'].str.upper().isin(months_upcase_q1) ,'quarter'] = 1
uk_feedproduction.loc[uk_feedproduction['month'].str.upper().isin(months_upcase_q2) ,'quarter'] = 2
uk_feedproduction.loc[uk_feedproduction['month'].str.upper().isin(months_upcase_q3) ,'quarter'] = 3
uk_feedproduction.loc[uk_feedproduction['month'].str.upper().isin(months_upcase_q4) ,'quarter'] = 4

## Sum quarterly
uk_feedproduction_qtrly = uk_feedproduction.pivot_table(
   index=['year' ,'quarter']           # Column(s) to make new index
   ,values=[         # Column(s) to aggregate
            'production_broiler_chicken_compounds_thsdtonnes'
            ,'production_layers_compounds_thsdtonnes'
            ,'production_total_poultry_feed_thsdtonnes'
            ]
   ,aggfunc='sum'                  # Aggregate function to use. Can pass list or dictionary {'colname':'function'}. See numpy functions https://docs.scipy.org/doc/numpy/reference/routines.statistics.html
)
uk_feedproduction_qtrly.reset_index(inplace=True)

# Merge quarterly price and production
uk_feedprice_withprod = pd.merge(
   left=uk_feedprice
   ,right=uk_feedproduction_qtrly
   ,on=['year' ,'quarter']
   ,how='left'
   # ,indicator=True
)
# uk_feedprice_withprod['_merge'].value_counts()          # Check number of rows from each table (requires indicator=True)

# ----------------------------------------------------------------------------
# Get average price per year, weighting each quarter by production
# ----------------------------------------------------------------------------
# Create weighted prices quarterly
uk_feedprice_withprod.eval(
   '''
   production_total_poultry_feed_tonnes = production_total_poultry_feed_thsdtonnes * 1000
   production_broiler_chicken_compounds_tonnes = production_broiler_chicken_compounds_thsdtonnes * 1000
   production_layers_compounds_tonnes = production_layers_compounds_thsdtonnes * 1000

   feeddollars_poultry_gbp = feedprice_poultry_gbppertonne * production_total_poultry_feed_tonnes
   '''
   ,inplace=True
)
datainfo(uk_feedprice_withprod)

# Sum dollars and production to yearly
uk_feedprice_withprod_agg = uk_feedprice_withprod.pivot_table(
   index='year'           # Column(s) to make new index
   # ,columns=                        # Optional column to make new columns
   ,values=[         # Column(s) to aggregate
      'production_total_poultry_feed_tonnes'
      ,'production_broiler_chicken_compounds_tonnes'
      ,'production_layers_compounds_tonnes'
      ,'feeddollars_poultry_gbp'
      ]
   ,aggfunc='sum'                  # Aggregate function to use. Can pass list or dictionary {'colname':'function'}. See numpy functions https://docs.scipy.org/doc/numpy/reference/routines.statistics.html
)
uk_feedprice_withprod_agg.reset_index(inplace=True)     # If pivoting changed columns to indexes, change them back

# Calculate average weighted price
uk_feedprice_withprod_agg.eval(
   '''
   feedprice_poultry_gbppertonne_wtavg = feeddollars_poultry_gbp / production_total_poultry_feed_tonnes
   '''
   ,inplace=True
)
datainfo(uk_feedprice_withprod_agg)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
uk_feedprice_withprod_agg_tomerge = uk_feedprice_withprod_agg.copy()

# Add Country
# Create an explicit uppercase version of country for merging
uk_feedprice_withprod_agg_tomerge['country'] = 'United Kingdom'
uk_feedprice_withprod_agg_tomerge['country_upcase'] = uk_feedprice_withprod_agg_tomerge['country'].str.upper()

# Add prefix to all columns indicating source table
uk_feedprice_withprod_agg_tomerge = uk_feedprice_withprod_agg_tomerge.add_prefix('ukfeed_')

datainfo(uk_feedprice_withprod_agg_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
chickens_merged = pd.merge(
    left=chickens_merged
    ,right=uk_feedprice_withprod_agg_tomerge
    ,left_on=['fao_country_upcase' ,'fao_year']
    ,right_on=['ukfeed_country_upcase' ,'ukfeed_year']
    ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

#%% Merge World Bank
# Inflation, Exchange rate, and GDP

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  wb_infl_exchg_gdp.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  wb_infl_exchg_gdp = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'wb_infl_exchg_gdp.pkl.gz'))
  print('> Data frame loaded.')
datainfo(wb_infl_exchg_gdp)

# ----------------------------------------------------------------------------
# Self-merge an EU country to get Euros per USD as a separate column
# ----------------------------------------------------------------------------
wb_infl_exchg_tomerge = wb_infl_exchg_gdp.copy()

_country_euros = (wb_infl_exchg_tomerge['country'].str.upper() == 'FRANCE')
wb_infl_exchg_tomerge = pd.merge(
   left=wb_infl_exchg_tomerge
   ,right=wb_infl_exchg_tomerge.loc[_country_euros ,['year' ,'exchangerate_lcuperusd']]
   ,on='year'
   ,how='left'
   ,suffixes=('', '_euro')
)
wb_infl_exchg_tomerge = wb_infl_exchg_tomerge.rename(columns={'exchangerate_lcuperusd_euro':'exchangerate_europerusd'})

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
# Get country list
countries_wb = list(wb_infl_exchg_tomerge['country'].unique())

# Recode Eurostat codes to country names to match FAO
# For now, only care about countries in scope.
wb_country_recode = {
   'United States':'United States of America'
   ,'Russian Federation':'Russia'
}
wb_infl_exchg_tomerge['country'] = wb_infl_exchg_tomerge['country'].replace(wb_country_recode)

# Create an explicit uppercase version of country for merging
wb_infl_exchg_tomerge['country_upcase'] = wb_infl_exchg_tomerge['country'].str.upper()

# Add prefixes to all columns indicating source table
wb_infl_exchg_tomerge = wb_infl_exchg_tomerge.add_prefix('wb_')

datainfo(wb_infl_exchg_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
chickens_merged = pd.merge(
    left=chickens_merged
    ,right=wb_infl_exchg_tomerge
    ,left_on=['fao_country_upcase' ,'fao_year']
    ,right_on=['wb_country_upcase' ,'wb_year']
    ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

#%% Merge WAHIS Diseases

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  wahis_birds_agg2_p.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  wahis_birds_agg2_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'wahis_birds_agg2_p.pkl.gz'))
  print('> Data frame loaded.')

datainfo(wahis_birds_agg2_p)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
wahis_birds_tomerge = wahis_birds_agg2_p.copy()

# Get country list
countries_wahis = list(wahis_birds_tomerge['country'].unique())

# Recode country names to match FAO
# This list can be any case. Will be converted to uppercase as needed.
wahis_country_recode = {
   'Brazil':'Brazil'
   ,"China (People's Rep. of)":"China"
   ,'France':'France'
   ,'Germany':'Germany'
   ,'India':'India'
   ,'Italy':'Italy'
   ,'Netherlands':'Netherlands'
   ,'Poland':'Poland'
   ,'United Kingdom':'United Kingdom'
   ,'United States of America':'United States of America'
}
wahis_birds_tomerge['country'].replace(wahis_country_recode ,inplace=True)

# Create an explicit uppercase version of country for merging
wahis_birds_tomerge['country_upcase'] = wahis_birds_tomerge['country'].str.upper()

# Add prefix to all columns indicating source table
wahis_birds_tomerge = wahis_birds_tomerge.add_prefix('wahis_')

datainfo(wahis_birds_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
chickens_merged = pd.merge(
    left=chickens_merged
    ,right=wahis_birds_tomerge
    ,left_on=['fao_country_upcase' ,'fao_year']
    ,right_on=['wahis_country_upcase' ,'wahis_year']
    ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

#%% Merge Brazil specifics

# =============================================================================
# Chicks placed
# =============================================================================
# ----------------------------------------------------------------------------
# Brazil
# ----------------------------------------------------------------------------
# Read Pickled data
try:
  brazil_chicksplaced_m.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  brazil_chicksplaced_m = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'brazil_chicksplaced_m.pkl.gz'))
  print('> Data frame loaded.')
datainfo(brazil_chicksplaced_m)

# Prep for merge
brazil_chicksplaced_tomerge = brazil_chicksplaced_m.copy()
brazil_chicksplaced_tomerge['country_upcase'] = 'BRAZIL'

# Add prefix to all columns indicating source table
brazil_chicksplaced_tomerge = brazil_chicksplaced_tomerge.add_prefix('brzl_')

datainfo(brazil_chicksplaced_tomerge)

mergenum = mergenum + 1

# Merge on country and year
chickens_merged = pd.merge(
   left=chickens_merged
   ,right=brazil_chicksplaced_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['brzl_country_upcase' ,'brzl_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

#%% Merge UK Condemns (FSA)

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  ukfsa_poultry_sum_p.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  ukfsa_poultry_sum_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'ukfsa_poultry_sum_p.pkl.gz'))
  print('> Data frame loaded.')
datainfo(ukfsa_poultry_sum_p)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
ukfsa_poultry_condemns_tomerge = ukfsa_poultry_sum_p.copy()

# Limit to broilers
ukfsa_poultry_condemns_tomerge = ukfsa_poultry_condemns_tomerge[[
   'year'
   ,'condemned_hd_broilers'
   ,'totalproduction_hd_broilers'
]]

# Add country
ukfsa_poultry_condemns_tomerge['country_upcase'] = 'UNITED KINGDOM'

# Add prefix to all columns indicating source table
ukfsa_poultry_condemns_tomerge = ukfsa_poultry_condemns_tomerge.add_prefix('ukcdm_')

datainfo(ukfsa_poultry_condemns_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
chickens_merged = pd.merge(
   left=chickens_merged
   ,right=ukfsa_poultry_condemns_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['ukcdm_country_upcase' ,'ukcdm_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

#%% Merge UK Misc
'''
Note: Average days on feed is a parameter in Dash, so is not needed in the data.
The other columns in UK Misc (average downtime and cycles per year) are not currently
used.
'''
# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  ukmisc_poultry.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  ukmisc_poultry = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'ukmisc_poultry.pkl.gz'))
  print('> Data frame loaded.')
datainfo(ukmisc_poultry)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
ukmisc_poultry_tomerge = ukmisc_poultry.copy()

# Add country
ukmisc_poultry_tomerge['country_upcase'] = ukmisc_poultry_tomerge['country'].str.upper()

# Add prefix to all columns indicating source table
ukmisc_poultry_tomerge = ukmisc_poultry_tomerge.add_prefix('ukmisc_')

datainfo(ukmisc_poultry_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
#!!! For now, assuming estimates apply to all years.
chickens_merged = pd.merge(
   left=chickens_merged
   ,right=ukmisc_poultry_tomerge
   ,left_on=['fao_country_upcase']
   ,right_on=['ukmisc_country_upcase']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

#%% Merge India specifics

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  india_poultry.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  india_poultry = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'india_poultry.pkl.gz'))
  print('> Data frame loaded.')
datainfo(india_poultry)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
india_poultry_tomerge = india_poultry.copy()

# Add country
india_poultry_tomerge['country_upcase'] = india_poultry_tomerge['country'].str.upper()

# Add prefix to all columns indicating source table
india_poultry_tomerge = india_poultry_tomerge.add_prefix('india_')

datainfo(india_poultry_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
chickens_merged = pd.merge(
   left=chickens_merged
   ,right=india_poultry_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['india_country_upcase' ,'india_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

#%% Merge China specifics

# China specific data is only a few numbers: average mortality and breakdown of production
# among bird types. Rather than merge it in, I can build it into the calcs below.

#%% Merge OECD data

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  oecd_ag_p.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  oecd_ag_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'oecd_ag_p.pkl.gz'))
  print('> Data frame loaded.')
datainfo(oecd_ag_p)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
# Subset to commodities of interest
_row_selection = (oecd_ag_p['commodity'].isin(['Poultry meat']))
print(f"> Selected {_row_selection.sum() :,} rows.")
oecd_ag_poultry = oecd_ag_p.loc[_row_selection].copy().reset_index(drop=True)

# Some variables are not reported for all commodities. Drop columns that are all missing.
oecd_ag_poultry = oecd_ag_poultry.dropna(
   axis=1                        # 0 = drop rows, 1 = drop columns
   ,how='all'                    # String: 'all' = drop rows / columns that have all missing values. 'any' = drop rows / columns that have any missing values.
)
datainfo(oecd_ag_poultry)

oecd_ag_poultry_tomerge = oecd_ag_poultry.copy()

# Rename columns

# Add country
oecd_ag_poultry_tomerge['country_upcase'] = oecd_ag_poultry_tomerge['country'].str.upper()

# Add prefix to all columns indicating source table
oecd_ag_poultry_tomerge = oecd_ag_poultry_tomerge.add_prefix('oecd_')

datainfo(oecd_ag_poultry_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
# mergenum = mergenum + 1

#%% Merge Breed Standards

# I am not merging breed standards onto this table. Instead, I perform a lookup to
# extract pieces as necessary when calculating the burden of disease.

# This is because the breed standard data has a fundamentally different structure
# than this data. I would have to make a lot of assumptions to merge it in now.

# IF I were merging onto this table, an approach would be to extract meaningful
# components of breed standard:
#     Starting weight
#     Weight at common day-on-feed endpoints (30, 40, 50, 60)
#        Better: weight at average DOF for each country
#     Days to Weight for common weights (e.g. days to 2kg)
#        Better: Days to Weight for avg live weight of slaughtered birds for each country
#              And feed consumption to get there
#              Note can calculate avg. live weight from assumed avg. yield

#%% Merge William's cost spreadsheet

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  poultry_costs_fromwill.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  poultry_costs_fromwill = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'poultry_costs_fromwill.pkl.gz'))
  print('> Data frame loaded.')
datainfo(poultry_costs_fromwill)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
poultry_costs_fromwill_tomerge = poultry_costs_fromwill.copy()

# Get country list
countries_will = list(poultry_costs_fromwill_tomerge['country'].unique())

# Recode Eurostat codes to country names to match FAO
country_recode = {
   # 'Brazil':''
   # 'China':''
   # 'France':''
   # 'Germany':''
   # 'India':''
   # 'Italy':''
   # 'Netherlands':''
   # 'Poland':''
   # 'Spain':''
   'UK':'United Kingdom'
   ,'USA':'United States of America'
}
poultry_costs_fromwill_tomerge['country'] = poultry_costs_fromwill_tomerge['country'].replace(country_recode)

# Create an explicit uppercase version of country for merging
poultry_costs_fromwill_tomerge['country_upcase'] = poultry_costs_fromwill_tomerge['country'].str.upper()

# Add prefix to all columns indicating source table
poultry_costs_fromwill_tomerge = poultry_costs_fromwill_tomerge.add_prefix('glb_')   # William Gilbert

datainfo(poultry_costs_fromwill_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
chickens_merged = pd.merge(
   left=chickens_merged
   ,right=poultry_costs_fromwill_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['glb_country_upcase' ,'glb_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
chickens_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Merge sources by country only so they are filled for every year
chickens_merged = pd.merge(
   left=chickens_merged
   ,right=poultry_costs_fromwill_tomerge[['glb_country_upcase' ,'glb_source']]
   ,left_on=['fao_country_upcase']
   ,right_on=['glb_country_upcase']
   ,how='left'
)

# Save this merged version as separate dataframe
exec(f"chickens_merged_{mergenum} = chickens_merged.copy()")
exec(f"datainfo(chickens_merged_{mergenum})")

# =============================================================================
#### Fill in costs
# Fill in costs for other years based on inflation
# Note this must take place after World Bank data has been merged in
# =============================================================================
# Apply CPI ratio
# Using method from https://www.census.gov/topics/income-poverty/income/guidance/current-vs-constant-dollars.html
# Also explained here https://www.investopedia.com/terms/c/constantdollar.asp

# For India, cost provided is for 2020
# So inflation-adjusted cost for year i = 2020 cost * (CPI_yeari / CPI_2020)

# Keys are country names as they appear in poultry_costs_fromwill_tomerge
# Values are reference years for each country from Will's spreadsheet
glb_ref_years = {
   'Brazil':2017
   ,'China':2020
   ,'France':2017
   ,'Germany':2017
   ,'India':2020
   ,'Italy':2017
   ,'Netherlands':2017
   ,'Poland':2017
   ,'Spain':2017
   ,'United Kingdom':2017
   ,'United States of America':2017
}
# Create lookup for converting currencies
# Keys are country names as they appear in poultry_costs_fromwill_tomerge
# Values are column names specifying exchange rate to use from World Bank
glb_lookup_exchg = {
   'Brazil':'wb_exchangerate_europerusd'
   ,'China':'wb_exchangerate_lcuperusd'
   ,'France':'wb_exchangerate_europerusd'
   ,'Germany':'wb_exchangerate_europerusd'
   ,'India':'wb_exchangerate_lcuperusd'
   ,'Italy':'wb_exchangerate_europerusd'
   ,'Netherlands':'wb_exchangerate_europerusd'
   ,'Poland':'wb_exchangerate_europerusd'
   ,'Spain':'wb_exchangerate_europerusd'
   ,'United Kingdom':'wb_exchangerate_europerusd'
   ,'United States of America':'wb_exchangerate_europerusd'
}
# List columns you want to fill
glb_cost_cols = [
   'glb_chickprice_perhd'
   ,'glb_chickcost_perkglive'
   ,'glb_feedcost_perkglive'
   ,'glb_laborcost_perkglive'
   ,'glb_landhousingcost_perkglive'
   ,'glb_medicinecost_perkglive'
   ,'glb_othercost_perkglive'
   ,'glb_totalcost_perkglive'
]

for COUNTRY in list(glb_ref_years):   # For each country...
   ref_year = glb_ref_years[COUNTRY]   # Lookup reference year
   ref_row = (chickens_merged['fao_country'] == COUNTRY) & (chickens_merged['fao_year'] == ref_year)
   try:
      ref_cpi = chickens_merged.loc[ref_row ,'wb_cpi_idx2010'].values[0]   # Get CPI from reference year
   except:   # If ref_row does not exist
      ref_cpi = np.nan

   chickens_merged.loc[chickens_merged['fao_country'] == COUNTRY ,'glb_reference_year'] = ref_year
   chickens_merged.loc[chickens_merged['fao_country'] == COUNTRY ,'glb_reference_cpi'] = ref_cpi
   chickens_merged.loc[chickens_merged['fao_country'] == COUNTRY ,'glb_exchgrate_perusd'] = chickens_merged[glb_lookup_exchg[COUNTRY]]

   for COST_COL in glb_cost_cols:   # For each cost column...
      try:
         ref_cost = chickens_merged.loc[ref_row ,COST_COL].values[0]   # Get cost from reference year
      except:   # If ref_row does not exist
         ref_cost = np.nan
      chickens_merged.loc[chickens_merged['fao_country'] == COUNTRY ,f'{COST_COL}_ref'] = ref_cost

      # Create column with inflation-adjusted values
      chickens_merged[f'{COST_COL}_filled'] = chickens_merged[f'{COST_COL}_ref'] * (chickens_merged['wb_cpi_idx2010'] / chickens_merged['glb_reference_cpi'])

      # Apply exchange rate from lookup
      chickens_merged[f'{COST_COL}_usd'] = chickens_merged[f'{COST_COL}_filled'] / chickens_merged['glb_exchgrate_perusd']

# Cleanup
chickens_merged = chickens_merged.drop(columns=['glb_reference_cpi' ,'glb_exchgrate_perusd'])
for COST_COL in glb_cost_cols:
   chickens_merged = chickens_merged.drop(columns=[f'{COST_COL}' ,f'{COST_COL}_ref' ,f'{COST_COL}_filled'])

exec(f"datainfo(chickens_merged)")

#%% Calcs and Reconciliation
'''
Variables created here have prefix acc_ meaning "accepted"

Two kinds of reconciliation:
1. Columns that report the same metric for the same countries
    - Summarize differences
    - Decide which one to use (or take average)
2. Columns that report the same metric for different countries
    - Collapse into one column
'''
# ============================================================================
#### Read data
# ============================================================================
try:
  chickens_merged.shape
  print('> Intermediate data in memory. Recreating result data.')
  gbads_chickens_merged = chickens_merged.copy()
except NameError:
  print('> Intermediate data not found. Reading result data from file...')
  gbads_chickens_merged = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'gbads_chickens_merged.pkl.gz'))
  print('> Data frame loaded.')
datainfo(gbads_chickens_merged)

# ============================================================================
#### Utility functions
# ============================================================================
# This function is used inside apply() to fill in a new column with the first
# non-missing value from a set of candidate columns
# Example usage:
#   df['newcol'] = take_first_nonmissing(df ,CANDIDATE_COLS)
def take_first_nonmissing(
      INPUT_DF
      ,CANDIDATE_COLS      # List of strings: columns to search for non-missing value, in this order
   ):
   # Initialize new column with first candidate column
   OUTPUT_SERIES = INPUT_DF[CANDIDATE_COLS[0]]

   for CANDIDATE in CANDIDATE_COLS:       # For each candidate column...
      newcol_null = (OUTPUT_SERIES.isnull())                 # ...where new column is missing...
      OUTPUT_SERIES.loc[newcol_null] = INPUT_DF.loc[newcol_null ,CANDIDATE]    # ...fill with candidate

   return OUTPUT_SERIES

# ============================================================================
#### Key columns and basic cleanup
# ============================================================================
# Use FAO country and year as key
gbads_chickens_merged = gbads_chickens_merged.rename(
   columns={"fao_country":"country" ,"fao_year":"year"}
)

# Drop source-specific country and year columns and _merge check columns
dropcols = [i for i in list(gbads_chickens_merged) if '_country' in i]              # Source-specific country columns
dropcols = dropcols + ['euimp_reporter']                                            # Country column for EU import/export
dropcols = dropcols + [i for i in list(gbads_chickens_merged) if '_year' in i]      # Source-specific year columns
dropcols = dropcols + [i for i in list(gbads_chickens_merged) if '_merge' in i]     # Merge check columns
gbads_chickens_merged = gbads_chickens_merged.drop(
   columns=dropcols
   ,errors='ignore'   # If some columns have been dropped already
)

datainfo(gbads_chickens_merged)

# ============================================================================
#### Days on Feed
# ============================================================================
# Note Avg Days On Feed is a parameter in Dash, allowing user to select the value used for calcs
# Data entered here is only displayed as a reference in Dash
# Assembling this column from miscellaneous sources. URL's from Liverpool data organizer.
def create_acc_avgdaysonfeed(INPUT_ROW):
   if INPUT_ROW['country'].upper() == 'BRAZIL':
      OUTPUT = 43
      OUTPUT_SOURCE = 'https://www.embrapa.br/suinos-e-aves/cias/custos/calcule/planilha'
   elif INPUT_ROW['country'].upper() == 'CHINA':
      OUTPUT = 44
      OUTPUT_SOURCE = 'https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7142404/'
   elif INPUT_ROW['country'].upper() == 'FRANCE':
      OUTPUT = 37
      OUTPUT_SOURCE = INPUT_ROW['glb_source_y']
   elif INPUT_ROW['country'].upper() == 'GERMANY':
      OUTPUT = 34
      OUTPUT_SOURCE = 'https://www.dlg.org/de/landwirtschaft/themen/tierhaltung/gefluegel/dlg-merkblatt-406'
   elif INPUT_ROW['country'].upper() == 'INDIA':
      OUTPUT = 42
      OUTPUT_SOURCE = 'https://www.theglobalstatistics.com/chicks-rate-today/ '
   # elif INPUT_ROW['country'].upper() == 'ITALY':
   #     OUTPUT =
   #     OUTPUT_SOURCE = None
   elif INPUT_ROW['country'].upper() == 'NETHERLANDS':
      OUTPUT = 41
      OUTPUT_SOURCE = INPUT_ROW['glb_source_y']
   # elif INPUT_ROW['country'].upper() == 'POLAND':
   #     OUTPUT =
   #     OUTPUT_SOURCE = None
   # elif INPUT_ROW['country'].upper() == 'SPAIN':
   #     OUTPUT =
   #     OUTPUT_SOURCE = None
   elif INPUT_ROW['country'].upper() == 'UNITED KINGDOM':
      OUTPUT = 35
      OUTPUT_SOURCE = 'https://www.nfuonline.com/archive?treeid=139718'
   elif INPUT_ROW['country'].upper() == 'UNITED STATES OF AMERICA':
      # OUTPUT = 47
      # OUTPUT_SOURCE = 'https://www.nationalchickencouncil.org/statistic/us-broiler-performance/'
      # 47 days produces breed standard potential incompatible with data. Probably due to bimodality of US broilers. Picking a higer number for now.
      OUTPUT = 52
      OUTPUT_SOURCE = None
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_avgdaysonfeed' ,'acc_avgdaysonfeed_src']] = gbads_chickens_merged.apply(create_acc_avgdaysonfeed ,axis=1)
gbads_chickens_merged['acc_avgdaysonfeed'] = round(gbads_chickens_merged['acc_avgdaysonfeed'])

# ============================================================================
#### Imports/Exports
# ============================================================================
# Currently using data for each country as REPORTER, considering total imports and exports (partner = WORLD)
# Alternative (William Gilbert): treat data on EXPORTS as more reliable than data on IMPORTS

#!!! Note: most import/export data does not distinguish birds used for breeding from those
# used for fattening. We are focused on meat production, so only want to adjust head placed
# for net import of birds used for fattening!
# An approach: make an assumption about the proportion of imported birds used for breeding vs. fattening
# Currently applies to both chicks and adults
acc_prpn_netimports_forslaughter = 0.8

def create_acc_netimport_chicks(INPUT_ROW):
   # Eurostat
   if pd.notnull(INPUT_ROW['euimp_net_import_live_gallusdom_lte185g_head']):
      OUTPUT = INPUT_ROW['euimp_net_import_live_gallusdom_lte185g_head'] * acc_prpn_netimports_forslaughter
      OUTPUT_SOURCE = 'Eurostat'
   # UN Comtrade
   elif pd.notnull(INPUT_ROW['uncom_net_import_live_gallusdom_lte185g_head']):
      OUTPUT = INPUT_ROW['uncom_net_import_live_gallusdom_lte185g_head'] * acc_prpn_netimports_forslaughter
      OUTPUT_SOURCE = 'UN Comtrade'
   # USDA
   else:   #!!! Net imports are generally small compared to head slaughtered. For now, assuming zero for years with no data.
      OUTPUT = 0
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_netimport_chicks' ,'acc_netimport_chicks_src']] = gbads_chickens_merged.apply(create_acc_netimport_chicks ,axis=1)
gbads_chickens_merged['acc_netimport_chicks'] = round(gbads_chickens_merged['acc_netimport_chicks'])

def create_acc_netimport_adults(INPUT_ROW):
   # Eurostat
   if pd.notnull(INPUT_ROW['euimp_net_import_live_gallusdom_gt185g_head']):
      OUTPUT = INPUT_ROW['euimp_net_import_live_gallusdom_gt185g_head'] * acc_prpn_netimports_forslaughter
      OUTPUT_SOURCE = 'Eurostat'
   # UN Comtrade does not have data on mature birds
   # USDA does not have data on mature birds
   else:   #!!! Net imports are generally small compared to head slaughtered. For now, assuming zero for years with no data.
      OUTPUT = 0
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_netimport_adults' ,'acc_netimport_adults_src']] = gbads_chickens_merged.apply(create_acc_netimport_adults ,axis=1)
gbads_chickens_merged['acc_netimport_adults'] = round(gbads_chickens_merged['acc_netimport_adults'])

# ============================================================================
#### Head Placed
# ============================================================================
# Idea: all adjustments to head count are built into head placed:
# - Imports/exports of chicks
# - Imports/exports of mature birds
# - Changes in stocks

# Working approach/assumptions:
# - Chicks PLACED is already adjusted for net import of chicks
# - Chicks HATCHED must be adjusted for net import of chicks
# - Total Head Placed is Chicks Placed + Net Import of Mature Birds
# - Stock changes are negligible. Non-breeding broilers are not kept in stock (always slaughtered).

# For some countries (China, India) we do not have data on chicks placed but
# instead have mortality rates. We will back-calculate chicks placed from head
# slaughtered and mortality.

def create_acc_headplaced(INPUT_ROW):
   # if INPUT_ROW['country'].upper() == 'UNITED KINGDOM':
   #     OUTPUT = INPUT_ROW['ukgov_chicksplaced_broilers_thsdhd'] * 1000

   # Idea: rather than checking country names, check variables for missingness.
   # Check country-specific variables first (e.g. ukgov) then broader databases (e.g. Eurostat).
   # Advantage: if any years are missing from country-specific data they can be filled in with broader databases.

   # UK
   if pd.notnull(INPUT_ROW['ukgov_chicksplaced_broilers_thsdhd']):
      OUTPUT = INPUT_ROW['ukgov_chicksplaced_broilers_thsdhd'] * 1000 \
         + INPUT_ROW['acc_netimport_adults']
      OUTPUT_SOURCE = 'UK Gov'

   # US
   elif pd.notnull(INPUT_ROW['usda_chicksplaced_broilers_thsdhd']):
      OUTPUT = INPUT_ROW['usda_chicksplaced_broilers_thsdhd'] * 1000 \
         + INPUT_ROW['acc_netimport_adults']
      OUTPUT_SOURCE = 'USDA'

   # EU
   elif pd.notnull(INPUT_ROW['euro_chickshatched_broilers_thsdhd']):
      OUTPUT = INPUT_ROW['euro_chickshatched_broilers_thsdhd'] * 1000 \
         + INPUT_ROW['acc_netimport_chicks'] \
            + INPUT_ROW['acc_netimport_adults']
      OUTPUT_SOURCE = 'Eurostat'

   # Brazil
   elif pd.notnull(INPUT_ROW['brzl_chicksplaced_thsdhd']):   #!!! Assuming broilers
      OUTPUT = INPUT_ROW['brzl_chicksplaced_thsdhd'] * 1000 \
         + INPUT_ROW['acc_netimport_adults']
      OUTPUT_SOURCE = 'AviSite'

   # India
   elif pd.notnull(INPUT_ROW['india_chicksplaced_broilers_thsdhd']):
      OUTPUT = INPUT_ROW['india_chicksplaced_broilers_thsdhd'] * 1000 \
         + INPUT_ROW['acc_netimport_adults']
      OUTPUT_SOURCE = 'InfoMetrics'

   # China - special case. Back calculating from head slaughtered and average mortality.
   # From reference R39
   # https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7142404/
   # Mortality percentages: 6.92, 3.79, 3.26 giving average of 4.66
   elif INPUT_ROW['country'].upper() == 'CHINA':
      OUTPUT = INPUT_ROW['fao_slaughtered_chickens_thsdhd'] * 1000 * 1.0466  #!!! Assuming FAO is primarily broilers
      OUTPUT_SOURCE = ''

   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_headplaced' ,'acc_headplaced_src']] = gbads_chickens_merged.apply(create_acc_headplaced ,axis=1)
gbads_chickens_merged['acc_headplaced'] = round(gbads_chickens_merged['acc_headplaced'])

# ============================================================================
#### Head Slaughtered, Carcass & Live Weight
# ============================================================================
def create_acc_headslaughtered(INPUT_ROW):
   # if INPUT_ROW['country'].upper() == 'UNITED KINGDOM':
   #    OUTPUT = INPUT_ROW['ukgov_slaughter_broilers_thsdhd'] * 1000
   # UK
   if pd.notnull(INPUT_ROW['ukgov_slaughter_broilers_thsdhd']):
      OUTPUT = INPUT_ROW['ukgov_slaughter_broilers_thsdhd'] * 1000
      OUTPUT_SOURCE = 'UK Gov'
   # US
   elif pd.notnull(INPUT_ROW['usda_production_broilers_thsdhd']):
      OUTPUT = INPUT_ROW['usda_production_broilers_thsdhd'] * 1000
      OUTPUT_SOURCE = 'USDA'
   # Euro
   elif pd.notnull(INPUT_ROW['euro_sl_est_broilers_thsdhd']):
      OUTPUT = INPUT_ROW['euro_sl_est_broilers_thsdhd'] * 1000
      OUTPUT_SOURCE = 'Eurostat'
   # FAO
   elif pd.notnull(INPUT_ROW['fao_slaughtered_chickens_thsdhd']):   #!!! Assuming FAO is primarily broilers
      OUTPUT = INPUT_ROW['fao_slaughtered_chickens_thsdhd'] * 1000
      OUTPUT_SOURCE = 'FAO'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_headslaughtered' ,'acc_headslaughtered_src']] = gbads_chickens_merged.apply(create_acc_headslaughtered ,axis=1)
gbads_chickens_merged['acc_headslaughtered'] = round(gbads_chickens_merged['acc_headslaughtered'])

def create_acc_totalcarcweight_tonnes(INPUT_ROW):
   # if INPUT_ROW['country'].upper() == 'UNITED KINGDOM':
   #    OUTPUT = INPUT_ROW['ukgov_slaughter_broilers_carcwt_thsdtonnes'] * 1000
   if pd.notnull(INPUT_ROW['ukgov_slaughter_broilers_carcwt_thsdtonnes']):
      OUTPUT = INPUT_ROW['ukgov_slaughter_broilers_carcwt_thsdtonnes'] * 1000
      OUTPUT_SOURCE = 'UK Gov'
   elif pd.notnull(INPUT_ROW['usda_production_broilers_thsdtonnes']):
      OUTPUT = INPUT_ROW['usda_production_broilers_thsdtonnes'] * 1000
      OUTPUT_SOURCE = 'USDA'
   elif pd.notnull(INPUT_ROW['euro_sl_est_broilers_thsdtonne']):
      OUTPUT = INPUT_ROW['euro_sl_est_broilers_thsdtonne'] * 1000
      OUTPUT_SOURCE = 'Eurostat'
   elif pd.notnull(INPUT_ROW['fao_production_chickens_tonnes']):   #!!! Assuming broilers
      OUTPUT = INPUT_ROW['fao_production_chickens_tonnes']
      OUTPUT_SOURCE = 'FAO'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_totalcarcweight_tonnes' ,'acc_totalcarcweight_tonnes_src']] = gbads_chickens_merged.apply(create_acc_totalcarcweight_tonnes ,axis=1)

def create_acc_avgcarcweight_kg(INPUT_ROW):
   OUTPUT = INPUT_ROW['acc_totalcarcweight_tonnes'] * 1000 / INPUT_ROW['acc_headslaughtered']
   return OUTPUT
gbads_chickens_merged['acc_avgcarcweight_kg'] = gbads_chickens_merged.apply(create_acc_avgcarcweight_kg ,axis=1)

# Have data on live weight for some countries
# For others, calculate from carcass weight assuming an average yield
def create_acc_avgliveweight_kg(
      INPUT_ROW
      ,AVG_CARC_YIELD=0.695   # Number [0,1]: average carcass yield in kg meat per kg live weight
      ):
   # UK
   if pd.notnull(INPUT_ROW['ukgov_slaughter_broilers_avglivewt_kg']):
      OUTPUT = INPUT_ROW['ukgov_slaughter_broilers_avglivewt_kg']
      OUTPUT_SOURCE = 'UK Gov'
   else:
      # Don't want to assume a carcass yield at this point
      # OUTPUT = INPUT_ROW['acc_avgcarcweight_kg'] / AVG_CARC_YIELD
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_avgliveweight_kg' ,'acc_avgliveweight_kg_src']] = gbads_chickens_merged.apply(create_acc_avgliveweight_kg ,axis=1)

# ============================================================================
#### Feed Consumption
# ============================================================================
def create_acc_feedconsumption_tonnes(INPUT_ROW):
   # UK
   if pd.notnull(INPUT_ROW['ukfeed_production_broiler_chicken_compounds_tonnes']):
      #!!! Assuming feed produced in UK is consumed in UK
      OUTPUT = INPUT_ROW['ukfeed_production_broiler_chicken_compounds_tonnes']
      OUTPUT_SOURCE = 'AHDB'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_feedconsumption_tonnes' ,'acc_feedconsumption_tonnes_src']] = gbads_chickens_merged.apply(create_acc_feedconsumption_tonnes ,axis=1)

# The team is uncomfortable assuming feed production is the same as feed consumption
# Instead, back-calculate feed consumption from feed price and total expenditure
def create_acc_feedconsumption_tonnes_2(INPUT_ROW):
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])

def create_acc_fcr_live(INPUT_ROW):
   OUTPUT = np.nan
   OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
# gbads_chickens_merged[['acc_fcr_live' ,'acc_fcr_live_src']] = gbads_chickens_merged.apply(create_acc_fcr_live ,axis=1)

# Average feed intake per head
# How much was eaten by the animals that died? Related to phase of life that the mortality occurred.
# Head slaughtered is too small a denominator because some was eaten by animals that didn't make it to slaughter
# Head placed is too large a denominator because not all those animals were eating the whole time
def create_acc_avgfeedintake_adj_kgperhd(INPUT_ROW):
   # Create adjusted feed consumption by removing some proportion of feed consumed by head that died
   feedcnsm_headthatdied_prpn = 0.1  # [0,1]: proportion of total feed consumed by head that died
   adj_feedconsumption_tonnes = INPUT_ROW['acc_feedconsumption_tonnes'] * (1 - feedcnsm_headthatdied_prpn)
   OUTPUT = adj_feedconsumption_tonnes / INPUT_ROW['acc_headslaughtered'] * 1000
   return OUTPUT
# gbads_chickens_merged['acc_avgfeedintake_adj_kgperhd'] = gbads_chickens_merged.apply(create_acc_avgfeedintake_adj_kgperhd ,axis=1)

# ============================================================================
#### Producer Price and Feed Price
# ============================================================================
def addcol_constant_currency(INPUT_DF ,CURRENCY_COLUMN ,CPI_COLUMN):
    # Apply CPI ratio to get constant currency
    # Using method from https://www.census.gov/topics/income-poverty/income/guidance/current-vs-constant-dollars.html
    # Also explained here https://www.investopedia.com/terms/c/constantdollar.asp
    constant_currency = INPUT_DF[CURRENCY_COLUMN] * (100 / INPUT_DF[CPI_COLUMN])  # Will return constant currency for same year that CPI is indexed to
    return constant_currency

def create_acc_feedprice_usdpertonne(INPUT_ROW):
   # Eurostat
   if pd.notnull(INPUT_ROW['euro_feed_bulk_broilers_priceper100kg_localcrncy']):
      OUTPUT = INPUT_ROW['euro_feed_bulk_broilers_priceper100kg_localcrncy'] * 10 / INPUT_ROW['wb_exchangerate_lcuperusd']
      OUTPUT_SOURCE = 'Eurostat'
   # UK
   elif pd.notnull(INPUT_ROW['ukfeed_feedprice_poultry_gbppertonne_wtavg']):
      OUTPUT = INPUT_ROW['ukfeed_feedprice_poultry_gbppertonne_wtavg'] / INPUT_ROW['wb_exchangerate_lcuperusd']
      OUTPUT_SOURCE = 'AHDB'
   # USA
   # elif INPUT_ROW['country'].upper() == 'UNITED STATES OF AMERICA':
   #    OUTPUT = 350
   #    OUTPUT_SOURCE = 'Expert opinion'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_feedprice_usdpertonne' ,'acc_feedprice_usdpertonne_src']] = gbads_chickens_merged.apply(create_acc_feedprice_usdpertonne ,axis=1)
gbads_chickens_merged['acc_feedprice_usdpertonne_cnst2010'] = addcol_constant_currency(gbads_chickens_merged ,'acc_feedprice_usdpertonne' ,'wb_cpi_idx2010')

def create_acc_producerprice_usdperkgcarc(INPUT_ROW):
   # FAO
   if pd.notnull(INPUT_ROW['fao_producerprice_chickens_carcass_usdpertonne']):
      OUTPUT = INPUT_ROW['fao_producerprice_chickens_carcass_usdpertonne'] / 1000
      OUTPUT_SOURCE = 'FAO'
   # Eurostat
   # Want carcass price not live price!!
   # elif pd.notnull(INPUT_ROW['euro_chickens_live1stchoice_priceper100kg_euros']):
   #     OUTPUT = (INPUT_ROW['euro_chickens_live1stchoice_priceper100kg_euros'] / 100) / INPUT_ROW['wb_exchangerate_europerusd']
   #     OUTPUT_SOURCE = 'Eurostat'
   # US
   elif pd.notnull(INPUT_ROW['usda_pricereceived_broilers_dollarsperkg']):
      OUTPUT = INPUT_ROW['usda_pricereceived_broilers_dollarsperkg']
      OUTPUT_SOURCE = 'USDA'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_producerprice_usdperkgcarc' ,'acc_producerprice_usdperkgcarc_src']] = gbads_chickens_merged.apply(create_acc_producerprice_usdperkgcarc ,axis=1)
gbads_chickens_merged['acc_producerprice_usdperkgcarc_cnst2010'] = addcol_constant_currency(gbads_chickens_merged ,'acc_producerprice_usdperkgcarc' ,'wb_cpi_idx2010')

def create_acc_chickprice_usdperhd(INPUT_ROW):
   # William's cost spreadsheet
   if pd.notnull(INPUT_ROW['glb_chickprice_perhd_usd']):
      OUTPUT = INPUT_ROW['glb_chickprice_perhd_usd']
      OUTPUT_SOURCE = INPUT_ROW['glb_source_y']
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_chickprice_usdperhd' ,'acc_chickprice_usdperhd_src']] = gbads_chickens_merged.apply(create_acc_chickprice_usdperhd ,axis=1)
gbads_chickens_merged['acc_chickprice_usdperhd_cnst2010'] = addcol_constant_currency(gbads_chickens_merged ,'acc_chickprice_usdperhd' ,'wb_cpi_idx2010')

# ============================================================================
#### Costs
# ============================================================================
#!!! Note these are per kg live weight rather than carcass weight because that
# is how William calculated them.
def create_acc_feedcost_usdperkglive(INPUT_ROW):
   # William's cost spreadsheet
   if pd.notnull(INPUT_ROW['glb_feedcost_perkglive_usd']):
      OUTPUT = INPUT_ROW['glb_feedcost_perkglive_usd']
      OUTPUT_SOURCE = INPUT_ROW['glb_source_y']
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_feedcost_usdperkglive' ,'acc_feedcost_usdperkglive_src']] = gbads_chickens_merged.apply(create_acc_feedcost_usdperkglive ,axis=1)
gbads_chickens_merged['acc_feedcost_usdperkglive_cnst2010'] = addcol_constant_currency(gbads_chickens_merged ,'acc_feedcost_usdperkglive' ,'wb_cpi_idx2010')

def create_acc_chickcost_usdperkglive(INPUT_ROW):
   # William's cost spreadsheet
   if pd.notnull(INPUT_ROW['glb_chickcost_perkglive_usd']):
      OUTPUT = INPUT_ROW['glb_chickcost_perkglive_usd']
      OUTPUT_SOURCE = INPUT_ROW['glb_source_y']
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_chickcost_usdperkglive' ,'acc_chickcost_usdperkglive_src']] = gbads_chickens_merged.apply(create_acc_chickcost_usdperkglive ,axis=1)
gbads_chickens_merged['acc_chickcost_usdperkglive_cnst2010'] = addcol_constant_currency(gbads_chickens_merged ,'acc_chickcost_usdperkglive' ,'wb_cpi_idx2010')

def create_acc_laborcost_usdperkglive(INPUT_ROW):
   # William's cost spreadsheet
   if pd.notnull(INPUT_ROW['glb_laborcost_perkglive_usd']):
      OUTPUT = INPUT_ROW['glb_laborcost_perkglive_usd']
      OUTPUT_SOURCE = INPUT_ROW['glb_source_y']
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_laborcost_usdperkglive' ,'acc_laborcost_usdperkglive_src']] = gbads_chickens_merged.apply(create_acc_laborcost_usdperkglive ,axis=1)
gbads_chickens_merged['acc_laborcost_usdperkglive_cnst2010'] = addcol_constant_currency(gbads_chickens_merged ,'acc_laborcost_usdperkglive' ,'wb_cpi_idx2010')

def create_acc_landhousingcost_usdperkglive(INPUT_ROW):
   # William's cost spreadsheet
   if pd.notnull(INPUT_ROW['glb_landhousingcost_perkglive_usd']):
      OUTPUT = INPUT_ROW['glb_landhousingcost_perkglive_usd']
      OUTPUT_SOURCE = INPUT_ROW['glb_source_y']
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_landhousingcost_usdperkglive' ,'acc_landhousingcost_usdperkglive_src']] = gbads_chickens_merged.apply(create_acc_landhousingcost_usdperkglive ,axis=1)
gbads_chickens_merged['acc_landhousingcost_usdperkglive_cnst2010'] = addcol_constant_currency(gbads_chickens_merged ,'acc_landhousingcost_usdperkglive' ,'wb_cpi_idx2010')

def create_acc_medcost_usdperkglive(INPUT_ROW):
   # William's cost spreadsheet
   if pd.notnull(INPUT_ROW['glb_medicinecost_perkglive_usd']):
      OUTPUT = INPUT_ROW['glb_medicinecost_perkglive_usd']
      OUTPUT_SOURCE = INPUT_ROW['glb_source_y']
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_medcost_usdperkglive' ,'acc_medcost_usdperkglive_src']] = gbads_chickens_merged.apply(create_acc_medcost_usdperkglive ,axis=1)
gbads_chickens_merged['acc_medcost_usdperkglive_cnst2010'] = addcol_constant_currency(gbads_chickens_merged ,'acc_medcost_usdperkglive' ,'wb_cpi_idx2010')

def create_acc_othercost_usdperkglive(INPUT_ROW):
   # William's cost spreadsheet
   if pd.notnull(INPUT_ROW['glb_othercost_perkglive_usd']):
      OUTPUT = INPUT_ROW['glb_othercost_perkglive_usd']
      OUTPUT_SOURCE = INPUT_ROW['glb_source_y']
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_chickens_merged[['acc_othercost_usdperkglive' ,'acc_othercost_usdperkglive_src']] = gbads_chickens_merged.apply(create_acc_othercost_usdperkglive ,axis=1)
gbads_chickens_merged['acc_othercost_usdperkglive_cnst2010'] = addcol_constant_currency(gbads_chickens_merged ,'acc_othercost_usdperkglive' ,'wb_cpi_idx2010')

# ============================================================================
#### Datainfo
# ============================================================================
datainfo(gbads_chickens_merged)

#%% Checks

# Create a copy so I can add _check columns without cluttering original
gbads_chickens_merged_check = gbads_chickens_merged.copy()

# ============================================================================
#### Imports/Exports
# ============================================================================
# Eurostat vs. UNComtrade for European countries
# UNComtrade vs. USDA for USA

# ============================================================================
#### Head Placed
# ============================================================================
# Eurostat vs. UKGov for UK

# ============================================================================
#### Head Slaughtered, Carcass & Live Weight
# ============================================================================
# Can we assume FAO slaughter count is primarily broilers?
# Compare 4 measures of head slaughtered:
   # FAO (chickens)
   # Eurostat (chickens)
   # Eurostat (est. broilers)
   # UKgov (just for United Kingdom)

# euro_sl_est_broilers_thsdhd
# ukgov_slaughter_broilers_thsdhd (just for United Kingdom)

# euro_sl_est_broilers_thsdtonne
# ukgov_slaughter_broilers_carcwt_thsdtonnes (just for United Kingdom)

# ============================================================================
#### Mortality
# ============================================================================
# Note mortality calcs are done in BOD code

# ----------------------------------------------------------------------------
# List countries and years with mortality wrong sign
# ----------------------------------------------------------------------------
gbads_chickens_merged_check.eval(
   '''
   check_mortality_head = acc_headplaced - acc_headslaughtered
   '''
   ,inplace=True
)
check_negative_mortality = gbads_chickens_merged_check.query('check_mortality_head < 0')

# ----------------------------------------------------------------------------
# Spot check Brazil mortality
# ----------------------------------------------------------------------------
check_brazil = gbads_chickens_merged_check.loc[gbads_chickens_merged_check['country'].str.upper() == 'BRAZIL'].copy()

# Using FAO head slaughtered. Assuming this is primarily Broilers.
check_brazil.eval(
   '''
   calc_mortality_thsdhd = brzl_chicksplaced_thsdhd - fao_slaughtered_chickens_thsdhd
   calc_mortality_prpnplaced = calc_mortality_thsdhd / brzl_chicksplaced_thsdhd
   '''
   ,inplace=True
)

#%% Describe and Export

# ============================================================================
#### Export basic FAO and Eurostat columns
# ============================================================================
cols_fao = [i for i in list(gbads_chickens_merged) if 'fao_' in i]
cols_euro = [i for i in list(gbads_chickens_merged) if 'euro_' in i]

chickens_fao_euro = gbads_chickens_merged[['country' ,'year'] + cols_fao + cols_euro]

datainfo(chickens_fao_euro)
datadesc(chickens_fao_euro ,CHARACTERIZE_FOLDER)

# profile = chickens_fao_euro.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'chickens_fao_euro_profile.html'))

chickens_fao_euro.to_pickle(os.path.join(PRODATA_FOLDER ,'chickens_fao_euro.pkl.gz'))
chickens_fao_euro.to_csv(os.path.join(EXPDATA_FOLDER ,'chickens_fao_euro.csv'))

# ============================================================================
#### Export full data
# ============================================================================
datainfo(gbads_chickens_merged)
datadesc(gbads_chickens_merged ,CHARACTERIZE_FOLDER)

# profile = gbads_chickens_merged.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'gbads_chickens_merged_profile.html'))

gbads_chickens_merged.to_pickle(os.path.join(PRODATA_FOLDER ,'gbads_chickens_merged.pkl.gz'))
gbads_chickens_merged.to_csv(os.path.join(EXPDATA_FOLDER ,'gbads_chickens_merged.csv'))

# ============================================================================
#### Export version for Dash
# ============================================================================
# ----------------------------------------------------------------------------
# Select rows and columns
# ----------------------------------------------------------------------------
# Mar. 17: Only the United Kingdom has all BOD elements calculated
# gbads_chickens_merged_fordash = gbads_chickens_merged.loc[
#    gbads_chickens_merged['country'].str.upper() == 'UNITED KINGDOM'
# ]

# Mar. 23: Limit to rows that have necessary columns filled in for calculating BOD
necessary_columns_fordash = [
   'country'
   ,'year'
   ,'acc_headplaced'
   # ,'acc_avgdaysonfeed'   # Parameter
   ,'acc_headslaughtered'
   ,'acc_totalcarcweight_tonnes'
   ,'acc_avgcarcweight_kg'
   # ,'acc_avgliveweight_kg'   # Not used
]

#!!! Can limit columns at this point if you don't want to use them for Dash calcs or display to user

gbads_chickens_merged_fordash = gbads_chickens_merged.dropna(
    axis=0                        # 0 = drop rows, 1 = drop columns
    ,subset=necessary_columns_fordash      # List (opt): if dropping rows, only consider these columns in NA check
    ,how='any'                    # String: 'all' = drop rows / columns that have all missing values. 'any' = drop rows / columns that have any missing values.
).reset_index(drop=True)          # If dropping rows, reset index

# ----------------------------------------------------------------------------
# Export
# ----------------------------------------------------------------------------
datainfo(gbads_chickens_merged_fordash)
datadesc(gbads_chickens_merged_fordash ,CHARACTERIZE_FOLDER)

gbads_chickens_merged_fordash.to_pickle(os.path.join(DASH_DATA_FOLDER ,'gbads_chickens_merged_fordash.pkl.gz'))
