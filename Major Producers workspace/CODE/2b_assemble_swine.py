#%% About
'''
This merges all swine data, performing the same basic steps on each input file:
   - Standardizing country names
   - Adding a prefix to all columns indicating their source

This also performs intermediate processing for some data sets, such as calculating
net imports for import/export data.

The FAO table is used as the base onto which all others are merged.
'''
#%% Base table: FAO

# FAOstat contains all countries in scope (European 6, UK, US, China, India, Brazil)
# so use this as the base table. Merge others onto it.

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  fao_pigcombo.shape
  print('Data frame already in memory.')
except NameError:
  print('Data frame not found, reading from file...')
  fao_pigcombo = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'fao_pigcombo.pkl.gz'))
  print('Data frame loaded.')

datainfo(fao_pigcombo)

# ----------------------------------------------------------------------------
# Create a filter for countries in scope
# ----------------------------------------------------------------------------
countries_fao = list(fao_pigcombo['country'].unique())

# Give United Kingdom a snappier name
rename_fao_countries = {
   'United Kingdom of Great Britain and Northern Ireland':'United Kingdom'
   ,'Russian Federation':'Russia'
}
fao_pigcombo['country'].replace(rename_fao_countries ,inplace=True)

# This list can be any case. Will be converted to uppercase as needed.
fao_pigs_countries_inscope = [
   # European
   'Denmark'
   ,'France'
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
   ,'Russia'
]
fao_pigs_countries_inscope_upcase = [x.upper() for x in fao_pigs_countries_inscope]
_fao_pigs_inscope = (fao_pigcombo['country'].str.upper().isin(fao_pigs_countries_inscope_upcase))
check_countries_inscope = list(fao_pigcombo.loc[_fao_pigs_inscope ,'country'].unique())

# Add column to data
fao_pigcombo['country_inscope'] = _fao_pigs_inscope

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
# Limit to countries in scope
# Because all tables are merged onto this, this limits scope for all tables
fao_pigs_tomerge = fao_pigcombo.loc[_fao_pigs_inscope].copy()

# Create an explicit uppercase version of country
fao_pigs_tomerge['country_upcase'] = fao_pigs_tomerge['country'].str.upper()

# Add prefixes to all columns indicating source table
fao_pigs_tomerge = fao_pigs_tomerge.add_prefix('fao_')

datainfo(fao_pigs_tomerge)

#%% Merge Eurostat

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  euro_pigcombo.shape
  print('Data frame already in memory.')
except NameError:
  print('Data frame not found, reading from file...')
  euro_pigcombo = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'euro_pigcombo.pkl.gz'))
  print('Data frame loaded.')

datainfo(euro_pigcombo)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
euro_pigcombo_tomerge = euro_pigcombo.copy()

# Get country list
countries_euro = list(euro_pigcombo_tomerge['geo'].unique())

# Recode Eurostat codes to country names to match FAO
# For now, only care about countries in scope.
eurostat_country_recode = {
   'FR':'France'
   ,'PL':'Poland'
   ,'ES':'Spain'
   ,'IT':'Italy'
   ,'NL':'Netherlands'
   ,'DE':'Germany'
   ,'DK':'Denmark'
   ,'UK':'United Kingdom'
}
euro_pigcombo_tomerge['country'] = euro_pigcombo_tomerge['geo'].replace(eurostat_country_recode)
check_eurostat_country_recode = euro_pigcombo_tomerge['country'].unique()

# Create an explicit uppercase version of country for merging
euro_pigcombo_tomerge['country_upcase'] = euro_pigcombo_tomerge['country'].str.upper()

# Rename
euro_pigcombo_tomerge = euro_pigcombo_tomerge.rename(columns={'time_period':'year'})

# Add prefixes to all columns indicating source table
euro_pigcombo_tomerge = euro_pigcombo_tomerge.add_prefix('euro_')

datainfo(euro_pigcombo_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = 1   # Initialize

# Merge on country and year
pigs_merged = pd.merge(
   left=fao_pigs_tomerge
   ,right=euro_pigcombo_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['euro_country_upcase' ,'euro_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
pigs_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"pigs_merged_{mergenum} = pigs_merged.copy()")
exec(f"datainfo(pigs_merged_{mergenum})")

#%% Merge USDA PSD
# Pig meat and animal numbers

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  usda_psd_swinemeat_p.shape
  print('Data frame already in memory.')
except NameError:
  print('Data frame not found, reading from file...')
  usda_psd_swinemeat_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'usda_psd_swinemeat_p.pkl.gz'))
  print('Data frame loaded.')
datainfo(usda_psd_swinemeat_p)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
usda_psd_swinemeat_p_tomerge = usda_psd_swinemeat_p.copy()

# Get country list
countries_psd = list(usda_psd_swinemeat_p_tomerge['country'].unique())
psd_country_recode = {
   "United States":"United States of America"
}
usda_psd_swinemeat_p_tomerge['country'] = usda_psd_swinemeat_p_tomerge['country'].replace(psd_country_recode)

# Create an explicit uppercase version of country for merging
usda_psd_swinemeat_p_tomerge['country_upcase'] = usda_psd_swinemeat_p_tomerge['country'].str.upper()

# Add net imports
usda_psd_swinemeat_p_tomerge.eval(
   '''
   net_imports__1000_head_ = imports__1000_head_ - exports__1000_head_
   '''
   ,inplace=True
)

# Add prefixes to all columns indicating source table
usda_psd_swinemeat_p_tomerge = usda_psd_swinemeat_p_tomerge.add_prefix('psd_')

datainfo(usda_psd_swinemeat_p_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
pigs_merged = pd.merge(
   left=pigs_merged
   ,right=usda_psd_swinemeat_p_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['psd_country_upcase' ,'psd_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
pigs_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"pigs_merged_{mergenum} = pigs_merged.copy()")
exec(f"datainfo(pigs_merged_{mergenum})")

#%% Merge USDA Swine

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  usda_swine_inventory_p2.shape
  print('Data frame already in memory.')
except NameError:
  print('Data frame not found, reading from file...')
  usda_swine_inventory_p2 = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'usda_swine_inventory_p2.pkl.gz'))
  print('Data frame loaded.')
datainfo(usda_swine_inventory_p2)

try:
  usda_swine_othermetrics_p.shape
  print('Data frame already in memory.')
except NameError:
  print('Data frame not found, reading from file...')
  usda_swine_othermetrics_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'usda_swine_othermetrics_p.pkl.gz'))
  print('Data frame loaded.')
datainfo(usda_swine_othermetrics_p)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
# Subset columns
inventory_columns = [
   'country'
   ,'year'
   ,'hogs_breeding_inventory_first_of_mar'
   ,'hogs_breeding_inventory_first_of_jun'
   ,'hogs_breeding_inventory_first_of_sep'
   ,'hogs_breeding_inventory_first_of_dec'
]
usda_swine_inventory_p2_tomerge = usda_swine_inventory_p2[inventory_columns]

metric_columns = [
   'country'
   ,'year'

   ,'hogs_pigsperlitter'
   ,'hogs_slaughter_hd'   # Total slaughter = Commercial slaughter + On-farm slaughter, but on-farm slaughter is negligible
   ,'hogs_production_kg'
   ,'hogs_production_dol'
   ,'hogs_pricerecvd_dolpercwt'

   ,'hogs_deathloss_hd'
   ,'hogs_pigcrop_hd'
   ,'hogs_shipments_hd'

   ,'hogs_slaughter_commercial_hd'   # Commercial slaughter = federally inspected + non-federally inspected, but non-federally inspected is negligible
   ,'hogs_slaughter_commercial_live_kg'
   ,'hogs_slaughter_commercial_live_kgperhd'
   # ,'hogs_slaughter_commercial_fedinsp_hd'
   # ,'hogs_slaughter_commercial_fedinsp_carc_kgperhd'
   # ,'hogs_slaughter_commercial_fedinsp_live_kgperhd'

   ,'hogs_sows_slaughter_commercial_fedinsp_hd'
   ,'hogs_sows_slaughter_commercial_fedinsp_carc_kgperhd'
   ,'hogs_sows_pricerecvd_dolpercwt'
]
usda_swine_othermetrics_p_tomerge = usda_swine_othermetrics_p[metric_columns]

# Merge inventory and other metrics
usda_swine_tomerge = pd.merge(
   left=usda_swine_inventory_p2_tomerge
   ,right=usda_swine_othermetrics_p_tomerge
   ,on=['country' ,'year']
   ,how='outer'
   ,indicator='_merge_usda'
)
usda_swine_tomerge['_merge_usda'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Create an explicit uppercase version of country for merging
usda_swine_tomerge['country_upcase'] = usda_swine_tomerge['country'].str.upper()

# Add prefixes to all columns indicating source table
usda_swine_tomerge = usda_swine_tomerge.add_prefix('usda_')

datainfo(usda_swine_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
pigs_merged = pd.merge(
    left=pigs_merged
    ,right=usda_swine_tomerge
    ,left_on=['fao_country_upcase' ,'fao_year']
    ,right_on=['usda_country_upcase' ,'usda_year']
    ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
pigs_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"pigs_merged_{mergenum} = pigs_merged.copy()")
exec(f"datainfo(pigs_merged_{mergenum})")

#%% Merge Pig333 Production and Price
# According to the site, data for European countries comes from Eurostat, which we are already using.
# So I am only bringing in Pig333 data for the non-European countries in scope (Brazil, China, and Russia).

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  pig333_production_price.shape
  print('Data frame already in memory.')
except NameError:
  print('Data frame not found, reading from file...')
  pig333_production_price = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'pig333_production_price.pkl.gz'))
  print('Data frame loaded.')
datainfo(pig333_production_price)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
pig333_production_price_tomerge = pig333_production_price.copy()

# Create an explicit uppercase version of country for merging
pig333_production_price_tomerge['country_upcase'] = pig333_production_price_tomerge['country'].str.upper()

# Add prefixes to all columns indicating source table
pig333_production_price_tomerge = pig333_production_price_tomerge.add_prefix('pig333_')

datainfo(pig333_production_price_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
pigs_merged = pd.merge(
    left=pigs_merged
    ,right=pig333_production_price_tomerge
    ,left_on=['fao_country_upcase' ,'fao_year']
    ,right_on=['pig333_country_upcase' ,'pig333_year']
    ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
pigs_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"pigs_merged_{mergenum} = pigs_merged.copy()")
exec(f"datainfo(pigs_merged_{mergenum})")

#%% Merge Eurostat Imports/Exports

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  euro_impexp_swine_p.shape
  print('Data frame already in memory.')
except NameError:
  print('Data frame not found, reading from file...')
  euro_impexp_swine_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'euro_impexp_swine_p.pkl.gz'))
  print('Data frame loaded.')
datainfo(euro_impexp_swine_p)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
euro_impexp_swine_tomerge = euro_impexp_swine_p.copy()

# An approach: use records where our in-scope countries are REPORTERS
# Partner = All countries of the world
_row_selection = (euro_impexp_swine_tomerge['partner'] == 'WORLD')
print(f"> Selected {_row_selection.sum() :,} rows.")
euro_impexp_swine_tomerge = euro_impexp_swine_tomerge.loc[_row_selection].reset_index(drop=True)

# Subset to columns of interest
keep_cols_thisorder = [
   'reporter'
   ,'year'

   # Update: not using the purebred non-breeding category as that appears to be mislabeled
   # We only want domestic and non-domestic animals
   # Also not using my estimated head_touse because domestic and nondomestic variables have nonzero head counts
   ,'export_live_swine_dom_lt50kg_head'
   ,'import_live_swine_dom_lt50kg_head'
   ,'export_live_swine_dom_gte50kg_head'
   ,'import_live_swine_dom_gte50kg_head'

   ,'export_live_swine_nondom_lt50kg_head'
   ,'import_live_swine_nondom_lt50kg_head'
   ,'export_live_swine_nondom_gte50kg_head'
   ,'import_live_swine_nondom_gte50kg_head'

   # ,'export_live_swine_dom_lt50kg_head_touse'
   # ,'import_live_swine_dom_lt50kg_head_touse'
   # ,'export_live_swine_dom_gte50kg_head_touse'
   # ,'import_live_swine_dom_gte50kg_head_touse'

   # ,'export_live_swine_nondom_lt50kg_head_touse'
   # ,'import_live_swine_nondom_lt50kg_head_touse'
   # ,'export_live_swine_nondom_gte50kg_head_touse'
   # ,'import_live_swine_nondom_gte50kg_head_touse'

   # ,'export_live_swine_purebred_lt50kg_head_touse'
   # ,'import_live_swine_purebred_lt50kg_head_touse'
   # ,'export_live_swine_purebred_gte50kg_head_touse'
   # ,'import_live_swine_purebred_gte50kg_head_touse'

   # Import and export of breeding sows
   ,'export_live_sows_dom_nongilt_gte160kg_head'
   ,'import_live_sows_dom_nongilt_gte160kg_head'

   ,'export_live_swine_purebred_breeding_head'
   ,'import_live_swine_purebred_breeding_head'
]
euro_impexp_swine_tomerge = euro_impexp_swine_tomerge.loc[: ,keep_cols_thisorder]

# Calculate net imports
euro_impexp_swine_tomerge.eval(
   '''
   net_import_live_swine_dom_lt50kg_head = import_live_swine_dom_lt50kg_head - export_live_swine_dom_lt50kg_head
   net_import_live_swine_dom_gte50kg_head = import_live_swine_dom_gte50kg_head - export_live_swine_dom_gte50kg_head

   net_import_live_swine_nondom_lt50kg_head = import_live_swine_nondom_lt50kg_head - export_live_swine_nondom_lt50kg_head
   net_import_live_swine_nondom_gte50kg_head = import_live_swine_nondom_gte50kg_head - export_live_swine_nondom_gte50kg_head

   net_import_live_swine_nonbreeding_lt50kg_head = net_import_live_swine_dom_lt50kg_head + net_import_live_swine_nondom_lt50kg_head
   net_import_live_swine_nonbreeding_gte50kg_head = net_import_live_swine_dom_gte50kg_head + net_import_live_swine_nondom_gte50kg_head

   net_import_live_sows_dom_nongilt_gte160kg_head = import_live_sows_dom_nongilt_gte160kg_head - export_live_sows_dom_nongilt_gte160kg_head
   net_import_live_swine_purebred_breeding_head = import_live_swine_purebred_breeding_head - export_live_swine_purebred_breeding_head
   '''
   # net_import_live_swine_purebred_lt50kg_head = import_live_swine_purebred_lt50kg_head_touse - export_live_swine_purebred_lt50kg_head_touse
   # net_import_live_swine_purebred_gte50kg_head = import_live_swine_purebred_gte50kg_head_touse - export_live_swine_purebred_gte50kg_head_touse

   ,inplace=True
)

# Recode country
countries = list(euro_impexp_swine_tomerge['reporter'].unique())
recode_country = {
   'Denmark':'Denmark'
   ,"France (incl. Saint Barthélemy 'BL' -> 2012; incl. French Guiana 'GF', Guadeloupe 'GP', Martinique 'MQ', Réunion 'RE' from 1997; incl. Mayotte 'YT' from 2014)":'France'
   ,"Germany (incl. German Democratic Republic 'DD' from 1991)":'Germany'
   ,"Italy (incl. San Marino 'SM' -> 1993)":'Italy'
   ,'Netherlands':'Netherlands'
   ,'Poland':'Poland'
   ,"Spain (incl. Canary Islands 'XB' from 1997)":'Spain'
   ,'United Kingdom':'United Kingdom'
}
euro_impexp_swine_tomerge['country'] = euro_impexp_swine_tomerge['reporter'].replace(recode_country)
euro_impexp_swine_tomerge['country_upcase'] = euro_impexp_swine_tomerge['country'].str.upper()
check_euro_impexp_swine_tomerge_countries = list(euro_impexp_swine_tomerge['country'].unique())

# Add prefix to all columns indicating source table
euro_impexp_swine_tomerge = euro_impexp_swine_tomerge.add_prefix('euimp_')

datainfo(euro_impexp_swine_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
pigs_merged = pd.merge(
    left=pigs_merged
    ,right=euro_impexp_swine_tomerge
    ,left_on=['fao_country_upcase' ,'fao_year']
    ,right_on=['euimp_country_upcase' ,'euimp_year']
    ,how='left'                        # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'     # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
pigs_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"pigs_merged_{mergenum} = pigs_merged.copy()")
exec(f"datainfo(pigs_merged_{mergenum})")

#%% Merge UN Comtrade Imports/Exports

#!!! This data uses HS commodity codes like the UDSA GATS data, but does not go to the same level of detail.
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
   net_import_live_swine_lt50kg_head = import_live_swine_lt50kg_number_of_items - export_live_swine_lt50kg_number_of_items
   net_import_live_swine_gte50kg_head = import_live_swine_gte50kg_number_of_items - export_live_swine_gte50kg_number_of_items
   '''
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

# Drop columns related to chickens
chickencols = [col for col in list(uncomtrade_tomerge) if 'chickens' in col]
uncomtrade_tomerge = uncomtrade_tomerge.drop(columns=chickencols)

# Add prefix to all columns indicating source table
uncomtrade_tomerge = uncomtrade_tomerge.add_prefix('uncom_')

datainfo(uncomtrade_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
pigs_merged = pd.merge(
   left=pigs_merged
   ,right=uncomtrade_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['uncom_country_upcase' ,'uncom_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
pigs_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"pigs_merged_{mergenum} = pigs_merged.copy()")
exec(f"datainfo(pigs_merged_{mergenum})")

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
  uk_feedproduction_selectpig_m_p.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  uk_feedproduction_selectpig_m_p = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'uk_feedproduction_selectpig_m_p.pkl.gz'))
  print('> Data frame loaded.')
datainfo(uk_feedproduction_selectpig_m_p)

uk_feedproduction = uk_feedproduction_selectpig_m_p.copy()

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
            'production_link_early_grower_feed_thsdtonnes'
            ,'production_pig_breeding_compounds_thsdtonnes'
            ,'production_pig_finishing_compounds_thsdtonnes'
            ,'production_pig_growing_compounds_thsdtonnes'
            ,'production_pig_protein_concentrates_thsdtonnes'
            ,'production_pig_starters_and_creep_feed_thsdtonnes'
            ,'production_total_pig_feed_thsdtonnes'
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
datainfo(uk_feedprice_withprod)

# ----------------------------------------------------------------------------
# Get average price per year, weighting each quarter by production
# ----------------------------------------------------------------------------
# Create weighted prices quarterly
# Prices are reported as average for the species, so assume they include all pig feeds
uk_feedprice_withprod.eval(
   '''
   production_total_pig_feed_tonnes = production_total_pig_feed_thsdtonnes * 1000
   feedcost_pig_gbp = feedprice_pig_gbppertonne * production_total_pig_feed_tonnes
   '''
   ,inplace=True
)
datainfo(uk_feedprice_withprod)

# Sum dollars and production to yearly
uk_feedprice_withprod_agg = uk_feedprice_withprod.pivot_table(
   index='year'           # Column(s) to make new index
   ,values=[         # Column(s) to aggregate
      'production_total_pig_feed_tonnes'
      ,'feedcost_pig_gbp'

      ,'production_link_early_grower_feed_thsdtonnes'
      ,'production_pig_breeding_compounds_thsdtonnes'
      ,'production_pig_finishing_compounds_thsdtonnes'
      ,'production_pig_growing_compounds_thsdtonnes'
      ,'production_pig_protein_concentrates_thsdtonnes'
      ,'production_pig_starters_and_creep_feed_thsdtonnes'
      ,'production_total_pig_feed_thsdtonnes'
   ]
   ,aggfunc='sum'                  # Aggregate function to use. Can pass list or dictionary {'colname':'function'}. See numpy functions https://docs.scipy.org/doc/numpy/reference/routines.statistics.html
)
uk_feedprice_withprod_agg.reset_index(inplace=True)     # If pivoting changed columns to indexes, change them back

# Calculate average weighted price
uk_feedprice_withprod_agg.eval(
   '''
   feedprice_pig_gbppertonne_wtavg = feedcost_pig_gbp / production_total_pig_feed_tonnes
   '''
   ,inplace=True
)
uk_feedprice_withprod_agg = uk_feedprice_withprod_agg.drop(columns=['production_total_pig_feed_tonnes'])
datainfo(uk_feedprice_withprod_agg)

# ----------------------------------------------------------------------------
# Feed calcs
# ----------------------------------------------------------------------------
# Create total feed without Breeder feed
uk_feedprice_withprod_agg.eval(
   '''
   production_total_pig_feed_exclbreeding_thsdtonnes = production_total_pig_feed_thsdtonnes - production_pig_breeding_compounds_thsdtonnes
   '''
   ,inplace=True
)

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
pigs_merged = pd.merge(
    left=pigs_merged
    ,right=uk_feedprice_withprod_agg_tomerge
    ,left_on=['fao_country_upcase' ,'fao_year']
    ,right_on=['ukfeed_country_upcase' ,'ukfeed_year']
    ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
pigs_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"pigs_merged_{mergenum} = pigs_merged.copy()")
exec(f"datainfo(pigs_merged_{mergenum})")

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

wb_infl_exchg_tomerge = wb_infl_exchg_gdp.copy()

# ----------------------------------------------------------------------------
# Self-merge an EU country to get Euros per USD as a separate column
# ----------------------------------------------------------------------------
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
# Self-merge UK to get GBP per USD as a separate column
# ----------------------------------------------------------------------------
_country_gbp = (wb_infl_exchg_tomerge['country'].str.upper() == 'UNITED KINGDOM')
wb_infl_exchg_tomerge = pd.merge(
   left=wb_infl_exchg_tomerge
   ,right=wb_infl_exchg_tomerge.loc[_country_gbp ,['year' ,'exchangerate_lcuperusd']]
   ,on='year'
   ,how='left'
   ,suffixes=('', '_gbp')
)
wb_infl_exchg_tomerge = wb_infl_exchg_tomerge.rename(columns={'exchangerate_lcuperusd_gbp':'exchangerate_gbpperusd'})

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
pigs_merged = pd.merge(
    left=pigs_merged
    ,right=wb_infl_exchg_tomerge
    ,left_on=['fao_country_upcase' ,'fao_year']
    ,right_on=['wb_country_upcase' ,'wb_year']
    ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
pigs_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"pigs_merged_{mergenum} = pigs_merged.copy()")
exec(f"datainfo(pigs_merged_{mergenum})")

#%% Merge interPIG

# ----------------------------------------------------------------------------
# Read Pickled data
# ----------------------------------------------------------------------------
try:
  interpig_combo.shape
  print('> Data frame already in memory.')
except NameError:
  print('> Data frame not found, reading from file...')
  interpig_combo = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'interpig_combo.pkl.gz'))
  print('> Data frame loaded.')
datainfo(interpig_combo)

# ----------------------------------------------------------------------------
# Reconcile countries
# ----------------------------------------------------------------------------
# For countries that report multiple segments, take average (weighted appropriately)
# Brazil (MT and SC)
# Relative weight for Brazil states is unknown, so taking simple average
_brazil = (interpig_combo['country_upcase'].str.contains('BRA' ,case=False ,na=False))
interpig_brazil = interpig_combo.loc[_brazil]
interpig_brazil_mean = interpig_combo.loc[_brazil].groupby('year').mean()
interpig_brazil_mean = interpig_brazil_mean.reset_index()
interpig_brazil_mean['country_upcase'] = 'BRA'

# Great Britain (indoor and outdoor)
# Great Britain is 60% indoor, 40% outdoor. For now, using simple average.
_gb = (interpig_combo['country_upcase'].str.contains('GB' ,case=False ,na=False))
interpig_gb = interpig_combo.loc[_gb]
interpig_gb_mean = interpig_combo.loc[_gb].groupby('year').mean()
interpig_gb_mean = interpig_gb_mean.reset_index()
interpig_gb_mean['country_upcase'] = 'GB'

# Append averages to data and drop original rows
interpig_combo_tomerge = pd.concat(
   [interpig_combo.loc[~ (_brazil | _gb)]
    ,interpig_brazil_mean
    ,interpig_gb_mean
    ]
   ,axis=0
   ,join='outer'
)

# ----------------------------------------------------------------------------
#### Feed consumption calcs
# interPIG is the only source with feed consumption data, so I'm doing related
# calcs here.
# ----------------------------------------------------------------------------
#!!! FCR_carc uses overall average feed price (sow, rearer, and finisher),
# although it does appear to be weighted by volume (weighted towards finisher).
interpig_combo_tomerge.eval(
   '''
   fcr_carc = feed_gbpperkgcarc / (averagefarmfeedprice_gbppertonne / 1000)

   feedperhead_kg = fcr_carc * averagecarcaseweightcoldkg
   totalfeed_kg = feedperhead_kg * annualpigslaughterings000head_ * 1000

   totalfeed_kg_2 = fcr_carc * pigmeatproduction000tonnes_ * 1000000
   feedperhead_kg_2 = totalfeed_kg / (annualpigslaughterings000head_ * 1000)

   fcr_live = feedperhead_kg / averageliveweightatslaughterkg

   totalfeed_tonnes = totalfeed_kg / 1000
   '''
   ,inplace=True
)

# Check feedperhead and totalfeed calculated both ways
# They match
snplt = sns.relplot(
    data=interpig_combo_tomerge
    ,x='feedperhead_kg'
    ,y='feedperhead_kg_2'
)
snplt = sns.relplot(
    data=interpig_combo_tomerge
    ,x='totalfeed_kg'
    ,y='totalfeed_kg_2'
)

# ----------------------------------------------------------------------------
# Prep for merge
# ----------------------------------------------------------------------------
# Get country list
countries_interpig = list(interpig_combo_tomerge['country_upcase'].unique())

# Recode country names to match FAO
interpig_country_recode = {
   # 'AUS':''
   # ,'BEL':''
   'BRA':'Brazil'
   # ,'CAN':''
   ,'DEN':'Denmark'
   # ,'EU AVERAGE':''
   # ,'FIN':''
   ,'FRA':'France'
   ,'GB':'United Kingdom'   #!!! Because all interPIG metrics are averages or ratios, I am assuming the values for GB apply to all of the UK.
   ,'GER':'Germany'
   # ,'HUN':''
   # ,'IRE':''
   ,'ITA':'Italy'
   ,'NL':'Netherlands'
   ,'SPA':'Spain'
   # ,'SWE':''
   ,'USA':'United States of America'
}
interpig_combo_tomerge['country_upcase'] = interpig_combo_tomerge['country_upcase'].replace(interpig_country_recode)

# Ensure country is still upper case
interpig_combo_tomerge['country_upcase'] = interpig_combo_tomerge['country_upcase'].str.upper()

# Add prefixes to all columns indicating source table
interpig_combo_tomerge = interpig_combo_tomerge.add_prefix('ip_')

datainfo(interpig_combo_tomerge)

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
mergenum = mergenum + 1

# Merge on country and year
pigs_merged = pd.merge(
   left=pigs_merged
   ,right=interpig_combo_tomerge
   ,left_on=['fao_country_upcase' ,'fao_year']
   ,right_on=['ip_country_upcase' ,'ip_year']
   ,how='left'                              # String: 'inner', 'outer', 'left', 'right', or 'cross'. Note: 'left' and 'right' are equivalent to SQL 'left outer' and 'right outer' joins.
   ,indicator=f'_merge_{mergenum}'          # True: Add column '_merge' indicating which table each row came from. String: e.g. '_merge1' to give this column a custom name.
)
pigs_merged[f'_merge_{mergenum}'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Save this merged version as separate dataframe
exec(f"pigs_merged_{mergenum} = pigs_merged.copy()")
exec(f"datainfo(pigs_merged_{mergenum})")

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
  pigs_merged.shape
  print('> Intermediate data in memory. Recreating result data.')
  gbads_pigs_merged = pigs_merged.copy()
except NameError:
  print('> Intermediate data not found. Reading result data from file...')
  gbads_pigs_merged = pd.read_pickle(os.path.join(PRODATA_FOLDER ,'gbads_pigs_merged.pkl.gz'))
  print('> Data frame loaded.')
datainfo(gbads_pigs_merged)

# ============================================================================
#### Key columns and basic cleanup
# ============================================================================
# Use FAO country and year as key
gbads_pigs_merged = gbads_pigs_merged.rename(
   columns={"fao_country":"country" ,"fao_year":"year"}
)

# Drop source-specific country and year columns and _merge check columns
dropcols = [i for i in list(gbads_pigs_merged) if '_country' in i]              # Source-specific country columns
dropcols = dropcols + ['euimp_reporter']                                        # Country column for EU import/export
dropcols = dropcols + [i for i in list(gbads_pigs_merged) if '_year' in i]      # Source-specific year columns
dropcols = dropcols + [i for i in list(gbads_pigs_merged) if '_merge' in i]     # Merge check columns
gbads_pigs_merged = gbads_pigs_merged.drop(
   columns=dropcols
   ,errors='ignore'   # If some columns have been dropped already
)

datainfo(gbads_pigs_merged)

# ============================================================================
#### Imports/Exports
# ============================================================================
def create_acc_netimport_lt50kg(INPUT_ROW):
   # Eurostat
   if pd.notnull(INPUT_ROW['euimp_net_import_live_swine_nonbreeding_lt50kg_head']):
      OUTPUT = INPUT_ROW['euimp_net_import_live_swine_nonbreeding_lt50kg_head']
      OUTPUT_SOURCE = 'Eurostat'
   # UN Comtrade
   elif pd.notnull(INPUT_ROW['uncom_net_import_live_swine_lt50kg_head']):
      OUTPUT = INPUT_ROW['uncom_net_import_live_swine_lt50kg_head']
      OUTPUT_SOURCE = 'UN Comtrade'
   # USDA FAS
   #!!! Note FAS does not distinguish weight categories
   elif pd.notnull(INPUT_ROW['psd_net_imports__1000_head_']):
      OUTPUT = INPUT_ROW['psd_net_imports__1000_head_']
      OUTPUT_SOURCE = 'USDA FAS'
   else:
      OUTPUT = 0   #!!! If no data, assume negligible
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_netimport_lt50kg' ,'acc_netimport_lt50kg_src']] = gbads_pigs_merged.apply(create_acc_netimport_lt50kg ,axis=1)
gbads_pigs_merged['acc_netimport_lt50kg'] = round(gbads_pigs_merged['acc_netimport_lt50kg'])

def create_acc_netimport_gte50kg(INPUT_ROW):
   # Eurostat
   if pd.notnull(INPUT_ROW['euimp_net_import_live_swine_nonbreeding_gte50kg_head']):
      OUTPUT = INPUT_ROW['euimp_net_import_live_swine_nonbreeding_gte50kg_head']
      OUTPUT_SOURCE = 'Eurostat'
   # UN Comtrade
   elif pd.notnull(INPUT_ROW['uncom_net_import_live_swine_gte50kg_head']):
      OUTPUT = INPUT_ROW['uncom_net_import_live_swine_gte50kg_head']
      OUTPUT_SOURCE = 'UN Comtrade'
   else:
      OUTPUT = 0   #!!! If no data, assume negligible
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_netimport_gte50kg' ,'acc_netimport_gte50kg_src']] = gbads_pigs_merged.apply(create_acc_netimport_gte50kg ,axis=1)
gbads_pigs_merged['acc_netimport_gte50kg'] = round(gbads_pigs_merged['acc_netimport_gte50kg'])

# ============================================================================
#### Head Weaned, Placed
# ============================================================================
# Estimate weaned pigs per year from number of sows, average litter size, and prewean mortality
#!!! Have data for some countries. For the rest, use averages specified here.
est_litters_persow_peryear = 2.3
est_avg_litter_size = 12
est_prewean_mortality = 0.14     # [0,1] Proportion of piglets that die before weaning

def create_acc_breedingsows(INPUT_ROW):
   # US
   if pd.notnull(INPUT_ROW['usda_hogs_breeding_inventory_first_of_jun']):
      OUTPUT = INPUT_ROW['usda_hogs_breeding_inventory_first_of_jun']
      OUTPUT_SOURCE = 'USDA'
   # EU
   elif pd.notnull(INPUT_ROW['euro_breedingsows_gte50kg_jun_thsdhd']):
      OUTPUT = INPUT_ROW['euro_breedingsows_gte50kg_jun_thsdhd'] * 1000
      OUTPUT_SOURCE = 'Eurostat'
   # USDA FAS
   elif pd.notnull(INPUT_ROW['psd_sow_beginning_stocks__1000_head_']):
      OUTPUT = INPUT_ROW['psd_sow_beginning_stocks__1000_head_'] * 1000
      OUTPUT_SOURCE = 'USDA FAS'
   # InterPIG
   elif pd.notnull(INPUT_ROW['ip_breedingsownumbers000head_']):
      OUTPUT = INPUT_ROW['ip_breedingsownumbers000head_'] * 1000
      OUTPUT_SOURCE = 'interPIG'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_breedingsows' ,'acc_breedingsows_src']] = gbads_pigs_merged.apply(create_acc_breedingsows ,axis=1)
gbads_pigs_merged['acc_breedingsows'] = round(gbads_pigs_merged['acc_breedingsows'])

def create_acc_litters_persow_peryear(INPUT_ROW):
   if pd.notnull(INPUT_ROW['ip_litterssowyear']):
      OUTPUT = INPUT_ROW['ip_litterssowyear']   # If present in data
      OUTPUT_SOURCE = 'interPIG'
   else:
      OUTPUT = est_litters_persow_peryear
      OUTPUT_SOURCE = 'Average'
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_litters_persow_peryear' ,'acc_litters_persow_peryear_src']] = gbads_pigs_merged.apply(create_acc_litters_persow_peryear ,axis=1)

def create_acc_pigsperlitter(INPUT_ROW):
   # US
   if pd.notnull(INPUT_ROW['usda_hogs_pigsperlitter']):
      OUTPUT = INPUT_ROW['usda_hogs_pigsperlitter']
      OUTPUT_SOURCE = 'USDA'
   else:
      OUTPUT = est_avg_litter_size
      OUTPUT_SOURCE = 'Average'
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_pigsperlitter' ,'acc_pigsperlitter_src']] = gbads_pigs_merged.apply(create_acc_pigsperlitter ,axis=1)

def create_acc_headfarrowed(INPUT_ROW):
    # US
    if pd.notnull(INPUT_ROW['usda_hogs_pigsperlitter']):
      OUTPUT = INPUT_ROW['acc_breedingsows'] \
          * INPUT_ROW['usda_hogs_pigsperlitter'] \
            * INPUT_ROW['acc_litters_persow_peryear']
      OUTPUT_SOURCE = 'USDA'
    # EU
    elif pd.notnull(INPUT_ROW['euro_breedingsows_gte50kg_jun_thsdhd']):
      OUTPUT = INPUT_ROW['euro_breedingsows_gte50kg_jun_thsdhd'] * 1000 \
          * est_avg_litter_size \
            * INPUT_ROW['acc_litters_persow_peryear']
      OUTPUT_SOURCE = 'Eurostat'
    # USDA FAS
    elif pd.notnull(INPUT_ROW['psd_sow_beginning_stocks__1000_head_']):
      OUTPUT = INPUT_ROW['psd_sow_beginning_stocks__1000_head_'] * 1000 \
          * est_avg_litter_size \
            * INPUT_ROW['acc_litters_persow_peryear']
      OUTPUT_SOURCE = 'USDA FAS'
    else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
    return pd.Series([OUTPUT ,OUTPUT_SOURCE])
# No longer using head farrowed
# gbads_pigs_merged[['acc_headfarrowed' ,'acc_headfarrowed_src']] = gbads_pigs_merged.apply(create_acc_headfarrowed ,axis=1)
# gbads_pigs_merged['acc_headfarrowed'] = round(gbads_pigs_merged['acc_headfarrowed'])

def create_acc_headweaned(INPUT_ROW):
   if pd.notnull(INPUT_ROW['ip_pigsweanedsowyear']):
      OUTPUT = INPUT_ROW['acc_breedingsows'] * INPUT_ROW['ip_pigsweanedsowyear']
   else:
      OUTPUT = INPUT_ROW['acc_breedingsows'] * INPUT_ROW['acc_litters_persow_peryear'] \
         * INPUT_ROW['acc_pigsperlitter'] * (1 - est_prewean_mortality)
   return OUTPUT
gbads_pigs_merged['acc_headweaned'] = gbads_pigs_merged.apply(create_acc_headweaned ,axis=1)
gbads_pigs_merged['acc_headweaned'] = round(gbads_pigs_merged['acc_headweaned'])

# Idea: all adjustments to head count are built into head placed:
# - Imports/exports of piglets
# - Imports/exports of mature pigs
# - Changes in stocks
def create_acc_headplaced(INPUT_ROW):
   OUTPUT = INPUT_ROW['acc_headweaned'] + INPUT_ROW['acc_netimport_lt50kg'] + INPUT_ROW['acc_netimport_gte50kg']
   return OUTPUT
gbads_pigs_merged['acc_headplaced'] = gbads_pigs_merged.apply(create_acc_headplaced ,axis=1)
gbads_pigs_merged['acc_headplaced'] = round(gbads_pigs_merged['acc_headplaced'])

# ============================================================================
#### Head Slaughtered, Carcass & Live Weight
# ============================================================================
def create_acc_headslaughtered(INPUT_ROW):
   # FAO
   if pd.notnull(INPUT_ROW['fao_slaughtered_pigs_hd']):
      OUTPUT = INPUT_ROW['fao_slaughtered_pigs_hd']
      OUTPUT_SOURCE = 'FAO'
   # Eurostat
   elif pd.notnull(INPUT_ROW['euro_sl_pigmeat_thsdhd']):
      OUTPUT = INPUT_ROW['euro_sl_pigmeat_thsdhd'] * 1000
      OUTPUT_SOURCE = 'Eurostat'
   # interPIG
   elif pd.notnull(INPUT_ROW['ip_annualpigslaughterings000head_']):
      OUTPUT = INPUT_ROW['ip_annualpigslaughterings000head_'] * 1000
      OUTPUT_SOURCE = 'interPIG'
   # US
   elif pd.notnull(INPUT_ROW['usda_hogs_slaughter_hd']):
      OUTPUT = INPUT_ROW['usda_hogs_slaughter_hd']
      OUTPUT_SOURCE = 'USDA'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_headslaughtered' ,'acc_headslaughtered_src']] = gbads_pigs_merged.apply(create_acc_headslaughtered ,axis=1)
gbads_pigs_merged['acc_headslaughtered'] = round(gbads_pigs_merged['acc_headslaughtered'])

def create_acc_totalcarcweight_tonnes(INPUT_ROW):
   # FAO
   if pd.notnull(INPUT_ROW['fao_production_pigs_tonnes']):
      OUTPUT = INPUT_ROW['fao_production_pigs_tonnes']
      OUTPUT_SOURCE = 'FAO'
   # Eurostat
   elif pd.notnull(INPUT_ROW['euro_sl_pigmeat_thsdtonne']):
      OUTPUT = INPUT_ROW['euro_sl_pigmeat_thsdtonne'] * 1000
      OUTPUT_SOURCE = 'Eurostat'
   # interPIG
   elif pd.notnull(INPUT_ROW['ip_pigmeatproduction000tonnes_']):
      OUTPUT = INPUT_ROW['ip_pigmeatproduction000tonnes_'] * 1000
      OUTPUT_SOURCE = 'interPIG'
   # US
   elif pd.notnull(INPUT_ROW['usda_hogs_production_kg']):
      OUTPUT = INPUT_ROW['usda_hogs_production_kg'] / 1000
      OUTPUT_SOURCE = 'USDA'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_totalcarcweight_tonnes' ,'acc_totalcarcweight_tonnes_src']] = gbads_pigs_merged.apply(create_acc_totalcarcweight_tonnes ,axis=1)

def create_acc_avgcarcweight_kg(INPUT_ROW):
   OUTPUT = INPUT_ROW['acc_totalcarcweight_tonnes'] * 1000 / INPUT_ROW['acc_headslaughtered']
   return OUTPUT
gbads_pigs_merged['acc_avgcarcweight_kg'] = gbads_pigs_merged.apply(create_acc_avgcarcweight_kg ,axis=1)

def create_acc_avgliveweight_kg(
      INPUT_ROW
      # ,AVG_CARC_YIELD=0.75   # Float [0,1]: average carcass yield in kg meat per kg live weight
      ):
   # interPIG
   if pd.notnull(INPUT_ROW['ip_averageliveweightatslaughterkg']):
      OUTPUT = INPUT_ROW['ip_averageliveweightatslaughterkg']
      OUTPUT_SOURCE = 'interPIG'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_avgliveweight_kg' ,'acc_avgliveweight_kg_src']] = gbads_pigs_merged.apply(create_acc_avgliveweight_kg ,axis=1)

# ============================================================================
#### Feed Consumption
# ============================================================================
def create_acc_feedconsumption_tonnes(INPUT_ROW):
   # UK
   if pd.notnull(INPUT_ROW['ukfeed_production_total_pig_feed_exclbreeding_thsdtonnes']):
      #!!! Assuming feed produced in UK is consumed in UK
      OUTPUT = INPUT_ROW['ukfeed_production_total_pig_feed_exclbreeding_thsdtonnes'] * 1000
      OUTPUT_SOURCE = 'AHDB'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
# gbads_pigs_merged[['acc_feedconsumption_tonnes' ,'acc_feedconsumption_tonnes_src']] = gbads_pigs_merged.apply(create_acc_feedconsumption_tonnes ,axis=1)

# The team is uncomfortable assuming feed production is the same as feed consumption
# Instead, back-calculate feed consumption from feed price and total expenditure
def create_acc_feedconsumption_tonnes_2(INPUT_ROW):
   # InterPIG
   if pd.notnull(INPUT_ROW['ip_totalfeed_tonnes']):
      OUTPUT = INPUT_ROW['ip_totalfeed_tonnes']
      OUTPUT_SOURCE = 'interPIG'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_feedconsumption_tonnes' ,'acc_feedconsumption_tonnes_src']] = gbads_pigs_merged.apply(create_acc_feedconsumption_tonnes_2 ,axis=1)

def create_acc_fcr_carc(INPUT_ROW):
   # InterPIG
   if pd.notnull(INPUT_ROW['ip_fcr_carc']):
      OUTPUT = INPUT_ROW['ip_fcr_carc']
      OUTPUT_SOURCE = 'interPIG'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_fcr_carc' ,'acc_fcr_carc_src']] = gbads_pigs_merged.apply(create_acc_fcr_carc ,axis=1)

def create_acc_fcr_live(INPUT_ROW):
   # InterPIG
   if pd.notnull(INPUT_ROW['ip_fcr_live']):
      OUTPUT = INPUT_ROW['ip_fcr_live']
      OUTPUT_SOURCE = 'interPIG'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_fcr_live' ,'acc_fcr_live_src']] = gbads_pigs_merged.apply(create_acc_fcr_live ,axis=1)

def create_acc_avgfeedintake_kgperhd(INPUT_ROW):
   # InterPIG
   if pd.notnull(INPUT_ROW['ip_feedperhead_kg']):
      OUTPUT = INPUT_ROW['ip_feedperhead_kg']
      OUTPUT_SOURCE = 'interPIG'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_avgfeedintake_kgperhd' ,'acc_avgfeedintake_kgperhd_src']] = gbads_pigs_merged.apply(create_acc_avgfeedintake_kgperhd ,axis=1)

# Average feed intake per head
# How much was eaten by the animals that died? Related to phase of life that the mortality occurred.
# Head slaughtered is too small a denominator because some was eaten by animals that didn't make it to slaughter
# Head placed is too large a denominator because not all those animals were eating the whole time
def create_acc_avgfeedintake_adj_kgperhd(
      INPUT_ROW
      ,PRPN_FEEDCNSM_DEAD=0.1  # [0,1]: proportion of total feed consumed by head that died
      ):
   adj_feedconsumption_tonnes = INPUT_ROW['acc_feedconsumption_tonnes'] * (1 - PRPN_FEEDCNSM_DEAD)
   OUTPUT = adj_feedconsumption_tonnes / INPUT_ROW['acc_headslaughtered'] * 1000
   return OUTPUT
# gbads_pigs_merged['acc_avgfeedintake_adj_kgperhd'] = gbads_pigs_merged.apply(create_acc_avgfeedintake_adj_kgperhd ,axis=1)

# Alternative average feed intake, estimating the feed consumed by animals that died,
# according to mortality rates and average weights in each phase of growout using the PIC standard.
# Create interpolators for weight and feed from PIC standard
#!!! Requires breed standard data available in this program
# interp_weight_from_feed = sp.interpolate.interp1d(
#    swinebreedstd_pic_growthandfeed['cml_feedintake_kg']
#    ,swinebreedstd_pic_growthandfeed['bodyweight_kg']
# )
# interp_feed_from_weight = sp.interpolate.interp1d(
#    swinebreedstd_pic_growthandfeed['bodyweight_kg']
#    ,swinebreedstd_pic_growthandfeed['cml_feedintake_kg']
# )
# def create_acc_avgfeedintake_kgperhd_2(INPUT_ROW):
#    return OUTPUT
# gbads_pigs_merged['acc_avgfeedintake_kgperhd'] = gbads_pigs_merged.apply(create_acc_avgfeedintake_kgperhd_2 ,axis=1)

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
   if pd.notnull(INPUT_ROW['euro_feed_bulk_fatteningpigs_priceper100kg_localcrncy']):
      OUTPUT = INPUT_ROW['euro_feed_bulk_fatteningpigs_priceper100kg_localcrncy'] * 10 / INPUT_ROW['wb_exchangerate_lcuperusd']
      OUTPUT_SOURCE = 'Eurostat'
   # UK
   elif pd.notnull(INPUT_ROW['ukfeed_feedprice_pig_gbppertonne_wtavg']):
      OUTPUT = INPUT_ROW['ukfeed_feedprice_pig_gbppertonne_wtavg'] / INPUT_ROW['wb_exchangerate_gbpperusd']
      OUTPUT_SOURCE = 'AHDB'
   # interPIG
   elif pd.notnull(INPUT_ROW['ip_averagefarmfeedprice_europertonne']):
      OUTPUT = INPUT_ROW['ip_averagefarmfeedprice_europertonne'] / INPUT_ROW['wb_exchangerate_europerusd']
      OUTPUT_SOURCE = 'interPIG'
   # interPIG alternative: back-calculate from feed cost per kg carcass weight
   # This ensures feed price is consistent with feed cost per kg carcass weight
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_feedprice_usdpertonne' ,'acc_feedprice_usdpertonne_src']] = gbads_pigs_merged.apply(create_acc_feedprice_usdpertonne ,axis=1)
gbads_pigs_merged['acc_feedprice_usdpertonne_cnst2010'] = addcol_constant_currency(gbads_pigs_merged ,'acc_feedprice_usdpertonne' ,'wb_cpi_idx2010')

def create_acc_producerprice_usdperkgcarc(INPUT_ROW):
   # FAO
   if pd.notnull(INPUT_ROW['fao_producerprice_pigs_carcass_usdpertonne']):
      OUTPUT = INPUT_ROW['fao_producerprice_pigs_carcass_usdpertonne'] / 1000
      OUTPUT_SOURCE = 'FAO'
   # Eurostat
   elif pd.notnull(INPUT_ROW['euro_pigs_grade1_carcass_priceper100kg_euros']):
      # Second price not used: 'euro_pigs_grade2_carcass_priceper100kg_euros'
      OUTPUT = (INPUT_ROW['euro_pigs_grade1_carcass_priceper100kg_euros'] / 100) / INPUT_ROW['wb_exchangerate_europerusd']
      OUTPUT_SOURCE = 'Eurostat'
   # US
   elif pd.notnull(INPUT_ROW['usda_hogs_pricerecvd_dolpercwt']):
      OUTPUT = (INPUT_ROW['usda_hogs_pricerecvd_dolpercwt'] / 100) * uc.lbs_per_kg
      OUTPUT_SOURCE = 'USDA'
   # Pig333
   elif pd.notnull(INPUT_ROW['pig333_pigprice_mean_lcuperkg']):
      OUTPUT = INPUT_ROW['pig333_pigprice_mean_lcuperkg'] / INPUT_ROW['wb_exchangerate_lcuperusd']
      OUTPUT_SOURCE = 'Pig333'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_producerprice_usdperkgcarc' ,'acc_producerprice_usdperkgcarc_src']] = gbads_pigs_merged.apply(create_acc_producerprice_usdperkgcarc ,axis=1)
gbads_pigs_merged['acc_producerprice_usdperkgcarc_cnst2010'] = addcol_constant_currency(gbads_pigs_merged ,'acc_producerprice_usdperkgcarc' ,'wb_cpi_idx2010')

def create_acc_pigletprice_usdperkg(INPUT_ROW):
   # Eurostat
   if pd.notnull(INPUT_ROW['euro_piglets_live_priceper100kg_euros']):
      OUTPUT = (INPUT_ROW['euro_piglets_live_priceper100kg_euros'] / 100) / INPUT_ROW['wb_exchangerate_europerusd']
      OUTPUT_SOURCE = 'Eurostat'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_pigletprice_usdperkg' ,'acc_pigletprice_usdperkg_src']] = gbads_pigs_merged.apply(create_acc_pigletprice_usdperkg ,axis=1)
gbads_pigs_merged['acc_pigletprice_usdperkg_cnst2010'] = addcol_constant_currency(gbads_pigs_merged ,'acc_pigletprice_usdperkg' ,'wb_cpi_idx2010')

# ============================================================================
#### Costs
# ============================================================================
def create_acc_feedcost_usdperkgcarc(INPUT_ROW):
   # interPIG
   if pd.notnull(INPUT_ROW['ip_feed_gbpperkgcarc']):
      OUTPUT = INPUT_ROW['ip_feed_gbpperkgcarc'] / INPUT_ROW['wb_exchangerate_gbpperusd']
      OUTPUT_SOURCE = 'interPIG'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_feedcost_usdperkgcarc' ,'acc_feedcost_usdperkgcarc_src']] = gbads_pigs_merged.apply(create_acc_feedcost_usdperkgcarc ,axis=1)
gbads_pigs_merged['acc_feedcost_usdperkgcarc_cnst2010'] = addcol_constant_currency(gbads_pigs_merged ,'acc_feedcost_usdperkgcarc' ,'wb_cpi_idx2010')

def create_acc_nonfeedvariablecost_usdperkgcarc(INPUT_ROW):
   # interPIG
   if pd.notnull(INPUT_ROW['ip_othervariablecosts_gbpperkgcarc']):
      OUTPUT = INPUT_ROW['ip_othervariablecosts_gbpperkgcarc'] / INPUT_ROW['wb_exchangerate_gbpperusd']
      OUTPUT_SOURCE = 'interPIG'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_nonfeedvariablecost_usdperkgcarc' ,'acc_nonfeedvariablecost_usdperkgcarc_src']] = gbads_pigs_merged.apply(create_acc_nonfeedvariablecost_usdperkgcarc ,axis=1)
gbads_pigs_merged['acc_nonfeedvariablecost_usdperkgcarc_cnst2010'] = addcol_constant_currency(gbads_pigs_merged ,'acc_nonfeedvariablecost_usdperkgcarc' ,'wb_cpi_idx2010')

def create_acc_pigletcost_usdperkgcarc(
      INPUT_ROW
      ,AVG_PIGLETWEIGHT_KG=7
      ):
   OUTPUT = INPUT_ROW['acc_headplaced'] * INPUT_ROW['acc_pigletprice_usdperkg'] * AVG_PIGLETWEIGHT_KG \
      / (INPUT_ROW['acc_totalcarcweight_tonnes'] * 1000)
   return OUTPUT
gbads_pigs_merged['acc_pigletcost_usdperkgcarc'] = gbads_pigs_merged.apply(create_acc_pigletcost_usdperkgcarc ,axis=1)
gbads_pigs_merged['acc_pigletcost_usdperkgcarc_cnst2010'] = addcol_constant_currency(gbads_pigs_merged ,'acc_pigletcost_usdperkgcarc' ,'wb_cpi_idx2010')

def create_acc_laborcost_usdperkgcarc(INPUT_ROW):
   # interPIG
   if pd.notnull(INPUT_ROW['ip_labour_gbpperkgcarc']):
      OUTPUT = INPUT_ROW['ip_labour_gbpperkgcarc'] / INPUT_ROW['wb_exchangerate_gbpperusd']
      OUTPUT_SOURCE = 'interPIG'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_laborcost_usdperkgcarc' ,'acc_laborcost_usdperkgcarc_src']] = gbads_pigs_merged.apply(create_acc_laborcost_usdperkgcarc ,axis=1)
gbads_pigs_merged['acc_laborcost_usdperkgcarc_cnst2010'] = addcol_constant_currency(gbads_pigs_merged ,'acc_laborcost_usdperkgcarc' ,'wb_cpi_idx2010')

def create_acc_landhousingcost_usdperkgcarc(INPUT_ROW):
   # interPIG
   if pd.notnull(INPUT_ROW['ip_depreciationandfinance_gbpperkgcarc']):
      OUTPUT = INPUT_ROW['ip_depreciationandfinance_gbpperkgcarc'] / INPUT_ROW['wb_exchangerate_gbpperusd']
      OUTPUT_SOURCE = 'interPIG'
   else:
      OUTPUT = np.nan
      OUTPUT_SOURCE = None
   return pd.Series([OUTPUT ,OUTPUT_SOURCE])
gbads_pigs_merged[['acc_landhousingcost_usdperkgcarc' ,'acc_landhousingcost_usdperkgcarc_src']] = gbads_pigs_merged.apply(create_acc_landhousingcost_usdperkgcarc ,axis=1)
gbads_pigs_merged['acc_landhousingcost_usdperkgcarc_cnst2010'] = addcol_constant_currency(gbads_pigs_merged ,'acc_landhousingcost_usdperkgcarc' ,'wb_cpi_idx2010')

# ============================================================================
#### Datainfo
# ============================================================================
datainfo(gbads_pigs_merged)

#%% Checks

# Create a copy so I can add _check columns without cluttering original
gbads_pigs_merged_check = gbads_pigs_merged.copy()

# ============================================================================
#### Mortality
# ============================================================================
# ----------------------------------------------------------------------------
# Check USDA
# ----------------------------------------------------------------------------
# My calculated mortality vs. hogs_deathloss_hd

#%% Describe and Export

# ============================================================================
#### Export basic FAO and Eurostat columns
# ============================================================================
cols_fao = [i for i in list(gbads_pigs_merged) if 'fao_' in i]
cols_euro = [i for i in list(gbads_pigs_merged) if 'euro_' in i]

pigs_fao_euro = gbads_pigs_merged[['country' ,'year'] + cols_fao + cols_euro]

datainfo(pigs_fao_euro)
datadesc(pigs_fao_euro ,CHARACTERIZE_FOLDER)

# profile = pigs_fao_euro.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'pigs_fao_euro_profile.html'))

pigs_fao_euro.to_csv(os.path.join(EXPDATA_FOLDER ,'pigs_fao_euro.csv'))

# ============================================================================
#### Export full data
# ============================================================================
datainfo(gbads_pigs_merged)
datadesc(gbads_pigs_merged ,CHARACTERIZE_FOLDER)

# profile = gbads_pigs_merged.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'gbads_pigs_merged_profile.html'))

gbads_pigs_merged.to_pickle(os.path.join(PRODATA_FOLDER ,'gbads_pigs_merged.pkl.gz'))
gbads_pigs_merged.to_csv(os.path.join(EXPDATA_FOLDER ,'gbads_pigs_merged.csv'))

# ============================================================================
#### Export version for Dash
# ============================================================================
# ----------------------------------------------------------------------------
# Select rows and columns
# ----------------------------------------------------------------------------
# Limit to rows that have necessary columns filled in for calculating BOD
necessary_columns_fordash = [
   'country'
   ,'year'
   ,'acc_headplaced'
   ,'acc_headslaughtered'
   ,'acc_totalcarcweight_tonnes'
   ,'acc_avgcarcweight_kg'
   # ,'acc_avgliveweight_kg'   # Not used
]

#!!! Can limit columns at this point if you don't want to use them for Dash calcs or display to user

gbads_pigs_merged_fordash = gbads_pigs_merged.dropna(
    axis=0                        # 0 = drop rows, 1 = drop columns
    ,subset=necessary_columns_fordash      # List (opt): if dropping rows, only consider these columns in NA check
    ,how='any'                    # String: 'all' = drop rows / columns that have all missing values. 'any' = drop rows / columns that have any missing values.
).reset_index(drop=True)          # If dropping rows, reset index

# ----------------------------------------------------------------------------
# Export
# ----------------------------------------------------------------------------
datainfo(gbads_pigs_merged_fordash)
datadesc(gbads_pigs_merged_fordash ,CHARACTERIZE_FOLDER)

gbads_pigs_merged_fordash.to_pickle(os.path.join(DASH_DATA_FOLDER ,'gbads_pigs_merged_fordash.pkl.gz'))
