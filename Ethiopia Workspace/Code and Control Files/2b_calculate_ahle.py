#%% ABOUT
'''
This program calculates animal health loss envelope using the combined
simulation results. The output is a CSV file to be used in the dashboard.

IMPORTANT: before running this, set Python's working directory to the folder
where this code is stored.
'''
#%% PACKAGES AND FUNCTIONS

import os
import inspect
import io
import time
import datetime as dtm
import numpy as np
import pandas as pd
import pickle
import wbdata         # To access World Bank data through API calls

# To clean up column names in a dataframe
def cleancolnames(INPUT_DF):
   # Comments inside the statement create errors. Putting all comments at the top.
   # Convert to lowercase
   # Strip leading and trailing spaces, then replace spaces with underscore
   # Replace slashes, parenthesis, and brackets with underscore
   # Replace some special characters with underscore
   # Replace other special characters with words
   INPUT_DF.columns = INPUT_DF.columns.str.lower() \
      .str.strip().str.replace(' ' ,'_' ,regex=False) \
      .str.replace('/' ,'_' ,regex=False).str.replace('\\' ,'_' ,regex=False) \
      .str.replace('(' ,'_' ,regex=False).str.replace(')' ,'_' ,regex=False) \
      .str.replace('[' ,'_' ,regex=False).str.replace(']' ,'_' ,regex=False) \
      .str.replace('{' ,'_' ,regex=False).str.replace('}' ,'_' ,regex=False) \
      .str.replace('!' ,'_' ,regex=False).str.replace('?' ,'_' ,regex=False) \
      .str.replace('-' ,'_' ,regex=False).str.replace('+' ,'_' ,regex=False) \
      .str.replace('^' ,'_' ,regex=False).str.replace('*' ,'_' ,regex=False) \
      .str.replace('.' ,'_' ,regex=False).str.replace(',' ,'_' ,regex=False) \
      .str.replace('|' ,'_' ,regex=False).str.replace('#' ,'_' ,regex=False) \
      .str.replace('>' ,'_gt_' ,regex=False) \
      .str.replace('<' ,'_lt_' ,regex=False) \
      .str.replace('=' ,'_eq_' ,regex=False) \
      .str.replace('@' ,'_at_' ,regex=False) \
      .str.replace('$' ,'_dol_' ,regex=False) \
      .str.replace('%' ,'_pct_' ,regex=False) \
      .str.replace('&' ,'_and_' ,regex=False)
   return None

# To print df.info() with header for readability, and optionally write data info to text file
def datainfo(
      INPUT_DF
      ,MAX_COLS=100
      ,OUTFOLDER=None     # String (opt): folder to output {dataname}_info.txt. If None, no file will be created.
   ):
   funcname = inspect.currentframe().f_code.co_name
   dataname = [x for x in globals() if globals()[x] is INPUT_DF][0]
   rowcount = INPUT_DF.shape[0]
   colcount = INPUT_DF.shape[1]
   idxcols = str(list(INPUT_DF.index.names))
   header = f"Data name: {dataname :>26s}\nRows:      {rowcount :>26,}\nColumns:   {colcount :>26,}\nIndex:     {idxcols :>26s}\n"
   divider = ('-'*26) + ('-'*11) + '\n'
   bigdivider = ('='*26) + ('='*11) + '\n'
   print(bigdivider + header + divider)
   INPUT_DF.info(max_cols=MAX_COLS)
   print(divider + f"End:       {dataname:>26s}\n" + bigdivider)

   if OUTFOLDER:     # If something has been passed to OUTFOLDER parameter
      filename = f"{dataname}_info"
      print(f"\n<{funcname}> Creating file {OUTFOLDER}\{filename}.txt")
      datetimestamp = 'Created on ' + time.strftime('%Y-%m-%d %X', time.gmtime()) + ' UTC' + '\n'
      buffer = io.StringIO()
      INPUT_DF.info(buf=buffer, max_cols=colcount)
      filecontents = header + divider + datetimestamp + buffer.getvalue()
      tofile = os.path.join(OUTFOLDER, f"{filename}.txt")
      with open(tofile, 'w', encoding='utf-8') as f: f.write(filecontents)
      print(f"<{funcname}> ...done.")
   return None

# To turn column indexes into names. Will remove multi-indexing.
# Must assign the output to a dataframe e.g. df = colnames_from_index(df).
def colnames_from_index(INPUT_DF):
   cols = list(INPUT_DF)
   cols_new = []
   for item in cols:
      if type(item) == str:   # Columns that already have names will be strings. Use unchanged.
         cols_new.append(item)
      else:   # Columns that are indexed or multi-indexed will appear as tuples. Turn them into strings joined by underscores.
         cols_new.append('_'.join(str(i) for i in item))   # Convert each element of tuple to string before joining. Avoids error if an element is nan.

   # Write dataframe with new column names
   dfmod = INPUT_DF
   dfmod.columns = cols_new
   return dfmod

#%% PATHS AND VARIABLES

CURRENT_FOLDER = os.getcwd()
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
GRANDPARENT_FOLDER = os.path.dirname(PARENT_FOLDER)

# Folder for shared code with Liverpool
ETHIOPIA_CODE_FOLDER = CURRENT_FOLDER
ETHIOPIA_OUTPUT_FOLDER = os.path.join(PARENT_FOLDER ,'Program outputs')
ETHIOPIA_DATA_FOLDER = os.path.join(PARENT_FOLDER ,'Data')

DASH_DATA_FOLDER = os.path.join(GRANDPARENT_FOLDER, 'AHLE Dashboard' ,'Dash App' ,'data')

#%% EXTERNAL DATA

# =============================================================================
#### Retrieve currency conversion data from World Bank
# =============================================================================
wb_search_exchg = wbdata.search_indicators('exchange')

# Indicators to retrieve as a dictionary
# Keys: World Bank indicator codes
# Values: desired column names in resulting dataframe
wb_indicators_toget = {
    'PA.NUS.FCRF':'exchg_rate_lcuperusdol'
}

# Date range to retrieve
wb_startdate = dtm.datetime(2010, 1, 1)
current_year = dtm.datetime.now().year
wb_enddate = dtm.datetime(current_year, 1, 1)

# Retrieve the data
wb_exchg_df = wbdata.get_dataframe(
    wb_indicators_toget
    ,country='all'
    ,data_date=(wb_startdate ,wb_enddate)   # Date range to retrieve
    ,freq='Y'                               # Frequency: Yearly
    ,convert_date=True                      # True: convert date column to datetime type
)

# Change index to columns and rename
wb_exchg_df = wb_exchg_df.reset_index()
wb_exchg_df['year'] = wb_exchg_df['date'].dt.year
wb_exchg_df = wb_exchg_df.rename(columns={'country':'country_name'})

# Filter to Ethiopia
exchg_data_tomerge = wb_exchg_df.query("country_name == 'Ethiopia'")

# Export
exchg_data_tomerge.to_pickle(os.path.join(ETHIOPIA_DATA_FOLDER ,'wb_exchg_data_processed.pkl.gz'))

#%% READ COMBINED SIMULATION DATA

ahle_combo_adj = pd.read_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_stacked_adj.csv'))
datainfo(ahle_combo_adj)

#%% ADD GROUP SUMMARIES
'''
Creating aggregate groups for filtering in the dashboard.

Note: many aggregate groups are already created in the compartmental model,
but not all. For simplicity, and to ensure consistency, I'm recalculating
all aggregate groups.
'''
mean_cols = [i for i in list(ahle_combo_adj) if 'mean' in i]
sd_cols = [i for i in list(ahle_combo_adj) if 'stdev' in i]

# =============================================================================
#### Drop aggregate groups
# =============================================================================
# Separate all existing overall or combined groups
_combined_rows = (ahle_combo_adj['group'].str.contains('OVERALL' ,case=False ,na=False))\
    | (ahle_combo_adj['group'].str.contains('COMBINED' ,case=False ,na=False))
ahle_combo_overall = ahle_combo_adj.loc[_combined_rows].copy()

# Create version without any aggregate groups
ahle_combo_indiv = ahle_combo_adj.loc[~ _combined_rows].copy()

# Get distinct values for ages and sexes without aggregates
age_group_values = list(ahle_combo_indiv['age_group'].unique())
sex_values = list(ahle_combo_indiv['sex'].unique())

# =============================================================================
#### Add placeholder items
# =============================================================================
'''
This is no longer needed because infrastructure cost is estimated inside the
compartmental model. I'm keeping the code in case we want to add any other item
placeholders.
'''
# # Get all combinations of key variables without item
# item_placeholder = ahle_combo_indiv[['region' ,'species' ,'production_system' ,'group' ,'age_group' ,'sex' ,'year']].drop_duplicates()
# item_placeholder['item'] = 'Cost of Infrastructure'

# # Stack placeholder item(s) with individual data
# ahle_combo_withplaceholders = pd.concat(
#     [ahle_combo_indiv ,item_placeholder]
#     ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
#     ,join='outer'        # 'outer': keep all index values from all data frames
#     ,ignore_index=True   # True: do not keep index values on concatenation axis
# )

# # Placeholder items get mean and SD zero
# for COL in [mean_cols + sd_cols]:
#     ahle_combo_withplaceholders[COL] = ahle_combo_withplaceholders[COL].replace(np.nan ,0)

# =============================================================================
#### Build aggregate age/sex groups
# =============================================================================
# Define full set of key variables
# Make tuple so immutable
all_byvars = ('species' ,'region' ,'production_system' ,'item' ,'item_type_code' ,'group' ,'age_group' ,'sex' ,'year')

# Only using mean and standard deviaion of each item, as the other statistics
# cannot be summed.
keepcols = list(all_byvars) + mean_cols + sd_cols
ahle_combo_indiv = ahle_combo_indiv[keepcols].copy()
datainfo(ahle_combo_indiv)

# -----------------------------------------------------------------------------
# Create variance columns
# -----------------------------------------------------------------------------
# Relying on the following properties of sums of random variables:
#    mean(aX + bY) = a*mean(X) + b*mean(Y), regardless of correlation
#    var(aX + bY) = a^2*var(X) + b^2*var(Y), assuming X and Y are uncorrelated
var_cols = ['sqrd_' + COLNAME for COLNAME in sd_cols]
for i ,VARCOL in enumerate(var_cols):
   SDCOL = sd_cols[i]
   ahle_combo_indiv[VARCOL] = ahle_combo_indiv[SDCOL]**2

datainfo(ahle_combo_indiv)

# -----------------------------------------------------------------------------
# Create Overall age/sex group
# -----------------------------------------------------------------------------
agg_vars = ['group' ,'age_group' ,'sex']
summarize_byvars = list(all_byvars)
for i in agg_vars:
    summarize_byvars.remove(i)

ahle_combo_sum_groups = ahle_combo_indiv.pivot_table(
    index=summarize_byvars
    ,values=mean_cols + var_cols
    ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
).reset_index()
ahle_combo_sum_groups['group'] = 'Overall'
ahle_combo_sum_groups['age_group'] = 'Overall'
ahle_combo_sum_groups['sex'] = 'Overall'

# -----------------------------------------------------------------------------
# Create Overall sex for each age group
# -----------------------------------------------------------------------------
agg_vars = ['group' ,'sex']
summarize_byvars = list(all_byvars)
for i in agg_vars:
    summarize_byvars.remove(i)

ahle_combo_sum_sexes = pd.DataFrame()    # Initialize
for AGE_GRP in age_group_values:
    ahle_combo_sum_sexes_oneage = ahle_combo_indiv.query(f"age_group == '{AGE_GRP}'").pivot_table(
        index=summarize_byvars
        ,values=mean_cols + var_cols
        ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
    ).reset_index()
    ahle_combo_sum_sexes_oneage['group'] = f'{AGE_GRP} Combined'
    ahle_combo_sum_sexes_oneage['sex'] = 'Overall'

    # Stack
    ahle_combo_sum_sexes = pd.concat(
        [ahle_combo_sum_sexes ,ahle_combo_sum_sexes_oneage]
        ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
        ,join='outer'        # 'outer': keep all index values from all data frames
        ,ignore_index=True   # True: do not keep index values on concatenation axis
    )
    del ahle_combo_sum_sexes_oneage

# Oxen are a special age group which is only male. Drop "combined" sex.
_oxen_combined = (ahle_combo_sum_sexes['group'].str.upper() == 'OXEN COMBINED')
ahle_combo_sum_sexes = ahle_combo_sum_sexes.loc[~ _oxen_combined].reset_index(drop=True)

# -----------------------------------------------------------------------------
# Create Overall age group for each sex
# -----------------------------------------------------------------------------
agg_vars = ['group' ,'age_group']
summarize_byvars = list(all_byvars)
for i in agg_vars:
    summarize_byvars.remove(i)

ahle_combo_sum_ages = pd.DataFrame()     # Initialize
for SEX_GRP in sex_values:
    ahle_combo_sum_ages_onesex = ahle_combo_indiv.query(f"sex == '{SEX_GRP}'").pivot_table(
        index=summarize_byvars
        ,values=mean_cols + var_cols
        ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
    ).reset_index()
    ahle_combo_sum_ages_onesex['group'] = f'Overall {SEX_GRP}'
    ahle_combo_sum_ages_onesex['age_group'] = 'Overall'

    # Stack
    ahle_combo_sum_ages = pd.concat(
        [ahle_combo_sum_ages ,ahle_combo_sum_ages_onesex]
        ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
        ,join='outer'        # 'outer': keep all index values from all data frames
        ,ignore_index=True   # True: do not keep index values on concatenation axis
    )
    del ahle_combo_sum_ages_onesex

# -----------------------------------------------------------------------------
# Concatenate all and de-dup
# -----------------------------------------------------------------------------
concat_dataframes = [
    ahle_combo_indiv
    ,ahle_combo_sum_groups
    ,ahle_combo_sum_sexes
    ,ahle_combo_sum_ages

    # Original overall group rows
    ,ahle_combo_overall     # Set at end so de-dup keeps newer groups if they exist
]
ahle_combo_withagg = pd.concat(
   concat_dataframes
   ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
   ,join='outer'        # 'outer': keep all columns
   ,ignore_index=True   # True: do not keep index values on concatenation axis
)
del ahle_combo_indiv ,ahle_combo_sum_groups ,ahle_combo_sum_sexes ,ahle_combo_sum_ages ,ahle_combo_overall

# De-Dup
ahle_combo_withagg = ahle_combo_withagg.drop_duplicates(subset=all_byvars ,keep='first')

# =============================================================================
#### Build aggregate species and production system groups
# =============================================================================
'''
These must be done after concatenating combined age/sex groups so that they are
calculated for the combined groups as well.
'''
# -----------------------------------------------------------------------------
# Create overall production system
# -----------------------------------------------------------------------------
agg_vars = ['production_system']
summarize_byvars = list(all_byvars)
for i in agg_vars:
    summarize_byvars.remove(i)

ahle_combo_sum_prodsys = ahle_combo_withagg.pivot_table(
   index=summarize_byvars
   ,values=mean_cols + var_cols
   ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
).reset_index()
ahle_combo_sum_prodsys['production_system'] = 'Overall'

ahle_combo_withagg = pd.concat(
    [ahle_combo_withagg ,ahle_combo_sum_prodsys]
    ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
    ,join='outer'        # 'outer': keep all index values from all data frames
    ,ignore_index=True   # True: do not keep index values on concatenation axis
)
del ahle_combo_sum_prodsys

# -----------------------------------------------------------------------------
# Create combined species
# -----------------------------------------------------------------------------
agg_vars = ['species']
summarize_byvars = list(all_byvars)
for i in agg_vars:
    summarize_byvars.remove(i)

# All Small Ruminants
ahle_combo_sum_species = ahle_combo_withagg.query("species.str.upper().isin(['SHEEP' ,'GOAT'])").pivot_table(
   index=summarize_byvars
   ,values=mean_cols + var_cols
   ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
).reset_index()
ahle_combo_sum_species['species'] = 'All Small Ruminants'

# All poultry
ahle_combo_sum_species2 = ahle_combo_withagg.query("species.str.contains('poultry' ,case=False ,na=False)").pivot_table(
   index=summarize_byvars
   ,values=mean_cols + var_cols
   ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
).reset_index()
ahle_combo_sum_species2['species'] = 'All Poultry'

ahle_combo_withagg = pd.concat(
    [ahle_combo_withagg ,ahle_combo_sum_species ,ahle_combo_sum_species2]
    ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
    ,join='outer'        # 'outer': keep all columns
    ,ignore_index=True   # True: do not keep index values on concatenation axis
)
del ahle_combo_sum_species ,ahle_combo_sum_species2

# =============================================================================
#### Calculate standard deviations
# =============================================================================
for i ,VARCOL in enumerate(var_cols):
   SDCOL = sd_cols[i]
   ahle_combo_withagg[SDCOL] = np.sqrt(ahle_combo_withagg[VARCOL])
   del ahle_combo_withagg[VARCOL]

datainfo(ahle_combo_withagg)

#%% MISC CALCS AND EXPORT

# =============================================================================
#### Calcs
# =============================================================================
# Columns for scenario differences
ahle_combo_withagg['mean_diff_ideal'] = ahle_combo_withagg['mean_ideal'] - ahle_combo_withagg['mean_current']
ahle_combo_withagg['stdev_diff_ideal'] = np.sqrt(ahle_combo_withagg['stdev_ideal']**2 + ahle_combo_withagg['stdev_current']**2)

# -----------------------------------------------------------------------------
# Columns per kg biomass
# -----------------------------------------------------------------------------
# Get current population liveweight by region into its own column
liveweight_byregion = ahle_combo_withagg.query("item == 'Population Liveweight (kg)'")[list(all_byvars) + ['mean_current']].drop_duplicates()
pivot_index = list(all_byvars)
pivot_index.remove('item')
pivot_index.remove('item_type_code')
liveweight_byregion = liveweight_byregion.pivot(
    index=pivot_index
    ,columns='item'
    ,values='mean_current'
).reset_index()
cleancolnames(liveweight_byregion)

# Merge with original data
ahle_combo_withagg = pd.merge(
    left=ahle_combo_withagg
    ,right=liveweight_byregion
    ,on=pivot_index
    ,how='left'
)

# Recreate column lists to include misc calcs columns
mean_cols_update = [i for i in list(ahle_combo_withagg) if 'mean' in i]
sd_cols_update = [i for i in list(ahle_combo_withagg) if 'stdev' in i]

# Calculate value columns per kg liveweight
for MEANCOL in mean_cols_update:
    NEWCOL_NAME = MEANCOL + '_perkgbiomass'
    ahle_combo_withagg[NEWCOL_NAME] = ahle_combo_withagg[MEANCOL] / ahle_combo_withagg['population_liveweight__kg_']

# For standard deviations, convert to variances then scale by the squared denominator
# VAR(aX) = a^2 * VAR(X). a = 1/exchange rate.
for SDCOL in sd_cols_update:
    NEWCOL_NAME = SDCOL + '_perkgbiomass'
    ahle_combo_withagg[NEWCOL_NAME] = np.sqrt(ahle_combo_withagg[SDCOL]**2 / ahle_combo_withagg['population_liveweight__kg_']**2)

# =============================================================================
#### Add currency conversion
# =============================================================================
# Merge exchange rates onto data
ahle_combo_withagg['country_name'] = 'Ethiopia'     # Add country for joining
ahle_combo_withagg = pd.merge(
    left=ahle_combo_withagg
    ,right=exchg_data_tomerge
    ,on=['country_name' ,'year']
    ,how='left'
    )
del ahle_combo_withagg['country_name']

# Recreate column lists to include perkgbiomass columns
mean_cols_update2 = [i for i in list(ahle_combo_withagg) if 'mean' in i]
sd_cols_update2 = [i for i in list(ahle_combo_withagg) if 'stdev' in i]

# Add columns in USD for currency items
_currency_items = (ahle_combo_withagg['item_type_code'].isin(['mv' ,'mc']))
for MEANCOL in mean_cols_update2:
   MEANCOL_USD = MEANCOL + '_usd'
   ahle_combo_withagg.loc[_currency_items ,MEANCOL_USD] = \
      ahle_combo_withagg[MEANCOL] / ahle_combo_withagg['exchg_rate_lcuperusdol']

# For standard deviations, scale variances by the squared exchange rate
# VAR(aX) = a^2 * VAR(X). a = 1/exchange rate.
for SDCOL in sd_cols_update2:
   SDCOL_USD = SDCOL + '_usd'
   ahle_combo_withagg.loc[_currency_items ,SDCOL_USD] = \
      np.sqrt(ahle_combo_withagg[SDCOL]**2 / ahle_combo_withagg['exchg_rate_lcuperusdol']**2)

datainfo(ahle_combo_withagg)

# =============================================================================
#### Cleanup and Export
# =============================================================================
# -----------------------------------------------------------------------------
# Subset and rename columns
# -----------------------------------------------------------------------------
# In this file, keeping a subset of scenarios
keepcols = list(all_byvars) + [
    # In Birr
    'mean_current'
    ,'stdev_current'
    ,'mean_ideal'
    ,'stdev_ideal'
    ,'mean_diff_ideal'
    ,'stdev_diff_ideal'

    ,'mean_ppr'
    ,'stdev_ppr'
    ,'mean_bruc'
    ,'stdev_bruc'
    ,'mean_fmd'
    ,'stdev_fmd'

    ,'mean_current_perkgbiomass'
    ,'stdev_current_perkgbiomass'
    ,'mean_ideal_perkgbiomass'
    ,'stdev_ideal_perkgbiomass'
    ,'mean_diff_ideal_perkgbiomass'
    ,'stdev_diff_ideal_perkgbiomass'

    ,'mean_ppr_perkgbiomass'
    ,'stdev_ppr_perkgbiomass'
    ,'mean_bruc_perkgbiomass'
    ,'stdev_bruc_perkgbiomass'
    ,'mean_fmd_perkgbiomass'
    ,'stdev_fmd_perkgbiomass'

    # In USD
    ,'exchg_rate_lcuperusdol'

    ,'mean_current_usd'
    ,'stdev_current_usd'
    ,'mean_ideal_usd'
    ,'stdev_ideal_usd'
    ,'mean_diff_ideal_usd'
    ,'stdev_diff_ideal_usd'

    ,'mean_ppr_usd'
    ,'stdev_ppr_usd'
    ,'mean_bruc_usd'
    ,'stdev_bruc_usd'
    ,'mean_fmd_usd'
    ,'stdev_fmd_usd'

    ,'mean_current_perkgbiomass_usd'
    ,'stdev_current_perkgbiomass_usd'
    ,'mean_ideal_perkgbiomass_usd'
    ,'stdev_ideal_perkgbiomass_usd'
    ,'mean_diff_ideal_perkgbiomass_usd'
    ,'stdev_diff_ideal_perkgbiomass_usd'

    ,'mean_ppr_perkgbiomass_usd'
    ,'stdev_ppr_perkgbiomass_usd'
    ,'mean_bruc_perkgbiomass_usd'
    ,'stdev_bruc_perkgbiomass_usd'
    ,'mean_fmd_perkgbiomass_usd'
    ,'stdev_fmd_perkgbiomass_usd'
]

ahle_combo_withagg_smry = ahle_combo_withagg[keepcols].copy()
datainfo(ahle_combo_withagg_smry)

ahle_combo_withagg_smry.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_summary.csv') ,index=False)
ahle_combo_withagg_smry.to_pickle(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_summary.pkl.gz'))

# Output for Dash
ahle_combo_withagg_smry.to_csv(os.path.join(DASH_DATA_FOLDER ,'ahle_all_summary.csv') ,index=False)

#%% CALCULATE AHLE AND EXPORT

# =============================================================================
#### Restructure
# =============================================================================
# For AHLE calcs, we want each item in a column
# Need means and standard deviations for later calculations
mean_cols_agg = [i for i in list(ahle_combo_withagg) if 'mean' in i]
sd_cols_agg = [i for i in list(ahle_combo_withagg) if 'stdev' in i]

# Only need some of the items
keep_items = [
    'gross margin'
    ,'health cost'
    # ,'value of total mortality'   # August 2023: this item does not exist for latest small ruminant and cattle results
    ,'total mortality'
    ,'value of herd increase'
    ,'cml pop growth'
    ]
keep_items_upper = [i.upper() for i in keep_items]
_items_for_ahle = (ahle_combo_withagg['item'].str.upper().isin(keep_items_upper))

ahle_combo_withagg_p = ahle_combo_withagg.loc[(_items_for_ahle)].pivot(
    index=['species' ,'region' ,'production_system' ,'group' ,'age_group' ,'sex' ,'year' ,'exchg_rate_lcuperusdol']
    ,columns='item'
    ,values=mean_cols_agg + sd_cols_agg
).reset_index()
ahle_combo_withagg_p = colnames_from_index(ahle_combo_withagg_p)   # Change multi-index to column names
cleancolnames(ahle_combo_withagg_p)

# Remove underscores added when collapsing column index
ahle_combo_withagg_p = ahle_combo_withagg_p.rename(
    columns={
        'species_':'species'
        ,'region_':'region'
        ,'production_system_':'production_system'
        ,'group_':'group'
        ,'age_group_':'age_group'
        ,'sex_':'sex'
        ,'year_':'year'
        ,'exchg_rate_lcuperusdol_':'exchg_rate_lcuperusdol'
    }
)
datainfo(ahle_combo_withagg_p)

# =============================================================================
#### Calculate AHLE
# =============================================================================
'''
Approach:
    - Total AHLE is difference in gross margin between ideal scenario and current scenario:
        ahle_total_mean = mean_ideal_gross_margin - mean_current_gross_margin
    - AHLE due to mortality is difference in gross margin between zero mortality scenario and
        current scenario:
        ahle_dueto_mortality_mean = mean_mortality_zero_gross_margin - mean_current_gross_margin

        Update August 2023: the zero mortality scenario is no longer being run for Small
        Ruminants or Cattle. I am going to take the same approach as I did for disease-specific
        ahle due to mortality, estimating it as (number of deaths) * (value per head).

        Number of deaths is simply Deaths a.k.a. Total Mortality.
        Value per head can be estimated 3 ways:
            (Value of Herd Increase) / (Cml Pop Growth)
            (Value of Offtake) / (Offtakes a.k.a. Num Offtake)
            (Value of Herd Increase plus Offtake) / (Total Number Increase)
            These 3 methods should give the same estimate for any age/sex groups that have
            non-zero values. WARNING: juveniles and neonates have zero offtake! Use the herd
            increase version.

    - AHLE due to health cost is simply current health cost (ideal health cost is zero).
        Note health cost is negative; we multiply it by negative 1 to express AHLE due to health
        cost as a positive.
    - AHLE due to production loss is the remainder needed to make total AHLE after accounting
        for mortality and health cost.
        Note production loss is hardest to measure because it is the lost potential production
        among the animals that survived. It's tempting to take the difference in "total
        production value" between the current and ideal scenario as the production loss. However,
        this includes lost value due to animals that died. We want the production loss just among
        the animals that survived.

Calculating mean and standard deviation for each AHLE component.
Relying on the following properties of sums of random variables:
    mean(aX + bY) = a*mean(X) + b*mean(Y), regardless of correlation
    var(aX + bY) = a^2*var(X) + b^2*var(Y), assuming X and Y are uncorrelated
'''
ahle_combo_withahle = ahle_combo_withagg_p.copy()

# -----------------------------------------------------------------------------
# Total AHLE
# -----------------------------------------------------------------------------
ahle_combo_withahle = ahle_combo_withahle.eval(
    '''
    ahle_total_mean = mean_ideal_gross_margin - mean_current_gross_margin
    ahle_dueto_ppr_total_mean = mean_ideal_gross_margin - mean_ppr_gross_margin
    ahle_dueto_bruc_total_mean = mean_ideal_gross_margin - mean_bruc_gross_margin
    ahle_dueto_fmd_total_mean = mean_ideal_gross_margin - mean_fmd_gross_margin
    '''
)
# AHLE due to Other Disease will depend on which diseases were estimated.
# It also requires an estimate of the total infectious disease impact, which we
# don't have until the attribution step.
# Don't calculate this here. Handle it when needed (after attribution).
# Should be: ahle_dueto_otherdisease_total_mean = ahle_total_infectious - ahle_dueto_ppr_total_mean - ahle_dueto_bruc_total_mean

# Standard Deviations
ahle_combo_withahle['ahle_total_stdev'] = np.sqrt(
    ahle_combo_withahle['stdev_ideal_gross_margin']**2 + ahle_combo_withahle['stdev_current_gross_margin']**2
)
ahle_combo_withahle['ahle_dueto_ppr_total_stdev'] = np.sqrt(
    ahle_combo_withahle['stdev_ideal_gross_margin']**2 + ahle_combo_withahle['stdev_ppr_gross_margin']**2
)
ahle_combo_withahle['ahle_dueto_bruc_total_stdev'] = np.sqrt(
    ahle_combo_withahle['stdev_ideal_gross_margin']**2 + ahle_combo_withahle['stdev_bruc_gross_margin']**2
)
ahle_combo_withahle['ahle_dueto_fmd_total_stdev'] = np.sqrt(
    ahle_combo_withahle['stdev_ideal_gross_margin']**2 + ahle_combo_withahle['stdev_fmd_gross_margin']**2
)

# -----------------------------------------------------------------------------
# AHLE due to mortality
# -----------------------------------------------------------------------------
ahle_combo_withahle = ahle_combo_withahle.eval(
    '''
    mean_current_valueperhead = mean_current_value_of_herd_increase / mean_current_cml_pop_growth
    ahle_dueto_mortality_mean = mean_current_total_mortality * mean_current_valueperhead

    mean_ppr_valueperhead = mean_ppr_value_of_herd_increase / mean_ppr_cml_pop_growth
    ahle_dueto_ppr_mortality_mean = mean_ppr_total_mortality * mean_ppr_valueperhead

    mean_bruc_valueperhead = mean_bruc_value_of_herd_increase / mean_bruc_cml_pop_growth
    ahle_dueto_bruc_mortality_mean = mean_bruc_total_mortality * mean_bruc_valueperhead

    mean_fmd_valueperhead = mean_fmd_value_of_herd_increase / mean_fmd_cml_pop_growth
    ahle_dueto_fmd_mortality_mean = mean_fmd_total_mortality * mean_fmd_valueperhead
    '''
)

# Standard Deviations
# The variance of a product of random variables (XY) is complex, and the variance
# of a ratio (X/Y) can only be found through simulation.
# So, to calculate ahle_dueto_mortality_stdev I'm treating mean_current_valueperhead
# as constant.
ahle_combo_withahle['ahle_dueto_mortality_stdev'] = np.sqrt(
    ahle_combo_withahle['mean_current_valueperhead']**2 + ahle_combo_withahle['stdev_current_total_mortality']**2
)
ahle_combo_withahle['ahle_dueto_ppr_mortality_stdev'] = np.sqrt(
    ahle_combo_withahle['mean_ppr_valueperhead']**2 + ahle_combo_withahle['stdev_ppr_total_mortality']**2
)
ahle_combo_withahle['ahle_dueto_bruc_mortality_stdev'] = np.sqrt(
    ahle_combo_withahle['mean_bruc_valueperhead']**2 + ahle_combo_withahle['stdev_bruc_total_mortality']**2
)
ahle_combo_withahle['ahle_dueto_fmd_mortality_stdev'] = np.sqrt(
    ahle_combo_withahle['mean_fmd_valueperhead']**2 + ahle_combo_withahle['stdev_fmd_total_mortality']**2
)

# Some age/sex groups for some species (e.g. Oxen in Cattle) have zero mortality,
# but due to zero cml pop growth, get a missing value for valueperhead and for
# ahle_dueto_mortality.
# Set ahle_dueto_mortality to zero if mortality is zero.
mortzero_ahle_lookup = {
    'mean_current_total_mortality':('ahle_dueto_mortality_mean' ,'ahle_dueto_mortality_stdev')
    ,'mean_ppr_total_mortality':('ahle_dueto_ppr_mortality_mean' ,'ahle_dueto_ppr_mortality_stdev')
    ,'mean_bruc_total_mortality':('ahle_dueto_bruc_mortality_mean' ,'ahle_dueto_bruc_mortality_stdev')
    ,'mean_fmd_total_mortality':('ahle_dueto_fmd_mortality_mean' ,'ahle_dueto_fmd_mortality_stdev')
}
for MORT_COL, AHLE_COLS in mortzero_ahle_lookup.items():
    _rowselect = (ahle_combo_withahle[MORT_COL] == 0)
    print(f"Found {_rowselect.sum()} rows where {MORT_COL} is zero. Setting {AHLE_COLS} to zero.")
    ahle_combo_withahle.loc[_rowselect ,AHLE_COLS[0]] = 0
    ahle_combo_withahle.loc[_rowselect ,AHLE_COLS[1]] = 0

# -----------------------------------------------------------------------------
# AHLE due to health cost
# -----------------------------------------------------------------------------
# Note health cost is negative, but the AHLE due to health cost is expressed as a positive.
ahle_combo_withahle = ahle_combo_withahle.eval(
    '''
    ahle_dueto_healthcost_mean = mean_current_health_cost * -1
    ahle_dueto_ppr_healthcost_mean = mean_ppr_health_cost * -1
    ahle_dueto_bruc_healthcost_mean = mean_bruc_health_cost * -1
    ahle_dueto_fmd_healthcost_mean = mean_fmd_health_cost * -1
    '''
)

# Standard Deviations
ahle_combo_withahle['ahle_dueto_healthcost_stdev'] = ahle_combo_withahle['stdev_current_health_cost']
ahle_combo_withahle['ahle_dueto_ppr_healthcost_stdev'] = ahle_combo_withahle['stdev_ppr_health_cost']
ahle_combo_withahle['ahle_dueto_bruc_healthcost_stdev'] = ahle_combo_withahle['stdev_bruc_health_cost']
ahle_combo_withahle['ahle_dueto_fmd_healthcost_stdev'] = ahle_combo_withahle['stdev_fmd_health_cost']

# -----------------------------------------------------------------------------
# AHLE due to production loss
# -----------------------------------------------------------------------------
ahle_combo_withahle = ahle_combo_withahle.eval(
    '''
    ahle_dueto_productionloss_mean = ahle_total_mean - ahle_dueto_mortality_mean - ahle_dueto_healthcost_mean
    ahle_dueto_ppr_productionloss_mean = ahle_dueto_ppr_total_mean - ahle_dueto_ppr_mortality_mean - ahle_dueto_ppr_healthcost_mean
    ahle_dueto_bruc_productionloss_mean = ahle_dueto_bruc_total_mean - ahle_dueto_bruc_mortality_mean - ahle_dueto_bruc_healthcost_mean
    ahle_dueto_fmd_productionloss_mean = ahle_dueto_fmd_total_mean - ahle_dueto_fmd_mortality_mean - ahle_dueto_fmd_healthcost_mean
    '''
)

# Standard Deviations
ahle_combo_withahle['ahle_dueto_productionloss_stdev'] = np.sqrt(
    ahle_combo_withahle['ahle_total_stdev']**2 + ahle_combo_withahle['ahle_dueto_mortality_stdev']**2 + ahle_combo_withahle['ahle_dueto_healthcost_stdev']**2
)
ahle_combo_withahle['ahle_dueto_ppr_productionloss_stdev'] = np.sqrt(
    ahle_combo_withahle['ahle_dueto_ppr_total_stdev']**2 + ahle_combo_withahle['ahle_dueto_ppr_mortality_stdev']**2 + ahle_combo_withahle['ahle_dueto_ppr_healthcost_stdev']**2
)
ahle_combo_withahle['ahle_dueto_bruc_productionloss_stdev'] = np.sqrt(
    ahle_combo_withahle['ahle_dueto_bruc_total_stdev']**2 + ahle_combo_withahle['ahle_dueto_bruc_mortality_stdev']**2 + ahle_combo_withahle['ahle_dueto_bruc_healthcost_stdev']**2
)
ahle_combo_withahle['ahle_dueto_fmd_productionloss_stdev'] = np.sqrt(
    ahle_combo_withahle['ahle_dueto_fmd_total_stdev']**2 + ahle_combo_withahle['ahle_dueto_fmd_mortality_stdev']**2 + ahle_combo_withahle['ahle_dueto_fmd_healthcost_stdev']**2
)

# -----------------------------------------------------------------------------
# AHLE from scenarios applied to specific age/sex groups
# -----------------------------------------------------------------------------
ahle_combo_withahle = ahle_combo_withahle.eval(
    '''
    ahle_when_af_ideal_mean = mean_ideal_af_gross_margin - mean_current_gross_margin
    ahle_when_am_ideal_mean = mean_ideal_am_gross_margin - mean_current_gross_margin
    ahle_when_jf_ideal_mean = mean_ideal_jf_gross_margin - mean_current_gross_margin
    ahle_when_jm_ideal_mean = mean_ideal_jm_gross_margin - mean_current_gross_margin
    ahle_when_nf_ideal_mean = mean_ideal_nf_gross_margin - mean_current_gross_margin
    ahle_when_nm_ideal_mean = mean_ideal_nm_gross_margin - mean_current_gross_margin
    ahle_when_o_ideal_mean = mean_ideal_o_gross_margin - mean_current_gross_margin

    ahle_when_a_ideal_mean = mean_ideal_a_gross_margin - mean_current_gross_margin
    ahle_when_j_ideal_mean = mean_ideal_j_gross_margin - mean_current_gross_margin
    ahle_when_n_ideal_mean = mean_ideal_n_gross_margin - mean_current_gross_margin
    '''
    # Mortality scenario applied to specific age/sex groups
    '''
    ahle_when_af_mort_imp100_mean = mean_mortality_zero_af_gross_margin - mean_current_gross_margin
    ahle_when_am_mort_imp100_mean = mean_mortality_zero_am_gross_margin - mean_current_gross_margin
    ahle_when_j_mort_imp100_mean = mean_mortality_zero_j_gross_margin - mean_current_gross_margin
    ahle_when_n_mort_imp100_mean = mean_mortality_zero_n_gross_margin - mean_current_gross_margin

    ahle_when_a_mort_imp100_mean = mean_mortality_zero_a_gross_margin - mean_current_gross_margin
    '''
    # Other scenarios applied to specific age/sex groups
    # NOTE August 2023: these only applied to small ruminants, and only with the legacy scenario parameters.
    # These scenarios do not exist in the updated small ruminant results.
    # '''
    # ahle_when_af_mort_imp25_mean = mean_mort_25_imp_af_gross_margin - mean_current_gross_margin
    # ahle_when_am_mort_imp25_mean = mean_mort_25_imp_am_gross_margin - mean_current_gross_margin
    # ahle_when_j_mort_imp25_mean = mean_mort_25_imp_j_gross_margin - mean_current_gross_margin
    # ahle_when_n_mort_imp25_mean = mean_mort_25_imp_n_gross_margin - mean_current_gross_margin

    # ahle_when_af_mort_imp50_mean = mean_mort_50_imp_af_gross_margin - mean_current_gross_margin
    # ahle_when_am_mort_imp50_mean = mean_mort_50_imp_am_gross_margin - mean_current_gross_margin
    # ahle_when_j_mort_imp50_mean = mean_mort_50_imp_j_gross_margin - mean_current_gross_margin
    # ahle_when_n_mort_imp50_mean = mean_mort_50_imp_n_gross_margin - mean_current_gross_margin

    # ahle_when_af_mort_imp75_mean = mean_mort_75_imp_af_gross_margin - mean_current_gross_margin
    # ahle_when_am_mort_imp75_mean = mean_mort_75_imp_am_gross_margin - mean_current_gross_margin
    # ahle_when_j_mort_imp75_mean = mean_mort_75_imp_j_gross_margin - mean_current_gross_margin
    # ahle_when_n_mort_imp75_mean = mean_mort_75_imp_n_gross_margin - mean_current_gross_margin

    # ahle_when_af_repro_imp25_mean = mean_current_repro_25_imp_gross_margin - mean_current_gross_margin
    # ahle_when_af_repro_imp50_mean = mean_current_repro_50_imp_gross_margin - mean_current_gross_margin
    # ahle_when_af_repro_imp75_mean = mean_current_repro_75_imp_gross_margin - mean_current_gross_margin
    # ahle_when_af_repro_imp100_mean = mean_current_repro_100_imp_gross_margin - mean_current_gross_margin

    # ahle_when_all_growth_imp25_mean = mean_current_growth_25_imp_all_gross_margin - mean_current_gross_margin
    # ahle_when_af_growth_imp25_mean = mean_current_growth_25_imp_af_gross_margin - mean_current_gross_margin
    # ahle_when_am_growth_imp25_mean = mean_current_growth_25_imp_am_gross_margin - mean_current_gross_margin
    # ahle_when_jf_growth_imp25_mean = mean_current_growth_25_imp_jf_gross_margin - mean_current_gross_margin
    # ahle_when_jm_growth_imp25_mean = mean_current_growth_25_imp_jm_gross_margin - mean_current_gross_margin
    # ahle_when_nf_growth_imp25_mean = mean_current_growth_25_imp_nf_gross_margin - mean_current_gross_margin
    # ahle_when_nm_growth_imp25_mean = mean_current_growth_25_imp_nm_gross_margin - mean_current_gross_margin

    # ahle_when_all_growth_imp50_mean = mean_current_growth_50_imp_all_gross_margin - mean_current_gross_margin
    # ahle_when_af_growth_imp50_mean = mean_current_growth_50_imp_af_gross_margin - mean_current_gross_margin
    # ahle_when_am_growth_imp50_mean = mean_current_growth_50_imp_am_gross_margin - mean_current_gross_margin
    # ahle_when_jf_growth_imp50_mean = mean_current_growth_50_imp_jf_gross_margin - mean_current_gross_margin
    # ahle_when_jm_growth_imp50_mean = mean_current_growth_50_imp_jm_gross_margin - mean_current_gross_margin
    # ahle_when_nf_growth_imp50_mean = mean_current_growth_50_imp_nf_gross_margin - mean_current_gross_margin
    # ahle_when_nm_growth_imp50_mean = mean_current_growth_50_imp_nm_gross_margin - mean_current_gross_margin

    # ahle_when_all_growth_imp75_mean = mean_current_growth_75_imp_all_gross_margin - mean_current_gross_margin
    # ahle_when_af_growth_imp75_mean = mean_current_growth_75_imp_af_gross_margin - mean_current_gross_margin
    # ahle_when_am_growth_imp75_mean = mean_current_growth_75_imp_am_gross_margin - mean_current_gross_margin
    # ahle_when_jf_growth_imp75_mean = mean_current_growth_75_imp_jf_gross_margin - mean_current_gross_margin
    # ahle_when_jm_growth_imp75_mean = mean_current_growth_75_imp_jm_gross_margin - mean_current_gross_margin
    # ahle_when_nf_growth_imp75_mean = mean_current_growth_75_imp_nf_gross_margin - mean_current_gross_margin
    # ahle_when_nm_growth_imp75_mean = mean_current_growth_75_imp_nm_gross_margin - mean_current_gross_margin

    # ahle_when_all_growth_imp100_mean = mean_current_growth_100_imp_all_gross_margin - mean_current_gross_margin
    # ahle_when_af_growth_imp100_mean = mean_current_growth_100_imp_af_gross_margin - mean_current_gross_margin
    # ahle_when_am_growth_imp100_mean = mean_current_growth_100_imp_am_gross_margin - mean_current_gross_margin
    # ahle_when_jf_growth_imp100_mean = mean_current_growth_100_imp_jf_gross_margin - mean_current_gross_margin
    # ahle_when_jm_growth_imp100_mean = mean_current_growth_100_imp_jm_gross_margin - mean_current_gross_margin
    # ahle_when_nf_growth_imp100_mean = mean_current_growth_100_imp_nf_gross_margin - mean_current_gross_margin
    # ahle_when_nm_growth_imp100_mean = mean_current_growth_100_imp_nm_gross_margin - mean_current_gross_margin
    # '''
)

# -----------------------------------------------------------------------------
# Set disease-specific AHLE to zero where it does not apply
# -----------------------------------------------------------------------------
# PPR only impacts small ruminants
_ppr_applies = (ahle_combo_withahle['species'].str.upper().isin(['SHEEP' ,'GOAT' ,'ALL SMALL RUMINANTS']))
ahle_dueto_ppr_cols = [i for i in list(ahle_combo_withahle) if 'ahle_dueto_ppr' in i]
for COL in ahle_dueto_ppr_cols:
    ahle_combo_withahle.loc[~ _ppr_applies ,COL] = \
        ahle_combo_withahle.loc[~ _ppr_applies ,COL].fillna(0)

# Brucellosis impacts small ruminants and cattle
_bruc_applies = (ahle_combo_withahle['species'].str.upper().isin(['SHEEP' ,'GOAT' ,'ALL SMALL RUMINANTS' ,'CATTLE']))
ahle_dueto_bruc_cols = [i for i in list(ahle_combo_withahle) if 'ahle_dueto_bruc' in i]
for COL in ahle_dueto_bruc_cols:
    ahle_combo_withahle.loc[~ _bruc_applies ,COL] = \
        ahle_combo_withahle.loc[~ _bruc_applies ,COL].fillna(0)

# FMD only impacts cattle
_fmd_applies = (ahle_combo_withahle['species'].str.upper().isin(['CATTLE']))
ahle_dueto_fmd_cols = [i for i in list(ahle_combo_withahle) if 'ahle_dueto_fmd' in i]
for COL in ahle_dueto_fmd_cols:
    ahle_combo_withahle.loc[~ _fmd_applies ,COL] = \
        ahle_combo_withahle.loc[~ _fmd_applies ,COL].fillna(0)

# =============================================================================
#### Add currency conversion
# =============================================================================
# Add columns in USD
mean_cols_ahle = [i for i in list(ahle_combo_withahle) if 'mean' in i and 'ahle' in i]
for MEANCOL in mean_cols_ahle:
    MEANCOL_USD = MEANCOL + '_usd'
    ahle_combo_withahle[MEANCOL_USD] = ahle_combo_withahle[MEANCOL] / ahle_combo_withahle['exchg_rate_lcuperusdol']

# For standard deviations, convert to variances then scale by the squared exchange rate
# VAR(aX) = a^2 * VAR(X). a = 1/exchange rate.
sd_cols_ahle = [i for i in list(ahle_combo_withahle) if 'stdev' in i and 'ahle' in i]
for SDCOL in sd_cols_ahle:
    SDCOL_USD = SDCOL + '_usd'
    ahle_combo_withahle[SDCOL_USD] = np.sqrt(
        ahle_combo_withahle[SDCOL]**2 / ahle_combo_withahle['exchg_rate_lcuperusdol']**2
    )

# =============================================================================
#### Cleanup and export
# =============================================================================
datainfo(ahle_combo_withahle)

# Keep only key columns and AHLE calcs
_ahle_cols = [i for i in list(ahle_combo_withahle) if 'ahle' in i]
_cols_for_summary = list(all_byvars) + _ahle_cols
_cols_for_summary.remove('item')
_cols_for_summary.remove('item_type_code')

ahle_combo_withahle_smry = ahle_combo_withahle[_cols_for_summary].reset_index(drop=True)
datainfo(ahle_combo_withahle_smry ,150)

ahle_combo_withahle_smry.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_summary2.csv') ,index=False)
ahle_combo_withahle_smry.to_pickle(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_summary2.pkl.gz'))

# Output for Dash
ahle_combo_withahle_smry.to_csv(os.path.join(DASH_DATA_FOLDER ,'ahle_all_summary2.csv') ,index=False)

#%% CHECKS ON CALCULATED AHLE

check_ahle_combo_withahle = ahle_combo_withahle.copy()

# =============================================================================
#### Sum of agesex AHLE compared to overall AHLE
# =============================================================================
check_ahle_combo_withahle.eval(
    '''
    sum_ahle_individual = ahle_when_af_ideal_mean + ahle_when_am_ideal_mean \
        + ahle_when_jf_ideal_mean + ahle_when_jm_ideal_mean \
        + ahle_when_nf_ideal_mean + ahle_when_nm_ideal_mean \
        + ahle_when_o_ideal_mean

    sum_ahle_individual_vs_overall = sum_ahle_individual / ahle_total_mean
    '''
    ,inplace=True
)
print('\n> Checking the sum AHLE for individual ideal scenarios against the overall')
print(check_ahle_combo_withahle[['region' ,'species' ,'production_system' ,'year' ,'sum_ahle_individual_vs_overall']])

# =============================================================================
#### Disease-specific AHLE vs. total
# =============================================================================
check_ahle_combo_withahle.eval(
    '''
    ahle_dueto_ppr_vs_total = ahle_dueto_ppr_total_mean / ahle_total_mean
    ahle_dueto_bruc_vs_total = ahle_dueto_bruc_total_mean / ahle_total_mean
    ahle_dueto_fmd_vs_total = ahle_dueto_fmd_total_mean / ahle_total_mean
    '''
    ,inplace=True
)
print('\n> Checking the AHLE for PPR against the overall')
print(check_ahle_combo_withahle.query("ahle_dueto_ppr_total_mean.notnull()")[['species' ,'production_system' ,'year' ,'ahle_dueto_ppr_vs_total']])

print('\n> Checking the AHLE for Brucellosis against the overall')
print(check_ahle_combo_withahle.query("ahle_dueto_bruc_total_mean.notnull()")[['species' ,'production_system' ,'year' ,'ahle_dueto_bruc_vs_total']])

print('\n> Checking the AHLE for FMD against the overall')
print(check_ahle_combo_withahle.query("ahle_dueto_fmd_total_mean.notnull()")[['species' ,'production_system' ,'year' ,'ahle_dueto_fmd_vs_total']])
