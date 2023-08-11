#%% ABOUT
'''
This program calculates animal health loss envelope using the combined
simulation results. The output is a CSV file to be used in the dashboard.

IMPORTANT: before running this, set Python's working directory to the folder
where this code is stored.
'''
#%% PACKAGES AND FUNCTIONS

import os                        # Operating system functions
import inspect
import io
import time
import numpy as np
import pandas as pd
import pickle                             # To save objects to disk

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

# Create a function to fill values of one column with another for a subset of rows
# Example usage:
# _row_select = (df['col'] == 'value')
# df = fill_column_where(df ,_row_select ,'col' ,'fill_col' ,DROP=True)
def fill_column_where(
        DATAFRAME           # Dataframe
        ,LOC                # Dataframe mask e.g. _loc = (df['col'] == 'Value')
        ,COLUMN_TOFILL      # String
        ,COLUMN_TOUSE       # String
        ,DROP=False         # True: drop COLUMN_TOUSE
    ):
    funcname = inspect.currentframe().f_code.co_name
    dfmod = DATAFRAME.copy()
    print(f"<{funcname}> Processing {dfmod.loc[LOC].shape[0]} rows.")
    try:
        dfmod[COLUMN_TOUSE]     # If column to use exists
        print(f"<{funcname}> - Filling {COLUMN_TOFILL} with {COLUMN_TOUSE}.")
        dfmod.loc[LOC ,COLUMN_TOFILL] = dfmod.loc[LOC ,COLUMN_TOUSE]
        if DROP:
            dfmod = dfmod.drop(columns=COLUMN_TOUSE)
            print(f"<{funcname}> Dropping {COLUMN_TOUSE}.")
    except:
        print(f"<{funcname}> - {COLUMN_TOUSE} not found. Filling {COLUMN_TOFILL} with nan.")
        dfmod.loc[LOC ,COLUMN_TOFILL] = np.nan
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
#### Prepare currency conversion data
# =============================================================================
# Read conversion data
exchg_data = pd.read_csv(os.path.join(ETHIOPIA_DATA_FOLDER ,'worldbank_inflation_exchangerate_gdp_2010_2021' ,'20475199-8fa4-4249-baec-98b6635f68e3_Data.csv'))
cleancolnames(exchg_data)
datainfo(exchg_data)

# Filter and rename
exchg_data_tomerge = exchg_data.query("country_name == 'Ethiopia'")
keep_rename_cols = {
    'country_name':'country_name'
    ,'time':'year'
    ,'official_exchange_rate__lcu_per_us_dol___period_average___pa_nus_fcrf_':'exchg_rate_lcuperusdol'
    }
exchg_data_tomerge = exchg_data_tomerge[list(keep_rename_cols)].rename(columns=keep_rename_cols)

# Fill coded values with nan
exchg_data_tomerge['exchg_rate_lcuperusdol'] = exchg_data_tomerge['exchg_rate_lcuperusdol'].replace('..' ,np.nan).astype('float64')

# Year 2021 is missing. Fill with 2020.
# This fills any missing year with the previous
exchg_data_tomerge['exchg_rate_lcuperusdol_prev'] = exchg_data_tomerge['exchg_rate_lcuperusdol'].shift(periods=1)
exchg_data_tomerge['exchg_rate_lcuperusdol'] = \
    exchg_data_tomerge['exchg_rate_lcuperusdol'].fillna(exchg_data_tomerge['exchg_rate_lcuperusdol_prev'])
del exchg_data_tomerge['exchg_rate_lcuperusdol_prev']

datainfo(exchg_data_tomerge)

# Export
exchg_data_tomerge.to_pickle(os.path.join(ETHIOPIA_DATA_FOLDER ,'wb_exchg_data_processed.pkl.gz'))

#%% READ COMBINED SIMULATION DATA

ahle_combo_adj = pd.read_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_stacked_adj.csv'))
datainfo(ahle_combo_adj)

#%% ADD GROUP SUMMARIES
'''
Creating aggregate groups for filtering in the dashboard.

Note: aggregate groups are already created for many items in the compartmental
model, but not all. For simplicity, and to ensure the totals are correct, I'm
recalculating the aggregate for all items.
'''
mean_cols = [i for i in list(ahle_combo_adj) if 'mean' in i]
sd_cols = [i for i in list(ahle_combo_adj) if 'stdev' in i]

# =============================================================================
#### Drop aggregate groups
# =============================================================================
# Separate all existing Overall records.
_combined_rows = (ahle_combo_adj['group'].str.upper() == 'OVERALL')\
    | (ahle_combo_adj['group'].str.contains('COMBINED' ,case=False ,na=False))
ahle_combo_overall = ahle_combo_adj.loc[_combined_rows].copy()

list_items_all = ahle_combo_adj['item'].value_counts()
list_items_overall = ahle_combo_adj.loc[_combined_rows]['item'].value_counts()

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
all_byvars = ['species' ,'region' ,'production_system' ,'item' ,'item_type_code' ,'group' ,'age_group' ,'sex' ,'year']

# Only using mean and standard deviaion of each item, as the other statistics
# cannot be summed.
keepcols = all_byvars + mean_cols + sd_cols
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
summarize_byvars = all_byvars.copy()
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
summarize_byvars = all_byvars.copy()
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
summarize_byvars = all_byvars.copy()
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
summarize_byvars = all_byvars.copy()
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
summarize_byvars = all_byvars.copy()
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

# Add columns in USD for currency items
for MEANCOL in mean_cols:
   MEANCOL_USD = MEANCOL + '_usd'
   ahle_combo_withagg.loc[ahle_combo_withagg['item_type_code'].isin(['mv' ,'mc']) ,MEANCOL_USD] = \
      ahle_combo_withagg[MEANCOL] / ahle_combo_withagg['exchg_rate_lcuperusdol']

# For standard deviations, scale variances by the squared exchange rate
# VAR(aX) = a^2 * VAR(X). a = 1/exchange rate.
for SDCOL in sd_cols:
   SDCOL_USD = SDCOL + '_usd'
   ahle_combo_withagg.loc[ahle_combo_withagg['item_type_code'].isin(['mv' ,'mc']) ,SDCOL_USD] = \
      np.sqrt(ahle_combo_withagg[SDCOL]**2 / ahle_combo_withagg['exchg_rate_lcuperusdol']**2)

datainfo(ahle_combo_withagg)

# =============================================================================
#### Cleanup and Export
# =============================================================================
# -----------------------------------------------------------------------------
# Subset and rename columns
# -----------------------------------------------------------------------------
# In this file, keeping only scenarios that apply to all groups
keepcols = [
    'region'
    ,'species'
    ,'production_system'
    ,'item'
    ,'group'
    ,'age_group'
    ,'sex'
    ,'year'

    # In Birr
    ,'mean_current'
    ,'stdev_current'
    ,'mean_ideal'
    ,'stdev_ideal'

    ,'mean_ppr'
    ,'stdev_ppr'
    ,'mean_bruc'
    ,'stdev_bruc'

    ,'mean_all_mort_25_imp'
    ,'stdev_all_mort_25_imp'
    ,'mean_all_mort_50_imp'
    ,'stdev_all_mort_50_imp'
    ,'mean_all_mort_75_imp'
    ,'stdev_all_mort_75_imp'
    ,'mean_mortality_zero'
    ,'stdev_mortality_zero'

    ,'mean_current_repro_25_imp'
    ,'stdev_current_repro_25_imp'
    ,'stdev_current_repro_50_imp'
    ,'mean_current_repro_50_imp'
    ,'mean_current_repro_75_imp'
    ,'stdev_current_repro_75_imp'
    ,'mean_current_repro_100_imp'
    ,'stdev_current_repro_100_imp'

    ,'mean_current_growth_25_imp_all'
    ,'stdev_current_growth_25_imp_all'
    ,'mean_current_growth_50_imp_all'
    ,'stdev_current_growth_50_imp_all'
    ,'mean_current_growth_75_imp_all'
    ,'stdev_current_growth_75_imp_all'
    ,'mean_current_growth_100_imp_all'
    ,'stdev_current_growth_100_imp_all'

    # In USD
    ,'exchg_rate_lcuperusdol'

    ,'mean_current_usd'
    ,'stdev_current_usd'
    ,'mean_ideal_usd'
    ,'stdev_ideal_usd'

    ,'mean_ppr_usd'
    ,'stdev_ppr_usd'
    ,'mean_bruc_usd'
    ,'stdev_bruc_usd'

    ,'mean_all_mort_25_imp_usd'
    ,'stdev_all_mort_25_imp_usd'
    ,'mean_all_mort_50_imp_usd'
    ,'stdev_all_mort_50_imp_usd'
    ,'mean_all_mort_75_imp_usd'
    ,'stdev_all_mort_75_imp_usd'
    ,'mean_mortality_zero_usd'
    ,'stdev_mortality_zero_usd'

    ,'mean_current_repro_25_imp_usd'
    ,'stdev_current_repro_25_imp_usd'
    ,'mean_current_repro_50_imp_usd'
    ,'stdev_current_repro_50_imp_usd'
    ,'mean_current_repro_75_imp_usd'
    ,'stdev_current_repro_75_imp_usd'
    ,'mean_current_repro_100_imp_usd'
    ,'stdev_current_repro_100_imp_usd'

    ,'mean_current_growth_25_imp_all_usd'
    ,'stdev_current_growth_25_imp_all_usd'
    ,'mean_current_growth_50_imp_all_usd'
    ,'stdev_current_growth_50_imp_all_usd'
    ,'mean_current_growth_75_imp_all_usd'
    ,'stdev_current_growth_75_imp_all_usd'
    ,'mean_current_growth_100_imp_all_usd'
    ,'stdev_current_growth_100_imp_all_usd'
]

ahle_combo_withagg_smry = ahle_combo_withagg[keepcols].copy()
datainfo(ahle_combo_withagg_smry)

ahle_combo_withagg_smry.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_summary.csv') ,index=False)
# ahle_combo_withagg_smry.to_pickle(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_summary.pkl.gz'))

# Output for Dash
# This is not currently used in Dash
# ahle_combo_withagg_smry.to_csv(os.path.join(DASH_DATA_FOLDER ,'ahle_all_summary.csv') ,index=False)

#%% CALCULATE AHLE

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
    ,'value of total mortality'
    ]
keep_items_upper = [i.upper() for i in keep_items]
_items_for_ahle = (ahle_combo_withagg['item'].str.upper().isin(keep_items_upper))

ahle_combo_withagg_p = ahle_combo_withagg.loc[(_items_for_ahle)].pivot(
    index=['region' ,'species' ,'production_system' ,'group' ,'age_group' ,'sex' ,'year' ,'exchg_rate_lcuperusdol']
    ,columns='item'
    ,values=mean_cols_agg + sd_cols_agg
).reset_index()
ahle_combo_withagg_p = colnames_from_index(ahle_combo_withagg_p)   # Change multi-index to column names
cleancolnames(ahle_combo_withagg_p)

# Remove underscores added when collapsing column index
ahle_combo_withagg_p = ahle_combo_withagg_p.rename(
    columns={
        'region_':'region'
        ,'species_':'species'
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
    - Total AHLE is difference in gross margin between ideal and current scenario
    - AHLE due to mortality is difference in gross margin between zero mortality and current scenario
    - AHLE due to health cost is current health cost (ideal health cost is zero)
    - AHLE due to production loss is the remainder needed to make total AHLE after accounting for mortality and health cost
        Note production loss is hardest to measure because it is the lost potential production among the animals that survived

        It's tempting to take the difference in "total production value" between the current
        and ideal scenario as the production loss. However, this includes lost value due to
        animals that died. We want the production loss just among the animals that survived.

Calculating mean and standard deviation for each AHLE component.
Relying on the following properties of sums of random variables:
    mean(aX + bY) = a*mean(X) + b*mean(Y), regardless of correlation
    var(aX + bY) = a^2*var(X) + b^2*var(Y), assuming X and Y are uncorrelated
'''
ahle_combo_withahle = ahle_combo_withagg_p.copy()

ahle_combo_withahle = ahle_combo_withahle.eval(
    # Top level
    # Note health cost is negative, but the AHLE due to health cost is expressed as a positive.
    '''
    ahle_total_mean = mean_ideal_gross_margin - mean_current_gross_margin
    ahle_dueto_mortality_mean = mean_mortality_zero_gross_margin - mean_current_gross_margin
    ahle_dueto_healthcost_mean = mean_current_health_cost * -1
    ahle_dueto_productionloss_mean = ahle_total_mean - ahle_dueto_mortality_mean - ahle_dueto_healthcost_mean
    '''
    # Disease-specific
    '''
    ahle_dueto_ppr_total_mean = mean_ideal_gross_margin - mean_ppr_gross_margin
    ahle_dueto_ppr_mortality_mean = mean_ppr_value_of_total_mortality * -1
    ahle_dueto_ppr_healthcost_mean = mean_ppr_health_cost * -1
    ahle_dueto_ppr_productionloss_mean = ahle_dueto_ppr_total_mean - ahle_dueto_ppr_mortality_mean - ahle_dueto_ppr_healthcost_mean

    ahle_dueto_bruc_total_mean = mean_ideal_gross_margin - mean_bruc_gross_margin
    ahle_dueto_bruc_mortality_mean = mean_bruc_value_of_total_mortality * -1
    ahle_dueto_bruc_healthcost_mean = mean_bruc_health_cost * -1
    ahle_dueto_bruc_productionloss_mean = ahle_dueto_bruc_total_mean - ahle_dueto_bruc_mortality_mean - ahle_dueto_bruc_healthcost_mean
    '''
    # AHLE due to Other Disease will depend on which diseases were estimated.
    # Don't calculate this here. Handle it when needed (attribution).
    # Should be: ahle_dueto_otherdisease_total_mean = ahle_total_infectious - ahle_dueto_ppr_total_mean - ahle_dueto_bruc_total_mean

    # Ideal scenario applied to specific age/sex groups
    '''
    ahle_when_af_ideal_mean = mean_ideal_af_gross_margin - mean_current_gross_margin
    ahle_when_am_ideal_mean = mean_ideal_am_gross_margin - mean_current_gross_margin
    ahle_when_jf_ideal_mean = mean_ideal_jf_gross_margin - mean_current_gross_margin
    ahle_when_jm_ideal_mean = mean_ideal_jm_gross_margin - mean_current_gross_margin
    ahle_when_nf_ideal_mean = mean_ideal_nf_gross_margin - mean_current_gross_margin
    ahle_when_nm_ideal_mean = mean_ideal_nm_gross_margin - mean_current_gross_margin
    ahle_when_o_ideal_mean = mean_ideal_o_gross_margin - mean_current_gross_margin
    '''
    # Mortality scenario applied to specific age/sex groups
    '''
    ahle_when_af_mort_imp25_mean = mean_mort_25_imp_af_gross_margin - mean_current_gross_margin
    ahle_when_am_mort_imp25_mean = mean_mort_25_imp_am_gross_margin - mean_current_gross_margin
    ahle_when_j_mort_imp25_mean = mean_mort_25_imp_j_gross_margin - mean_current_gross_margin
    ahle_when_n_mort_imp25_mean = mean_mort_25_imp_n_gross_margin - mean_current_gross_margin

    ahle_when_af_mort_imp50_mean = mean_mort_50_imp_af_gross_margin - mean_current_gross_margin
    ahle_when_am_mort_imp50_mean = mean_mort_50_imp_am_gross_margin - mean_current_gross_margin
    ahle_when_j_mort_imp50_mean = mean_mort_50_imp_j_gross_margin - mean_current_gross_margin
    ahle_when_n_mort_imp50_mean = mean_mort_50_imp_n_gross_margin - mean_current_gross_margin

    ahle_when_af_mort_imp75_mean = mean_mort_75_imp_af_gross_margin - mean_current_gross_margin
    ahle_when_am_mort_imp75_mean = mean_mort_75_imp_am_gross_margin - mean_current_gross_margin
    ahle_when_j_mort_imp75_mean = mean_mort_75_imp_j_gross_margin - mean_current_gross_margin
    ahle_when_n_mort_imp75_mean = mean_mort_75_imp_n_gross_margin - mean_current_gross_margin

    ahle_when_af_mort_imp100_mean = mean_mortality_zero_af_gross_margin - mean_current_gross_margin
    ahle_when_am_mort_imp100_mean = mean_mortality_zero_am_gross_margin - mean_current_gross_margin
    ahle_when_j_mort_imp100_mean = mean_mortality_zero_j_gross_margin - mean_current_gross_margin
    ahle_when_n_mort_imp100_mean = mean_mortality_zero_n_gross_margin - mean_current_gross_margin
    '''
    # These apply to poultry
    '''
    ahle_when_a_ideal_mean = mean_ideal_a_gross_margin - mean_current_gross_margin
    ahle_when_j_ideal_mean = mean_ideal_j_gross_margin - mean_current_gross_margin
    ahle_when_n_ideal_mean = mean_ideal_n_gross_margin - mean_current_gross_margin

    ahle_when_a_mort_imp100_mean = mean_mortality_zero_a_gross_margin - mean_current_gross_margin
    ahle_when_j_mort_imp100_mean = mean_mortality_zero_j_gross_margin - mean_current_gross_margin
    ahle_when_n_mort_imp100_mean = mean_mortality_zero_n_gross_margin - mean_current_gross_margin
    '''
    # Other scenarios applied to specific age/sex groups
    '''
    ahle_when_af_repro_imp25_mean = mean_current_repro_25_imp_gross_margin - mean_current_gross_margin
    ahle_when_af_repro_imp50_mean = mean_current_repro_50_imp_gross_margin - mean_current_gross_margin
    ahle_when_af_repro_imp75_mean = mean_current_repro_75_imp_gross_margin - mean_current_gross_margin
    ahle_when_af_repro_imp100_mean = mean_current_repro_100_imp_gross_margin - mean_current_gross_margin

    ahle_when_all_growth_imp25_mean = mean_current_growth_25_imp_all_gross_margin - mean_current_gross_margin
    ahle_when_af_growth_imp25_mean = mean_current_growth_25_imp_af_gross_margin - mean_current_gross_margin
    ahle_when_am_growth_imp25_mean = mean_current_growth_25_imp_am_gross_margin - mean_current_gross_margin
    ahle_when_jf_growth_imp25_mean = mean_current_growth_25_imp_jf_gross_margin - mean_current_gross_margin
    ahle_when_jm_growth_imp25_mean = mean_current_growth_25_imp_jm_gross_margin - mean_current_gross_margin
    ahle_when_nf_growth_imp25_mean = mean_current_growth_25_imp_nf_gross_margin - mean_current_gross_margin
    ahle_when_nm_growth_imp25_mean = mean_current_growth_25_imp_nm_gross_margin - mean_current_gross_margin

    ahle_when_all_growth_imp50_mean = mean_current_growth_50_imp_all_gross_margin - mean_current_gross_margin
    ahle_when_af_growth_imp50_mean = mean_current_growth_50_imp_af_gross_margin - mean_current_gross_margin
    ahle_when_am_growth_imp50_mean = mean_current_growth_50_imp_am_gross_margin - mean_current_gross_margin
    ahle_when_jf_growth_imp50_mean = mean_current_growth_50_imp_jf_gross_margin - mean_current_gross_margin
    ahle_when_jm_growth_imp50_mean = mean_current_growth_50_imp_jm_gross_margin - mean_current_gross_margin
    ahle_when_nf_growth_imp50_mean = mean_current_growth_50_imp_nf_gross_margin - mean_current_gross_margin
    ahle_when_nm_growth_imp50_mean = mean_current_growth_50_imp_nm_gross_margin - mean_current_gross_margin

    ahle_when_all_growth_imp75_mean = mean_current_growth_75_imp_all_gross_margin - mean_current_gross_margin
    ahle_when_af_growth_imp75_mean = mean_current_growth_75_imp_af_gross_margin - mean_current_gross_margin
    ahle_when_am_growth_imp75_mean = mean_current_growth_75_imp_am_gross_margin - mean_current_gross_margin
    ahle_when_jf_growth_imp75_mean = mean_current_growth_75_imp_jf_gross_margin - mean_current_gross_margin
    ahle_when_jm_growth_imp75_mean = mean_current_growth_75_imp_jm_gross_margin - mean_current_gross_margin
    ahle_when_nf_growth_imp75_mean = mean_current_growth_75_imp_nf_gross_margin - mean_current_gross_margin
    ahle_when_nm_growth_imp75_mean = mean_current_growth_75_imp_nm_gross_margin - mean_current_gross_margin

    ahle_when_all_growth_imp100_mean = mean_current_growth_100_imp_all_gross_margin - mean_current_gross_margin
    ahle_when_af_growth_imp100_mean = mean_current_growth_100_imp_af_gross_margin - mean_current_gross_margin
    ahle_when_am_growth_imp100_mean = mean_current_growth_100_imp_am_gross_margin - mean_current_gross_margin
    ahle_when_jf_growth_imp100_mean = mean_current_growth_100_imp_jf_gross_margin - mean_current_gross_margin
    ahle_when_jm_growth_imp100_mean = mean_current_growth_100_imp_jm_gross_margin - mean_current_gross_margin
    ahle_when_nf_growth_imp100_mean = mean_current_growth_100_imp_nf_gross_margin - mean_current_gross_margin
    ahle_when_nm_growth_imp100_mean = mean_current_growth_100_imp_nm_gross_margin - mean_current_gross_margin
    '''
)

# -----------------------------------------------------------------------------
# Standard deviations
# -----------------------------------------------------------------------------
# Require summing variances and taking square root. Must be done outside eval().
# Base
ahle_combo_withahle['ahle_total_stdev'] = np.sqrt(
    ahle_combo_withahle['stdev_ideal_gross_margin']**2 + ahle_combo_withahle['stdev_current_gross_margin']**2
    )
ahle_combo_withahle['ahle_dueto_mortality_stdev'] = np.sqrt(
    ahle_combo_withahle['stdev_mortality_zero_gross_margin']**2 + ahle_combo_withahle['stdev_current_gross_margin']**2
    )
ahle_combo_withahle['ahle_dueto_healthcost_stdev'] = ahle_combo_withahle['stdev_current_health_cost']
ahle_combo_withahle['ahle_dueto_productionloss_stdev'] = np.sqrt(
    ahle_combo_withahle['ahle_total_stdev']**2 + ahle_combo_withahle['ahle_dueto_mortality_stdev']**2 + ahle_combo_withahle['ahle_dueto_healthcost_stdev']**2
    )

# PPR
ahle_combo_withahle['ahle_dueto_ppr_total_stdev'] = np.sqrt(
    ahle_combo_withahle['stdev_ideal_gross_margin']**2 + ahle_combo_withahle['stdev_ppr_gross_margin']**2
    )
ahle_combo_withahle['ahle_dueto_ppr_mortality_stdev'] = ahle_combo_withahle['stdev_ppr_value_of_total_mortality']
ahle_combo_withahle['ahle_dueto_ppr_healthcost_stdev'] = ahle_combo_withahle['stdev_ppr_health_cost']
ahle_combo_withahle['ahle_dueto_ppr_productionloss_stdev'] = np.sqrt(
    ahle_combo_withahle['ahle_dueto_ppr_total_stdev']**2 + ahle_combo_withahle['ahle_dueto_ppr_mortality_stdev']**2 + ahle_combo_withahle['ahle_dueto_ppr_healthcost_stdev']**2
    )

# Brucellosis
ahle_combo_withahle['ahle_dueto_bruc_total_stdev'] = np.sqrt(
    ahle_combo_withahle['stdev_ideal_gross_margin']**2 + ahle_combo_withahle['stdev_bruc_gross_margin']**2
    )
ahle_combo_withahle['ahle_dueto_bruc_mortality_stdev'] = ahle_combo_withahle['stdev_bruc_value_of_total_mortality']
ahle_combo_withahle['ahle_dueto_bruc_healthcost_stdev'] = ahle_combo_withahle['stdev_bruc_health_cost']
ahle_combo_withahle['ahle_dueto_bruc_productionloss_stdev'] = np.sqrt(
    ahle_combo_withahle['ahle_dueto_bruc_total_stdev']**2 + ahle_combo_withahle['ahle_dueto_bruc_mortality_stdev']**2 + ahle_combo_withahle['ahle_dueto_bruc_healthcost_stdev']**2
    )

# -----------------------------------------------------------------------------
# Set disease-specific AHLE to zero where it does not apply
# -----------------------------------------------------------------------------
# PPR only impacts small ruminants
ahle_dueto_ppr_cols = [i for i in list(ahle_combo_withahle) if 'ahle_dueto_ppr' in i]
_ppr_applies = (ahle_combo_withahle['species'].str.upper().isin(['SHEEP' ,'GOAT' ,'ALL SMALL RUMINANTS']))
for COL in ahle_dueto_ppr_cols:
    ahle_combo_withahle.loc[~ _ppr_applies ,COL] = \
        ahle_combo_withahle.loc[~ _ppr_applies ,COL].fillna(0)

# Brucellosis impacts small ruminants and cattle
ahle_dueto_bruc_cols = [i for i in list(ahle_combo_withahle) if 'ahle_dueto_bruc' in i]
_bruc_applies = (ahle_combo_withahle['species'].str.upper().isin(['SHEEP' ,'GOAT' ,'ALL SMALL RUMINANTS' ,'CATTLE']))
for COL in ahle_dueto_bruc_cols:
    ahle_combo_withahle.loc[~ _bruc_applies ,COL] = \
        ahle_combo_withahle.loc[~ _bruc_applies ,COL].fillna(0)

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
    ahle_combo_withahle[SDCOL_USD] = np.sqrt(ahle_combo_withahle[SDCOL]**2 / ahle_combo_withahle['exchg_rate_lcuperusdol']**2)

# =============================================================================
#### Cleanup and export
# =============================================================================
datainfo(ahle_combo_withahle)

# Keep only key columns and AHLE calcs
_ahle_cols = [i for i in list(ahle_combo_withahle) if 'ahle' in i]
_cols_for_summary = ['region' ,'species' ,'production_system' ,'group' ,'age_group' ,'sex' ,'year'] + _ahle_cols

ahle_combo_withahle_smry = ahle_combo_withahle[_cols_for_summary].reset_index(drop=True)
datainfo(ahle_combo_withahle_smry)

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
    '''
    ,inplace=True
)
print('\n> Checking the AHLE for PPR against the overall')
print(check_ahle_combo_withahle.query("ahle_dueto_ppr_total_mean.notnull()")[['species' ,'production_system' ,'year' ,'ahle_dueto_ppr_vs_total']])

print('\n> Checking the AHLE for Brucellosis against the overall')
print(check_ahle_combo_withahle.query("ahle_dueto_bruc_total_mean.notnull()")[['species' ,'production_system' ,'year' ,'ahle_dueto_bruc_vs_total']])
