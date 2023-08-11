#%% ABOUT
'''
This program produces a summary data frame for the compartmental model with a
different structure. This uses only the total system value for each item.
It then creates a row for each scenario. Scenarios set either specific age/sex
groups to ideal conditions or all age/sex groups simultaneously.

For example, the ideal_AF scenario sets adult females to ideal conditions while
leaving other age/sex groups at their current conditions; the resulting values are
interpreted as the total system values of gross margin, health cost, etc., when
adult females are at their ideal.

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

#%% READ DATA

# =============================================================================
#### Combined compartmental model results
# =============================================================================
ahle_combo_adj = pd.read_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_stacked_adj.csv'))

# =============================================================================
#### Currency exchange data
# =============================================================================
exchg_data_tomerge = pd.read_pickle(os.path.join(ETHIOPIA_DATA_FOLDER ,'wb_exchg_data_processed.pkl.gz'))

#%% CREATE SCENARIO SUMMARY TABLE
'''
Plan to minimize changes needed in Dash:
    Columns will retain names of system total scenarios, e.g:
        mean_ideal, mean_mortality_zero, etc.
        mean_ideal_usd, mean_mortality_zero_usd, etc.
    agesex_scenario column will signify the scope of each scenario, e.g.:
        Where agesex_scenario == 'Overall', entry is result of system total scenario
        Where agesex_scenario == 'Adult Female', entry is result of AF-specific scenario
        etc.
'''
# =============================================================================
#### Create a row for each age/sex scenario and the overall scenario
# =============================================================================
scenario_basetable = pd.DataFrame({
   'agesex_scenario':[
      'Adult Female'
      ,'Adult Male'
      ,'Adult Combined'

      ,'Juvenile Female'
      ,'Juvenile Male'
      ,'Juvenile Combined'

      ,'Neonatal Female'
      ,'Neonatal Male'
      ,'Neonatal Combined'

      ,'Oxen'       # Only applies to Cattle

      ,'Overall'
      ]
   ,'group':'Overall'
   })

ahle_combo_scensmry = pd.merge(
   left=scenario_basetable
   ,right=ahle_combo_adj.query("group.str.upper() == 'OVERALL'")    # Keep only System Total results (group = "Overall")
   ,on='group'
   ,how='outer'
   )

# Drop rows for age*sex scenarios that don't apply to species
_droprows = (ahle_combo_scensmry['agesex_scenario'].str.upper() == 'OXEN') \
    & (ahle_combo_scensmry['species'].str.upper() != 'CATTLE')
print(f"> Dropping {_droprows.sum() :,} rows where agesex_scenario does not apply to species.")
ahle_combo_scensmry = ahle_combo_scensmry.drop(ahle_combo_scensmry.loc[_droprows].index).reset_index(drop=True)

# =============================================================================
#### Assign results from correct scenario column to each row
# =============================================================================
'''
Note that current scenario column applies to every row.
Note also that agesex_scenario Overall uses columns unchanged.
'''
# -----------------------------------------------------------------------------
# Adult Female
# -----------------------------------------------------------------------------
select_agesex_scenario = 'adult female'
select_agesex_scenario_upcase = select_agesex_scenario.upper()
_scen_select = (ahle_combo_scensmry['agesex_scenario'].str.upper() == select_agesex_scenario_upcase)
print(f"\n> Selected {_scen_select.sum(): ,} rows where agesex_scenario is {select_agesex_scenario_upcase}.")

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ideal' ,'mean_ideal_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ideal' ,'stdev_ideal_af' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ppr' ,'mean_ppr_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ppr' ,'stdev_ppr_af' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_bruc' ,'mean_bruc_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_bruc' ,'stdev_bruc_af' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_mortality_zero' ,'mean_mortality_zero_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_mortality_zero' ,'stdev_mortality_zero_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_25_imp' ,'mean_mort_25_imp_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_25_imp' ,'stdev_mort_25_imp_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_50_imp' ,'mean_mort_50_imp_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_50_imp' ,'stdev_mort_50_imp_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_75_imp' ,'mean_mort_75_imp_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_75_imp' ,'stdev_mort_75_imp_af' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_25_imp_all' ,'mean_current_growth_25_imp_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_25_imp_all' ,'stdev_current_growth_25_imp_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_50_imp_all' ,'mean_current_growth_50_imp_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_50_imp_all' ,'stdev_current_growth_50_imp_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_75_imp_all' ,'mean_current_growth_75_imp_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_75_imp_all' ,'stdev_current_growth_75_imp_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_100_imp_all' ,'mean_current_growth_100_imp_af' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_100_imp_all' ,'stdev_current_growth_100_imp_af' ,DROP=True)

# Reproduction scenario already applies to adult females
# 'mean_current_repro_25_imp'
# 'mean_current_repro_50_imp'
# 'mean_current_repro_75_imp'
# 'mean_current_repro_100_imp'

# -----------------------------------------------------------------------------
# Adult Male
# -----------------------------------------------------------------------------
select_agesex_scenario = 'adult male'
select_agesex_scenario_upcase = select_agesex_scenario.upper()
_scen_select = (ahle_combo_scensmry['agesex_scenario'].str.upper() == select_agesex_scenario_upcase)
print(f"\n> Selected {_scen_select.sum(): ,} rows where agesex_scenario is {select_agesex_scenario_upcase}.")

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ideal' ,'mean_ideal_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ideal' ,'stdev_ideal_am' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ppr' ,'mean_ppr_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ppr' ,'stdev_ppr_am' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_bruc' ,'mean_bruc_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_bruc' ,'stdev_bruc_am' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_mortality_zero' ,'mean_mortality_zero_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_mortality_zero' ,'stdev_mortality_zero_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_25_imp' ,'mean_mort_25_imp_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_25_imp' ,'stdev_mort_25_imp_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_50_imp' ,'mean_mort_50_imp_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_50_imp' ,'stdev_mort_50_imp_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_75_imp' ,'mean_mort_75_imp_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_75_imp' ,'stdev_mort_75_imp_am' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_25_imp_all' ,'mean_current_growth_25_imp_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_25_imp_all' ,'stdev_current_growth_25_imp_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_50_imp_all' ,'mean_current_growth_50_imp_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_50_imp_all' ,'stdev_current_growth_50_imp_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_75_imp_all' ,'mean_current_growth_75_imp_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_75_imp_all' ,'stdev_current_growth_75_imp_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_100_imp_all' ,'mean_current_growth_100_imp_am' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_100_imp_all' ,'stdev_current_growth_100_imp_am' ,DROP=True)

# Reproduction scenario only applies to adult females
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_100_imp'] = np.nan

ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_100_imp'] = np.nan

# -----------------------------------------------------------------------------
# Adult combined
# -----------------------------------------------------------------------------
select_agesex_scenario = 'adult combined'
select_agesex_scenario_upcase = select_agesex_scenario.upper()
_scen_select = (ahle_combo_scensmry['agesex_scenario'].str.upper() == select_agesex_scenario_upcase)
print(f"\n> Selected {_scen_select.sum(): ,} rows where agesex_scenario is {select_agesex_scenario_upcase}.")

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ideal' ,'mean_ideal_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ideal' ,'stdev_ideal_a' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ppr' ,'mean_ppr_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ppr' ,'stdev_ppr_a' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_bruc' ,'mean_bruc_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_bruc' ,'stdev_bruc_a' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_mortality_zero' ,'mean_mortality_zero_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_mortality_zero' ,'stdev_mortality_zero_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_25_imp' ,'mean_mort_25_imp_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_25_imp' ,'stdev_mort_25_imp_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_50_imp' ,'mean_mort_50_imp_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_50_imp' ,'stdev_mort_50_imp_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_75_imp' ,'mean_mort_75_imp_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_75_imp' ,'stdev_mort_75_imp_a' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_25_imp_all' ,'mean_current_growth_25_imp_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_25_imp_all' ,'stdev_current_growth_25_imp_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_50_imp_all' ,'mean_current_growth_50_imp_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_50_imp_all' ,'stdev_current_growth_50_imp_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_75_imp_all' ,'mean_current_growth_75_imp_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_75_imp_all' ,'stdev_current_growth_75_imp_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_100_imp_all' ,'mean_current_growth_100_imp_a' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_100_imp_all' ,'stdev_current_growth_100_imp_a' ,DROP=True)

# Reproduction scenario only applies to adult females
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_100_imp'] = np.nan

ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_100_imp'] = np.nan

# -----------------------------------------------------------------------------
# Juvenile Female
# -----------------------------------------------------------------------------
select_agesex_scenario = 'juvenile female'
select_agesex_scenario_upcase = select_agesex_scenario.upper()
_scen_select = (ahle_combo_scensmry['agesex_scenario'].str.upper() == select_agesex_scenario_upcase)
print(f"\n> Selected {_scen_select.sum(): ,} rows where agesex_scenario is {select_agesex_scenario_upcase}.")

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ideal' ,'mean_ideal_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ideal' ,'stdev_ideal_jf' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ppr' ,'mean_ppr_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ppr' ,'stdev_ppr_jf' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_bruc' ,'mean_bruc_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_bruc' ,'stdev_bruc_jf' ,DROP=True)

# For juveniles, sex-specific mortality scenarios are missing
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_mortality_zero' ,'mean_mortality_zero_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_mortality_zero' ,'stdev_mortality_zero_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_25_imp' ,'mean_mort_25_imp_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_25_imp' ,'stdev_mort_25_imp_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_50_imp' ,'mean_mort_50_imp_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_50_imp' ,'stdev_mort_50_imp_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_75_imp' ,'mean_mort_75_imp_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_75_imp' ,'stdev_mort_75_imp_jf' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_25_imp_all' ,'mean_current_growth_25_imp_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_25_imp_all' ,'stdev_current_growth_25_imp_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_50_imp_all' ,'mean_current_growth_50_imp_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_50_imp_all' ,'stdev_current_growth_50_imp_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_75_imp_all' ,'mean_current_growth_75_imp_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_75_imp_all' ,'stdev_current_growth_75_imp_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_100_imp_all' ,'mean_current_growth_100_imp_jf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_100_imp_all' ,'stdev_current_growth_100_imp_jf' ,DROP=True)

# Reproduction scenario only applies to adult females
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_100_imp'] = np.nan

ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_100_imp'] = np.nan

# -----------------------------------------------------------------------------
# Juvenile Male
# -----------------------------------------------------------------------------
select_agesex_scenario = 'juvenile male'
select_agesex_scenario_upcase = select_agesex_scenario.upper()
_scen_select = (ahle_combo_scensmry['agesex_scenario'].str.upper() == select_agesex_scenario_upcase)
print(f"\n> Selected {_scen_select.sum(): ,} rows where agesex_scenario is {select_agesex_scenario_upcase}.")

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ideal' ,'mean_ideal_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ideal' ,'stdev_ideal_jm' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ppr' ,'mean_ppr_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ppr' ,'stdev_ppr_jm' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_bruc' ,'mean_bruc_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_bruc' ,'stdev_bruc_jm' ,DROP=True)

# For juveniles, sex-specific mortality scenarios are missing
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_mortality_zero' ,'mean_mortality_zero_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_mortality_zero' ,'stdev_mortality_zero_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_25_imp' ,'mean_mort_25_imp_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_25_imp' ,'stdev_mort_25_imp_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_50_imp' ,'mean_mort_50_imp_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_50_imp' ,'stdev_mort_50_imp_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_75_imp' ,'mean_mort_75_imp_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_75_imp' ,'stdev_mort_75_imp_jm' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_25_imp_all' ,'mean_current_growth_25_imp_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_25_imp_all' ,'stdev_current_growth_25_imp_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_50_imp_all' ,'mean_current_growth_50_imp_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_50_imp_all' ,'stdev_current_growth_50_imp_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_75_imp_all' ,'mean_current_growth_75_imp_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_75_imp_all' ,'stdev_current_growth_75_imp_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_100_imp_all' ,'mean_current_growth_100_imp_jm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_100_imp_all' ,'stdev_current_growth_100_imp_jm' ,DROP=True)

# Reproduction scenario only applies to adult females
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_100_imp'] = np.nan

ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_100_imp'] = np.nan

# -----------------------------------------------------------------------------
# Juvenile combined
# -----------------------------------------------------------------------------
select_agesex_scenario = 'juvenile combined'
select_agesex_scenario_upcase = select_agesex_scenario.upper()
_scen_select = (ahle_combo_scensmry['agesex_scenario'].str.upper() == select_agesex_scenario_upcase)
print(f"\n> Selected {_scen_select.sum(): ,} rows where agesex_scenario is {select_agesex_scenario_upcase}.")

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ideal' ,'mean_ideal_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ideal' ,'stdev_ideal_j' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ppr' ,'mean_ppr_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ppr' ,'stdev_ppr_j' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_bruc' ,'mean_bruc_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_bruc' ,'stdev_bruc_j' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_mortality_zero' ,'mean_mortality_zero_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_mortality_zero' ,'stdev_mortality_zero_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_25_imp' ,'mean_mort_25_imp_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_25_imp' ,'stdev_mort_25_imp_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_50_imp' ,'mean_mort_50_imp_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_50_imp' ,'stdev_mort_50_imp_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_75_imp' ,'mean_mort_75_imp_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_75_imp' ,'stdev_mort_75_imp_j' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_25_imp_all' ,'mean_current_growth_25_imp_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_25_imp_all' ,'stdev_current_growth_25_imp_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_50_imp_all' ,'mean_current_growth_50_imp_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_50_imp_all' ,'stdev_current_growth_50_imp_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_75_imp_all' ,'mean_current_growth_75_imp_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_75_imp_all' ,'stdev_current_growth_75_imp_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_100_imp_all' ,'mean_current_growth_100_imp_j' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_100_imp_all' ,'stdev_current_growth_100_imp_j' ,DROP=True)

# Reproduction scenario only applies to adult females
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_100_imp'] = np.nan

ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_100_imp'] = np.nan

# -----------------------------------------------------------------------------
# Neonatal Female
# -----------------------------------------------------------------------------
select_agesex_scenario = 'neonatal female'
select_agesex_scenario_upcase = select_agesex_scenario.upper()
_scen_select = (ahle_combo_scensmry['agesex_scenario'].str.upper() == select_agesex_scenario_upcase)
print(f"\n> Selected {_scen_select.sum(): ,} rows where agesex_scenario is {select_agesex_scenario_upcase}.")

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ideal' ,'mean_ideal_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ideal' ,'stdev_ideal_nf' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ppr' ,'mean_ppr_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ppr' ,'stdev_ppr_nf' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_bruc' ,'mean_bruc_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_bruc' ,'stdev_bruc_nf' ,DROP=True)

# For neonates, sex-specific mortality scenarios are missing
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_mortality_zero' ,'mean_mortality_zero_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_mortality_zero' ,'stdev_mortality_zero_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_25_imp' ,'mean_mort_25_imp_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_25_imp' ,'stdev_mort_25_imp_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_50_imp' ,'mean_mort_50_imp_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_50_imp' ,'stdev_mort_50_imp_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_75_imp' ,'mean_mort_75_imp_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_75_imp' ,'stdev_mort_75_imp_nf' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_25_imp_all' ,'mean_current_growth_25_imp_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_25_imp_all' ,'stdev_current_growth_25_imp_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_50_imp_all' ,'mean_current_growth_50_imp_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_50_imp_all' ,'stdev_current_growth_50_imp_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_75_imp_all' ,'mean_current_growth_75_imp_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_75_imp_all' ,'stdev_current_growth_75_imp_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_100_imp_all' ,'mean_current_growth_100_imp_nf' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_100_imp_all' ,'stdev_current_growth_100_imp_nf' ,DROP=True)

# Reproduction scenario only applies to adult females
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_100_imp'] = np.nan

ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_100_imp'] = np.nan

# -----------------------------------------------------------------------------
# Neonatal Male
# -----------------------------------------------------------------------------
select_agesex_scenario = 'neonatal male'
select_agesex_scenario_upcase = select_agesex_scenario.upper()
_scen_select = (ahle_combo_scensmry['agesex_scenario'].str.upper() == select_agesex_scenario_upcase)
print(f"\n> Selected {_scen_select.sum(): ,} rows where agesex_scenario is {select_agesex_scenario_upcase}.")

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ideal' ,'mean_ideal_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ideal' ,'stdev_ideal_nm' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ppr' ,'mean_ppr_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ppr' ,'stdev_ppr_nm' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_bruc' ,'mean_bruc_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_bruc' ,'stdev_bruc_nm' ,DROP=True)

# For neonates, sex-specific mortality scenarios are missing
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_mortality_zero' ,'mean_mortality_zero_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_mortality_zero' ,'stdev_mortality_zero_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_25_imp' ,'mean_mort_25_imp_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_25_imp' ,'stdev_mort_25_imp_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_50_imp' ,'mean_mort_50_imp_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_50_imp' ,'stdev_mort_50_imp_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_75_imp' ,'mean_mort_75_imp_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_75_imp' ,'stdev_mort_75_imp_nm' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_25_imp_all' ,'mean_current_growth_25_imp_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_25_imp_all' ,'stdev_current_growth_25_imp_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_50_imp_all' ,'mean_current_growth_50_imp_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_50_imp_all' ,'stdev_current_growth_50_imp_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_75_imp_all' ,'mean_current_growth_75_imp_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_75_imp_all' ,'stdev_current_growth_75_imp_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_100_imp_all' ,'mean_current_growth_100_imp_nm' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_100_imp_all' ,'stdev_current_growth_100_imp_nm' ,DROP=True)

# Reproduction scenario only applies to adult females
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_100_imp'] = np.nan

ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_100_imp'] = np.nan

# -----------------------------------------------------------------------------
# Neonatal combined
# -----------------------------------------------------------------------------
select_agesex_scenario = 'neonatal combined'
select_agesex_scenario_upcase = select_agesex_scenario.upper()
_scen_select = (ahle_combo_scensmry['agesex_scenario'].str.upper() == select_agesex_scenario_upcase)
print(f"\n> Selected {_scen_select.sum(): ,} rows where agesex_scenario is {select_agesex_scenario_upcase}.")

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ideal' ,'mean_ideal_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ideal' ,'stdev_ideal_n' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ppr' ,'mean_ppr_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ppr' ,'stdev_ppr_n' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_bruc' ,'mean_bruc_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_bruc' ,'stdev_bruc_n' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_mortality_zero' ,'mean_mortality_zero_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_mortality_zero' ,'stdev_mortality_zero_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_25_imp' ,'mean_mort_25_imp_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_25_imp' ,'stdev_mort_25_imp_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_50_imp' ,'mean_mort_50_imp_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_50_imp' ,'stdev_mort_50_imp_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_75_imp' ,'mean_mort_75_imp_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_75_imp' ,'stdev_mort_75_imp_n' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_25_imp_all' ,'mean_current_growth_25_imp_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_25_imp_all' ,'stdev_current_growth_25_imp_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_50_imp_all' ,'mean_current_growth_50_imp_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_50_imp_all' ,'stdev_current_growth_50_imp_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_75_imp_all' ,'mean_current_growth_75_imp_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_75_imp_all' ,'stdev_current_growth_75_imp_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_100_imp_all' ,'mean_current_growth_100_imp_n' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_100_imp_all' ,'stdev_current_growth_100_imp_n' ,DROP=True)

# Reproduction scenario only applies to adult females
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_100_imp'] = np.nan

ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_100_imp'] = np.nan

# -----------------------------------------------------------------------------
# Oxen
# -----------------------------------------------------------------------------
select_agesex_scenario = 'oxen'
select_agesex_scenario_upcase = select_agesex_scenario.upper()
_scen_select = (ahle_combo_scensmry['agesex_scenario'].str.upper() == select_agesex_scenario_upcase)
print(f"\n> Selected {_scen_select.sum(): ,} rows where agesex_scenario is {select_agesex_scenario_upcase}.")

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ideal' ,'mean_ideal_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ideal' ,'stdev_ideal_o' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_ppr' ,'mean_ppr_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_ppr' ,'stdev_ppr_o' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_bruc' ,'mean_bruc_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_bruc' ,'stdev_bruc_o' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_mortality_zero' ,'mean_mortality_zero_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_mortality_zero' ,'stdev_mortality_zero_o' ,DROP=True)

# Mortality and growth improvement scenarios have not been run for cattle
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_25_imp' ,'mean_mort_25_imp_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_25_imp' ,'stdev_mort_25_imp_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_50_imp' ,'mean_mort_50_imp_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_50_imp' ,'stdev_mort_50_imp_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_all_mort_75_imp' ,'mean_mort_75_imp_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_all_mort_75_imp' ,'stdev_mort_75_imp_o' ,DROP=True)

ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_25_imp_all' ,'mean_current_growth_25_imp_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_25_imp_all' ,'stdev_current_growth_25_imp_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_50_imp_all' ,'mean_current_growth_50_imp_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_50_imp_all' ,'stdev_current_growth_50_imp_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_75_imp_all' ,'mean_current_growth_75_imp_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_75_imp_all' ,'stdev_current_growth_75_imp_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'mean_current_growth_100_imp_all' ,'mean_current_growth_100_imp_o' ,DROP=True)
ahle_combo_scensmry = fill_column_where(ahle_combo_scensmry ,_scen_select ,'stdev_current_growth_100_imp_all' ,'stdev_current_growth_100_imp_o' ,DROP=True)

# Reproduction scenario only applies to adult females
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'mean_current_repro_100_imp'] = np.nan

ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_25_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_50_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_75_imp'] = np.nan
ahle_combo_scensmry.loc[_scen_select ,'stdev_current_repro_100_imp'] = np.nan

# =============================================================================
#### Create aggregate Species and Production System
# =============================================================================
'''
Creating aggregate groups for filtering in the dashboard

Note: this handles all items the same, whether they are animal (head) counts,
mass (kg), or dollar values. Be careful when using the results that you are not
mixing apples and oranges.

Note: while item values do not sum across age/sex scenarios, they do sum across
species and production systems.
'''
mean_cols_scensmry = [i for i in list(ahle_combo_scensmry) if 'mean' in i]
sd_cols_scensmry = [i for i in list(ahle_combo_scensmry) if 'stdev' in i]
var_cols = ['sqrd_' + COLNAME for COLNAME in sd_cols_scensmry]
for i ,VARCOL in enumerate(var_cols):
   SDCOL = sd_cols_scensmry[i]
   ahle_combo_scensmry[VARCOL] = ahle_combo_scensmry[SDCOL]**2

# -----------------------------------------------------------------------------
# Create overall production system
# -----------------------------------------------------------------------------
ahle_combo_scensmry_sumprod = ahle_combo_scensmry.pivot_table(
   index=['region' ,'species' ,'item' ,'item_type_code' ,'agesex_scenario' ,'year']
   ,values=mean_cols_scensmry + var_cols
   ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
).reset_index()
ahle_combo_scensmry_sumprod['production_system'] = 'Overall'

ahle_combo_scensmry = pd.concat(
   [ahle_combo_scensmry ,ahle_combo_scensmry_sumprod]
   ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
   ,join='outer'        # 'outer': keep all index values from all data frames
   ,ignore_index=True   # True: do not keep index values on concatenation axis
)
del ahle_combo_scensmry_sumprod

# -----------------------------------------------------------------------------
# Create combined species
# -----------------------------------------------------------------------------
# "All Small Ruminants" for Sheep and Goats
ahle_combo_scensmry_sumspec1 = ahle_combo_scensmry.query("species.str.upper().isin(['SHEEP' ,'GOAT'])").pivot_table(
   index=['region' ,'production_system' ,'item' ,'item_type_code' ,'agesex_scenario' ,'year']
   ,values=mean_cols_scensmry + var_cols
   ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
).reset_index()
ahle_combo_scensmry_sumspec1['species'] = 'All Small Ruminants'

# "All poultry"
ahle_combo_scensmry_sumspec2 = ahle_combo_scensmry.query("species.str.contains('poultry' ,case=False ,na=False)").pivot_table(
   index=['region' ,'production_system' ,'item' ,'item_type_code' ,'agesex_scenario' ,'year']
   ,values=mean_cols_scensmry + var_cols
   ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
).reset_index()
ahle_combo_scensmry_sumspec2['species'] = 'All Poultry'

# Concatenate
ahle_combo_scensmry = pd.concat(
   [ahle_combo_scensmry ,ahle_combo_scensmry_sumspec1 ,ahle_combo_scensmry_sumspec2]
   ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
   ,join='outer'        # 'outer': keep all index values from all data frames
   ,ignore_index=True   # True: do not keep index values on concatenation axis
)
del ahle_combo_scensmry_sumspec1 ,ahle_combo_scensmry_sumspec2

# -----------------------------------------------------------------------------
# Calculate standard deviations
# -----------------------------------------------------------------------------
for i ,VARCOL in enumerate(var_cols):
   SDCOL = sd_cols_scensmry[i]
   ahle_combo_scensmry[SDCOL] = np.sqrt(ahle_combo_scensmry[VARCOL])
   del ahle_combo_scensmry[VARCOL]

datainfo(ahle_combo_scensmry)

#%% ADD CALCS

ahle_combo_scensmry_diffs = ahle_combo_scensmry.copy()

# =============================================================================
#### Calculate scenario differences
# =============================================================================
'''
These calculate the difference between current values and values under the ideal
scenario (or other scenarios) for every item.

NOTE: mean values for all cost items are made negative before these calcs.
'''
# -----------------------------------------------------------------------------
# Ideal and mortality zero scenarios
# -----------------------------------------------------------------------------
ahle_combo_scensmry_diffs['mean_diff_ideal'] = ahle_combo_scensmry_diffs['mean_ideal'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_ideal'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_ideal']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)

ahle_combo_scensmry_diffs['mean_diff_mortzero'] = ahle_combo_scensmry_diffs['mean_mortality_zero'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_mortzero'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_mortality_zero']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)

# -----------------------------------------------------------------------------
# Incremental improvement scenarios
# -----------------------------------------------------------------------------
# Mortality
ahle_combo_scensmry_diffs['mean_diff_mortimp25'] = ahle_combo_scensmry_diffs['mean_all_mort_25_imp'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_mortimp25'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_all_mort_25_imp']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)
ahle_combo_scensmry_diffs['mean_diff_mortimp50'] = ahle_combo_scensmry_diffs['mean_all_mort_50_imp'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_mortimp50'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_all_mort_50_imp']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)
ahle_combo_scensmry_diffs['mean_diff_mortimp75'] = ahle_combo_scensmry_diffs['mean_all_mort_75_imp'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_mortimp75'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_all_mort_75_imp']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)

# Parturition
ahle_combo_scensmry_diffs['mean_diff_reprimp25'] = ahle_combo_scensmry_diffs['mean_current_repro_25_imp'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_reprimp25'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_current_repro_25_imp']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)
ahle_combo_scensmry_diffs['mean_diff_reprimp50'] = ahle_combo_scensmry_diffs['mean_current_repro_50_imp'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_reprimp50'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_current_repro_50_imp']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)
ahle_combo_scensmry_diffs['mean_diff_reprimp75'] = ahle_combo_scensmry_diffs['mean_current_repro_75_imp'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_reprimp75'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_current_repro_75_imp']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)
ahle_combo_scensmry_diffs['mean_diff_reprimp100'] = ahle_combo_scensmry_diffs['mean_current_repro_100_imp'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_reprimp100'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_current_repro_100_imp']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)

# Live Weight
ahle_combo_scensmry_diffs['mean_diff_growimp25'] = ahle_combo_scensmry_diffs['mean_current_growth_25_imp_all'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_growimp25'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_current_growth_25_imp_all']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)
ahle_combo_scensmry_diffs['mean_diff_growimp50'] = ahle_combo_scensmry_diffs['mean_current_growth_50_imp_all'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_growimp50'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_current_growth_50_imp_all']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)
ahle_combo_scensmry_diffs['mean_diff_growimp75'] = ahle_combo_scensmry_diffs['mean_current_growth_75_imp_all'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_growimp75'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_current_growth_75_imp_all']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)
ahle_combo_scensmry_diffs['mean_diff_growimp100'] = ahle_combo_scensmry_diffs['mean_current_growth_100_imp_all'] - ahle_combo_scensmry_diffs['mean_current']
ahle_combo_scensmry_diffs['stdev_diff_growimp100'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_current_growth_100_imp_all']**2 + ahle_combo_scensmry_diffs['stdev_current']**2)

# -----------------------------------------------------------------------------
# Disease-specific scenarios
#
# These scenarios are modifications of the ideal scenario where the indicated
# disease is the only one present. Disease-specific impacts are calculated as
# the difference from the ideal. These are currently only run for
# agesex_scenario 'overall'.
# -----------------------------------------------------------------------------
# PPR
ahle_combo_scensmry_diffs['mean_diff_ppr'] = ahle_combo_scensmry_diffs['mean_ideal'] - ahle_combo_scensmry_diffs['mean_ppr']
ahle_combo_scensmry_diffs['stdev_diff_ppr'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_ideal']**2 + ahle_combo_scensmry_diffs['stdev_ppr']**2)

# Brucellosis
ahle_combo_scensmry_diffs['mean_diff_bruc'] = ahle_combo_scensmry_diffs['mean_ideal'] - ahle_combo_scensmry_diffs['mean_bruc']
ahle_combo_scensmry_diffs['stdev_diff_bruc'] = np.sqrt(ahle_combo_scensmry_diffs['stdev_ideal']**2 + ahle_combo_scensmry_diffs['stdev_bruc']**2)

# =============================================================================
#### Add currency conversion
# =============================================================================
# Merge exchange rates onto data
ahle_combo_scensmry_diffs['country_name'] = 'Ethiopia'     # Add country for joining
ahle_combo_scensmry_diffs = pd.merge(
    left=ahle_combo_scensmry_diffs
    ,right=exchg_data_tomerge
    ,on=['country_name' ,'year']
    ,how='left'
    )
del ahle_combo_scensmry_diffs['country_name']

# Create column lists to include AHLE cols
mean_cols_scensmry_diffs = [i for i in list(ahle_combo_scensmry_diffs) if 'mean' in i]
sd_cols_scensmry_diffs = [i for i in list(ahle_combo_scensmry_diffs) if 'stdev' in i]

# Add columns in USD for currency items
for MEANCOL in mean_cols_scensmry_diffs:
    MEANCOL_USD = MEANCOL + '_usd'
    ahle_combo_scensmry_diffs.loc[ahle_combo_scensmry_diffs['item_type_code'].isin(['mv' ,'mc']) ,MEANCOL_USD] = \
        ahle_combo_scensmry_diffs[MEANCOL] / ahle_combo_scensmry_diffs['exchg_rate_lcuperusdol']

# For standard deviations, convert to variances then scale by the squared denominator
# VAR(aX) = a^2 * VAR(X). a = 1/exchange rate.
for SDCOL in sd_cols_scensmry_diffs:
    SDCOL_USD = SDCOL + '_usd'
    ahle_combo_scensmry_diffs.loc[ahle_combo_scensmry_diffs['item_type_code'].isin(['mv' ,'mc']) ,SDCOL_USD] = \
        np.sqrt(ahle_combo_scensmry_diffs[SDCOL]**2 / ahle_combo_scensmry_diffs['exchg_rate_lcuperusdol']**2)

# =============================================================================
#### Add columns per kg biomass
# =============================================================================
# Get current population liveweight by region into its own column
regional_wt_byvars = ['region' ,'species' ,'production_system' ,'year']
liveweight_byregion = ahle_combo_scensmry_diffs.query("item == 'Population Liveweight (kg)'")[regional_wt_byvars + ['item' ,'mean_current']].drop_duplicates()
liveweight_byregion = liveweight_byregion.pivot(
    index=regional_wt_byvars
    ,columns='item'
    ,values='mean_current'
).reset_index()
cleancolnames(liveweight_byregion)

# Merge with original data
ahle_combo_scensmry_diffs = pd.merge(
    left=ahle_combo_scensmry_diffs
    ,right=liveweight_byregion
    ,on=regional_wt_byvars
    ,how='left'
)

# Recreate column lists to include USD columns
mean_cols_scensmry_diffs_usd = [i for i in list(ahle_combo_scensmry_diffs) if 'mean' in i]
sd_cols_scensmry_diffs_usd = [i for i in list(ahle_combo_scensmry_diffs) if 'stdev' in i]

# Calculate value columns per kg liveweight
for MEANCOL in mean_cols_scensmry_diffs_usd:
    NEWCOL_NAME = MEANCOL + '_perkgbiomass'
    ahle_combo_scensmry_diffs[NEWCOL_NAME] = ahle_combo_scensmry_diffs[MEANCOL] / ahle_combo_scensmry_diffs['population_liveweight__kg_']

# For standard deviations, convert to variances then scale by the squared denominator
# VAR(aX) = a^2 * VAR(X). a = 1/exchange rate.
for SDCOL in sd_cols_scensmry_diffs_usd:
    NEWCOL_NAME = SDCOL + '_perkgbiomass'
    ahle_combo_scensmry_diffs[NEWCOL_NAME] = np.sqrt(ahle_combo_scensmry_diffs[SDCOL]**2 / ahle_combo_scensmry_diffs['population_liveweight__kg_']**2)

# =============================================================================
#### Cleanup and export
# =============================================================================
# Drop columns with unused distributional attributes
drop_distr_containing = ['min_' ,'q1_' ,'median_' ,'q3_' ,'max_']
drop_distr_cols = []
for STR in drop_distr_containing:
   drop_distr_cols = drop_distr_cols + [item for item in list(ahle_combo_scensmry_diffs) if STR.upper() in item.upper()]

# Drop columns with original age/sex groups (this file uses only the Overall group)
dropcols = ['group' ,'age_group' ,'sex'] + drop_distr_cols
ahle_combo_scensmry_diffs = ahle_combo_scensmry_diffs.drop(columns=dropcols)

datainfo(ahle_combo_scensmry_diffs)

ahle_combo_scensmry_diffs.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_scensmry.csv') ,index=False)
# ahle_combo_scensmry_diffs.to_pickle(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_scensmry.pkl.gz'))

# Output for Dash
ahle_combo_scensmry_diffs.to_csv(os.path.join(DASH_DATA_FOLDER ,'ahle_all_scensmry.csv') ,index=False)

#%% SUMMARIZE AHLE

# =============================================================================
#### Restructure
# =============================================================================
# For AHLE calcs, we want each item in a column
# Need means and standard deviations for later calculations
mean_cols_scensmry_diffs = [i for i in list(ahle_combo_scensmry_diffs) if 'mean' in i]
sd_cols_scensmry_diffs = [i for i in list(ahle_combo_scensmry_diffs) if 'stdev' in i]

# Only need a few items for AHLE calcs
keep_items = [
    'gross margin'
    ,'health cost'
    ,'value of total mortality'
    ]
keep_items_upper = [i.upper() for i in keep_items]
_items_for_ahle = (ahle_combo_scensmry_diffs['item'].str.upper().isin(keep_items_upper))

ahle_combo_scensmry_diffs_p = ahle_combo_scensmry_diffs.loc[_items_for_ahle].pivot(
    index=['region' ,'species' ,'production_system' ,'agesex_scenario' ,'year']
    ,columns='item'
    ,values=mean_cols_scensmry_diffs + sd_cols_scensmry_diffs
).reset_index()
ahle_combo_scensmry_diffs_p = colnames_from_index(ahle_combo_scensmry_diffs_p)   # Change multi-index to column names
cleancolnames(ahle_combo_scensmry_diffs_p)

# Remove underscores added when collapsing column index
ahle_combo_scensmry_diffs_p = ahle_combo_scensmry_diffs_p.rename(
    columns={
        'region_':'region'
        ,'species_':'species'
        ,'production_system_':'production_system'
        ,'agesex_scenario_':'agesex_scenario'
        ,'year_':'year'
    }
)
datainfo(ahle_combo_scensmry_diffs_p)

# =============================================================================
#### Calcs
# =============================================================================
'''
Most of the AHLE components are item-level (gross margin or health cost)
differences between scenarios, which have already been calculated.
'''
ahle_combo_scensmry_diffs_p = ahle_combo_scensmry_diffs_p.eval(
    # Top level
    '''
    ahle_total_mean = mean_diff_ideal_gross_margin
    ahle_total_stdev = stdev_diff_ideal_gross_margin
    ahle_dueto_mortality_mean = mean_diff_mortzero_gross_margin
    ahle_dueto_mortality_stdev = stdev_diff_mortzero_gross_margin
    ahle_dueto_healthcost_mean = mean_diff_ideal_health_cost
    ahle_dueto_healthcost_stdev = stdev_diff_ideal_health_cost
    ahle_dueto_productionloss_mean = ahle_total_mean - ahle_dueto_mortality_mean - ahle_dueto_healthcost_mean

    ahle_total_mean_usd = mean_diff_ideal_usd_gross_margin
    ahle_total_stdev_usd = stdev_diff_ideal_usd_gross_margin
    ahle_dueto_mortality_mean_usd = mean_diff_mortzero_usd_gross_margin
    ahle_dueto_mortality_stdev_usd = stdev_diff_mortzero_usd_gross_margin
    ahle_dueto_healthcost_mean_usd = mean_diff_ideal_usd_health_cost
    ahle_dueto_healthcost_stdev_usd = stdev_diff_ideal_usd_health_cost
    ahle_dueto_productionloss_mean_usd = ahle_total_mean_usd - ahle_dueto_mortality_mean_usd - ahle_dueto_healthcost_mean_usd
    '''
    # Disease-specific
    '''
    ahle_dueto_ppr_total_mean = mean_diff_ppr_gross_margin
    ahle_dueto_ppr_total_stdev = stdev_diff_ppr_gross_margin
    ahle_dueto_ppr_mortality_mean = mean_diff_ppr_value_of_total_mortality * -1
    ahle_dueto_ppr_mortality_stdev = stdev_diff_ppr_value_of_total_mortality * -1
    ahle_dueto_ppr_healthcost_mean = mean_diff_ppr_health_cost
    ahle_dueto_ppr_healthcost_stdev = stdev_diff_ppr_health_cost
    ahle_dueto_ppr_productionloss_mean = ahle_dueto_ppr_total_mean - ahle_dueto_ppr_mortality_mean - ahle_dueto_ppr_healthcost_mean

    ahle_dueto_ppr_total_mean_usd = mean_diff_ppr_usd_gross_margin
    ahle_dueto_ppr_total_stdev_usd = stdev_diff_ppr_usd_gross_margin
    ahle_dueto_ppr_mortality_mean_usd = mean_diff_ppr_usd_value_of_total_mortality * -1
    ahle_dueto_ppr_mortality_stdev_usd = stdev_diff_ppr_usd_value_of_total_mortality * -1
    ahle_dueto_ppr_healthcost_mean_usd = mean_diff_ppr_usd_health_cost
    ahle_dueto_ppr_healthcost_stdev_usd = stdev_diff_ppr_usd_health_cost
    ahle_dueto_ppr_productionloss_mean_usd = ahle_dueto_ppr_total_mean_usd - ahle_dueto_ppr_mortality_mean_usd - ahle_dueto_ppr_healthcost_mean_usd

    ahle_dueto_bruc_total_mean = mean_diff_bruc_gross_margin
    ahle_dueto_bruc_total_stdev = stdev_diff_bruc_gross_margin
    ahle_dueto_bruc_mortality_mean = mean_diff_bruc_value_of_total_mortality * -1
    ahle_dueto_bruc_mortality_stdev = stdev_diff_bruc_value_of_total_mortality * -1
    ahle_dueto_bruc_healthcost_mean = mean_diff_bruc_health_cost
    ahle_dueto_bruc_healthcost_stdev = stdev_diff_bruc_health_cost
    ahle_dueto_bruc_productionloss_mean = ahle_dueto_bruc_total_mean - ahle_dueto_bruc_mortality_mean - ahle_dueto_bruc_healthcost_mean

    ahle_dueto_bruc_total_mean_usd = mean_diff_bruc_usd_gross_margin
    ahle_dueto_bruc_total_stdev_usd = stdev_diff_bruc_usd_gross_margin
    ahle_dueto_bruc_mortality_mean_usd = mean_diff_bruc_usd_value_of_total_mortality * -1
    ahle_dueto_bruc_mortality_stdev_usd = stdev_diff_bruc_usd_value_of_total_mortality * -1
    ahle_dueto_bruc_healthcost_mean_usd = mean_diff_bruc_usd_health_cost
    ahle_dueto_bruc_healthcost_stdev_usd = stdev_diff_bruc_usd_health_cost
    ahle_dueto_bruc_productionloss_mean_usd = ahle_dueto_bruc_total_mean_usd - ahle_dueto_bruc_mortality_mean_usd - ahle_dueto_bruc_healthcost_mean_usd
    '''
    # Marginal improvement
    # '''
    # ahle_when_mort_imp25_mean = mean_diff_mortimp25_gross_margin
    # ahle_when_mort_imp25_stdev = stdev_diff_mortimp25_gross_margin
    # ahle_when_mort_imp50_mean = mean_diff_mortimp50_gross_margin
    # ahle_when_mort_imp50_stdev = stdev_diff_mortimp50_gross_margin
    # ahle_when_mort_imp75_mean = mean_diff_mortimp75_gross_margin
    # ahle_when_mort_imp75_stdev = stdev_diff_mortimp75_gross_margin

    # ahle_when_mort_imp25_mean_usd = mean_diff_mortimp25_usd_gross_margin
    # ahle_when_mort_imp25_stdev_usd = stdev_diff_mortimp25_usd_gross_margin
    # ahle_when_mort_imp50_mean_usd = mean_diff_mortimp50_usd_gross_margin
    # ahle_when_mort_imp50_stdev_usd = stdev_diff_mortimp50_usd_gross_margin
    # ahle_when_mort_imp75_mean_usd = mean_diff_mortimp75_usd_gross_margin
    # ahle_when_mort_imp75_stdev_usd = stdev_diff_mortimp75_usd_gross_margin

    # ahle_when_repro_imp25_mean = mean_diff_reprimp25_gross_margin
    # ahle_when_repro_imp25_stdev = stdev_diff_reprimp25_gross_margin
    # ahle_when_repro_imp50_mean = mean_diff_reprimp50_gross_margin
    # ahle_when_repro_imp50_stdev = stdev_diff_reprimp50_gross_margin
    # ahle_when_repro_imp75_mean = mean_diff_reprimp75_gross_margin
    # ahle_when_repro_imp75_stdev = stdev_diff_reprimp75_gross_margin
    # ahle_when_repro_imp100_mean = mean_diff_reprimp100_gross_margin
    # ahle_when_repro_imp100_stdev = stdev_diff_reprimp100_gross_margin

    # ahle_when_repro_imp25_mean_usd = mean_diff_reprimp25_usd_gross_margin
    # ahle_when_repro_imp25_stdev_usd = stdev_diff_reprimp25_usd_gross_margin
    # ahle_when_repro_imp50_mean_usd = mean_diff_reprimp50_usd_gross_margin
    # ahle_when_repro_imp50_stdev_usd = stdev_diff_reprimp50_usd_gross_margin
    # ahle_when_repro_imp75_mean_usd = mean_diff_reprimp75_usd_gross_margin
    # ahle_when_repro_imp75_stdev_usd = stdev_diff_reprimp75_usd_gross_margin
    # ahle_when_repro_imp100_mean_usd = mean_diff_reprimp100_usd_gross_margin
    # ahle_when_repro_imp100_stdev_usd = stdev_diff_reprimp100_usd_gross_margin

    # ahle_when_all_growth_imp25_mean = mean_diff_growimp25_gross_margin
    # ahle_when_all_growth_imp25_stdev = stdev_diff_growimp25_gross_margin
    # ahle_when_all_growth_imp50_mean = mean_diff_growimp50_gross_margin
    # ahle_when_all_growth_imp50_stdev = stdev_diff_growimp50_gross_margin
    # ahle_when_all_growth_imp75_mean = mean_diff_growimp75_gross_margin
    # ahle_when_all_growth_imp75_stdev = stdev_diff_growimp75_gross_margin
    # ahle_when_all_growth_imp100_mean = mean_diff_growimp100_gross_margin
    # ahle_when_all_growth_imp100_stdev = stdev_diff_growimp100_gross_margin

    # ahle_when_all_growth_imp25_mean_usd = mean_diff_growimp25_usd_gross_margin
    # ahle_when_all_growth_imp25_stdev_usd = stdev_diff_growimp25_usd_gross_margin
    # ahle_when_all_growth_imp50_mean_usd = mean_diff_growimp50_usd_gross_margin
    # ahle_when_all_growth_imp50_stdev_usd = stdev_diff_growimp50_usd_gross_margin
    # ahle_when_all_growth_imp75_mean_usd = mean_diff_growimp75_usd_gross_margin
    # ahle_when_all_growth_imp75_stdev_usd = stdev_diff_growimp75_usd_gross_margin
    # ahle_when_all_growth_imp100_mean_usd = mean_diff_growimp100_usd_gross_margin
    # ahle_when_all_growth_imp100_stdev_usd = stdev_diff_growimp100_usd_gross_margin
    # '''
)

# Standard deviations
ahle_combo_scensmry_diffs_p['ahle_dueto_productionloss_stdev'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_total_stdev']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_mortality_stdev']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_healthcost_stdev']**2
    )
ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_productionloss_stdev'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_total_stdev']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_mortality_stdev']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_healthcost_stdev']**2
    )
ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_productionloss_stdev'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_total_stdev']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_mortality_stdev']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_healthcost_stdev']**2
    )

# In USD
ahle_combo_scensmry_diffs_p['ahle_dueto_productionloss_stdev_usd'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_total_stdev_usd']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_mortality_stdev_usd']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_healthcost_stdev_usd']**2
    )
ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_productionloss_stdev_usd'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_total_stdev_usd']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_mortality_stdev_usd']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_healthcost_stdev_usd']**2
    )
ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_productionloss_stdev_usd'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_total_stdev_usd']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_mortality_stdev_usd']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_healthcost_stdev_usd']**2
    )

# -----------------------------------------------------------------------------
# Fill missing values of disease-specific AHLE with zero for species where they do not apply
# -----------------------------------------------------------------------------
# PPR only impacts small ruminants
ahle_dueto_ppr_cols = [i for i in list(ahle_combo_scensmry_diffs_p) if 'ahle_dueto_ppr' in i]
_ppr_applies = (ahle_combo_scensmry_diffs_p['species'].str.upper().isin(['SHEEP' ,'GOAT' ,'ALL SMALL RUMINANTS']))
for COL in ahle_dueto_ppr_cols:
    ahle_combo_scensmry_diffs_p.loc[~ _ppr_applies ,COL] = \
        ahle_combo_scensmry_diffs_p.loc[~ _ppr_applies ,COL].fillna(0)

# Brucellosis impacts small ruminants and cattle
ahle_dueto_bruc_cols = [i for i in list(ahle_combo_scensmry_diffs_p) if 'ahle_dueto_bruc' in i]
_bruc_applies = (ahle_combo_scensmry_diffs_p['species'].str.upper().isin(['SHEEP' ,'GOAT' ,'ALL SMALL RUMINANTS' ,'CATTLE']))
for COL in ahle_dueto_bruc_cols:
    ahle_combo_scensmry_diffs_p.loc[~ _bruc_applies ,COL] = \
        ahle_combo_scensmry_diffs_p.loc[~ _bruc_applies ,COL].fillna(0)

# -----------------------------------------------------------------------------
# AHLE due to Other Diseases
# -----------------------------------------------------------------------------
# Will depend on which diseases were estimated for each species

# Calculate ahle due to other disease
#!!! This is incorrect logic! Individual disease impacts (PPR, Brucellosis, Other) will add up to Infectious component, not to total AHLE!
#!!! Should be: ahle_dueto_otherdisease_total_mean = ahle_infectious_mean - ahle_dueto_ppr_total_mean - ahle_dueto_bruc_total_mean
#!!! We don't know ahle_infectious_mean until we add attribution from expert opinion.
ahle_combo_scensmry_diffs_p = ahle_combo_scensmry_diffs_p.eval(
    '''
    ahle_dueto_otherdisease_total_mean = ahle_total_mean - ahle_dueto_ppr_total_mean - ahle_dueto_bruc_total_mean
    ahle_dueto_otherdisease_mortality_mean = ahle_dueto_mortality_mean - ahle_dueto_ppr_mortality_mean - ahle_dueto_bruc_mortality_mean
    ahle_dueto_otherdisease_healthcost_mean = ahle_dueto_healthcost_mean - ahle_dueto_ppr_healthcost_mean - ahle_dueto_bruc_healthcost_mean
    ahle_dueto_otherdisease_productionloss_mean = ahle_dueto_otherdisease_total_mean - ahle_dueto_otherdisease_mortality_mean - ahle_dueto_otherdisease_healthcost_mean

    ahle_dueto_otherdisease_total_mean_usd = ahle_total_mean_usd - ahle_dueto_ppr_total_mean_usd - ahle_dueto_bruc_total_mean_usd
    ahle_dueto_otherdisease_mortality_mean_usd = ahle_dueto_mortality_mean_usd - ahle_dueto_ppr_mortality_mean_usd - ahle_dueto_bruc_mortality_mean_usd
    ahle_dueto_otherdisease_healthcost_mean_usd = ahle_dueto_healthcost_mean_usd - ahle_dueto_ppr_healthcost_mean_usd - ahle_dueto_bruc_healthcost_mean_usd
    ahle_dueto_otherdisease_productionloss_mean_usd = ahle_dueto_otherdisease_total_mean_usd - ahle_dueto_otherdisease_mortality_mean_usd - ahle_dueto_otherdisease_healthcost_mean_usd
    '''
)

# Standard deviations
ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_total_stdev'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_total_stdev']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_total_stdev']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_total_stdev']**2
    )
ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_mortality_stdev'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_dueto_mortality_stdev']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_mortality_stdev']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_mortality_stdev']**2
    )
ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_healthcost_stdev'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_dueto_healthcost_stdev']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_healthcost_stdev']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_healthcost_stdev']**2
    )
ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_productionloss_stdev'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_total_stdev']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_mortality_stdev']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_healthcost_stdev']**2
    )

# In USD
ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_total_stdev_usd'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_total_stdev_usd']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_total_stdev_usd']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_total_stdev_usd']**2
    )
ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_mortality_stdev_usd'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_dueto_mortality_stdev_usd']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_mortality_stdev_usd']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_mortality_stdev_usd']**2
    )
ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_healthcost_stdev_usd'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_dueto_healthcost_stdev_usd']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_ppr_healthcost_stdev_usd']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_bruc_healthcost_stdev_usd']**2
    )
ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_productionloss_stdev_usd'] = np.sqrt(
    ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_total_stdev_usd']**2 \
        + ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_mortality_stdev_usd']**2 \
            + ahle_combo_scensmry_diffs_p['ahle_dueto_otherdisease_healthcost_stdev_usd']**2
    )

# =============================================================================
#### Cleanup and export
# =============================================================================
datainfo(ahle_combo_scensmry_diffs_p)

# Keep only key columns and AHLE columns
_cols_for_summary = [i for i in list(ahle_combo_scensmry_diffs_p) if 'ahle' in i]
_keepcols = ['region' ,'species' ,'production_system' ,'agesex_scenario' ,'year'] + _cols_for_summary
ahle_combo_scensmry_diffs_p_sub = ahle_combo_scensmry_diffs_p[_keepcols]

datainfo(ahle_combo_scensmry_diffs_p_sub)

ahle_combo_scensmry_diffs_p_sub.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_scensmry_ahle.csv') ,index=False)
ahle_combo_scensmry_diffs_p_sub.to_pickle(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_scensmry_ahle.pkl.gz'))

# Output for Dash
# ahle_combo_scensmry_withahle_sub.to_csv(os.path.join(DASH_DATA_FOLDER ,'ahle_all_scensmry_ahle.csv') ,index=False)

#%% CHECKS ON AHLE USING SCENARIO SUMMARIES

check_ahle_combo_scensmry_diffs_p = ahle_combo_scensmry_diffs_p.copy()

# =============================================================================
#### Change in Gross Margin overall vs. individual ideal scenarios
# =============================================================================
print('\n> Checking the change in Gross Margin for ideal overall vs. individual ideal scenarios')
print(check_ahle_combo_scensmry_diffs_p[['region' ,'species' ,'production_system' ,'year' ,'agesex_scenario' ,'ahle_total_mean']])
