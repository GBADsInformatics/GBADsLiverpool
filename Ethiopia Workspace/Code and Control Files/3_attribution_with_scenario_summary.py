#%% ABOUT
'''
This runs the R program provided by Murdoch University to apply expert opinions
to estimate attribution of the AHLE to infectious, non-infectious, and external
causes.

The expert opinions are recorded in CSV files.
There is a separate file for each species:
    Small ruminants (sheep and goats)
    Cattle
    Poultry

There are also differences in the production systems and age classes for
each species which require differences in the code to prepare AHLE outputs
for the attribution function.

This code separates the AHLE output by species and processes each one individually.
It then calls the attribution function separately, once for each species,
before concatenating the results into a single file for export.

IMPORTANT: before running this, set Python's working directory to the folder
where this code is stored.
'''
#%% PACKAGES AND FUNCTIONS

import os                        # Operating system functions
import subprocess                # For running command prompt or other programs
import inspect                   # For inspecting objects
import io
import time
import datetime as dt            # Date and time functions
import numpy as np
import pandas as pd
import pickle                    # To save objects to disk

# Run a command on the command line using subprocess package
# Example usage: run_cmd(['dir' ,'c:\\users'] ,SHELL=True ,SHOW_MAXLINES=10)
# To run an R program:
   # r_executable = 'C:\\Program Files\\R\\R-4.0.3\\bin\\x64\\Rscript'    # Full path to the Rscript executable
   # r_script = os.path.join(CURRENT_FOLDER ,'test_script.r')             # Full path to the R program you want to run
   # r_args = ['3']                                                       # List of arguments to pass to script, if any
   # run_cmd([r_executable ,r_script] + r_args)
def run_cmd(
      CMD                 # String or List of strings: the command to run. IMPORTANT: use double backslashes (\\) so they are not interpreted as escape characters.
      ,SHELL=False        # True: submit CMD to command prompt (for builtin commands: dir, del, mkdir, etc.). False (default): run another program. First argument in CMD must be an executable.
      ,SHOW_MAXLINES=99
   ):
   funcname = inspect.currentframe().f_code.co_name

   print(f'\n<{funcname}> Running command:\n    {" ".join(CMD)}')
   cmd_status = subprocess.run(CMD, capture_output=True, shell=SHELL)

   stderr_list = []
   stdout_list = []
   if cmd_status.stderr:
      stderr_txt  = cmd_status.stderr.decode()
      stderr_list = stderr_txt.strip().splitlines()
      print(f'\n<{funcname}> stderr messages:')
      for line in stderr_list:
         print(f'    {line}')
   if cmd_status.stdout:
      stdout_txt  = cmd_status.stdout.decode()
      stdout_list = stdout_txt.strip().splitlines()
   if SHOW_MAXLINES:
      print(f'\n<{funcname}> stdout messages (max={SHOW_MAXLINES}):')
      for line in stdout_list[:SHOW_MAXLINES]:
         print(f'    {line}')
   print(f'<{funcname}> Ended with returncode = {cmd_status.returncode}')
   if cmd_status.returncode == 3221225477:
       print(f'<{funcname}> This return code indicates that a file was not found. Check your working directory and folder locations.')

   return cmd_status.returncode    # If you want to use something that is returned, add it here. Assign it when you call the function e.g. returned_object = run_cmd().

# To time a piece of code
def timerstart(LABEL=None):      # String (opt): add a label to the printed timer messages
   global _timerstart ,_timerstart_label
   funcname = inspect.currentframe().f_code.co_name
   _timerstart_label = LABEL
   _timerstart = dt.datetime.now()
   if _timerstart_label:
      print(f"\n<{funcname}> {_timerstart_label} {_timerstart :%I:%M:%S %p} \n")
   else:
      print(f"\n<{funcname}> {_timerstart :%I:%M:%S %p} \n")
   return None

def timerstop():
   global _timerstop
   funcname = inspect.currentframe().f_code.co_name
   if '_timerstart' in globals():
      _timerstop = dt.datetime.now()
      elapsed = _timerstop - _timerstart
      hours = (elapsed.days * 24) + (elapsed.seconds // 3600)
      minutes = (elapsed.seconds % 3600) // 60
      seconds = (elapsed.seconds % 60) + (elapsed.microseconds / 1e6)
      print(f"\n<{funcname}> {_timerstop :%I:%M:%S %p}")
      if _timerstart_label:
         print(f"<{funcname}> {_timerstart_label} Elapsed {hours}h: {minutes}m: {seconds :.1f}s \n")
      else:
         print(f"<{funcname}> Elapsed {hours}h: {minutes}m: {seconds :.1f}s \n")
   else:
      print(f"<{funcname}> Error: no start time defined. Call timerstart() first.")
   return None

# To print df.info() with header for readability, and optionally write data info to text file
def datainfo(
      INPUT_DF
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
   INPUT_DF.info()
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

# This function is used to fill in a new column with the first non-missing value
# from a set of candidate columns.
# Example usage:
#   candidates = ['col1' ,'col2']
#   df['newcol'] = take_first_nonmissing(df ,candidates)
def take_first_nonmissing(
		INPUT_DF
		,CANDIDATE_COLS       # List of strings: columns to search for non-missing value, in this order
		,FILL_ZEROS=False     # True: treat zeros like missing values and continue searching candidate columns until they're nonzero
	):
	# Initialize new column with first candidate column
	OUTPUT_SERIES = INPUT_DF[CANDIDATE_COLS[0]].copy()

	for CANDIDATE in CANDIDATE_COLS:       # For each candidate column...
		if FILL_ZEROS:
			newcol_null = (OUTPUT_SERIES.isnull()) | (OUTPUT_SERIES == 0)   # ...where new column is missing or zero...
		else:
			newcol_null = (OUTPUT_SERIES.isnull())                        # ...where new column is missing...

		OUTPUT_SERIES.loc[newcol_null] = INPUT_DF.loc[newcol_null ,CANDIDATE]    # ...fill with candidate

	return OUTPUT_SERIES

#%% PATHS AND VARIABLES

CURRENT_FOLDER = os.getcwd()
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
GRANDPARENT_FOLDER = os.path.dirname(PARENT_FOLDER)

# Folder for shared code with Liverpool
ETHIOPIA_CODE_FOLDER = CURRENT_FOLDER
ETHIOPIA_OUTPUT_FOLDER = os.path.join(PARENT_FOLDER ,'Program outputs')
ETHIOPIA_DATA_FOLDER = os.path.join(PARENT_FOLDER ,'Data')

DASH_DATA_FOLDER = os.path.join(GRANDPARENT_FOLDER, 'AHLE Dashboard' ,'Dash App' ,'data')

# Full path to rscript.exe
r_executable = 'C:\\Program Files\\R\\R-4.3.1\\bin\\x64\\Rscript.exe'

#%% EXTERNAL DATA

# =============================================================================
#### Read currency conversion data
# =============================================================================
# Note: this is created in 2_process_simulation_results_standalone.py
exchg_data_tomerge = pd.read_pickle(os.path.join(ETHIOPIA_DATA_FOLDER ,'wb_exchg_data_processed.pkl.gz'))

#%% RUN ATTRIBUTION USING EXAMPLE INPUTS

r_script = os.path.join(ETHIOPIA_CODE_FOLDER ,'Attribution function.R')    # Full path to the R program you want to run

# Arguments to R function, as list of strings.
# ORDER MATTERS! SEE HOW THIS LIST IS PARSED INSIDE R SCRIPT.
r_args = [
    os.path.join(ETHIOPIA_CODE_FOLDER ,'Attribution function input - example AHLE.csv')                # String: full path to AHLE estimates file (csv)
    ,os.path.join(ETHIOPIA_CODE_FOLDER ,'attribution_experts_smallruminants.csv')               # String: full path to expert opinion attribution file (csv)
    ,os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'attribution_summary_example.csv')    # String: full path to output file (csv)
]

timerstart()
run_cmd([r_executable ,r_script] + r_args)
timerstop()

#%% READ DATA AND PREP FOR ATTRIBUTION
'''
Restructuring is the same for all species.
'''
# =============================================================================
#### Read data
# =============================================================================
ahle_combo_forattr = pd.read_pickle(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_scensmry_ahle.pkl.gz'))
datainfo(ahle_combo_forattr)

# =============================================================================
#### Fill in missing components
# =============================================================================
'''
With the exception of adults for small ruminants, the expert attribution files
use combined-sex groups for all species and age groups.

Combined-sex AHLE components will be missing for some species and ages depending
on the scenarios run in the simulation model.  While these cannot be calculated
exactly outside of the simulation model, I will fill in values here based on
the scenarios that are present.

Plan: create Combined sex estimates for ahle_total_mean, ahle_dueto_mortality_mean,
and ahle_dueto_healthcost_mean wherever they are missing by summing the individual
sex estimates (e.g. Adult Combined = Adult Female + Adult Male), then recalculate
ahle_dueto_productionloss_mean.

The agesex_scenarios are in separate rows, so this requires summing across
rows and must be done carefully to use the correct rows for each distinct BY group
(by region, species, production system, and year).
'''
# Split age and sex groups into their own columns
ahle_combo_forattr[['age_group' ,'sex']] = \
    ahle_combo_forattr['agesex_scenario'].str.split(' ' ,expand=True)

# Add variance columns for summing
ahle_combo_forattr['ahle_total_variance'] = ahle_combo_forattr['ahle_total_stdev']**2
ahle_combo_forattr['ahle_dueto_mortality_variance'] = ahle_combo_forattr['ahle_dueto_mortality_stdev']**2
ahle_combo_forattr['ahle_dueto_healthcost_variance'] = ahle_combo_forattr['ahle_dueto_healthcost_stdev']**2

# -----------------------------------------------------------------------------
# For each species, production system, region, year, and age group:
#    Calculate the Combined sex result based on the Male and Female results
# -----------------------------------------------------------------------------
fillsex_byvars = [
    'region'
    ,'species'
    ,'production_system'
    ,'age_group'
    ,'year'
    ]

# Total
ahle_combo_forattr['ahle_total_mean_combined'] = \
    ahle_combo_forattr.groupby(fillsex_byvars)['ahle_total_mean'].transform('sum')
ahle_combo_forattr['ahle_total_variance_combined'] = \
    ahle_combo_forattr.groupby(fillsex_byvars)['ahle_total_variance'].transform('sum')
ahle_combo_forattr['ahle_total_stdev_combined'] = \
    np.sqrt(ahle_combo_forattr['ahle_total_variance_combined'])
ahle_combo_forattr = ahle_combo_forattr.drop(columns=['ahle_total_variance' ,'ahle_total_variance_combined'])

# Mortality
ahle_combo_forattr['ahle_dueto_mortality_mean_combined'] = \
    ahle_combo_forattr.groupby(fillsex_byvars)['ahle_dueto_mortality_mean'].transform('sum')
ahle_combo_forattr['ahle_dueto_mortality_variance_combined'] = \
    ahle_combo_forattr.groupby(fillsex_byvars)['ahle_dueto_mortality_variance'].transform('sum')
ahle_combo_forattr['ahle_dueto_mortality_stdev_combined'] = \
    np.sqrt(ahle_combo_forattr['ahle_dueto_mortality_variance_combined'])
ahle_combo_forattr = ahle_combo_forattr.drop(columns=['ahle_dueto_mortality_variance' ,'ahle_dueto_mortality_variance_combined'])

# Health cost
ahle_combo_forattr['ahle_dueto_healthcost_mean_combined'] = \
    ahle_combo_forattr.groupby(fillsex_byvars)['ahle_dueto_healthcost_mean'].transform('sum')
ahle_combo_forattr['ahle_dueto_healthcost_variance_combined'] = \
    ahle_combo_forattr.groupby(fillsex_byvars)['ahle_dueto_healthcost_variance'].transform('sum')
ahle_combo_forattr['ahle_dueto_healthcost_stdev_combined'] = \
    np.sqrt(ahle_combo_forattr['ahle_dueto_healthcost_variance_combined'])
ahle_combo_forattr = ahle_combo_forattr.drop(columns=['ahle_dueto_healthcost_variance' ,'ahle_dueto_healthcost_variance_combined'])

# -----------------------------------------------------------------------------
# For Combined sex rows, fill in missing values with sums
# -----------------------------------------------------------------------------
_combined_sex = (ahle_combo_forattr['sex'] == 'Combined')

# Total
_ahle_total_missing = (ahle_combo_forattr['ahle_total_mean'].isnull())
ahle_combo_forattr = fill_column_where(
    ahle_combo_forattr
    ,(_combined_sex & _ahle_total_missing)
    ,'ahle_total_mean'
    ,'ahle_total_mean_combined'
    ,DROP=True
    )
ahle_combo_forattr = fill_column_where(
    ahle_combo_forattr
    ,(_combined_sex & _ahle_total_missing)
    ,'ahle_total_stdev'
    ,'ahle_total_stdev_combined'
    ,DROP=True
    )

# Mortality
_ahle_mortality_missing = (ahle_combo_forattr['ahle_dueto_mortality_mean'].isnull())
ahle_combo_forattr = fill_column_where(
    ahle_combo_forattr
    ,(_combined_sex & _ahle_mortality_missing)
    ,'ahle_dueto_mortality_mean'
    ,'ahle_dueto_mortality_mean_combined'
    ,DROP=True
    )
ahle_combo_forattr = fill_column_where(
    ahle_combo_forattr
    ,(_combined_sex & _ahle_mortality_missing)
    ,'ahle_dueto_mortality_stdev'
    ,'ahle_dueto_mortality_stdev_combined'
    ,DROP=True
    )

# Health cost
_ahle_healthcost_missing = (ahle_combo_forattr['ahle_dueto_healthcost_mean'].isnull())
ahle_combo_forattr = fill_column_where(
    ahle_combo_forattr
    ,(_combined_sex & _ahle_healthcost_missing)
    ,'ahle_dueto_healthcost_mean'
    ,'ahle_dueto_healthcost_mean_combined'
    ,DROP=True
    )
ahle_combo_forattr = fill_column_where(
    ahle_combo_forattr
    ,(_combined_sex & _ahle_healthcost_missing)
    ,'ahle_dueto_healthcost_stdev'
    ,'ahle_dueto_healthcost_stdev_combined'
    ,DROP=True
    )

# Recalculate AHLE due to production loss
ahle_combo_forattr['ahle_dueto_productionloss_mean'] = \
    ahle_combo_forattr['ahle_total_mean'] - ahle_combo_forattr['ahle_dueto_mortality_mean'] - ahle_combo_forattr['ahle_dueto_healthcost_mean']

ahle_combo_forattr['ahle_dueto_productionloss_stdev'] = \
    np.sqrt(ahle_combo_forattr['ahle_total_stdev']**2 \
            + ahle_combo_forattr['ahle_dueto_mortality_stdev']**2 \
                + ahle_combo_forattr['ahle_dueto_healthcost_stdev']**2)

# =============================================================================
#### Restructure for Attribution function
# =============================================================================
attr_byvars = ['region' ,'species' ,'production_system' ,'agesex_scenario' ,'year']
ahle_combo_forattr_means = ahle_combo_forattr.melt(
   id_vars=attr_byvars
   ,value_vars=['ahle_dueto_mortality_mean' ,'ahle_dueto_healthcost_mean' ,'ahle_dueto_productionloss_mean']
   ,var_name='ahle_component'
   ,value_name='mean'
)
ahle_combo_forattr_stdev = ahle_combo_forattr.melt(
   id_vars=attr_byvars
   ,value_vars=['ahle_dueto_mortality_stdev' ,'ahle_dueto_healthcost_stdev' ,'ahle_dueto_productionloss_stdev']
   ,var_name='ahle_component'
   ,value_name='stdev'
)

# Rename AHLE components to match expert opinion file
simplify_ahle_comps = {
   "ahle_dueto_mortality_mean":"Mortality"
   ,"ahle_dueto_healthcost_mean":"Health cost"
   ,"ahle_dueto_productionloss_mean":"Production loss"

   ,"ahle_dueto_mortality_stdev":"Mortality"
   ,"ahle_dueto_healthcost_stdev":"Health cost"
   ,"ahle_dueto_productionloss_stdev":"Production loss"
}
ahle_combo_forattr_means['ahle_component'] = ahle_combo_forattr_means['ahle_component'].replace(simplify_ahle_comps)
ahle_combo_forattr_stdev['ahle_component'] = ahle_combo_forattr_stdev['ahle_component'].replace(simplify_ahle_comps)

# Merge means and standard deviations
ahle_combo_forattr_m = pd.merge(
   left=ahle_combo_forattr_means
   ,right=ahle_combo_forattr_stdev
   ,on=attr_byvars + ['ahle_component']
   ,how='outer'
)
del ahle_combo_forattr_means ,ahle_combo_forattr_stdev

# Add variance column for summing
ahle_combo_forattr_m['variance'] = ahle_combo_forattr_m['stdev']**2

# =============================================================================
#### Drop unneeded rows
# =============================================================================
'''
Attribution function does not need aggregate production system or age class.
'''
_droprows = (ahle_combo_forattr_m['production_system'].str.upper() == 'OVERALL') \
    | (ahle_combo_forattr_m['agesex_scenario'].str.upper() == 'OVERALL')
print(f"> Dropping {_droprows.sum() :,} rows where production_system or agesex_scenario are 'Overall'.")
ahle_combo_forattr_m = \
    ahle_combo_forattr_m.drop(ahle_combo_forattr_m.loc[_droprows].index).reset_index(drop=True)

# =============================================================================
#### Rename and reorder columns
# =============================================================================
'''
The attribution function refers to some columns by position and others by name
Put all the expected columns first, with correct names and ordering
'''
colnames_ordered_forattr = {
    "species":"Species"
    ,"production_system":"Production system"
    ,"agesex_scenario":"Age class"
    ,"ahle_component":"AHLE"
    ,"mean":"mean"
    ,"stdev":"sd"
}
cols_first = list(colnames_ordered_forattr)
cols_other = [i for i in list(ahle_combo_forattr_m) if i not in cols_first]

ahle_combo_forattr_m = ahle_combo_forattr_m[cols_first + cols_other].rename(columns=colnames_ordered_forattr)

#%% SPECIES-SPECIFIC PREP

# =============================================================================
#### Small Ruminants
# =============================================================================
'''
For sheep and goats, expert attribution file:
    - Uses sex-specific groups for Adults
    - Uses non-sex-specific groups for Juvenile and Neonatal age groups
'''
# Subset data to correct species
_row_selection = (ahle_combo_forattr_m['Species'].str.upper().isin(['SHEEP' ,'GOAT']))
print(f"> Selected {_row_selection.sum() :,} rows.")
ahle_combo_forattr_m_smallrum = ahle_combo_forattr_m.loc[_row_selection].reset_index(drop=True)

datainfo(ahle_combo_forattr_m_smallrum)

# -----------------------------------------------------------------------------
# Filter groups and rename
# -----------------------------------------------------------------------------
# Agesex groups
groups_for_attribution_smallrum = {
   'Adult Female':'Adult female'
   ,'Adult Male':'Adult male'
   ,'Juvenile Combined':'Juvenile'
   ,'Neonatal Combined':'Neonate'
}
groups_for_attribution_smallrum_upper = [i.upper() for i in list(groups_for_attribution_smallrum)]

# Filter agesex groups
_row_selection = (ahle_combo_forattr_m_smallrum['Age class'].str.upper().isin(groups_for_attribution_smallrum_upper))
print(f"> Selected {_row_selection.sum() :,} rows.")
ahle_combo_forattr_m_smallrum = ahle_combo_forattr_m_smallrum.loc[_row_selection].reset_index(drop=True)

# Rename groups to match attribution code
ahle_combo_forattr_m_smallrum['Age class'] = ahle_combo_forattr_m_smallrum['Age class'].replace(groups_for_attribution_smallrum)

# =============================================================================
#### Cattle
# =============================================================================
'''
For cattle, expert attribution file:
    - Uses non-sex-specific groups for all ages
    - Has an additional group 'oxen'
    - Has different labels for groups:
        'Juvenile' maps to 'Neonate' in the AHLE file
        'Sub-adult' maps to 'Juvenile' in the AHLE file
'''
# Subset data to correct species
_row_selection = (ahle_combo_forattr_m['Species'].str.upper() == 'CATTLE')
print(f"> Selected {_row_selection.sum() :,} rows.")
ahle_combo_forattr_m_cattle = ahle_combo_forattr_m.loc[_row_selection].reset_index(drop=True)

datainfo(ahle_combo_forattr_m_cattle)

# -----------------------------------------------------------------------------
# Filter groups and rename
# -----------------------------------------------------------------------------
# Agesex groups
groups_for_attribution_cattle = {
   'Adult Combined':'Adult'
   ,'Juvenile Combined':'Sub-adult'
   ,'Neonatal Combined':'Juvenile'
   ,'Oxen':'Oxen'
}
groups_for_attribution_cattle_upper = [i.upper() for i in list(groups_for_attribution_cattle)]

# Filter
_row_selection = (ahle_combo_forattr_m_cattle['Age class'].str.upper().isin(groups_for_attribution_cattle_upper))
print(f"> Selected {_row_selection.sum() :,} rows.")
ahle_combo_forattr_m_cattle = ahle_combo_forattr_m_cattle.loc[_row_selection].reset_index(drop=True)

# Rename to match attribution code
ahle_combo_forattr_m_cattle['Age class'] = ahle_combo_forattr_m_cattle['Age class'].replace(groups_for_attribution_cattle)

# Production systems
cattle_prodsys_forattribution = {
    'Crop livestock mixed':'Crop livestock mixed'
    ,'Pastoral':'Pastoral'
    ,'Periurban dairy':'Dairy'
}
cattle_prodsys_forattribution_upper = [i.upper() for i in list(cattle_prodsys_forattribution)]

# Filter
_row_selection = (ahle_combo_forattr_m_cattle['Production system'].str.upper().isin(cattle_prodsys_forattribution_upper))
print(f"> Selected {_row_selection.sum() :,} rows.")
ahle_combo_forattr_m_cattle = ahle_combo_forattr_m_cattle.loc[_row_selection].reset_index(drop=True)

# Rename to match attribution code
ahle_combo_forattr_m_cattle['Production system'] = ahle_combo_forattr_m_cattle['Production system'].replace(cattle_prodsys_forattribution)

# =============================================================================
#### Poultry
# =============================================================================
'''
For poultry, expert attribution file:
    - Uses non-sex-specific groups for all ages. This matches the AHLE scenarios.
    - Has different labels for groups:
        'Chick' maps to 'Neonate' in the AHLE file
'''
# Subset data to correct species
_row_selection = (ahle_combo_forattr_m['Species'].str.upper() == 'ALL POULTRY')     # Applying attribution to combined poultry species
print(f"> Selected {_row_selection.sum() :,} rows.")
ahle_combo_forattr_m_poultry = ahle_combo_forattr_m.loc[_row_selection].reset_index(drop=True)

datainfo(ahle_combo_forattr_m_poultry)

# -----------------------------------------------------------------------------
# Filter groups and rename
# -----------------------------------------------------------------------------
# Agesex groups
groups_for_attribution_poultry = {
   'Adult Combined':'Adult'
   ,'Juvenile Combined':'Juvenile'
   ,'Neonatal Combined':'Chick'
}
groups_for_attribution_poultry_upper = [i.upper() for i in list(groups_for_attribution_poultry)]

# Filter agesex groups
_row_selection = (ahle_combo_forattr_m_poultry['Age class'].str.upper().isin(groups_for_attribution_poultry_upper))
print(f"> Selected {_row_selection.sum() :,} rows.")
ahle_combo_forattr_m_poultry = ahle_combo_forattr_m_poultry.loc[_row_selection].reset_index(drop=True)

# Rename groups to match attribution code
ahle_combo_forattr_m_poultry['Age class'] = ahle_combo_forattr_m_poultry['Age class'].replace(groups_for_attribution_poultry)

#%% RUN ATTRIBUTION

r_script = os.path.join(ETHIOPIA_CODE_FOLDER ,'Attribution function.R')    # Full path to the R program you want to run

# =============================================================================
#### Small ruminants
# =============================================================================
attribution_summary_smallruminants = pd.DataFrame()     # Initialize to hold all years

# Initialize list to save return codes
attr_smallrum_returncodes = []

# Loop over years
for YEAR in ahle_combo_forattr_m_smallrum['year'].unique():
    # Loop over regions
    for REGION in ahle_combo_forattr_m_smallrum.query(f"year == {YEAR}")['region'].unique():
        print(f'Running attribution for small ruminants, {YEAR=} and {REGION=}...')

        # Filter to year
        ahle_combo_forattr_m_smallrum_oneyear_oneregion = ahle_combo_forattr_m_smallrum.query(f"year == {YEAR}").query(f"region == '{REGION}'")

        # Write to CSV
        ahle_combo_forattr_m_smallrum_oneyear_oneregion.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_combo_forattr_m_smallrum_oneyear_oneregion.csv') ,index=False)

        # Run attribution
        # Arguments to R function, as list of strings.
        # ORDER MATTERS! SEE HOW THIS LIST IS PARSED INSIDE R SCRIPT.
        r_args = [
           os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_combo_forattr_m_smallrum_oneyear_oneregion.csv')  # String: full path to AHLE estimates file (csv)
           ,os.path.join(ETHIOPIA_CODE_FOLDER ,'attribution_experts_smallruminants.csv')    # String: full path to expert opinion attribution file (csv)
           ,os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'attribution_summary_smallruminants_oneyear_oneregion.csv')    # String: full path to output file (csv)
        ]
        timerstart()
        rc = run_cmd([r_executable ,r_script] + r_args)
        attr_smallrum_returncodes.append(rc)
        timerstop()

        # Read CSV with attribution
        attribution_summary_smallruminants_oneyear_oneregion = pd.read_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'attribution_summary_smallruminants_oneyear_oneregion.csv'))

        # Add back filter columns
        attribution_summary_smallruminants_oneyear_oneregion['year'] = YEAR
        attribution_summary_smallruminants_oneyear_oneregion['region'] = REGION

        # Append to result dataframe
        attribution_summary_smallruminants = pd.concat([attribution_summary_smallruminants ,attribution_summary_smallruminants_oneyear_oneregion] ,ignore_index=True)

# Add species label
attribution_summary_smallruminants['species'] = 'All Small Ruminants'

# Delete intermediate data frames
del ahle_combo_forattr_m_smallrum_oneyear_oneregion ,attribution_summary_smallruminants_oneyear_oneregion

# Delete intermediate CSVs
os.remove(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_combo_forattr_m_smallrum_oneyear_oneregion.csv'))
os.remove(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'attribution_summary_smallruminants_oneyear_oneregion.csv'))

# =============================================================================
#### Cattle
# =============================================================================
attribution_summary_cattle = pd.DataFrame()     # Initialize to hold all years

# Initialize list to save return codes
attr_cattle_returncodes = []

# Loop over years
for YEAR in ahle_combo_forattr_m_cattle['year'].unique():
    # Loop over regions
    for REGION in ahle_combo_forattr_m_cattle.query(f"year == {YEAR}")['region'].unique():
        print(f'Running attribution for cattle, {YEAR=} and {REGION=}...')

        # Filter to year and region
        ahle_combo_forattr_m_cattle_oneyear_oneregion = ahle_combo_forattr_m_cattle.query(f"year == {YEAR}").query(f"region == '{REGION}'")

        # Write to CSV
        ahle_combo_forattr_m_cattle_oneyear_oneregion.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_combo_forattr_m_cattle_oneyear_oneregion.csv') ,index=False)

        # Run attribution
        # Arguments to R function, as list of strings.
        # ORDER MATTERS! SEE HOW THIS LIST IS PARSED INSIDE R SCRIPT.
        r_args = [
           os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_combo_forattr_m_cattle_oneyear_oneregion.csv')  # String: full path to AHLE estimates file (csv)
           ,os.path.join(ETHIOPIA_CODE_FOLDER ,'attribution_experts_cattle.csv')    # String: full path to expert opinion attribution file (csv)
           ,os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'attribution_summary_cattle_oneyear_oneregion.csv')    # String: full path to output file (csv)
        ]
        timerstart()
        rc = run_cmd([r_executable ,r_script] + r_args)
        attr_cattle_returncodes.append(rc)
        timerstop()

        # Read CSV with attribution
        attribution_summary_cattle_oneyear_oneregion = pd.read_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'attribution_summary_cattle_oneyear_oneregion.csv'))

        # Add back filter columns
        attribution_summary_cattle_oneyear_oneregion['year'] = YEAR
        attribution_summary_cattle_oneyear_oneregion['region'] = REGION

        # Append to result dataframe
        attribution_summary_cattle = pd.concat([attribution_summary_cattle ,attribution_summary_cattle_oneyear_oneregion] ,ignore_index=True)

# Add species label
attribution_summary_cattle['species'] = 'Cattle'

# Delete intermediate data frames
del ahle_combo_forattr_m_cattle_oneyear_oneregion ,attribution_summary_cattle_oneyear_oneregion

# Delete intermediate CSVs
os.remove(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_combo_forattr_m_cattle_oneyear_oneregion.csv'))
os.remove(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'attribution_summary_cattle_oneyear_oneregion.csv'))

# =============================================================================
#### Poultry
# =============================================================================
attribution_summary_poultry = pd.DataFrame()     # Initialize to hold all years

# Initialize list to save return codes
attr_poultry_returncodes = []

# Loop over years
for YEAR in ahle_combo_forattr_m_poultry['year'].unique():
    # Loop over regions
    for REGION in ahle_combo_forattr_m_poultry.query(f"year == {YEAR}")['region'].unique():
        print(f'Running attribution for poultry, {YEAR=} and {REGION=}...')

        # Filter to year
        ahle_combo_forattr_m_poultry_oneyear_oneregion = ahle_combo_forattr_m_poultry.query(f"year == {YEAR}").query(f"region == '{REGION}'")

        # Write to CSV
        ahle_combo_forattr_m_poultry_oneyear_oneregion.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_combo_forattr_m_poultry_oneyear_oneregion.csv') ,index=False)

        # Run attribution
        # Arguments to R function, as list of strings.
        # ORDER MATTERS! SEE HOW THIS LIST IS PARSED INSIDE R SCRIPT.
        r_args = [
           os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_combo_forattr_m_poultry_oneyear_oneregion.csv')  # String: full path to AHLE estimates file (csv)
           ,os.path.join(ETHIOPIA_CODE_FOLDER ,'attribution_experts_chickens.csv')    # String: full path to expert opinion attribution file (csv)
           ,os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'attribution_summary_poultry_oneyear_oneregion.csv')    # String: full path to output file (csv)
        ]
        timerstart()
        rc = run_cmd([r_executable ,r_script] + r_args)
        attr_poultry_returncodes.append(rc)
        timerstop()

        # Read CSV with attribution
        attribution_summary_poultry_oneyear_oneregion = pd.read_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'attribution_summary_poultry_oneyear_oneregion.csv'))

        # Add back filter columns
        attribution_summary_poultry_oneyear_oneregion['year'] = YEAR
        attribution_summary_poultry_oneyear_oneregion['region'] = REGION

        # Append to result dataframe
        attribution_summary_poultry = pd.concat([attribution_summary_poultry ,attribution_summary_poultry_oneyear_oneregion] ,ignore_index=True)

# Add species label
attribution_summary_poultry['species'] = 'All Poultry'

# Delete intermediate data frames
del ahle_combo_forattr_m_poultry_oneyear_oneregion ,attribution_summary_poultry_oneyear_oneregion

# Delete intermediate CSVs
os.remove(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_combo_forattr_m_poultry_oneyear_oneregion.csv'))
os.remove(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'attribution_summary_poultry_oneyear_oneregion.csv'))

# =============================================================================
#### Combine attribution results
# =============================================================================
ahle_combo_withattr = pd.concat(
    [attribution_summary_smallruminants ,attribution_summary_cattle ,attribution_summary_poultry]
    ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
    ,join='outer'        # 'outer': keep all index values from all data frames
    ,ignore_index=True   # True: do not keep index values on concatenation axis
    )
cleancolnames(ahle_combo_withattr)

#%% ADD HEALTH COST COMPONENT

# =============================================================================
#### Define placeholder attribution categories
# =============================================================================
# healthcost_category_list = ['Treatment' ,'Prevention' ,'Professional time' ,'Other']
healthcost_category_list = ['Infectious' ,'Non-infectious' ,'External']
healthcost_category_df = pd.DataFrame(
    {'cause':healthcost_category_list
     ,'ahle':'Health cost'
     }
)

# =============================================================================
#### Small Ruminants
# =============================================================================
# Get health cost AHLE rows
_row_selection = (ahle_combo_forattr_m_smallrum['AHLE'].str.upper() == 'HEALTH COST')
print(f"> Selected {_row_selection.sum() :,} rows.")
healthcost_smallrum = ahle_combo_forattr_m_smallrum.loc[_row_selection].reset_index(drop=True).copy()
cleancolnames(healthcost_smallrum)

# Sum sheep and goats
healthcost_smallrum = healthcost_smallrum.pivot_table(
   index=['production_system' ,'age_class' ,'ahle' ,'region' ,'year']
   ,values=['mean' ,'variance']
   ,aggfunc='sum'
).reset_index()
healthcost_smallrum['species'] = 'All Small Ruminants'

# Add placeholder attribution categories
healthcost_smallrum = pd.merge(
    left=healthcost_smallrum
    ,right=healthcost_category_df
    ,on='ahle'
    ,how='outer'
)

# Allocate health cost AHLE equally to categories
healthcost_smallrum['mean'] = healthcost_smallrum['mean'] / len(healthcost_category_list)               # Mean(1/3 X) = 1/3 Mean(X)
healthcost_smallrum['variance'] = healthcost_smallrum['variance'] / (len(healthcost_category_list)**2)      # Var(1/3 X) = 1/9 Var(X)

# Calc standard deviation and upper and lower 95% CI
healthcost_smallrum['sd'] = np.sqrt(healthcost_smallrum['variance'])
del healthcost_smallrum['variance']

healthcost_smallrum['lower95'] = healthcost_smallrum['mean'] - 1.96 * healthcost_smallrum['sd']
healthcost_smallrum['upper95'] = healthcost_smallrum['mean'] + 1.96 * healthcost_smallrum['sd']

# Add rows to attribution data
ahle_combo_withattr = pd.concat(
    [ahle_combo_withattr ,healthcost_smallrum]
    ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
    ,join='outer'        # 'outer': keep all index values from all data frames
    ,ignore_index=True   # True: do not keep index values on concatenation axis
)

# =============================================================================
#### Cattle
# =============================================================================
# Get health cost AHLE rows
_row_selection = (ahle_combo_forattr_m_cattle['AHLE'].str.upper() == 'HEALTH COST')
print(f"> Selected {_row_selection.sum() :,} rows.")
healthcost_cattle = ahle_combo_forattr_m_cattle.loc[_row_selection].reset_index(drop=True).copy()
cleancolnames(healthcost_cattle)

# Add placeholder attribution categories
healthcost_cattle = pd.merge(
    left=healthcost_cattle
    ,right=healthcost_category_df
    ,on='ahle'
    ,how='outer'
)

# Allocate health cost AHLE equally to categories
healthcost_cattle['mean'] = healthcost_cattle['mean'] / len(healthcost_category_list)               # Mean(1/3 X) = 1/3 Mean(X)
healthcost_cattle['variance'] = healthcost_cattle['variance'] / (len(healthcost_category_list)**2)    # Var(1/3 X) = 1/9 Var(X)

# Calc standard deviation and upper and lower 95% CI
healthcost_cattle['sd'] = np.sqrt(healthcost_cattle['variance'])
del healthcost_cattle['variance']

healthcost_cattle['lower95'] = healthcost_cattle['mean'] - 1.96 * healthcost_cattle['sd']
healthcost_cattle['upper95'] = healthcost_cattle['mean'] + 1.96 * healthcost_cattle['sd']

# Add rows to attribution data
ahle_combo_withattr = pd.concat(
    [ahle_combo_withattr ,healthcost_cattle]
    ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
    ,join='outer'        # 'outer': keep all index values from all data frames
    ,ignore_index=True   # True: do not keep index values on concatenation axis
)

# =============================================================================
#### Poultry
# =============================================================================
# Get health cost AHLE rows
_row_selection = (ahle_combo_forattr_m_poultry['AHLE'].str.upper() == 'HEALTH COST')
print(f"> Selected {_row_selection.sum() :,} rows.")
healthcost_poultry = ahle_combo_forattr_m_poultry.loc[_row_selection].reset_index(drop=True).copy()
cleancolnames(healthcost_poultry)

# Add placeholder attribution categories
healthcost_poultry = pd.merge(
    left=healthcost_poultry
    ,right=healthcost_category_df
    ,on='ahle'
    ,how='outer'
)

# Allocate health cost AHLE equally to categories
healthcost_poultry['mean'] = healthcost_poultry['mean'] / len(healthcost_category_list)               # Mean(1/3 X) = 1/3 Mean(X)
healthcost_poultry['variance'] = healthcost_poultry['variance'] / (len(healthcost_category_list)**2)    # Var(1/3 X) = 1/9 Var(X)

# Calc standard deviation and upper and lower 95% CI
healthcost_poultry['sd'] = np.sqrt(healthcost_poultry['variance'])
del healthcost_poultry['variance']

healthcost_poultry['lower95'] = healthcost_poultry['mean'] - 1.96 * healthcost_poultry['sd']
healthcost_poultry['upper95'] = healthcost_poultry['mean'] + 1.96 * healthcost_poultry['sd']

# Add rows to attribution data
ahle_combo_withattr = pd.concat(
    [ahle_combo_withattr ,healthcost_poultry]
    ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
    ,join='outer'        # 'outer': keep all index values from all data frames
    ,ignore_index=True   # True: do not keep index values on concatenation axis
)

# =============================================================================
#### Cleanup and Export
# =============================================================================
# Rename columns to those Dash will look for
rename_cols = {
    "ahle":"ahle_component"
    ,"age_class":"group"
}
ahle_combo_withattr = ahle_combo_withattr.rename(columns=rename_cols)

# Split age and sex groups into their own columns
ahle_combo_withattr[['age_group' ,'sex']] = ahle_combo_withattr['group'].str.split(' ' ,expand=True)

# Recode columns to values Dash will look for
recode_sex = {
   None:'Overall'
   ,'female':'Female'
   ,'male':'Male'
}
ahle_combo_withattr['sex'] = ahle_combo_withattr['sex'].replace(recode_sex)

recode_age = {
   'Neonate':'Neonatal'
}
ahle_combo_withattr['age_group'] = ahle_combo_withattr['age_group'].replace(recode_age)

recode_prodsys = {
    "Dairy":"Periurban dairy"
}
ahle_combo_withattr['production_system'] = ahle_combo_withattr['production_system'].replace(recode_prodsys)

datainfo(ahle_combo_withattr)

# Drop Median column as it will not be valid after adding placeholders
del ahle_combo_withattr['median']

# Reorder columns
cols_first = [
    'species'
    ,'region'
    ,'production_system'
    ,'year'
    ,'group'
    ,'age_group'
    ,'sex'
    ,'ahle_component'
    ,'cause'
]
cols_other = [i for i in list(ahle_combo_withattr) if i not in cols_first]
ahle_combo_withattr = ahle_combo_withattr.reindex(columns=cols_first + cols_other)
ahle_combo_withattr = ahle_combo_withattr.sort_values(by=cols_first ,ignore_index=True)

datainfo(ahle_combo_withattr)

ahle_combo_withattr.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_withattr.csv') ,index=False)
# ahle_combo_withattr.to_csv(os.path.join(DASH_DATA_FOLDER ,'ahle_all_withattr.csv') ,index=False)

#%% DEV DISEASE-SPECIFIC 1
'''
This adds estimates of AHLE due to individual diseases by apportioning total AHLE
for each disease to age_class and ahle components.

THIS MAY NOT WORK.
'''
# # =============================================================================
# #### Prepare attribution data
# # =============================================================================
# # Get disaggregation proportions within each cause to age_class and ahle component
# # Separately by region, species, production_system, and year
# disagg_byvars = [
#     'region'
#     ,'species'
#     ,'production_system'
#     ,'year'
#     ,'cause'
#     ]
# disagg_into = [
#    'group'
#    ,'age_group'
#    ,'sex'
#    ,'ahle_component'
#     ]
# ahle_combo_withattr['total_bycause'] = ahle_combo_withattr.groupby(disagg_byvars)['mean'].transform('sum')
# ahle_combo_withattr['disagg_prpn_bycause'] = ahle_combo_withattr['mean'] / ahle_combo_withattr['total_bycause']

# # =============================================================================
# #### Cause: Infectious
# # =============================================================================
# # Get impact of PPR and Brucellosis from AHLE data
# # By region, species, production_system, and year
# disease_byvars = [
#     'region'
#     ,'species'
#     ,'production_system'
#     ,'year'
#     ]
# disease_inf_vars = {    # Dictionary specifying label for each disease
#     'ahle_dueto_ppr_total_mean':'PPR'
#     ,'ahle_dueto_ppr_total_stdev':'PPR'

#     ,'ahle_dueto_bruc_total_mean':'Brucellosis'
#     ,'ahle_dueto_bruc_total_stdev':'Brucellosis'

#     ,'ahle_dueto_otherdisease_total_mean':'Other Infectious'
#     ,'ahle_dueto_otherdisease_total_stdev':'Other Infectious'
#     }
# # These are only estimated for Overall agesex_scenario (we do not specify which age/sex groups are infected)
# ahle_diseases_inf = ahle_combo_forattr.query("agesex_scenario == 'Overall'")[disease_byvars + list(disease_inf_vars)]
# ahle_diseases_inf['cause'] = 'Infectious'

# # -----------------------------------------------------------------------------
# # Approach A: melt first
# # -----------------------------------------------------------------------------
# # Restructure so each disease column becomes a row
# ahle_diseases_inf_m_means = ahle_diseases_inf.melt(
#     id_vars=disagg_byvars
#     ,value_vars=[i for i in list(disease_inf_vars) if 'mean' in i]
#     ,var_name='disease'
#     ,value_name='disease_systemtotal_mean'
# )
# ahle_diseases_inf_m_stdev = ahle_diseases_inf.melt(
#     id_vars=disagg_byvars
#     ,value_vars=[i for i in list(disease_inf_vars) if 'stdev' in i]
#     ,var_name='disease'
#     ,value_name='disease_systemtotal_stdev'
# )

# # Replace variable names with disease labels
# ahle_diseases_inf_m_means['disease'] = ahle_diseases_inf_m_means['disease'].replace(disease_inf_vars)
# ahle_diseases_inf_m_stdev['disease'] = ahle_diseases_inf_m_stdev['disease'].replace(disease_inf_vars)

# # Combine disease means and standard deviations
# ahle_diseases_inf_m_tomerge = pd.merge(
#     left=ahle_diseases_inf_m_means
#     ,right=ahle_diseases_inf_m_stdev
#     ,on=disagg_byvars + ['disease']
#     ,how='inner'
#     )

# # Merge disease-specific columns onto attribution file
# # Note this will duplicate rows, creating a new row for each disease
# # Recode disease_byvars to match results of attribution
# ahle_diseases_inf_m_tomerge.loc[ahle_diseases_inf_m_tomerge['species'] == 'Cattle' ,'production_system'] = \
#     ahle_diseases_inf_m_tomerge.loc[ahle_diseases_inf_m_tomerge['species'] == 'Cattle' ,'production_system'].replace(cattle_prodsys_forattribution)

# ahle_combo_withattr_diseases = pd.merge(
#     left=ahle_combo_withattr
#     ,right=ahle_diseases_inf_m_tomerge
#     ,on=disagg_byvars
#     ,how='left'
#     )

# # Assign disease-specific AHLE to each age_class and ahle component according to disaggregation proportions
# ahle_combo_withattr_diseases['disease_disagg_mean'] = \
#     ahle_combo_withattr_diseases['disease_systemtotal_mean'] * ahle_combo_withattr_diseases['disagg_prpn_bycause']

# # Var(aX) = a**2 Var(X)
# ahle_combo_withattr_diseases['disease_disagg_stdev'] = np.sqrt(
#     ahle_combo_withattr_diseases['disease_systemtotal_stdev']**2 * ahle_combo_withattr_diseases['disagg_prpn_bycause']**2
#     )

# # -----------------------------------------------------------------------------
# # Approach B: melt last
# # -----------------------------------------------------------------------------
# # Merge disease columns onto attribution data
# ahle_combo_withattr_infcols = pd.merge(
#     left=ahle_combo_withattr
#     ,right=ahle_diseases_inf
#     ,on=disagg_byvars
#     ,how='left'
#     )

# # Assign disease-specific AHLE to each age_class and ahle component according to disaggregation proportions
# # Var(aX) = a**2 Var(X)
# ahle_combo_withattr_infcols['ahle_dueto_ppr_total_mean'] = \
#     ahle_combo_withattr_infcols['ahle_dueto_ppr_total_mean'] * ahle_combo_withattr_infcols['disagg_prpn_bycause']
# ahle_combo_withattr_infcols['ahle_dueto_ppr_total_stdev'] = np.sqrt(
#     ahle_combo_withattr_infcols['ahle_dueto_ppr_total_stdev']**2 * ahle_combo_withattr_infcols['disagg_prpn_bycause']**2
#     )

# ahle_combo_withattr_infcols['ahle_dueto_bruc_total_mean'] = \
#     ahle_combo_withattr_infcols['ahle_dueto_bruc_total_mean'] * ahle_combo_withattr_infcols['disagg_prpn_bycause']
# ahle_combo_withattr_infcols['ahle_dueto_bruc_total_stdev'] = np.sqrt(
#     ahle_combo_withattr_infcols['ahle_dueto_bruc_total_stdev']**2 * ahle_combo_withattr_infcols['disagg_prpn_bycause']**2
#     )

# # Calculate Other Disease as difference
# # This will overwrite the values of ahle_dueto_otherdisease_total_mean
# # This is necessary to make sums work because of inconsistencies introduced through attribution sampling.
# ahle_combo_withattr_infcols['ahle_dueto_otherdisease_total_mean'] = \
#     ahle_combo_withattr_infcols['mean'] \
#         - ahle_combo_withattr_infcols['ahle_dueto_ppr_total_mean'] \
#             - ahle_combo_withattr_infcols['ahle_dueto_bruc_total_mean']

# ahle_combo_withattr_infcols['ahle_dueto_otherdisease_total_stdev'] = np.sqrt(
#     ahle_combo_withattr_infcols['sd']**2 \
#         + ahle_combo_withattr_infcols['ahle_dueto_ppr_total_stdev']**2 \
#             + ahle_combo_withattr_infcols['ahle_dueto_bruc_total_stdev']**2
#     )

# # Melt disease columns into rows
# ahle_combo_withattr_infcols_means = ahle_combo_withattr_infcols.melt(
#     id_vars=disagg_byvars + disagg_into
#     ,value_vars=[i for i in list(disease_inf_vars) if 'mean' in i]
#     ,var_name='disease'
#     ,value_name='disease_mean'
#     )
# ahle_combo_withattr_infcols_stdev = ahle_combo_withattr_infcols.melt(
#     id_vars=disagg_byvars + disagg_into
#     ,value_vars=[i for i in list(disease_inf_vars) if 'stdev' in i]
#     ,var_name='disease'
#     ,value_name='disease_stdev'
#     )

# # Replace variable names with disease labels
# ahle_combo_withattr_infcols_means['disease'] = ahle_combo_withattr_infcols_means['disease'].replace(disease_inf_vars)
# ahle_combo_withattr_infcols_stdev['disease'] = ahle_combo_withattr_infcols_stdev['disease'].replace(disease_inf_vars)

# # Merge disease means and standard deviations
# ahle_combo_withattr_infcols_combo = pd.merge(
#     left=ahle_combo_withattr_infcols_means
#     ,right=ahle_combo_withattr_infcols_stdev
#     ,on=disagg_byvars + disagg_into + ['disease']
#     ,how='inner'
#     )

#%% DEV DISEASE-SPECIFIC 2
'''
Mean AHLE for PPR and Brucellosis over all age/sex groups is estimated with
the compartmental model, including components of mortality, health cost, and
production loss.

Objective 1: derive estimates of these impacts for individual age/sex groups.
- Split among age/sex groups according to their proportions of total AHLE?
According to their proportions of Infectious AHLE?
- THIS IS FLAWED. See brucellosis example in cattle. Should have zero impact
on sub-adult mortality. I do not believe that the current structure of applying
disease to individual age/sex groups makes sense for specific diseases.

Objective 2: calculate the AHLE due to "Other Diseases".
- Use the fact that all disease-specific AHLE estimates must add up to the
total Infectious AHLE.
'''
# =============================================================================
#### Objective 1: derive estimates of disease-specific impacts for individual age/sex groups
# =============================================================================
# Get total AHLE for each ahle_component and cause, separately for each region,
# species, production system, and year
agg_separately = [
    'region'
    ,'species'
    ,'production_system'
    ,'year'
]
agg_byvars = [
    'ahle_component'
    ,'cause'
]
ahle_group_prpns = ahle_combo_withattr.copy()
ahle_group_prpns['allgroups_total_bycompcause'] = ahle_group_prpns.groupby(agg_separately + agg_byvars)['mean'].transform('sum')
ahle_group_prpns['group_prpn_bycompcause'] = ahle_group_prpns['mean'] / ahle_group_prpns['allgroups_total_bycompcause']

ahle_group_prpns = ahle_group_prpns.drop(columns=['mean' ,'sd' ,'lower95' ,'upper95' ,'allgroups_total_bycompcause'])

# -----------------------------------------------------------------------------
# Cause: Infectious
# -----------------------------------------------------------------------------
# Get impact of PPR and Brucellosis estimated from compartmental model
# Separately for each region, species, production system, and year
# Define dictionary of tuples:
    # Key: variable name from ahle_combo_forattr
    # Value: tuple specifying ('Disease label', 'AHLE component')
disease_inf_labels = {
    'ahle_dueto_ppr_mortality_mean':('PPR' ,'Mortality')
    ,'ahle_dueto_ppr_mortality_stdev':('PPR' ,'Mortality')
    ,'ahle_dueto_ppr_healthcost_mean':('PPR' ,'Health cost')
    ,'ahle_dueto_ppr_healthcost_stdev':('PPR' ,'Health cost')
    ,'ahle_dueto_ppr_productionloss_mean':('PPR' ,'Production loss')
    ,'ahle_dueto_ppr_productionloss_stdev':('PPR' ,'Production loss')

    ,'ahle_dueto_bruc_mortality_mean':('Brucellosis' ,'Mortality')
    ,'ahle_dueto_bruc_mortality_stdev':('Brucellosis' ,'Mortality')
    ,'ahle_dueto_bruc_healthcost_mean':('Brucellosis' ,'Health cost')
    ,'ahle_dueto_bruc_healthcost_stdev':('Brucellosis' ,'Health cost')
    ,'ahle_dueto_bruc_productionloss_mean':('Brucellosis' ,'Production loss')
    ,'ahle_dueto_bruc_productionloss_stdev':('Brucellosis' ,'Production loss')
}

# These are only estimated for the Overall agesex_scenario (we do not specify which age/sex groups are infected)
ahle_diseases_inf = ahle_combo_forattr.query("agesex_scenario == 'Overall'")[agg_separately + list(disease_inf_labels)]
ahle_diseases_inf['cause'] = 'Infectious'

# Merge group proportions lookup
ahle_diseases_inf_prpn = pd.merge(
    left=ahle_diseases_inf
    ,right=ahle_group_prpns
    ,on=agg_separately + ['cause']
    ,how='left'
)

# # -----------------------------------------------------------------------------
# # Melt first - does not work well
# # -----------------------------------------------------------------------------
# # Restructure so each disease column becomes a row
# ahle_diseases_inf_m_means = ahle_diseases_inf.melt(
#     id_vars=agg_separately + ['cause']
#     ,value_vars=[i for i in list(disease_inf_labels) if 'mean' in i]
#     ,var_name='ahle_column'
#     ,value_name='disease_allagesex_mean'
# )
# ahle_diseases_inf_m_stdev = ahle_diseases_inf.melt(
#     id_vars=agg_separately + ['cause']
#     ,value_vars=[i for i in list(disease_inf_labels) if 'stdev' in i]
#     ,var_name='ahle_column'
#     ,value_name='disease_allagesex_stdev'
# )

# # Add disease labels and ahle component labels from lookup dictionary
# def lookup_column_from_dct(INPUT_COL ,DICT):
#     try:    # If key is found in dictionary, return value
#         return pd.Series(DICT[INPUT_COL])
#     except:
#         print('Data value not found in dictionary keys.')
#         return None
# ahle_diseases_inf_m_means[['disease' ,'ahle_component']] = ahle_diseases_inf_m_means['ahle_column'].apply(
#     lookup_column_from_dct ,DICT=disease_inf_labels
# )
# del ahle_diseases_inf_m_means['ahle_column']
# ahle_diseases_inf_m_stdev[['disease' ,'ahle_component']] = ahle_diseases_inf_m_stdev['ahle_column'].apply(
#     lookup_column_from_dct ,DICT=disease_inf_labels
# )
# del ahle_diseases_inf_m_stdev['ahle_column']

# # Combine disease means and standard deviations
# ahle_diseases_inf_m = pd.merge(
#     left=ahle_diseases_inf_m_means
#     ,right=ahle_diseases_inf_m_stdev
#     ,on=agg_separately + ['cause'] + ['disease' ,'ahle_component']
#     ,how='outer'
# )

# =============================================================================
#### Objective 2: calculate the AHLE due to "Other Diseases".
# =============================================================================
# Restructure expert attribution results to get each component into a column
ahle_combo_withattr_p = ahle_combo_withattr.pivot(
	index=['region' ,'species' ,'production_system' ,'group' ,'age_group' ,'sex','year' ]
	,columns=['ahle_component' ,'cause']
	,values='mean'
)
ahle_combo_withattr_p = colnames_from_index(ahle_combo_withattr_p)
ahle_combo_withattr_p = ahle_combo_withattr_p.add_prefix('ahle_dueto_')
ahle_combo_withattr_p = ahle_combo_withattr_p.reset_index()
cleancolnames(ahle_combo_withattr_p)

datainfo(ahle_combo_withattr_p)

# -----------------------------------------------------------------------------
# Merge ahle_combo_forattr onto expert attribution
# -----------------------------------------------------------------------------
# Recode agesex_scenario column to match groups, using pre-defined dictionaries
ahle_combo_forattr_tomerge = ahle_combo_forattr.copy()

_smallrum = (ahle_combo_forattr_tomerge['species'].str.upper() == 'ALL SMALL RUMINANTS')
ahle_combo_forattr_tomerge.loc[_smallrum ,'group'] = \
    ahle_combo_forattr_tomerge.loc[_smallrum ,'agesex_scenario'].replace(groups_for_attribution_smallrum)

_cattle = (ahle_combo_forattr_tomerge['species'].str.upper() == 'CATTLE')
ahle_combo_forattr_tomerge.loc[_cattle ,'group'] = \
    ahle_combo_forattr_tomerge.loc[_cattle ,'agesex_scenario'].replace(groups_for_attribution_cattle)

_poultry = (ahle_combo_forattr_tomerge['species'].str.upper() == 'ALL POULTRY')
ahle_combo_forattr_tomerge.loc[_poultry ,'group'] = \
    ahle_combo_forattr_tomerge.loc[_poultry ,'agesex_scenario'].replace(groups_for_attribution_poultry)

# Merge, keeping all rows from ahle_combo_withattr_p
#!!! Note: ahle_combo_withattr only has rows for specific age/sex groups, not the "overall" age/sex group,
# while disease-specific estimates on ahle_combo_forattr only exist for the "overall" age/sex group.
ahle_combo_forattr_tomerge = ahle_combo_forattr_tomerge.drop(columns=['age_group' ,'sex'])  # Keep age_group and sex from withattr file only
ahle_combo_withattr_p2 = pd.merge(
    left=ahle_combo_withattr_p
    ,right=ahle_combo_forattr_tomerge
    ,on=['region' ,'species' ,'production_system' ,'group' ,'year']
    ,how='left'
)
datainfo(ahle_combo_withattr_p2)

# -----------------------------------------------------------------------------
# Calculate AHLE due to "Other Diseases" as difference from total Infectious
# -----------------------------------------------------------------------------
ahle_combo_withattr_p2 = ahle_combo_withattr_p2.eval(
    '''
    ahle_dueto_otherinf_mortality_mean = ahle_dueto_mortality_infectious - ahle_dueto_ppr_mortality_mean - ahle_dueto_bruc_mortality_mean
    ahle_dueto_otherinf_healthcost_mean = ahle_dueto_health_cost_infectious - ahle_dueto_ppr_healthcost_mean - ahle_dueto_bruc_healthcost_mean
    ahle_dueto_otherinf_productionloss_mean = ahle_dueto_production_loss_infectious - ahle_dueto_ppr_productionloss_mean - ahle_dueto_bruc_productionloss_mean
    '''
)

#%% ADD DISEASE-SPECIFIC ATTRIBUTION

# =============================================================================
#### Cause: Infectious
# =============================================================================
'''
Infectious disease impacts have been estimated, so we will use the AHLE data.
'''
# Disease proportions may differ by region, species, production_system, and year
disease_byvars = [
    'region'
    ,'species'
    ,'production_system'
    ,'year'
    ]
disease_inf_vars = {    # Dictionary specifying label for each disease
    'ahle_dueto_ppr_total_mean':'PPR'
    ,'ahle_dueto_bruc_total_mean':'Brucellosis'
    ,'ahle_dueto_otherdisease_total_mean':'Other Infectious'
    }
# These are only estimated for Overall agesex_scenario (we do not specify which age/sex groups are infected)
ahle_diseases_inf = ahle_combo_forattr.query("agesex_scenario == 'Overall'")[disease_byvars + list(disease_inf_vars) + ['ahle_total_mean']]
ahle_diseases_inf['cause'] = 'Infectious'

# Get the proportion each disease makes up of total AHLE
# Note I am overwriting the mean columns with proportions to simplify renaming
#!!! This is flawed. We want the proportion each disease makes up of total Infectious AHLE.
for COL in list(disease_inf_vars):
    ahle_diseases_inf[COL] = ahle_diseases_inf[COL] / ahle_diseases_inf['ahle_total_mean']

ahle_diseases_inf['check_prpns'] = ahle_diseases_inf[list(disease_inf_vars)].sum(axis=1).round(1)
print('Checking proportions of disease-specific AHLE (should sum to 1):')
print(ahle_diseases_inf['check_prpns'].value_counts())
del ahle_diseases_inf['check_prpns']

# Melt disease columns into rows
ahle_diseases_inf_m = ahle_diseases_inf.melt(
    id_vars=disease_byvars + ['cause']
    ,value_vars=list(disease_inf_vars)
    ,var_name='disease'
    ,value_name='disease_proportion'
    )

# Rename diseases
ahle_diseases_inf_m['disease'] = ahle_diseases_inf_m['disease'].replace(disease_inf_vars)

# -----------------------------------------------------------------------------
# Merge onto attribution file
# -----------------------------------------------------------------------------
'''
Note this will duplicate rows, creating a new row for each disease.
'''
ahle_diseases_inf_m_tomerge = ahle_diseases_inf_m.copy()

# Recode disease_byvars to match results of attribution
ahle_diseases_inf_m_tomerge.loc[ahle_diseases_inf_m_tomerge['species'] == 'Cattle' ,'production_system'] = \
    ahle_diseases_inf_m_tomerge.loc[ahle_diseases_inf_m_tomerge['species'] == 'Cattle' ,'production_system'].replace(cattle_prodsys_forattribution)

# Use just year 2021 as that is the only year disease estimates were made
# Do not include year in merge - this will duplicate single year for all years
ahle_diseases_inf_m_tomerge = ahle_diseases_inf_m_tomerge.query("year == 2021").drop(columns='year')
merge_disease_on = disease_byvars + ['cause']
merge_disease_on.remove('year')

ahle_combo_withattr_diseases = pd.merge(
    left=ahle_combo_withattr
    ,right=ahle_diseases_inf_m_tomerge
    ,on=merge_disease_on
    ,how='left'
    )

# =============================================================================
#### Create placeholder data frames
# =============================================================================
# Infectious diseases are using real data. Placeholders not necessary.
# disease_plhd_inf = pd.DataFrame({
#     "cause":'Infectious'
#     ,"disease":['Pathogen A' ,'Pathogen B' ,'Pathogen C' ,'Pathogen D']
#     ,"disease_proportion":[0.50 ,0.25 ,0.15 ,0.10]     # List: proportion of attribution going to each disease. Must add up to 1.
#     }
# )
# Simplifying to a single cause
# disease_plhd_non = pd.DataFrame({
#     "cause":'Non-infectious'
#     ,"disease":['Condition A' ,'Condition B' ,'Condition C']
#     ,"disease_proportion":[0.50 ,0.35 ,0.15]     # List: proportion of attribution going to each disease. Must add up to 1.
#     }
# )
# disease_plhd_ext = pd.DataFrame({
#     "cause":'External'
#     ,"disease":['Cause A' ,'Cause B' ,'Cause C']
#     ,"disease_proportion":[0.50 ,0.35 ,0.15]     # List: proportion of attribution going to each disease. Must add up to 1.
#     }
# )
disease_plhd_non = pd.DataFrame({
    "cause":'Non-infectious'
    ,"disease":['All conditions']
    ,"disease_proportion":[1]     # List: proportion of attribution going to each disease. Must add up to 1.
    }
)
disease_plhd_ext = pd.DataFrame({
    "cause":'External'
    ,"disease":['All causes']
    ,"disease_proportion":[1]     # List: proportion of attribution going to each disease. Must add up to 1.
    }
)

# Concatenate
disease_plhd = pd.concat(
    # [disease_plhd_ext ,disease_plhd_inf ,disease_plhd_non]
    [disease_plhd_ext ,disease_plhd_non]
    ,axis=0
    ,join='outer'        # 'outer': keep all index values from all data frames
    ,ignore_index=True   # True: do not keep index values on concatenation axis
)

# -----------------------------------------------------------------------------
# Merge onto attribution file
# -----------------------------------------------------------------------------
ahle_combo_withattr_diseases = pd.merge(
    left=ahle_combo_withattr_diseases
    ,right=disease_plhd
    ,on='cause'
    ,how='outer'
    )

# =============================================================================
#### Calculate values based on disease proportions
# =============================================================================
# Reconcile disease and proportion columns created from different merges
ahle_combo_withattr_diseases['disease'] = take_first_nonmissing(
    ahle_combo_withattr_diseases ,['disease_x' ,'disease_y']
    )
ahle_combo_withattr_diseases = ahle_combo_withattr_diseases.drop(columns=['disease_x' ,'disease_y'])

ahle_combo_withattr_diseases['disease_proportion'] = take_first_nonmissing(
    ahle_combo_withattr_diseases ,['disease_proportion_x' ,'disease_proportion_y']
    )
ahle_combo_withattr_diseases = ahle_combo_withattr_diseases.drop(columns=['disease_proportion_x' ,'disease_proportion_y'])

# Apply disease proportions
ahle_combo_withattr_diseases['mean'] = ahle_combo_withattr_diseases['mean'] * ahle_combo_withattr_diseases['disease_proportion']
ahle_combo_withattr_diseases['sd'] = np.sqrt(ahle_combo_withattr_diseases['sd']**2 * ahle_combo_withattr_diseases['disease_proportion']**2)
ahle_combo_withattr_diseases['lower95'] = ahle_combo_withattr_diseases['mean'] - (1.96 * ahle_combo_withattr_diseases['sd'])
ahle_combo_withattr_diseases['upper95'] = ahle_combo_withattr_diseases['mean'] + (1.96 * ahle_combo_withattr_diseases['sd'])

#%% CALCULATIONS

# =============================================================================
#### Calculate as percent of total
# =============================================================================
# REVISIT: this must be BY SPECIES if you want to use it
# total_ahle = ahle_combo_withattr_diseases['mean'].sum()
# ahle_combo_withattr_diseases['pct_of_total'] = (ahle_combo_withattr_diseases['mean'] / total_ahle) * 100

# =============================================================================
#### Add currency conversion
# =============================================================================
# Merge exchange rates onto data
ahle_combo_withattr_diseases['country_name'] = 'Ethiopia'     # Add country for joining
ahle_combo_withattr_diseases = pd.merge(
    left=ahle_combo_withattr_diseases
    ,right=exchg_data_tomerge
    ,on=['country_name' ,'year']
    ,how='left'
    )
ahle_combo_withattr_diseases = ahle_combo_withattr_diseases.drop(columns=['country_name'])

# Add columns in USD
ahle_combo_withattr_diseases['mean_usd'] = ahle_combo_withattr_diseases['mean'] / ahle_combo_withattr_diseases['exchg_rate_lcuperusdol']
# For standard deviations, scale variances by the squared exchange rate.
# VAR(aX) = a^2 * VAR(X).  a = 1/exchange rate.
ahle_combo_withattr_diseases['sd_usd'] = np.sqrt(ahle_combo_withattr_diseases['sd']**2 / ahle_combo_withattr_diseases['exchg_rate_lcuperusdol']**2)
ahle_combo_withattr_diseases['lower95_usd'] = ahle_combo_withattr_diseases['lower95'] / ahle_combo_withattr_diseases['exchg_rate_lcuperusdol']
ahle_combo_withattr_diseases['upper95_usd'] = ahle_combo_withattr_diseases['upper95'] / ahle_combo_withattr_diseases['exchg_rate_lcuperusdol']

#%% CLEANUP AND EXPORT

# Reorder columns
cols_first = [
    'species'
    ,'region'
    ,'production_system'
    ,'year'
    ,'group'
    ,'age_group'
    ,'sex'
    ,'ahle_component'
    ,'cause'
    ,'disease'
    ,'disease_proportion'
]
cols_other = [i for i in list(ahle_combo_withattr_diseases) if i not in cols_first]
ahle_combo_withattr_diseases = ahle_combo_withattr_diseases.reindex(columns=cols_first + cols_other)
ahle_combo_withattr_diseases = ahle_combo_withattr_diseases.sort_values(by=cols_first ,ignore_index=True)

datainfo(ahle_combo_withattr_diseases)

# Write CSV
ahle_combo_withattr_diseases.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_withattr_disease.csv') ,index=False)
ahle_combo_withattr_diseases.to_csv(os.path.join(DASH_DATA_FOLDER ,'ahle_all_withattr_disease.csv') ,index=False)
