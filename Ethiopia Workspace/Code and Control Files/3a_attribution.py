#%% ABOUT
'''
This runs the R program provided by Murdoch University to apply expert opinions
to estimate attribution of the AHLE to infectious, non-infectious, and external
causes.

The expert opinions are recorded in CSV files.
There is a separate file for each species group:
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

# Add a column or replace values by looking up values in a dictionary
## Note this function definition will work whether values are tuples or singles
## If dictionary returns single value for each key, df.replace() is the best way to do this
## When calling function, specify new columns to match length of tuple returned by dictionary
## Example usage: df[['new_col_1' ,'new_col_2']] = df['key_col'].apply(lookup_from_dct ,DICT=mydictionary)
def lookup_from_dct(INPUT_COL ,DICT):
	try:    # If key is found in dictionary, return value
		return pd.Series(DICT[INPUT_COL])
	except:
		print('Data value not found in dictionary keys.')
		return None

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
    os.path.join(ETHIOPIA_CODE_FOLDER ,'Attribution function input - example AHLE.csv')     # String: full path to AHLE estimates file (csv)
    ,os.path.join(ETHIOPIA_CODE_FOLDER ,'attribution_experts_smallruminants.csv')           # String: full path to expert opinion attribution file (csv)
    ,os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'attribution_summary_example.csv')                # String: full path to output file (csv)
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
ahle_combo_forattr = pd.read_pickle(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_summary2.pkl.gz'))
datainfo(ahle_combo_forattr ,200)

# =============================================================================
#### Restructure for Attribution function
# =============================================================================
attr_byvars = ['species' ,'region' ,'production_system' ,'group' ,'age_group' ,'sex' ,'year']

# Define ahle variables to use and their labels to match expert attribution file
ahle_component_labels = {
   "ahle_dueto_mortality_mean":"Mortality"
   ,"ahle_dueto_mortality_stdev":"Mortality"

   ,"ahle_dueto_healthcost_mean":"Health cost"
   ,"ahle_dueto_healthcost_stdev":"Health cost"

   ,"ahle_dueto_productionloss_mean":"Production loss"
   ,"ahle_dueto_productionloss_stdev":"Production loss"
}

ahle_combo_forattr_means = ahle_combo_forattr.melt(
   id_vars=attr_byvars
   ,value_vars=[i for i in list(ahle_component_labels) if 'mean' in i]
   ,var_name='ahle_component'
   ,value_name='mean'
)
ahle_combo_forattr_stdev = ahle_combo_forattr.melt(
   id_vars=attr_byvars
   ,value_vars=[i for i in list(ahle_component_labels) if 'stdev' in i]
   ,var_name='ahle_component'
   ,value_name='stdev'
)

# Recode AHLE components to match expert opinion file
ahle_combo_forattr_means['ahle_component'] = ahle_combo_forattr_means['ahle_component'].apply(lookup_from_dct ,DICT=ahle_component_labels)
ahle_combo_forattr_stdev['ahle_component'] = ahle_combo_forattr_stdev['ahle_component'].apply(lookup_from_dct ,DICT=ahle_component_labels)

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
Attribution function does not need aggregate production system.
Age/sex groups will be subset in species-specific preparations below.
'''
_droprows = (ahle_combo_forattr_m['production_system'].str.upper() == 'OVERALL')
print(f"> Dropping {_droprows.sum() :,} rows where production_system is 'Overall'.")
ahle_combo_forattr_m = \
    ahle_combo_forattr_m.drop(ahle_combo_forattr_m.loc[_droprows].index).reset_index(drop=True)

# =============================================================================
#### Rename and reorder columns
# =============================================================================
'''
The attribution function refers to some columns by position and others by name.
Put all the expected columns first, with correct names and ordering.
'''
colnames_ordered_forattr = {
    "species":"Species"
    ,"production_system":"Production system"
    ,"group":"Age class"
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

datainfo(ahle_combo_forattr_m_smallrum)

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

datainfo(ahle_combo_forattr_m_cattle)

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

datainfo(ahle_combo_forattr_m_poultry)

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
'''
Expert opinions do not apply to health cost. Filling in placeholders by
attributing health cost evenly to infectious, non-infectious, and external
causes.
'''
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

# -----------------------------------------------------------------------------
# Recode columns to values Dash will look for
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# Drop and reorder columns
# -----------------------------------------------------------------------------
# Drop Median column as it will not be valid after adding placeholders
del ahle_combo_withattr['median']

# Reorder columns
cols_first = attr_byvars + ['ahle_component' ,'cause']
cols_other = [i for i in list(ahle_combo_withattr) if i not in cols_first]
ahle_combo_withattr = ahle_combo_withattr.reindex(columns=cols_first + cols_other)
ahle_combo_withattr = ahle_combo_withattr.sort_values(by=cols_first ,ignore_index=True)

datainfo(ahle_combo_withattr)

ahle_combo_withattr.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_withattr.csv') ,index=False)
# ahle_combo_withattr.to_csv(os.path.join(DASH_DATA_FOLDER ,'ahle_all_withattr.csv') ,index=False)

#%% *DEV A* ADD DISEASE-SPECIFIC COMPONENTS

# =============================================================================
#### Get disease-specific AHLE
# =============================================================================
# -----------------------------------------------------------------------------
# Cause: Infectious
# -----------------------------------------------------------------------------
'''
AHLE for infectious diseases PPR and Brucellosis has been estimated with the
compartmental model, including components of mortality, health cost, and
production loss.
'''
# Define lookup dictionary for infectious diseases
# Key: disease label - whatever you want
# Value: dictionary with all columns from ahle_combo_forattr due to that disease and the ahle_component label for each
diseases_infectious = {
    'PPR':{
        'ahle_dueto_ppr_mortality_mean':'Mortality'
        ,'ahle_dueto_ppr_mortality_stdev':'Mortality'

        ,'ahle_dueto_ppr_healthcost_mean':'Health cost'
        ,'ahle_dueto_ppr_healthcost_stdev':'Health cost'

        ,'ahle_dueto_ppr_productionloss_mean':'Production loss'
        ,'ahle_dueto_ppr_productionloss_stdev':'Production loss'
    }
    ,'Brucellosis':{
        'ahle_dueto_bruc_mortality_mean':'Mortality'
        ,'ahle_dueto_bruc_mortality_stdev':'Mortality'

        ,'ahle_dueto_bruc_healthcost_mean':'Health cost'
        ,'ahle_dueto_bruc_healthcost_stdev':'Health cost'

        ,'ahle_dueto_bruc_productionloss_mean':'Production loss'
        ,'ahle_dueto_bruc_productionloss_stdev':'Production loss'
    }
    ,'FMD':{
        'ahle_dueto_fmd_mortality_mean':'Mortality'
        ,'ahle_dueto_fmd_mortality_stdev':'Mortality'

        ,'ahle_dueto_fmd_healthcost_mean':'Health cost'
        ,'ahle_dueto_fmd_healthcost_stdev':'Health cost'

        ,'ahle_dueto_fmd_productionloss_mean':'Production loss'
        ,'ahle_dueto_fmd_productionloss_stdev':'Production loss'
    }
}

# For each disease, melt columns of mortality, health cost, and production loss
# to get a row for each ahle_component
for DISEASE in list(diseases_infectious):
    single_disease_means = ahle_combo_forattr.melt(
        id_vars=attr_byvars
        ,value_vars=[i for i in list(diseases_infectious[DISEASE]) if 'mean' in i]
        ,var_name='ahle_column'
        ,value_name=f'ahle_dueto_{DISEASE}_mean'
    )
    single_disease_stdev = ahle_combo_forattr.melt(
        id_vars=attr_byvars
        ,value_vars=[i for i in list(diseases_infectious[DISEASE]) if 'stdev' in i]
        ,var_name='ahle_column'
        ,value_name=f'ahle_dueto_{DISEASE}_stdev'
    )

    # Add ahle component labels
    single_disease_means['ahle_component'] = single_disease_means['ahle_column'].replace(diseases_infectious[DISEASE])
    del single_disease_means['ahle_column']
    single_disease_stdev['ahle_component'] = single_disease_stdev['ahle_column'].replace(diseases_infectious[DISEASE])
    del single_disease_stdev['ahle_column']

    # Merge means and standard deviations
    single_disease_ahle = pd.merge(
        left=single_disease_means
        ,right=single_disease_stdev
        ,on=attr_byvars + ['ahle_component']
        ,how='left'
    )

    # Merge onto master disease-specific dataframe
    if DISEASE == list(diseases_infectious)[0]:     # If first disease, copy
        ahle_disease_inf = single_disease_ahle.copy()
    else:       # Otherwise, merge
        ahle_disease_inf = pd.merge(
            left=ahle_disease_inf
            ,right=single_disease_ahle
            ,on=attr_byvars + ['ahle_component']
            ,how='left'
        )
del single_disease_means ,single_disease_stdev ,single_disease_ahle
ahle_disease_inf['cause'] = 'Infectious'
cleancolnames(ahle_disease_inf)

# Calculate total contribution of known diseases
ahle_disease_inf['ahle_dueto_knowninfectious_mean'] = \
    ahle_disease_inf['ahle_dueto_ppr_mean'] + ahle_disease_inf['ahle_dueto_brucellosis_mean'] + ahle_disease_inf['ahle_dueto_fmd_mean']

ahle_disease_inf['ahle_dueto_knowninfectious_stdev'] = \
    np.sqrt(ahle_disease_inf['ahle_dueto_ppr_stdev']**2 + ahle_disease_inf['ahle_dueto_brucellosis_stdev']**2 + ahle_disease_inf['ahle_dueto_fmd_stdev']**2)

datainfo(ahle_disease_inf)

# -----------------------------------------------------------------------------
# Cause: Non-Infectious
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Cause: External
# -----------------------------------------------------------------------------

# =============================================================================
#### Merge and calcs
# =============================================================================
# -----------------------------------------------------------------------------
# Apply species-specific group labels
# -----------------------------------------------------------------------------
# NOTE species-specific filters will be taken care of by left-merging onto attribution results
_row_selection = (ahle_disease_inf['species'].str.upper() == 'ALL SMALL RUMINANTS')
ahle_disease_inf.loc[_row_selection ,'group'] = \
    ahle_disease_inf.loc[_row_selection ,'group'].replace(groups_for_attribution_smallrum)

_row_selection = (ahle_disease_inf['species'].str.upper() == 'CATTLE')
ahle_disease_inf.loc[_row_selection ,'group'] = \
    ahle_disease_inf.loc[_row_selection ,'group'].replace(groups_for_attribution_cattle)

_row_selection = (ahle_disease_inf['species'].str.upper() == 'ALL POULTRY')
ahle_disease_inf.loc[_row_selection ,'group'] = \
    ahle_disease_inf.loc[_row_selection ,'group'].replace(groups_for_attribution_poultry)

# Remove age_group and sex as these no longer match group labels
del ahle_disease_inf['age_group'] ,ahle_disease_inf['sex']

# -----------------------------------------------------------------------------
# Merge disease-specific columns onto ahle_combo_withattr
# -----------------------------------------------------------------------------
merge_on = attr_byvars + ['ahle_component' ,'cause']
merge_on.remove('age_group')
merge_on.remove('sex')
ahle_combo_withattr_plusinf = pd.merge(
    left=ahle_combo_withattr
    ,right=ahle_disease_inf
    ,on=merge_on
    ,how='left'
)

# -----------------------------------------------------------------------------
# Adjust health cost placeholder
# -----------------------------------------------------------------------------
'''
Disease-specific health costs may add up to more than the total Infectious health
cost because we split health cost evenly among infectious, non-infectious, and
external causes.

If total infectious health cost is less than the sum of disease-specific health
cost, set it equal to the sum.
'''
_cause_infectious = (ahle_combo_withattr_plusinf['cause'] == 'Infectious')

# -----------------------------------------------------------------------------
# Calculate ahle due to "other" diseases
# -----------------------------------------------------------------------------
ahle_combo_withattr_plusinf['ahle_dueto_otherinfectious_mean'] = \
    ahle_combo_withattr_plusinf['mean'] - ahle_combo_withattr_plusinf['ahle_dueto_knowninfectious_mean']

ahle_combo_withattr_plusinf['ahle_dueto_otherinfectious_stdev'] = \
    np.sqrt(ahle_combo_withattr_plusinf['sd']**2 + ahle_combo_withattr_plusinf['ahle_dueto_knowninfectious_stdev']**2)

datainfo(ahle_combo_withattr_plusinf)

# =============================================================================
#### Melt disease-specific columns into rows
# =============================================================================
# Means
ahle_combo_withattr_plusinf_means = ahle_combo_withattr_plusinf.melt(
    id_vars=attr_byvars + ['ahle_component' ,'cause']
    #!!! ,value_vars=[i for i in list(ahle_combo_withattr_plusinf) if 'ahle_dueto' in i and 'mean' in i]
    ,value_vars=[i for i in list(ahle_combo_withattr_plusinf) if 'mean' in i]
    ,var_name='disease_column'
    ,value_name='disease_mean'
)
ahle_combo_withattr_plusinf_means[['drop_a' ,'drop_b' ,'disease' ,'drop_c']] = \
    ahle_combo_withattr_plusinf_means['disease_column'].str.split('_' ,n=3 ,expand=True)
ahle_combo_withattr_plusinf_means = ahle_combo_withattr_plusinf_means.drop(columns={
    'disease_column'
    ,'drop_a' ,'drop_b' ,'drop_c'
})

# Standard deviations
ahle_combo_withattr_plusinf_stdev = ahle_combo_withattr_plusinf.melt(
    id_vars=attr_byvars + ['ahle_component' ,'cause']
    ,value_vars=[i for i in list(ahle_combo_withattr_plusinf) if 'ahle_dueto' in i and 'stdev' in i]
    ,var_name='disease_column'
    ,value_name='disease_stdev'
)
ahle_combo_withattr_plusinf_stdev[['drop_a' ,'drop_b' ,'disease' ,'drop_c']] = \
    ahle_combo_withattr_plusinf_stdev['disease_column'].str.split('_' ,n=3 ,expand=True)
ahle_combo_withattr_plusinf_stdev = ahle_combo_withattr_plusinf_stdev.drop(columns={
    'disease_column'
    ,'drop_a' ,'drop_b' ,'drop_c'
})

# Merge
ahle_combo_withattr_diseases = pd.merge(
    left=ahle_combo_withattr_plusinf_means
    ,right=ahle_combo_withattr_plusinf_stdev
    ,on=attr_byvars + ['ahle_component' ,'cause' ,'disease']
    ,how='left'
)
del ahle_combo_withattr_plusinf_means ,ahle_combo_withattr_plusinf_stdev

datainfo(ahle_combo_withattr_diseases)

#%% *DEV B* ADD DISEASE-SPECIFIC COMPONENTS - MELT FIRST

# =============================================================================
#### Cause: Infectious
# =============================================================================
'''
AHLE for infectious diseases PPR and Brucellosis has been estimated with the
compartmental model, including components of mortality, health cost, and
production loss.
'''
# Get impact of PPR and Brucellosis estimated from compartmental model
# Separately for each region, species, production system, and year
# Define dictionary:
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

    ,'ahle_dueto_fmd_mortality_mean':('FMD' ,'Mortality')
    ,'ahle_dueto_fmd_mortality_stdev':('FMD' ,'Mortality')
    ,'ahle_dueto_fmd_healthcost_mean':('FMD' ,'Health cost')
    ,'ahle_dueto_fmd_healthcost_stdev':('FMD' ,'Health cost')
    ,'ahle_dueto_fmd_productionloss_mean':('FMD' ,'Production loss')
    ,'ahle_dueto_fmd_productionloss_stdev':('FMD' ,'Production loss')
}

# -----------------------------------------------------------------------------
# Restructure so each disease column becomes a row
# -----------------------------------------------------------------------------
ahle_diseases_inf_m_means = ahle_combo_forattr.melt(
    id_vars=attr_byvars
    ,value_vars=[i for i in list(disease_inf_labels) if 'mean' in i]
    ,var_name='ahle_column'
    ,value_name='disease_mean'
)
ahle_diseases_inf_m_stdev = ahle_combo_forattr.melt(
    id_vars=attr_byvars
    ,value_vars=[i for i in list(disease_inf_labels) if 'stdev' in i]
    ,var_name='ahle_column'
    ,value_name='disease_stdev'
)

# Add disease labels and ahle component labels from lookup dictionary
ahle_diseases_inf_m_means[['disease' ,'ahle_component']] = ahle_diseases_inf_m_means['ahle_column'].apply(
    lookup_from_dct ,DICT=disease_inf_labels
)
del ahle_diseases_inf_m_means['ahle_column']
ahle_diseases_inf_m_stdev[['disease' ,'ahle_component']] = ahle_diseases_inf_m_stdev['ahle_column'].apply(
    lookup_from_dct ,DICT=disease_inf_labels
)
del ahle_diseases_inf_m_stdev['ahle_column']

# Merge means and standard deviations
ahle_diseases_inf_m = pd.merge(
    left=ahle_diseases_inf_m_means
    ,right=ahle_diseases_inf_m_stdev
    ,on=attr_byvars + ['disease' ,'ahle_component']
    ,how='outer'
)
del ahle_diseases_inf_m_means ,ahle_diseases_inf_m_stdev
ahle_diseases_inf_m['cause'] = 'Infectious'

# -----------------------------------------------------------------------------
# Merge onto attribution result
# -----------------------------------------------------------------------------
# Apply species-specific group labels
## NOTE species-specific filters will be taken care of by left-merging onto attribution results
_row_selection = (ahle_diseases_inf_m['species'].str.upper() == 'ALL SMALL RUMINANTS')
ahle_diseases_inf_m.loc[_row_selection ,'group'] = \
    ahle_diseases_inf_m.loc[_row_selection ,'group'].replace(groups_for_attribution_smallrum)

_row_selection = (ahle_diseases_inf_m['species'].str.upper() == 'CATTLE')
ahle_diseases_inf_m.loc[_row_selection ,'group'] = \
    ahle_diseases_inf_m.loc[_row_selection ,'group'].replace(groups_for_attribution_cattle)

_row_selection = (ahle_diseases_inf_m['species'].str.upper() == 'ALL POULTRY')
ahle_diseases_inf_m.loc[_row_selection ,'group'] = \
    ahle_diseases_inf_m.loc[_row_selection ,'group'].replace(groups_for_attribution_poultry)

# Remove age_group and sex as these no longer match group labels
del ahle_diseases_inf_m['age_group'] ,ahle_diseases_inf_m['sex']

# Merge
## This will create new rows, one for each disease
merge_on = attr_byvars + ['ahle_component' ,'cause']
merge_on.remove('age_group')
merge_on.remove('sex')
ahle_combo_withattr_diseases = pd.merge(
    left=ahle_combo_withattr
    ,right=ahle_diseases_inf_m
    ,on=merge_on
    ,how='left'
)

# -----------------------------------------------------------------------------
# Add calcs
# -----------------------------------------------------------------------------
# Calculate total due to specific diseases by ahle_component and cause
ahle_combo_withattr_diseases['specific_disease_total'] = \
    ahle_combo_withattr_diseases.groupby(attr_byvars + ['ahle_component' ,'cause'])['disease_mean'].transform('sum')

# Get total due to other diseases as difference from total by cause

#%% DISEASE-SPECIFIC PLACEHOLDERS

# =============================================================================
#### Cause: Infectious
# =============================================================================
'''
Infectious disease impacts have been estimated, so we will use the AHLE data.
'''
# Infectious diseases are using real data. Placeholders not necessary.
# disease_plhd_inf = pd.DataFrame({
#     "cause":'Infectious'
#     ,"disease":['Pathogen A' ,'Pathogen B' ,'Pathogen C']
#     ,"disease_proportion":[0.50 ,0.25 ,0.15 ,0.10]     # List: proportion of attribution going to each disease. Must add up to 1.
#     }
# )

# =============================================================================
#### Cause: Non-infectious
# =============================================================================
# Simplifying to a single cause
# disease_plhd_non = pd.DataFrame({
#     "cause":'Non-infectious'
#     ,"disease":['Condition A' ,'Condition B' ,'Condition C']
#     ,"disease_proportion":[0.50 ,0.35 ,0.15]     # List: proportion of attribution going to each disease. Must add up to 1.
#     }
# )
disease_plhd_non = pd.DataFrame({
    "cause":'Non-infectious'
    ,"disease":['All conditions']
    ,"disease_proportion":[1]     # List: proportion of attribution going to each disease. Must add up to 1.
    }
)
# =============================================================================
#### Cause: External
# =============================================================================
# disease_plhd_ext = pd.DataFrame({
#     "cause":'External'
#     ,"disease":['Cause A' ,'Cause B' ,'Cause C']
#     ,"disease_proportion":[0.50 ,0.35 ,0.15]     # List: proportion of attribution going to each disease. Must add up to 1.
#     }
# )
disease_plhd_ext = pd.DataFrame({
    "cause":'External'
    ,"disease":['All causes']
    ,"disease_proportion":[1]     # List: proportion of attribution going to each disease. Must add up to 1.
    }
)

# =============================================================================
#### Merge onto attribution file
# =============================================================================
# Concatenate
disease_plhd = pd.concat(
    [disease_plhd_ext ,disease_plhd_non]
    ,axis=0
    ,join='outer'        # 'outer': keep all index values from all data frames
    ,ignore_index=True   # True: do not keep index values on concatenation axis
)

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
