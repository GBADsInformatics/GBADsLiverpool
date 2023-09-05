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

#%% EXTERNAL DATA

# =============================================================================
#### Read currency conversion data
# =============================================================================
# Note: this is created in 2_process_simulation_results_standalone.py
exchg_data_tomerge = pd.read_pickle(os.path.join(ETHIOPIA_DATA_FOLDER ,'wb_exchg_data_processed.pkl.gz'))

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
attr_byvars = ('species' ,'region' ,'production_system' ,'group' ,'age_group' ,'sex' ,'year')   # Make tuple so it is immutable

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
ahle_combo_forattr_means['ahle_component'] = \
    ahle_combo_forattr_means['ahle_component'].apply(lookup_from_dct ,DICT=ahle_component_labels)
ahle_combo_forattr_stdev['ahle_component'] = \
    ahle_combo_forattr_stdev['ahle_component'].apply(lookup_from_dct ,DICT=ahle_component_labels)

# Merge means and standard deviations
ahle_combo_forattr_m = pd.merge(
   left=ahle_combo_forattr_means
   ,right=ahle_combo_forattr_stdev
   ,on=list(attr_byvars) + ['ahle_component']
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

#%% RUN EXPERT ATTRIBUTION

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
#### Combine and cleanup
# =============================================================================
ahle_combo_withattr_raw = pd.concat(
    [attribution_summary_smallruminants ,attribution_summary_cattle ,attribution_summary_poultry]
    ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
    ,join='outer'        # 'outer': keep all index values from all data frames
    ,ignore_index=True   # True: do not keep index values on concatenation axis
    )
cleancolnames(ahle_combo_withattr_raw)

# -----------------------------------------------------------------------------
# Rename and Recode columns
# -----------------------------------------------------------------------------
ahle_combo_withattr = ahle_combo_withattr_raw.copy()

rename_cols = {
    "ahle":"ahle_component"
    ,"age_class":"group"
}
ahle_combo_withattr = ahle_combo_withattr.rename(columns=rename_cols)

# Split age and sex groups into their own columns
ahle_combo_withattr[['age_group' ,'sex']] = ahle_combo_withattr['group'].str.split(' ' ,expand=True)

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
cols_first = list(attr_byvars) + ['ahle_component' ,'cause']
cols_other = [i for i in list(ahle_combo_withattr) if i not in cols_first]
ahle_combo_withattr = ahle_combo_withattr.reindex(columns=cols_first + cols_other)
ahle_combo_withattr = ahle_combo_withattr.sort_values(by=cols_first ,ignore_index=True)

datainfo(ahle_combo_withattr)

# =============================================================================
#### Export
# =============================================================================
ahle_combo_withattr.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_withattr.csv') ,index=False)

#%% COMBINE EXPERT ATTRIBUTION WITH AHLE
'''
Combine results of expert attribution with output of compartmental model to
calculate health cost and disease-specific attribution (both actuals and placeholders).
'''
# =============================================================================
#### Merge AHLE columns onto attribution
# =============================================================================
# -----------------------------------------------------------------------------
# Pivot attribution rows into columns
# -----------------------------------------------------------------------------
ahle_combo_withattr_p = ahle_combo_withattr.pivot(
	index=attr_byvars
    ,columns=['ahle_component' ,'cause']
    ,values=['mean' ,'sd']
)
ahle_combo_withattr_p = colnames_from_index(ahle_combo_withattr_p) 	# If multi-indexed columns were created, flatten index
ahle_combo_withattr_p = ahle_combo_withattr_p.reset_index()           # Pivoting will change columns to indexes. Change them back.
cleancolnames(ahle_combo_withattr_p)

datainfo(ahle_combo_withattr_p)

# -----------------------------------------------------------------------------
# Prep for merge
# -----------------------------------------------------------------------------
ahle_combo_forattr_tomerge = ahle_combo_forattr.copy()

# Recode groups on ahle_combo_forattr
# NOTE species-specific filters will be taken care of by left-merging onto attribution results
_row_selection = (ahle_combo_forattr_tomerge['species'].str.upper() == 'ALL SMALL RUMINANTS')
ahle_combo_forattr_tomerge.loc[_row_selection ,'group'] = \
    ahle_combo_forattr_tomerge.loc[_row_selection ,'group'].replace(groups_for_attribution_smallrum)

_row_selection = (ahle_combo_forattr_tomerge['species'].str.upper() == 'CATTLE')
ahle_combo_forattr_tomerge.loc[_row_selection ,'group'] = \
    ahle_combo_forattr_tomerge.loc[_row_selection ,'group'].replace(groups_for_attribution_cattle)

_row_selection = (ahle_combo_forattr_tomerge['species'].str.upper() == 'ALL POULTRY')
ahle_combo_forattr_tomerge.loc[_row_selection ,'group'] = \
    ahle_combo_forattr_tomerge.loc[_row_selection ,'group'].replace(groups_for_attribution_poultry)

# Remove age_group and sex as these no longer match group labels
del ahle_combo_forattr_tomerge['age_group'] ,ahle_combo_forattr_tomerge['sex']

# -----------------------------------------------------------------------------
# Merge AHLE columns onto ahle_combo_withattr
# -----------------------------------------------------------------------------
merge_on = list(attr_byvars)
merge_on.remove('age_group')
merge_on.remove('sex')
ahle_combo_attrmerged = pd.merge(
    left=ahle_combo_withattr_p
    ,right=ahle_combo_forattr_tomerge
    ,on=merge_on
    ,how='left'
)

# =============================================================================
#### Calcs
# =============================================================================
'''
Create columns for each combination of:
    ahle_component (mortality, production loss, health cost)
    cause (infectious, noninfectious, external)
    disease (ppr, bruc, fmd, other)

Column naming pattern: ahle_attr_[component]_[cause]_[disease]_[mean|stdev]
    e.g.:
    ahle_attr_mortality_allcause_alldisease_mean
    ahle_attr_mortality_infectious_alldisease_mean
    ahle_attr_mortality_infectious_ppr_mean
'''
# -----------------------------------------------------------------------------
# Mortality
# -----------------------------------------------------------------------------
ahle_combo_attrmerged = ahle_combo_attrmerged.eval(
    '''
    ahle_attr_mortality_allcause_alldisease_mean = ahle_dueto_mortality_mean

    ahle_attr_mortality_infectious_alldisease_mean = mean_mortality_infectious
    ahle_attr_mortality_noninfectious_alldisease_mean = mean_mortality_non_infectious
    ahle_attr_mortality_external_alldisease_mean = mean_mortality_external

    ahle_attr_mortality_infectious_ppr_mean = ahle_dueto_ppr_mortality_mean
    ahle_attr_mortality_infectious_bruc_mean = ahle_dueto_bruc_mortality_mean
    ahle_attr_mortality_infectious_fmd_mean = ahle_dueto_fmd_mortality_mean

    ahle_attr_mortality_infectious_known_mean = \
        ahle_attr_mortality_infectious_ppr_mean \
            + ahle_attr_mortality_infectious_bruc_mean \
                + ahle_attr_mortality_infectious_fmd_mean
    ahle_attr_mortality_infectious_other_mean = \
        ahle_attr_mortality_infectious_alldisease_mean - ahle_attr_mortality_infectious_known_mean
    '''
    # Standard deviations
    '''
    ahle_attr_mortality_allcause_alldisease_stdev = ahle_dueto_mortality_stdev

    ahle_attr_mortality_infectious_alldisease_stdev = sd_mortality_infectious
    ahle_attr_mortality_noninfectious_alldisease_stdev = sd_mortality_non_infectious
    ahle_attr_mortality_external_alldisease_stdev = sd_mortality_external

    ahle_attr_mortality_infectious_ppr_stdev = ahle_dueto_ppr_mortality_stdev
    ahle_attr_mortality_infectious_bruc_stdev = ahle_dueto_bruc_mortality_stdev
    ahle_attr_mortality_infectious_fmd_stdev = ahle_dueto_fmd_mortality_stdev
    '''
)

# Remaining standard deviations must be calculated outside eval() to use sqrt()
ahle_combo_attrmerged['ahle_attr_mortality_infectious_known_stdev'] = np.sqrt(
    ahle_combo_attrmerged['ahle_attr_mortality_infectious_ppr_stdev']**2 \
        + ahle_combo_attrmerged['ahle_attr_mortality_infectious_bruc_stdev']**2 \
            + ahle_combo_attrmerged['ahle_attr_mortality_infectious_fmd_stdev']**2
)
ahle_combo_attrmerged['ahle_attr_mortality_infectious_other_stdev'] = np.sqrt(
    ahle_combo_attrmerged['ahle_attr_mortality_infectious_alldisease_stdev']**2 \
        + ahle_combo_attrmerged['ahle_attr_mortality_infectious_known_stdev']**2
)

# -----------------------------------------------------------------------------
# Production loss
# -----------------------------------------------------------------------------
ahle_combo_attrmerged = ahle_combo_attrmerged.eval(
    '''
    ahle_attr_productionloss_allcause_alldisease_mean = ahle_dueto_productionloss_mean

    ahle_attr_productionloss_infectious_alldisease_mean = mean_production_loss_infectious
    ahle_attr_productionloss_noninfectious_alldisease_mean = mean_production_loss_non_infectious
    ahle_attr_productionloss_external_alldisease_mean = mean_production_loss_external

    ahle_attr_productionloss_infectious_ppr_mean = ahle_dueto_ppr_productionloss_mean
    ahle_attr_productionloss_infectious_bruc_mean = ahle_dueto_bruc_productionloss_mean
    ahle_attr_productionloss_infectious_fmd_mean = ahle_dueto_fmd_productionloss_mean

    ahle_attr_productionloss_infectious_known_mean = \
        ahle_attr_productionloss_infectious_ppr_mean \
            + ahle_attr_productionloss_infectious_bruc_mean \
                + ahle_attr_productionloss_infectious_fmd_mean
    ahle_attr_productionloss_infectious_other_mean = \
        ahle_attr_productionloss_infectious_alldisease_mean - ahle_attr_productionloss_infectious_known_mean
    '''
    # Standard deviations
    '''
    ahle_attr_productionloss_allcause_alldisease_stdev = ahle_dueto_productionloss_stdev

    ahle_attr_productionloss_infectious_alldisease_stdev = sd_production_loss_infectious
    ahle_attr_productionloss_noninfectious_alldisease_stdev = sd_production_loss_non_infectious
    ahle_attr_productionloss_external_alldisease_stdev = sd_production_loss_external

    ahle_attr_productionloss_infectious_ppr_stdev = ahle_dueto_ppr_productionloss_stdev
    ahle_attr_productionloss_infectious_bruc_stdev = ahle_dueto_bruc_productionloss_stdev
    ahle_attr_productionloss_infectious_fmd_stdev = ahle_dueto_fmd_productionloss_stdev
    '''
)

# Remaining standard deviations must be calculated outside eval() to use sqrt()
ahle_combo_attrmerged['ahle_attr_productionloss_infectious_known_stdev'] = np.sqrt(
    ahle_combo_attrmerged['ahle_attr_productionloss_infectious_ppr_stdev']**2 \
        + ahle_combo_attrmerged['ahle_attr_productionloss_infectious_bruc_stdev']**2 \
            + ahle_combo_attrmerged['ahle_attr_productionloss_infectious_fmd_stdev']**2
)
ahle_combo_attrmerged['ahle_attr_productionloss_infectious_other_stdev'] = np.sqrt(
    ahle_combo_attrmerged['ahle_attr_productionloss_infectious_alldisease_stdev']**2 \
        + ahle_combo_attrmerged['ahle_attr_productionloss_infectious_known_stdev']**2
)

# -----------------------------------------------------------------------------
# Health cost
# -----------------------------------------------------------------------------
# Note infectious, noninfectious, and external totals are unknown because they
# are not part of expert elicitation.
# Naive: health cost split equally among causes
# ahle_combo_attrmerged['ahle_attr_healthcost_infectious_alldisease_mean_a'] = ahle_combo_attrmerged['ahle_attr_healthcost_allcause_alldisease_mean'] / 3

# Total infectious must be at least equal to known diseases
# Note this does not leave any for 'other' diseases
# ahle_combo_attrmerged['ahle_attr_healthcost_infectious_alldisease_mean_b'] = ahle_combo_attrmerged['ahle_attr_healthcost_infectious_known_mean']

# ahle_combo_attrmerged['ahle_attr_healthcost_infectious_alldisease_mean'] = \
#     ahle_combo_attrmerged[['ahle_attr_healthcost_infectious_alldisease_mean_a' ,'ahle_attr_healthcost_infectious_alldisease_mean_b']].max(axis=1)
# ahle_combo_attrmerged = ahle_combo_attrmerged.drop(columns=['ahle_attr_healthcost_infectious_alldisease_mean_a' ,'ahle_attr_healthcost_infectious_alldisease_mean_b'])

# Make an assumption about the proportion of ahle_attr_healthcost_infectious_alldisease_mean captured by known diseases
healthcost_known_disease_prpn = 0.9

ahle_combo_attrmerged = ahle_combo_attrmerged.eval(
    '''
    ahle_attr_healthcost_allcause_alldisease_mean = ahle_dueto_healthcost_mean

    ahle_attr_healthcost_infectious_ppr_mean = ahle_dueto_ppr_healthcost_mean
    ahle_attr_healthcost_infectious_bruc_mean = ahle_dueto_bruc_healthcost_mean
    ahle_attr_healthcost_infectious_fmd_mean = ahle_dueto_fmd_healthcost_mean

    ahle_attr_healthcost_infectious_known_mean = \
        ahle_attr_healthcost_infectious_ppr_mean \
            + ahle_attr_healthcost_infectious_bruc_mean \
                + ahle_attr_healthcost_infectious_fmd_mean
    '''
    # Estimate health cost for all infectious diseases based on known diseases
    f'''
    ahle_attr_healthcost_infectious_alldisease_mean = \
        ahle_attr_healthcost_infectious_known_mean / {healthcost_known_disease_prpn}

    '''
    # Standard deviations
    '''
    ahle_attr_healthcost_allcause_alldisease_stdev = ahle_dueto_healthcost_stdev

    ahle_attr_healthcost_infectious_ppr_stdev = ahle_dueto_ppr_healthcost_stdev
    ahle_attr_healthcost_infectious_bruc_stdev = ahle_dueto_bruc_healthcost_stdev
    ahle_attr_healthcost_infectious_fmd_stdev = ahle_dueto_fmd_healthcost_stdev
    '''
)

# There are cases where ahle_attr_healthcost_allcause_alldisease_mean < ahle_attr_healthcost_infectious_alldisease_mean
# These actually have ahle_attr_healthcost_allcause_alldisease_mean < ahle_attr_healthcost_infectious_known_mean
# Small Ruminants: Neonate and Juvenile, CLM and Pastoral
# For these, set ahle_attr_healthcost_allcause_alldisease_mean EQUAL TO ahle_attr_healthcost_infectious_alldisease_mean
# Inflate ahle_attr_healthcost_allcause_alldisease_mean slightly so that ahle_attr_healthcost_noninfectious_alldisease_mean
# and ahle_attr_healthcost_external_alldisease_mean are nonzero.
healthcost_allcause_inflation_factor = 1.05

_total_healthcost_toolow = (ahle_combo_attrmerged['ahle_attr_healthcost_allcause_alldisease_mean'] < ahle_combo_attrmerged['ahle_attr_healthcost_infectious_alldisease_mean']*healthcost_allcause_inflation_factor)
print(f"> Found {_total_healthcost_toolow.sum()} records where total AHLE due to health cost is less than infectious AHLE health cost.")
print(f">> Setting total AHLE due to health cost equal to infectious AHLE health cost times {healthcost_allcause_inflation_factor}.")
ahle_combo_attrmerged.loc[_total_healthcost_toolow ,'ahle_attr_healthcost_allcause_alldisease_mean'] = ahle_combo_attrmerged['ahle_attr_healthcost_infectious_alldisease_mean']*healthcost_allcause_inflation_factor

# For poultry, ahle_attr_healthcost_infectious_alldisease_mean = 0 because no specific diseases are estimated.
# If no known diseases, want ahle_attr_healthcost_infectious_alldisease_mean to get another placeholder.
# Use ahle_attr_healthcost_allcause_alldisease_mean / 3.
_no_diseases_estimated = (ahle_combo_attrmerged['ahle_attr_healthcost_infectious_known_mean'] == 0)
ahle_combo_attrmerged.loc[_no_diseases_estimated ,'ahle_attr_healthcost_infectious_alldisease_mean'] = \
    ahle_combo_attrmerged['ahle_attr_healthcost_allcause_alldisease_mean'] / 3

ahle_combo_attrmerged = ahle_combo_attrmerged.eval(
    '''
    ahle_attr_healthcost_infectious_other_mean = \
        ahle_attr_healthcost_infectious_alldisease_mean - ahle_attr_healthcost_infectious_known_mean
    '''
    # Assign remaining health cost equally to noninfectious and external causes
    '''
    ahle_attr_healthcost_noninfectious_alldisease_mean = \
        (ahle_attr_healthcost_allcause_alldisease_mean - ahle_attr_healthcost_infectious_alldisease_mean) / 2
    ahle_attr_healthcost_external_alldisease_mean = \
        (ahle_attr_healthcost_allcause_alldisease_mean - ahle_attr_healthcost_infectious_alldisease_mean) / 2
    '''
)

# Remaining standard deviations must be calculated outside eval() to use sqrt()
ahle_combo_attrmerged['ahle_attr_healthcost_infectious_known_stdev'] = np.sqrt(
    ahle_combo_attrmerged['ahle_attr_healthcost_infectious_ppr_stdev']**2 \
        + ahle_combo_attrmerged['ahle_attr_healthcost_infectious_bruc_stdev']**2 \
            + ahle_combo_attrmerged['ahle_attr_healthcost_infectious_fmd_stdev']**2
)
ahle_combo_attrmerged['ahle_attr_healthcost_infectious_alldisease_stdev'] = np.sqrt(
    ahle_combo_attrmerged['ahle_attr_healthcost_infectious_known_stdev']**2 / healthcost_known_disease_prpn**2
)
ahle_combo_attrmerged['ahle_attr_healthcost_infectious_other_stdev'] = np.sqrt(
    ahle_combo_attrmerged['ahle_attr_healthcost_infectious_alldisease_stdev']**2 \
        + ahle_combo_attrmerged['ahle_attr_healthcost_infectious_known_stdev']**2
)
ahle_combo_attrmerged['ahle_attr_healthcost_noninfectious_alldisease_stdev'] = np.sqrt(
    (ahle_combo_attrmerged['ahle_attr_healthcost_allcause_alldisease_stdev']**2 \
        + ahle_combo_attrmerged['ahle_attr_healthcost_infectious_alldisease_mean']**2) / 4
)
ahle_combo_attrmerged['ahle_attr_healthcost_external_alldisease_stdev'] = np.sqrt(
    (ahle_combo_attrmerged['ahle_attr_healthcost_allcause_alldisease_stdev']**2 \
        + ahle_combo_attrmerged['ahle_attr_healthcost_infectious_alldisease_mean']**2) / 4
)

datainfo(ahle_combo_attrmerged ,150)

# =============================================================================
#### Melt attribution columns into rows
# =============================================================================
# -----------------------------------------------------------------------------
# Means
# -----------------------------------------------------------------------------
ahle_combo_attrmerged_means = ahle_combo_attrmerged.melt(
    id_vars=attr_byvars
    ,value_vars=[i for i in list(ahle_combo_attrmerged) if 'ahle_attr' in i and 'mean' in i]
    ,var_name='attr_column'
    ,value_name='mean'
)

# Decode attribution names into columns
ahle_combo_attrmerged_means[['drop_a' ,'drop_b' ,'ahle_component' ,'cause' ,'disease' ,'drop_c']] = \
    ahle_combo_attrmerged_means['attr_column'].str.split('_' ,expand=True)
ahle_combo_attrmerged_means = ahle_combo_attrmerged_means.drop(columns=['attr_column' ,'drop_a' ,'drop_b' ,'drop_c'])

# -----------------------------------------------------------------------------
# Standard deviations
# -----------------------------------------------------------------------------
ahle_combo_attrmerged_stdev = ahle_combo_attrmerged.melt(
    id_vars=attr_byvars
    ,value_vars=[i for i in list(ahle_combo_attrmerged) if 'ahle_attr' in i and 'stdev' in i]
    ,var_name='attr_column'
    ,value_name='sd'
)

# Decode attribution names into columns
ahle_combo_attrmerged_stdev[['drop_a' ,'drop_b' ,'ahle_component' ,'cause' ,'disease' ,'drop_c']] = \
    ahle_combo_attrmerged_stdev['attr_column'].str.split('_' ,expand=True)
ahle_combo_attrmerged_stdev = ahle_combo_attrmerged_stdev.drop(columns=['attr_column' ,'drop_a' ,'drop_b' ,'drop_c'])

# -----------------------------------------------------------------------------
# Merge
# -----------------------------------------------------------------------------
ahle_combo_attrmerged_m = pd.merge(
    left=ahle_combo_attrmerged_means
    ,right=ahle_combo_attrmerged_stdev
    ,on=list(attr_byvars) + ['ahle_component' ,'cause' ,'disease']
    ,how='left'
)
del ahle_combo_attrmerged_means ,ahle_combo_attrmerged_stdev

ahle_combo_attrmerged_m['lower95'] = ahle_combo_attrmerged_m['mean'] - 1.96 * ahle_combo_attrmerged_m['sd']
ahle_combo_attrmerged_m['upper95'] = ahle_combo_attrmerged_m['mean'] + 1.96 * ahle_combo_attrmerged_m['sd']

# =============================================================================
#### Add currency conversion
# =============================================================================
# Merge exchange rates onto data
ahle_combo_attrmerged_m['country_name'] = 'Ethiopia'     # Add country for joining
ahle_combo_attrmerged_m = pd.merge(
    left=ahle_combo_attrmerged_m
    ,right=exchg_data_tomerge
    ,on=['country_name' ,'year']
    ,how='left'
    )
ahle_combo_attrmerged_m = ahle_combo_attrmerged_m.drop(columns=['country_name'])

# Add columns in USD
ahle_combo_attrmerged_m['mean_usd'] = ahle_combo_attrmerged_m['mean'] / ahle_combo_attrmerged_m['exchg_rate_lcuperusdol']
# For standard deviations, scale variances by the squared exchange rate.
# VAR(aX) = a^2 * VAR(X).  a = 1/exchange rate.
ahle_combo_attrmerged_m['sd_usd'] = np.sqrt(ahle_combo_attrmerged_m['sd']**2 / ahle_combo_attrmerged_m['exchg_rate_lcuperusdol']**2)
ahle_combo_attrmerged_m['lower95_usd'] = ahle_combo_attrmerged_m['lower95'] / ahle_combo_attrmerged_m['exchg_rate_lcuperusdol']
ahle_combo_attrmerged_m['upper95_usd'] = ahle_combo_attrmerged_m['upper95'] / ahle_combo_attrmerged_m['exchg_rate_lcuperusdol']

#%% CLEANUP AND EXPORT

# -----------------------------------------------------------------------------
# Recode columns to match previous versions
# -----------------------------------------------------------------------------
recode_ahle_comp = {
    "healthcost":"Health cost"
    ,"mortality":"Mortality"
    ,"productionloss":"Production loss"
}
ahle_combo_attrmerged_m['ahle_component'] = ahle_combo_attrmerged_m['ahle_component'].replace(recode_ahle_comp)

recode_cause = {
    "infectious":"Infectious"
    ,"noninfectious":"Non-infectious"
    ,"external":"External"
}
ahle_combo_attrmerged_m['cause'] = ahle_combo_attrmerged_m['cause'].replace(recode_cause)

recode_disease = {
    "bruc":"Brucellosis"
    ,"fmd":"FMD"
    ,"ppr":"PPR"
}
ahle_combo_attrmerged_m['disease'] = ahle_combo_attrmerged_m['disease'].replace(recode_disease)

# Conditional recoding for diseases
recode_disease_inf = {
    "alldisease":"All diseases"
    ,"other":"Other diseases"
}
_cause_inf = (ahle_combo_attrmerged_m['cause'] == 'Infectious')
ahle_combo_attrmerged_m.loc[_cause_inf ,'disease'] = ahle_combo_attrmerged_m.loc[_cause_inf ,'disease'].replace(recode_disease_inf)

recode_disease_non = {
    "alldisease":"All conditions"
    ,"other":"Other conditions"
}
_cause_non = (ahle_combo_attrmerged_m['cause'] == 'Non-infectious')
ahle_combo_attrmerged_m.loc[_cause_non ,'disease'] = ahle_combo_attrmerged_m.loc[_cause_non ,'disease'].replace(recode_disease_non)

recode_disease_ext = {
    "alldisease":"All causes"
    ,"other":"Other causes"
}
_cause_ext = (ahle_combo_attrmerged_m['cause'] == 'External')
ahle_combo_attrmerged_m.loc[_cause_ext ,'disease'] = ahle_combo_attrmerged_m.loc[_cause_ext ,'disease'].replace(recode_disease_ext)

# -----------------------------------------------------------------------------
# Reorder columns
# -----------------------------------------------------------------------------
cols_first = list(attr_byvars) + ['ahle_component' ,'cause' ,'disease']
cols_other = [i for i in list(ahle_combo_attrmerged_m) if i not in cols_first]
ahle_combo_attrmerged_m = ahle_combo_attrmerged_m.reindex(columns=cols_first + cols_other)
ahle_combo_attrmerged_m = ahle_combo_attrmerged_m.sort_values(by=cols_first ,ignore_index=True)

datainfo(ahle_combo_attrmerged_m)

# -----------------------------------------------------------------------------
# Write CSV
# -----------------------------------------------------------------------------
ahle_combo_attrmerged_m.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_withattr_disease_full.csv') ,index=False)

# Filter out summary rows
_droprows = (ahle_combo_attrmerged_m['cause'] == 'allcause') \
    | (ahle_combo_attrmerged_m['disease'].isin(['known'])) \
        | (ahle_combo_attrmerged_m['disease'].isin(['All diseases']))
print(f"> Dropping {_droprows.sum() :,} rows with summary measures.")
ahle_combo_attrmerged_m_nosmry = ahle_combo_attrmerged_m.loc[~ _droprows].reset_index(drop=True)

ahle_combo_attrmerged_m_nosmry.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_withattr_disease.csv') ,index=False)
ahle_combo_attrmerged_m_nosmry.to_csv(os.path.join(DASH_DATA_FOLDER ,'ahle_all_withattr_disease.csv') ,index=False)
