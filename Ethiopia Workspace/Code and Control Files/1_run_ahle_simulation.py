#%% About
'''
The University of Liverpool (UoL) has built R code to run the simulation
compartmental model to estimate the production values and costs for different
species.

This program runs the UoL R code using the subprocess library.
Required R libraries must be installed first.

This code does not need to be run if UoL has already run the simulations.

IMPORTANT: before running this, set Python's working directory to the folder
where this code is stored.
'''
#%% Packages and functions

import os                        # Operating system functions
import subprocess                # For running command prompt or other programs
import inspect                   # For inspecting objects
import datetime as dt            # Date and time functions

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

#%% Paths and variables

CURRENT_FOLDER = os.getcwd()
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)

# Folder for shared code with Liverpool
ETHIOPIA_CODE_FOLDER = CURRENT_FOLDER
ETHIOPIA_OUTPUT_FOLDER = os.path.join(PARENT_FOLDER ,'Program outputs')
ETHIOPIA_DATA_FOLDER = os.path.join(PARENT_FOLDER ,'Data')

# Folder managed by Murdoch University with disease-specific parameters and other updates
MURDOCH_BASE_FOLDER = os.path.join(CURRENT_FOLDER ,'Disease specific attribution')
MURDOCH_SCENARIO_FOLDER = os.path.join(MURDOCH_BASE_FOLDER ,'scenarios')
MURDOCH_OUTPUT_FOLDER = os.path.join(MURDOCH_BASE_FOLDER ,'output')

# Full path to rscript.exe
r_executable = 'C:\\Program Files\\R\\R-4.3.1\\bin\\x64\\Rscript.exe'

N_RUNS = '1000'   # String: number of simulation runs for each scenario

#%% Small ruminants

# Full path to the AHLE function in R
r_script = os.path.join(ETHIOPIA_CODE_FOLDER ,'Run AHLE with control table_SMALLRUMINANTS.R')

# =============================================================================
#### Base scenarios
# =============================================================================
# Arguments to R function, as list of strings.
# ORDER MATTERS! SEE HOW THIS LIST IS PARSED INSIDE R SCRIPT.
r_args = [
    # Arg 1: Number of simulation runs
    N_RUNS

    # Arg 2: Folder location for saving output files
    ,os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle SMALL RUMINANTS')

    # Arg 3: full path to scenario control file
    # ,os.path.join(ETHIOPIA_CODE_FOLDER ,'AHLE scenario parameters SMALLRUMINANTS.xlsx')
    ,os.path.join(ETHIOPIA_CODE_FOLDER ,'AHLE scenario parameters SMALLRUMINANTS_20230504.xlsx')     # Updated 5/4/2023 but does not contain marginal improvement scenarios

    # Arg 4: only run the first N scenarios from the control file
    # -1: use all scenarios
    # 9/28: Gemma removed the code that performed this task
    # ,'-1'
]
timerstart()
returncode_smallrum = run_cmd([r_executable ,r_script] + r_args ,SHOW_MAXLINES=999)
timerstop()

# =============================================================================
#### PPR scenario
# =============================================================================
'''
Note: any scenarios that exist in this file will overwrite results of previous
run. As of April 2023, this includes ideal and current scenarios in addition to
PPR.
'''
# Arguments to R function, as list of strings.
# ORDER MATTERS! SEE HOW THIS LIST IS PARSED INSIDE R SCRIPT.
r_args = [
    # Arg 1: Number of simulation runs
    N_RUNS

    # Arg 2: Folder location for saving output files
    ,os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle SMALL RUMINANTS')

    # Arg 3: full path to scenario control file
    ,os.path.join(ETHIOPIA_CODE_FOLDER ,'PPR_AHLE scenario parameters SMALLRUMINANTS_20230329.xlsx')

    # Arg 4: only run the first N scenarios from the control file
    # -1: use all scenarios
    # 9/28: Gemma removed the code that performed this task
    # ,'-1'
]
timerstart()
returncode_smallrum_ppr = run_cmd([r_executable ,r_script] + r_args ,SHOW_MAXLINES=999)
timerstop()

# =============================================================================
#### Brucellosis scenario
# =============================================================================
'''
Note: any scenarios that exist in this file will overwrite results of previous
run. As of April 2023, this includes ideal and current scenarios in addition to
PPR.
'''
# Arguments to R function, as list of strings.
# ORDER MATTERS! SEE HOW THIS LIST IS PARSED INSIDE R SCRIPT.
r_args = [
    # Arg 1: Number of simulation runs
    N_RUNS

    # Arg 2: Folder location for saving output files
    ,os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle SMALL RUMINANTS')

    # Arg 3: full path to scenario control file
    ,os.path.join(ETHIOPIA_CODE_FOLDER ,'Bruc_AHLE scenario parameters SMALLRUMINANTS.xlsx')

    # Arg 4: only run the first N scenarios from the control file
    # -1: use all scenarios
    # 9/28: Gemma removed the code that performed this task
    # ,'-1'
]
timerstart()
returncode_smallrum_bruc = run_cmd([r_executable ,r_script] + r_args ,SHOW_MAXLINES=999)
timerstop()

#%% Small Ruminants using Murdoch's updated function
'''
August 2023: this is not needed because Murdoch is running the model with their
updated function and uploading the outputs to Github.
'''
# Full path to the AHLE function in R
r_script = os.path.join(CURRENT_FOLDER ,'ahle_sr.R')
run_cmd([r_executable ,r_script] ,SHOW_MAXLINES=999)

# Now call the function and pass arguments

#%% Cattle

# Full path to the AHLE function in R
r_script = os.path.join(ETHIOPIA_CODE_FOLDER ,'Run AHLE with control table_CATTLE.R')

# =============================================================================
#### Base scenarios
# =============================================================================
'''
August 2023: Using updated parameters provided by Murdoch University.
'''
# Arguments to R function, as list of strings.
# ORDER MATTERS! SEE HOW THIS LIST IS PARSED INSIDE R SCRIPT.
r_args = [
    # Arg 1: Number of simulation runs
    N_RUNS

    # Arg 2: Folder location for saving output files
    ,os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle CATTLE')

    # Arg 3: full path to scenario control file
    # ,os.path.join(ETHIOPIA_CODE_FOLDER ,'AHLE scenario parameters CATTLE.xlsx')
    ,os.path.join(MURDOCH_SCENARIO_FOLDER ,'AHLE scenario parameters CATTLE.xlsx')

    # Arg 4: only run the first N scenarios from the control file
    # -1: use all scenarios
    ,'-1'
]
timerstart()
returncode_cattle = run_cmd([r_executable ,r_script] + r_args ,SHOW_MAXLINES=999)
timerstop()

# =============================================================================
#### Disease scenarios
# =============================================================================
'''
Note: any scenarios that exist in this file will overwrite results of previous
run. As of August 2023, this includes ideal and current scenarios in addition to
PPR.

August 2023: Using updated parameters provided by Murdoch University.
'''
# Arguments to R function, as list of strings.
# ORDER MATTERS! SEE HOW THIS LIST IS PARSED INSIDE R SCRIPT.
r_args = [
    # Arg 1: Number of simulation runs
    N_RUNS

    # Arg 2: Folder location for saving output files
    # Note putting this in year 2021 folder although it has only been produced for a single year.
    # ,os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle CATTLE' ,'Yearly results' ,'2021')
    ,os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle CATTLE')

    # Arg 3: full path to scenario control file
    # ,os.path.join(ETHIOPIA_CODE_FOLDER ,'Bruc_AHLE scenario parameters CATTLE.xlsx')
    ,os.path.join(MURDOCH_SCENARIO_FOLDER ,'cattle_disease_scenarios.xlsx')

    # Arg 4: only run the first N scenarios from the control file
    # -1: use all scenarios
    ,'-1'
]
timerstart()
returncode_cattle_disease = run_cmd([r_executable ,r_script] + r_args ,SHOW_MAXLINES=999)
timerstop()

# =============================================================================
#### Yearly scenarios
# =============================================================================
'''
#!!! As of August 2023, yearly scenarios are not being updated. Murdoch University
is providing updated cattle scenarios for the base single-year.
'''
list_years = list(range(2017, 2022))

# Initialize list to save return codes
returncode_cattle_yearly = []

# Loop through years, calling scenario file for each and saving outputs to a new folder
for YEAR in list_years:
    print(f"> Running compartmental model for year {YEAR}...")

    # Define input scenario file
    SCENARIO_FILE = os.path.join(
        ETHIOPIA_CODE_FOLDER
        ,'Yearly parameters'
        ,f'{YEAR}_AHLE scenario parameters CATTLE_20230209 scenarios only.xlsx'
        )

    # Create subfolder for results if it doesn't exist
    OUTFOLDER = os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle CATTLE' ,'Yearly results' ,f'{YEAR}')
    os.makedirs(OUTFOLDER ,exist_ok=True)

    # Arguments to R function, as list of strings.
    # ORDER MATTERS! SEE HOW THIS LIST IS PARSED INSIDE R SCRIPT.
    r_args = [
        # Arg 1: Number of simulation runs
        # N_RUNS
        '1000'

        # Arg 2: Folder location for saving output files
        ,OUTFOLDER

        # Arg 3: full path to scenario control file
        ,SCENARIO_FILE

        # Arg 4: only run the first N scenarios from the control file
        # -1: use all scenarios
        ,'-1'
    ]
    timerstart()
    rc = run_cmd([r_executable ,r_script] + r_args ,SHOW_MAXLINES=999)
    returncode_cattle_yearly.append(rc)
    timerstop()

    print(f"> Finished compartmental model for year {YEAR}.")

# =============================================================================
#### Subnational/regional scenarios
# =============================================================================
'''
#!!! As of August 2023, regional scenarios are not being updated. Murdoch University
is providing updated cattle scenarios for the national level only.
'''
# List names as they appear in regional scenario files
list_eth_regions = [
    'Afar'
    ,'Amhara'
    ,'BG'
    ,'Gambella'
    ,'Oromia'
    ,'Sidama'
    ,'SNNP'
    ,'Somali'
    ,'Tigray'
    ]

# Initialize list to save return codes
returncode_cattle_regional = []

# Loop through regions, calling scenario file for each and saving outputs to a new folder
for REGION in list_eth_regions:
    print(f"> Running compartmental model for region {REGION}...")

    # Define input scenario file
    SCENARIO_FILE = os.path.join(
        ETHIOPIA_CODE_FOLDER
        ,'Subnational parameters'
        ,f'{REGION} 2021_AHLE scenario parameters CATTLE scenarios only.xlsx'
        )

    # Create subfolder for results if it doesn't exist
    OUTFOLDER = os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle CATTLE' ,'Subnational results' ,f'{REGION}')
    os.makedirs(OUTFOLDER ,exist_ok=True)

    # Arguments to R function, as list of strings.
    # ORDER MATTERS! SEE HOW THIS LIST IS PARSED INSIDE R SCRIPT.
    r_args = [
        # Arg 1: Number of simulation runs
        # N_RUNS
        '1000'

        # Arg 2: Folder location for saving output files
        ,OUTFOLDER

        # Arg 3: full path to scenario control file
        ,SCENARIO_FILE

        # Arg 4: only run the first N scenarios from the control file
        # -1: use all scenarios
        ,'-1'
    ]
    timerstart()
    rc = run_cmd([r_executable ,r_script] + r_args ,SHOW_MAXLINES=999)
    returncode_cattle_regional.append(rc)
    timerstop()

    print(f"> Finished compartmental model for region {REGION}.")

#%% Poultry

# Full path to the AHLE function in R
r_script = os.path.join(ETHIOPIA_CODE_FOLDER ,'Run AHLE with control table _ POULTRY.R')

# Arguments to R function, as list of strings.
# ORDER MATTERS! SEE HOW THIS LIST IS PARSED INSIDE R SCRIPT.
r_args = [
    # Arg 1: Number of simulation runs
    N_RUNS

    # Arg 2: Folder location for saving output files
    ,os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle POULTRY')

    # Arg 3: full path to scenario control file
    ,os.path.join(ETHIOPIA_CODE_FOLDER ,'AHLE scenario parameters POULTRY.xlsx')

    # Arg 4: only run the first N scenarios from the control file
    # -1: use all scenarios
    ,'-1'
]
timerstart()
returncode_poultry = run_cmd([r_executable ,r_script] + r_args ,SHOW_MAXLINES=999)
timerstop()
