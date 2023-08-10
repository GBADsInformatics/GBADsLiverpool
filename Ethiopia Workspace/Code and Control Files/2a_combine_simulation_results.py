#%% ABOUT
'''
This program combines the output files for all scenarios created by the AHLE
simulation model and adds basic adjustments.

IMPORTANT: before running this, set Python's working directory to the folder
where this code is stored.
'''
#%% PACKAGES AND FUNCTIONS

import os                # Operating system functions
import inspect
import io
import time
import numpy as np
import pandas as pd
import pickle            # To save objects to disk

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

#%% COMBINE SCENARIO RESULT FILES
'''
This imports CSV files that are output from the compartmental model.
'''
def combine_ahle_scenarios(
        input_folder
        ,input_file_prefix      # String
        ,input_file_suffixes    # List of strings. Each one combined with input_file_prefix uniquely identifies a file to be read
        ,label_species          # String: add column 'species' with this label
        ,label_prodsys          # String: add column 'production_system' with this label
        ,label_year             # Numeric: add column 'year' with this value
        ,label_region           # String: add column 'region' with this value
    ):
    dfcombined = pd.DataFrame()   # Initialize merged data

    for i ,suffix in enumerate(input_file_suffixes):
        # Read file if it exists
        try:
            df = pd.read_csv(os.path.join(input_folder ,f'{input_file_prefix}_{suffix}.csv'))

            # Add column suffixes
            if suffix.upper() == 'ALL_MORTALITY_ZERO':      # Recode for consistency
                suffix = 'MORTALITY_ZERO'

            df = df.add_suffix(f'_{suffix}')
            df = df.rename(columns={f'Item_{suffix}':'Item' ,f'Group_{suffix}':'Group'})

            # Add to merged data
            if i == 0:
                dfcombined = df.copy()
            else:
                dfcombined = pd.merge(left=dfcombined ,right=df, on=['Item' ,'Group'] ,how='outer')
        except FileNotFoundError:
            print('> File not found: ' ,os.path.join(input_folder ,f'{input_file_prefix}_{suffix}.csv'))
            print('> Moving to next file.')

    # Add label columns
    dfcombined['species'] = label_species
    dfcombined['production_system'] = label_prodsys
    dfcombined['year'] = label_year
    dfcombined['region'] = label_region

    # Reorder columns
    cols_first = ['region' ,'species' ,'production_system' ,'year']
    cols_other = [i for i in list(dfcombined) if i not in cols_first]
    dfcombined = dfcombined.reindex(columns=cols_first + cols_other)

    # Cleanup column names
    cleancolnames(dfcombined)

    return dfcombined

# =============================================================================
#### Small ruminants
# =============================================================================
'''
These scenarios have only been produced for a single year (2021) and at the
national level.
'''
small_rum_suffixes=[
    'Current'

    ,'Ideal'
    ,'ideal_AF'
    ,'ideal_AM'
    ,'ideal_JF'
    ,'ideal_JM'
    ,'ideal_NF'
    ,'ideal_NM'

    # Disease-specific scenarios
    ,'PPR'
    ,'Bruc'

    ,'all_mortality_zero'
    ,'mortality_zero_AF'
    ,'mortality_zero_AM'
    ,'mortality_zero_J'
    ,'mortality_zero_N'

    ,'all_mort_25_imp'
    ,'mort_25_imp_AF'
    ,'mort_25_imp_AM'
    ,'mort_25_imp_J'
    ,'mort_25_imp_N'

    ,'all_mort_50_imp'
    ,'mort_50_imp_AF'
    ,'mort_50_imp_AM'
    ,'mort_50_imp_J'
    ,'mort_50_imp_N'

    ,'all_mort_75_imp'
    ,'mort_75_imp_AF'
    ,'mort_75_imp_AM'
    ,'mort_75_imp_J'
    ,'mort_75_imp_N'

    ,'Current_growth_25_imp_All'
    ,'Current_growth_25_imp_AF'
    ,'Current_growth_25_imp_AM'
    ,'Current_growth_25_imp_JF'
    ,'Current_growth_25_imp_JM'
    ,'Current_growth_25_imp_NF'
    ,'Current_growth_25_imp_NM'

    ,'Current_growth_50_imp_All'
    ,'Current_growth_50_imp_AF'
    ,'Current_growth_50_imp_AM'
    ,'Current_growth_50_imp_JF'
    ,'Current_growth_50_imp_JM'
    ,'Current_growth_50_imp_NF'
    ,'Current_growth_50_imp_NM'

    ,'Current_growth_75_imp_All'
    ,'Current_growth_75_imp_AF'
    ,'Current_growth_75_imp_AM'
    ,'Current_growth_75_imp_JF'
    ,'Current_growth_75_imp_JM'
    ,'Current_growth_75_imp_NF'
    ,'Current_growth_75_imp_NM'

    ,'Current_growth_100_imp_All'
    ,'Current_growth_100_imp_AF'
    ,'Current_growth_100_imp_AM'
    ,'Current_growth_100_imp_JF'
    ,'Current_growth_100_imp_JM'
    ,'Current_growth_100_imp_NF'
    ,'Current_growth_100_imp_NM'

    ,'Current_repro_25_imp'
    ,'Current_repro_50_imp'
    ,'Current_repro_75_imp'
    ,'Current_repro_100_imp'
]

ahle_sheep_clm = combine_ahle_scenarios(
    input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle SMALL RUMINANTS')
    ,input_file_prefix='ahle_CLM_S'
    ,input_file_suffixes=small_rum_suffixes
    ,label_species='Sheep'
    ,label_prodsys='Crop livestock mixed'
    ,label_year=2021
    ,label_region='National'
)
datainfo(ahle_sheep_clm)

ahle_sheep_past = combine_ahle_scenarios(
    input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle SMALL RUMINANTS')
    ,input_file_prefix='ahle_Past_S'
    ,input_file_suffixes=small_rum_suffixes
    ,label_species='Sheep'
    ,label_prodsys='Pastoral'
    ,label_year=2021
    ,label_region='National'
)
datainfo(ahle_sheep_past)

ahle_goat_clm = combine_ahle_scenarios(
    input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle SMALL RUMINANTS')
    ,input_file_prefix='ahle_CLM_G'
    ,input_file_suffixes=small_rum_suffixes
    ,label_species='Goat'
    ,label_prodsys='Crop livestock mixed'
    ,label_year=2021
    ,label_region='National'
)
datainfo(ahle_goat_clm)

ahle_goat_past = combine_ahle_scenarios(
    input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle SMALL RUMINANTS')
    ,input_file_prefix='ahle_Past_G'
    ,input_file_suffixes=small_rum_suffixes
    ,label_species='Goat'
    ,label_prodsys='Pastoral'
    ,label_year=2021
    ,label_region='National'
)
datainfo(ahle_goat_past)

# Adjust column names to match other species
ahle_sheep_clm.columns = ahle_sheep_clm.columns.str.replace('_all_mortality_zero' ,'_mortality_zero')
ahle_sheep_past.columns = ahle_sheep_past.columns.str.replace('_all_mortality_zero' ,'_mortality_zero')
ahle_goat_clm.columns = ahle_goat_clm.columns.str.replace('_all_mortality_zero' ,'_mortality_zero')
ahle_goat_past.columns = ahle_goat_past.columns.str.replace('_all_mortality_zero' ,'_mortality_zero')

# =============================================================================
#### Cattle base
# =============================================================================
'''
These are not being used as they have been replaced by year-specific scenarios.
'''
# =============================================================================
#### Cattle Yearly
# =============================================================================
'''
These scenarios have been run for 5 years (2017-2021), so this includes a loop
to import each year and append to a master cattle dataframe.
'''
cattle_suffixes = [
    'current'

    ,'ideal'
    ,'ideal_AF'
    ,'ideal_AM'
    ,'ideal_JF'
    ,'ideal_JM'
    ,'ideal_NF'
    ,'ideal_NM'
    ,'ideal_O'

    ,'all_mortality_zero'
    ,'mortality_zero'
    ,'mortality_zero_AF'
    ,'mortality_zero_AM'
    ,'mortality_zero_J'
    ,'mortality_zero_N'
    ,'mortality_zero_O'

    # Disease-specific scenarios
    ,'Bruc'
]

ahle_cattle_yearly_aslist = []         # Initialize
for YEAR in range(2017 ,2022):
    # Import CLM
    ahle_cattle_clm = combine_ahle_scenarios(
        input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle CATTLE' ,f"{YEAR}")
        ,input_file_prefix='ahle_cattle_trial_CLM'
        ,input_file_suffixes=cattle_suffixes
        ,label_species='Cattle'
        ,label_prodsys='Crop livestock mixed'
        ,label_year=YEAR
        ,label_region='National'
        )
    datainfo(ahle_cattle_clm ,120)

	# Turn into list and append to master
    ahle_cattle_clm_aslist = ahle_cattle_clm.to_dict(orient='records')
    ahle_cattle_yearly_aslist.extend(ahle_cattle_clm_aslist)
    del ahle_cattle_clm_aslist

    # Import pastoral
    ahle_cattle_past = combine_ahle_scenarios(
        input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle CATTLE' ,f"{YEAR}")
        ,input_file_prefix='ahle_cattle_trial_past'
        ,input_file_suffixes=cattle_suffixes
        ,label_species='Cattle'
        ,label_prodsys='Pastoral'
        ,label_year=YEAR
        ,label_region='National'
        )
    datainfo(ahle_cattle_past ,120)

	# Turn into list and append to master
    ahle_cattle_past_aslist = ahle_cattle_past.to_dict(orient='records')
    ahle_cattle_yearly_aslist.extend(ahle_cattle_past_aslist)
    del ahle_cattle_past_aslist

    # Import periurban dairy
    ahle_cattle_peri = combine_ahle_scenarios(
        input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle CATTLE' ,f"{YEAR}")
        ,input_file_prefix='ahle_cattle_trial_periurban_dairy'
        ,input_file_suffixes=cattle_suffixes
        ,label_species='Cattle'
        ,label_prodsys='Periurban dairy'
        ,label_year=YEAR
        ,label_region='National'
        )
    datainfo(ahle_cattle_peri ,120)

	# Turn into list and append to master
    ahle_cattle_peri_aslist = ahle_cattle_peri.to_dict(orient='records')
    ahle_cattle_yearly_aslist.extend(ahle_cattle_peri_aslist)
    del ahle_cattle_peri_aslist

# Convert master list into data frame
ahle_cattle_yearly = pd.DataFrame.from_dict(ahle_cattle_yearly_aslist ,orient='columns')
del ahle_cattle_yearly_aslist
datainfo(ahle_cattle_yearly ,120)

# =============================================================================
#### Cattle Regional
# =============================================================================
'''
These scenarios have been run for regions within Ethiopia, so this uses a loop
to import each region and append to a master regional dataframe.

These have not been run for different years. They are being assigned year 2021.
'''
# Should match list defined in 1_run_ahle_simulation_standalone.py
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

ahle_cattle_regional_aslist = []        # Initialize
for REGION in list_eth_regions:
    # Import CLM
    ahle_cattle_regional_clm = combine_ahle_scenarios(
        input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle CATTLE' ,'Subnational results' ,f'{REGION}')
        ,input_file_prefix='ahle_cattle_trial_CLM'
        ,input_file_suffixes=cattle_suffixes
        ,label_species='Cattle'
        ,label_prodsys='Crop livestock mixed'
        ,label_year=2021
        ,label_region=f'{REGION}'
        )
    datainfo(ahle_cattle_regional_clm ,120)

	# Turn into list and append to master
    ahle_cattle_regional_clm_aslist = ahle_cattle_regional_clm.to_dict(orient='records')
    ahle_cattle_regional_aslist.extend(ahle_cattle_regional_clm_aslist)
    del ahle_cattle_regional_clm_aslist

    # Import pastoral
    ahle_cattle_regional_past = combine_ahle_scenarios(
        input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle CATTLE' ,'Subnational results' ,f'{REGION}')
        ,input_file_prefix='ahle_cattle_trial_past'
        ,input_file_suffixes=cattle_suffixes
        ,label_species='Cattle'
        ,label_prodsys='Pastoral'
        ,label_year=2021
        ,label_region=f'{REGION}'
        )
    datainfo(ahle_cattle_regional_past ,120)

	# Turn into list and append to master
    ahle_cattle_regional_past_aslist = ahle_cattle_regional_past.to_dict(orient='records')
    ahle_cattle_regional_aslist.extend(ahle_cattle_regional_past_aslist)
    del ahle_cattle_regional_past_aslist

    # Import periurban dairy
    ahle_cattle_regional_peri = combine_ahle_scenarios(
        input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle CATTLE' ,'Subnational results' ,f'{REGION}')
        ,input_file_prefix='ahle_cattle_trial_periurban_dairy'
        ,input_file_suffixes=cattle_suffixes
        ,label_species='Cattle'
        ,label_prodsys='Periurban dairy'
        ,label_year=2021
        ,label_region=f'{REGION}'
        )
    datainfo(ahle_cattle_regional_peri ,120)

	# Turn into list and append to master
    ahle_cattle_regional_peri_aslist = ahle_cattle_regional_peri.to_dict(orient='records')
    ahle_cattle_regional_aslist.extend(ahle_cattle_regional_peri_aslist)
    del ahle_cattle_regional_peri_aslist

# Convert master list into data frame
ahle_cattle_regional = pd.DataFrame.from_dict(ahle_cattle_regional_aslist ,orient='columns')
del ahle_cattle_regional_aslist
datainfo(ahle_cattle_regional ,120)

# Recode region names to match those in geojson for mapping
rename_regions = {
    'Afar':'Afar'
    ,'Amhara':'Amhara'
    ,'BG':'Benishangul Gumz'
    ,'Gambella':'Gambela'
    ,'Oromia':'Oromia'
    ,'Sidama':'Sidama'
    ,'SNNP':'SNNP'
    ,'Somali':'Somali'
    ,'Tigray':'Tigray'
    }
ahle_cattle_regional['region'] = ahle_cattle_regional['region'].replace(rename_regions)

# Create values for South West Ethiopia by replicating SNNP (they were the same region until recently)
ahle_cattle_region_swe = ahle_cattle_regional.query("region == 'SNNP'").copy()
ahle_cattle_region_swe['region'] = 'South West Ethiopia'
ahle_cattle_regional = pd.concat([ahle_cattle_regional ,ahle_cattle_region_swe] ,ignore_index=True)

# =============================================================================
#### Poultry
# =============================================================================
'''
These scenarios have only been produced for a single year (2021) and at the
national level.
'''
poultry_suffixes = [
    'current'
    ,'ideal'
    ,'ideal_A'
    ,'ideal_J'
    ,'ideal_N'
    ,'mortality_zero'
    ,'mortality_zero_A'
    ,'mortality_zero_J'
    ,'mortality_zero_N'
]

ahle_poultry_smallholder = combine_ahle_scenarios(
    input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle POULTRY')
    ,input_file_prefix='ahle_Smallholder_hybrid'
    ,input_file_suffixes=poultry_suffixes
    ,label_species='Poultry hybrid'
    ,label_prodsys='Small holder'
    ,label_year=2021
    ,label_region='National'
)
datainfo(ahle_poultry_smallholder)

ahle_poultry_villagehybrid = combine_ahle_scenarios(
    input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle POULTRY')
    ,input_file_prefix='ahle_Village_hybrid'
    ,input_file_suffixes=poultry_suffixes
    ,label_species='Poultry hybrid'
    ,label_prodsys='Village'
    ,label_year=2021
    ,label_region='National'
)
datainfo(ahle_poultry_villagehybrid)

ahle_poultry_villageindig = combine_ahle_scenarios(
    input_folder=os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle POULTRY')
    ,input_file_prefix='ahle_Village_indigenous'
    ,input_file_suffixes=poultry_suffixes
    ,label_species='Poultry indigenous'
    ,label_prodsys='Village'
    ,label_year=2021
    ,label_region='National'
)
datainfo(ahle_poultry_villageindig)

# =============================================================================
#### Stack all
# =============================================================================
concat_list = [
    ahle_sheep_clm
    ,ahle_sheep_past
    ,ahle_goat_clm
    ,ahle_goat_past

    ,ahle_cattle_yearly
    ,ahle_cattle_regional

    ,ahle_poultry_smallholder
    ,ahle_poultry_villagehybrid
    ,ahle_poultry_villageindig
]
ahle_combo_cat = pd.concat(
   concat_list      # List of dataframes to concatenate
   ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
   ,join='outer'        # 'outer': keep all index values from all data frames
   ,ignore_index=True   # True: do not keep index values on concatenation axis
)

# Reset dataframe with a copy due to warning about fragmented data
ahle_combo = ahle_combo_cat.copy()
del ahle_combo_cat

# Split age and sex groups into their own columns
ahle_combo[['age_group' ,'sex']] = ahle_combo['group'].str.split(' ' ,expand=True)

# Recode sex
recode_sex = {
    'Combined':'Overall'
    ,np.nan:'Overall'
    }
ahle_combo['sex'] = ahle_combo['sex'].replace(recode_sex)

# Special handling for Oxen
ahle_combo.loc[ahle_combo['group'].str.upper() == 'OXEN' ,'age_group'] = 'Oxen'
ahle_combo.loc[ahle_combo['group'].str.upper() == 'OXEN' ,'sex'] = 'Male'

# Reorder columns
cols_first = ['species' ,'production_system' ,'item' ,'group' ,'age_group' ,'sex']
cols_other = [i for i in list(ahle_combo) if i not in cols_first]
ahle_combo = ahle_combo.reindex(columns=cols_first + cols_other)

# =============================================================================
#### Export
# =============================================================================
datainfo(ahle_combo)

ahle_combo.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_stacked.csv') ,index=False)

#%% CHECKS ON RAW SIMULATION OUTPUT
'''
This creates some check_ datasets and prints checks to the console.
'''
check_ahle_combo = ahle_combo.copy()

_group_overall = (check_ahle_combo['group'].str.upper() == 'OVERALL')
_item_grossmargin = (check_ahle_combo['item'].str.upper() == 'GROSS MARGIN')

# =============================================================================
#### Change in Gross Margin
# =============================================================================
check_grossmargin_overall = check_ahle_combo.loc[_group_overall].loc[_item_grossmargin]
check_grossmargin_overall = check_grossmargin_overall.eval(
    # Change in Gross Margin overall vs. individual ideal scenarios
    '''
    gmchange_ideal_overall = mean_ideal - mean_current

    gmchange_ideal_af = mean_ideal_af - mean_current
    gmchange_ideal_am = mean_ideal_am - mean_current
    gmchange_ideal_jf = mean_ideal_jf - mean_current
    gmchange_ideal_jm = mean_ideal_jm - mean_current
    gmchange_ideal_nf = mean_ideal_nf - mean_current
    gmchange_ideal_nm = mean_ideal_nm - mean_current
    gmchange_ideal_o = mean_ideal_o - mean_current
    '''
    # Mortality as proportion of total AHLE
    '''
    gmchange_dueto_mortality = mean_mortality_zero - mean_current
    gmchange_dueto_production = gmchange_ideal_overall - gmchange_dueto_mortality
    gmchange_dueto_mortality_prpn = gmchange_dueto_mortality / gmchange_ideal_overall
    '''
    # Change in gross margin due to specific diseases
    '''
    gmchange_dueto_ppr = mean_ideal - mean_ppr
    gmchange_dueto_ppr_ratio = gmchange_dueto_ppr / gmchange_ideal_overall

    gmchange_dueto_bruc = mean_ideal - mean_bruc
    gmchange_dueto_bruc_ratio = gmchange_dueto_bruc / gmchange_ideal_overall
    '''
)
# Not all agesex scenarios apply to every species. Set missing to zero.
gmchange_ind_list = [
    'gmchange_ideal_af'
    ,'gmchange_ideal_am'
    ,'gmchange_ideal_jf'
    ,'gmchange_ideal_jm'
    ,'gmchange_ideal_nf'
    ,'gmchange_ideal_nm'
    ,'gmchange_ideal_o'
]
for COL in gmchange_ind_list:
    check_grossmargin_overall[COL] = check_grossmargin_overall[COL].fillna(0)

check_grossmargin_overall = check_grossmargin_overall.eval(
    '''
    gmchange_ideal_sumind = gmchange_ideal_af + gmchange_ideal_am \
        + gmchange_ideal_jf + gmchange_ideal_jm \
            + gmchange_ideal_nf + gmchange_ideal_nm \
                + gmchange_ideal_o
    gmchange_ideal_check = gmchange_ideal_sumind / gmchange_ideal_overall
    '''
)

print('\n> Summarizing the change in Gross Margin for ideal overall')
print(check_grossmargin_overall['gmchange_ideal_overall'].describe())
print(check_grossmargin_overall[['region' ,'species' ,'production_system' ,'year' ,'gmchange_ideal_overall']])

print('\n> Checking the change in Gross Margin for ideal overall vs. individual ideal scenarios')
print(check_grossmargin_overall[['region' ,'species' ,'production_system' ,'year' ,'gmchange_ideal_check']])

print('\n> Checking mortality as proportion of total AHLE')
print(check_grossmargin_overall[['region' ,'species' ,'production_system' ,'year' ,'gmchange_dueto_mortality_prpn']])

print('\n> Checking the change in gross margin due to PPR as proportion of total AHLE')
print(check_grossmargin_overall[['region' ,'species' ,'production_system' ,'year' ,'gmchange_dueto_ppr_ratio']])

print('\n> Checking the change in gross margin due to Brucellosis as proportion of total AHLE')
print(check_grossmargin_overall[['region' ,'species' ,'production_system' ,'year' ,'gmchange_dueto_bruc_ratio']])

# =============================================================================
#### Sum of agesex groups compared to system total for each item
# =============================================================================
# Sum individual agesex groups for each item
_sex_combined = (check_ahle_combo['sex'].str.upper() == 'OVERALL')
check_agesex_sums = pd.DataFrame(check_ahle_combo.loc[~ _sex_combined]\
    .groupby(['region' ,'species' ,'production_system' ,'year' ,'item'] ,observed=True)['mean_current'].sum())
check_agesex_sums.columns = ['mean_current_sumagesex']

# Merge group total for each item
check_agesex_sums = pd.merge(
    left=check_agesex_sums
    ,right=check_ahle_combo.loc[_group_overall ,['region' ,'species' ,'production_system' ,'year' ,'item' ,'mean_current']]
    ,on=['region' ,'species' ,'production_system' ,'year' ,'item']
    ,how='left'
)
check_agesex_sums = check_agesex_sums.rename(columns={'mean_current':'mean_current_overall'})

check_agesex_sums = check_agesex_sums.eval(
    '''
    check_ratio = mean_current_sumagesex / mean_current_overall
    '''
)
print('\n> Checking the sum of individual age/sex compared to the overall for each item')
print('\nMaximum ratio \n-------------')
print(check_agesex_sums.groupby(['region' ,'species' ,'production_system' ,'year'])['check_ratio'].max())
print('\nMinimum ratio \n-------------')
print(check_agesex_sums.groupby(['region' ,'species' ,'production_system' ,'year'])['check_ratio'].min())

#%% BASIC ADJUSTMENTS

ahle_combo_adj = ahle_combo.copy()

# =============================================================================
#### Adjustments
# =============================================================================
# Add item type code
# pq = physical quantity, mv = monetary value, mc = monetary cost
item_type_code = {
    'Num Offtake':'pq'
    ,'Cml Pop Growth':'pq'
    ,'Total Number Increase':'pq'
    ,'Total Mortality':'pq'
    ,'Population Liveweight (kg)':'pq'
    ,'Offtake Liveweight (kg)':'pq'
    ,'Meat (kg)':'pq'
    ,'Manure':'pq'
    ,'Hides':'pq'
    ,'Milk':'pq'
    ,'Wool':'pq'
    ,'Cml Dry Matter':'pq'
    ,'Eggs Sold':'pq'
    ,'Eggs Consumed':'pq'

    ,'Value of Offtake':'mv'
    ,'Value of Herd Increase':'mv'
    ,'Value of Herd Increase plus Offtake':'mv'
    ,'Value of Manure':'mv'
    ,'Value of Hides':'mv'
    ,'Value of Milk':'mv'
    ,'Total Production Value':'mv'
    ,'Gross Margin':'mv'
    ,'Value of draught':'mv'
    ,'Value of Eggs sold':'mv'
    ,'Value of Eggs consumed':'mv'

    ,'Feed Cost':'mc'
    ,'Labour Cost':'mc'
    ,'Health Cost':'mc'
    ,'Capital Cost':'mc'
    ,'Value of Total Mortality':'mc'
    ,'Infrastructure Cost':'mc'
    ,'Total Expenditure':'mc'
}
ahle_combo_adj['item_type_code'] = ahle_combo_adj['item'].replace(item_type_code)

# Make all monetary cost items negative
float_cols = list(ahle_combo_adj.select_dtypes(include='float'))
for COL in float_cols:
    ahle_combo_adj[COL] = np.where(
        ahle_combo_adj['item_type_code'] == 'mc'
        ,ahle_combo_adj[COL] * -1
        ,ahle_combo_adj[COL]
    )

# Reorder columns
cols_first = ['species' ,'production_system' ,'item' ,'item_type_code' ,'group' ,'age_group' ,'sex']
cols_other = [i for i in list(ahle_combo_adj) if i not in cols_first]
ahle_combo_adj = ahle_combo_adj.reindex(columns=cols_first + cols_other)

datainfo(ahle_combo_adj)

# =============================================================================
#### Add yearly placeholder rows
# =============================================================================
'''
Goal: add yearly placeholder values for any species, production system, item,
and group that does not have them. Keep actual yearly values if they exist.
'''
# Each numeric column gets inflated/deflated by a percentage
yearly_adjustment = 1.05    # Desired yearly change in values

# Get list of columns for which to add placeholders
vary_by_year = list(ahle_combo_adj.select_dtypes(include='float'))  # All columns of type Float

# Turn data into list
ahle_combo_adj_plhdyear = ahle_combo_adj.loc[ahle_combo_adj['region'] == 'National']    # Only creating yearly placeholders for national results, not regional
ahle_combo_adj_plhdyear_aslist = ahle_combo_adj_plhdyear.to_dict(orient='records')

base_year = 2021
create_years = list(range(2017 ,2022))
for YEAR in create_years:
    # Create dataset for this year
    single_year_df = ahle_combo_adj_plhdyear.copy()
    single_year_df['year'] = YEAR

    # Adjust numeric columns
    adj_factor = yearly_adjustment**(YEAR - base_year)
    for COL in vary_by_year:
        single_year_df[COL] = single_year_df[COL] * adj_factor

    # Turn data into list and append
    single_year_df_aslist = single_year_df.to_dict(orient='records')
    ahle_combo_adj_plhdyear_aslist.extend(single_year_df_aslist)

# Convert list of dictionaries into data frame
ahle_combo_adj_plhdyear = pd.DataFrame.from_dict(ahle_combo_adj_plhdyear_aslist ,orient='columns')
del ahle_combo_adj_plhdyear_aslist ,single_year_df ,single_year_df_aslist

# Concatenate with original
ahle_combo_adj = pd.concat([ahle_combo_adj ,ahle_combo_adj_plhdyear] ,axis=0 ,ignore_index=True)
del ahle_combo_adj_plhdyear

# Remove duplicate values, keeping the first (the first is the actual value for that year if it exists)
ahle_combo_adj = ahle_combo_adj.drop_duplicates(
    subset=['region' ,'species' ,'production_system' ,'item' ,'group' ,'age_group' ,'sex' ,'year']
    ,keep='first'
)

# =============================================================================
#### Export
# =============================================================================
datainfo(ahle_combo_adj)

ahle_combo_adj.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_stacked_adj.csv') ,index=False)
