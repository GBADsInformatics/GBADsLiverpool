#%% About
'''
This program combines the output files for all scenarios created by the AHLE
simulation model and adds calculations. The output is a CSV file to be used
in the dashboard.

IMPORTANT: before running this, set Python's working directory to the folder
where this code is stored.
'''
#%% Packages and functions

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

#%% Paths and variables

CURRENT_FOLDER = os.getcwd()
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
GRANDPARENT_FOLDER = os.path.dirname(PARENT_FOLDER)

# Folder for shared code with Liverpool
ETHIOPIA_CODE_FOLDER = CURRENT_FOLDER
ETHIOPIA_OUTPUT_FOLDER = os.path.join(PARENT_FOLDER ,'Program outputs')
ETHIOPIA_DATA_FOLDER = os.path.join(PARENT_FOLDER ,'Data')

DASH_DATA_FOLDER = os.path.join(GRANDPARENT_FOLDER, 'AHLE Dashboard' ,'Dash App' ,'data')

#%% External data

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

#%% Combine scenario result files
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

#%% Checks on raw simulation output
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

#%% Basic Adjustments

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

#%% Add group summaries
'''
Creating aggregate groups for filtering in the dashboard

Note: this handles all items the same, whether they are animal (head) counts,
mass (kg), or dollar values. Be careful when using the results that you are not
mixing apples and oranges.
'''
mean_cols = [i for i in list(ahle_combo_adj) if 'mean' in i]
sd_cols = [i for i in list(ahle_combo_adj) if 'stdev' in i]

# =============================================================================
#### Drop aggregate groups
# =============================================================================
# Some items only exist for Overall group in original file. Separate all existing Overall records.
_combined_rows = (ahle_combo_adj['group'].str.upper() == 'OVERALL')\
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
#### Build aggregate groups
# =============================================================================
# Only using MEAN and VARIANCE of each item, as the other statistics cannot
# be summed.
keepcols = ['region' ,'species' ,'production_system' ,'item' ,'group' ,'age_group' ,'sex' ,'year'] \
    + mean_cols + sd_cols

ahle_combo_withagg = ahle_combo_indiv[keepcols].copy()
datainfo(ahle_combo_withagg)

# -----------------------------------------------------------------------------
# Create variance columns
# -----------------------------------------------------------------------------
# Relying on the following properties of sums of random variables:
#    mean(aX + bY) = a*mean(X) + b*mean(Y), regardless of correlation
#    var(aX + bY) = a^2*var(X) + b^2*var(Y), assuming X and Y are uncorrelated
var_cols = ['sqrd_' + COLNAME for COLNAME in sd_cols]
for i ,VARCOL in enumerate(var_cols):
   SDCOL = sd_cols[i]
   ahle_combo_withagg[VARCOL] = ahle_combo_withagg[SDCOL]**2

# -----------------------------------------------------------------------------
# Create Overall age/sex group
# -----------------------------------------------------------------------------
# Must be first sum to avoid double-counting!
ahle_combo_withagg_sumall = ahle_combo_withagg.pivot_table(
    index=['region' ,'species' ,'production_system' ,'item' ,'year']
    ,values=mean_cols + var_cols
    ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
).reset_index()
ahle_combo_withagg_sumall['group'] = 'Overall'
ahle_combo_withagg_sumall['age_group'] = 'Overall'
ahle_combo_withagg_sumall['sex'] = 'Overall'

ahle_combo_withagg = pd.concat(
    [ahle_combo_withagg ,ahle_combo_withagg_sumall]
    ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
    ,join='outer'        # 'outer': keep all index values from all data frames
    ,ignore_index=True   # True: do not keep index values on concatenation axis
)
del ahle_combo_withagg_sumall

# -----------------------------------------------------------------------------
# Create Overall sex for each age group
# -----------------------------------------------------------------------------
for AGE_GRP in age_group_values:
    ahle_combo_withagg_sumsexes = ahle_combo_withagg.query(f"age_group == '{AGE_GRP}'").pivot_table(
        index=['region' ,'species' ,'production_system' ,'item' ,'age_group' ,'year']
        ,values=mean_cols + var_cols
        ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
    ).reset_index()
    ahle_combo_withagg_sumsexes['group'] = f'{AGE_GRP} Combined'
    ahle_combo_withagg_sumsexes['sex'] = 'Overall'

    # Stack
    ahle_combo_withagg = pd.concat(
        [ahle_combo_withagg ,ahle_combo_withagg_sumsexes]
        ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
        ,join='outer'        # 'outer': keep all index values from all data frames
        ,ignore_index=True   # True: do not keep index values on concatenation axis
    )
del ahle_combo_withagg_sumsexes

# Oxen are a special age group which is only male. Drop "combined" sex.
_oxen_combined = (ahle_combo_withagg['group'].str.upper() == 'OXEN COMBINED')
ahle_combo_withagg = ahle_combo_withagg.drop(ahle_combo_withagg.loc[_oxen_combined].index).reset_index(drop=True)

# -----------------------------------------------------------------------------
# Create Overall age group for each sex
# -----------------------------------------------------------------------------
for SEX_GRP in sex_values:
    ahle_combo_withagg_sumages = ahle_combo_withagg.query(f"sex == '{SEX_GRP}'").pivot_table(
        index=['region' ,'species' ,'production_system' ,'item' ,'sex' ,'year']
        ,values=mean_cols + var_cols
        ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
    ).reset_index()
    ahle_combo_withagg_sumages['group'] = f'Overall {SEX_GRP}'
    ahle_combo_withagg_sumages['age_group'] = 'Overall'

    # Stack
    ahle_combo_withagg = pd.concat(
        [ahle_combo_withagg ,ahle_combo_withagg_sumages]
        ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
        ,join='outer'        # 'outer': keep all index values from all data frames
        ,ignore_index=True   # True: do not keep index values on concatenation axis
    )
del ahle_combo_withagg_sumages

# -----------------------------------------------------------------------------
# Add back original Overall age/sex and de-dup
# -----------------------------------------------------------------------------
# Concatenate original Overall group data
ahle_combo_withagg = pd.concat(
   [ahle_combo_withagg ,ahle_combo_overall]
   ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
   ,join='outer'        # 'outer': keep all index values from all data frames
   ,ignore_index=True   # True: do not keep index values on concatenation axis
)

# De-Dup, keeping new Overall group if it exists
ahle_combo_withagg = ahle_combo_withagg.drop_duplicates(
   subset=['region' ,'species' ,'production_system' ,'item' ,'group' ,'year']       # List (opt): only consider these columns when identifying duplicates. If None, consider all columns.
   ,keep='first'
)

# -----------------------------------------------------------------------------
# Create overall production system
# -----------------------------------------------------------------------------
ahle_combo_withagg_sumprod = ahle_combo_withagg.pivot_table(
   index=['region' ,'species' ,'item' ,'group' ,'age_group' ,'sex' ,'year']
   ,values=mean_cols + var_cols
   ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
).reset_index()
ahle_combo_withagg_sumprod['production_system'] = 'Overall'

ahle_combo_withagg = pd.concat(
   [ahle_combo_withagg ,ahle_combo_withagg_sumprod]
   ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
   ,join='outer'        # 'outer': keep all index values from all data frames
   ,ignore_index=True   # True: do not keep index values on concatenation axis
)
del ahle_combo_withagg_sumprod

# -----------------------------------------------------------------------------
# Create combined species
# -----------------------------------------------------------------------------
# All Small Ruminants
ahle_combo_withagg_sumspec = ahle_combo_withagg.query("species.str.upper().isin(['SHEEP' ,'GOAT'])").pivot_table(
   index=['region' ,'production_system' ,'item' ,'group' ,'age_group' ,'sex' ,'year']
   ,values=mean_cols + var_cols
   ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
).reset_index()
ahle_combo_withagg_sumspec['species'] = 'All Small Ruminants'

# All poultry
ahle_combo_withagg_sumspec2 = ahle_combo_withagg.query("species.str.contains('poultry' ,case=False ,na=False)").pivot_table(
   index=['region' ,'production_system' ,'item' ,'group' ,'age_group' ,'sex' ,'year']
   ,values=mean_cols + var_cols
   ,aggfunc=lambda x: x.mean() * x.count()  # Hack: sum is equal to zero if all values are missing. This will cause all missings to produce missing.
).reset_index()
ahle_combo_withagg_sumspec2['species'] = 'All Poultry'

ahle_combo_withagg = pd.concat(
   [ahle_combo_withagg ,ahle_combo_withagg_sumspec ,ahle_combo_withagg_sumspec2]
   ,axis=0              # axis=0: concatenate rows (stack), axis=1: concatenate columns (merge)
   ,join='outer'        # 'outer': keep all index values from all data frames
   ,ignore_index=True   # True: do not keep index values on concatenation axis
)
del ahle_combo_withagg_sumspec ,ahle_combo_withagg_sumspec2

# -----------------------------------------------------------------------------
# Calculate standard deviations
# -----------------------------------------------------------------------------
# Will overwrite standard deviations for the handful of groups that have them,
# but that's OK.
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

# For standard deviations, convert to variances then scale by the squared exchange rate
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
    ,'exchg_rate_lcuperusdol'

    # In Birr
    ,'mean_current'
    ,'stdev_current'
    ,'mean_ideal'
    ,'stdev_ideal'

    ,'mean_all_mort_25_imp'
    ,'mean_all_mort_50_imp'
    ,'mean_all_mort_75_imp'
    ,'mean_mortality_zero'
    ,'stdev_mortality_zero'

    ,'mean_current_repro_25_imp'
    ,'mean_current_repro_50_imp'
    ,'mean_current_repro_75_imp'
    ,'mean_current_repro_100_imp'

    ,'mean_current_growth_25_imp_all'
    ,'mean_current_growth_50_imp_all'
    ,'mean_current_growth_75_imp_all'
    ,'mean_current_growth_100_imp_all'

    # In USD
    ,'mean_current_usd'
    ,'stdev_current_usd'
    ,'mean_ideal_usd'
    ,'stdev_ideal_usd'

    ,'mean_all_mort_25_imp_usd'
    ,'mean_all_mort_50_imp_usd'
    ,'mean_all_mort_75_imp_usd'
    ,'mean_mortality_zero_usd'
    ,'stdev_mortality_zero_usd'

    ,'mean_current_repro_25_imp_usd'
    ,'mean_current_repro_50_imp_usd'
    ,'mean_current_repro_75_imp_usd'
    ,'mean_current_repro_100_imp_usd'

    ,'mean_current_growth_25_imp_all_usd'
    ,'mean_current_growth_50_imp_all_usd'
    ,'mean_current_growth_75_imp_all_usd'
    ,'mean_current_growth_100_imp_all_usd'
]

ahle_combo_withagg_smry = ahle_combo_withagg[keepcols].copy()
datainfo(ahle_combo_withagg_smry)

ahle_combo_withagg_smry.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_summary.csv') ,index=False)
# ahle_combo_withagg_smry.to_pickle(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_summary.pkl.gz'))

# Output for Dash
# ahle_combo_withagg_smry.to_csv(os.path.join(DASH_DATA_FOLDER ,'ahle_all_summary.csv') ,index=False)

#%% Calculate AHLE

# =============================================================================
#### Restructure
# =============================================================================
# For AHLE calcs, we want each item in a column
# Need means and standard deviations for later calculations
mean_cols_agg = [i for i in list(ahle_combo_withagg) if 'mean' in i]
sd_cols_agg = [i for i in list(ahle_combo_withagg) if 'stdev' in i]

# Only need the system total for each item: 'Overall' group
_groups_for_summary = (ahle_combo_withagg['group'].str.upper() == 'OVERALL')

# Only need some of the items
keep_items = [
    'gross margin'
    ,'health cost'
    ,'value of total mortality'
    ]
keep_items_upper = [i.upper() for i in keep_items]
_items_for_ahle = (ahle_combo_withagg['item'].str.upper().isin(keep_items_upper))

ahle_combo_withagg_p = ahle_combo_withagg.loc[(_items_for_ahle & _groups_for_summary)].pivot(
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

ahle_combo_withahle.eval(
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
    ahle_dueto_ppr_mortality_mean = mean_ppr_value_of_total_mortality
    ahle_dueto_ppr_healthcost_mean = mean_ppr_health_cost
    ahle_dueto_ppr_productionloss_mean = ahle_dueto_ppr_total_mean - ahle_dueto_ppr_mortality_mean - ahle_dueto_ppr_healthcost_mean

    ahle_dueto_bruc_total_mean = mean_ideal_gross_margin - mean_bruc_gross_margin
    ahle_dueto_bruc_mortality_mean = mean_bruc_value_of_total_mortality
    ahle_dueto_bruc_healthcost_mean = mean_bruc_health_cost
    ahle_dueto_bruc_productionloss_mean = ahle_dueto_bruc_total_mean - ahle_dueto_bruc_mortality_mean - ahle_dueto_bruc_healthcost_mean
    '''
    # AHLE due to Other Disease will depend on which diseases were estimated.
    # E.g. PPR only impacts small ruminants, so should not be part of the calculation for Cattle.
    # Don't calculate this here. Handle it when needed (attribution).
    # '''
    # ahle_dueto_otherdisease_total_mean = ahle_total_mean - ahle_dueto_ppr_total_mean - ahle_dueto_bruc_total_mean
    # ahle_dueto_otherdisease_mortality_mean = ahle_dueto_mortality_mean - ahle_dueto_ppr_mortality_mean - ahle_dueto_bruc_mortality_mean
    # ahle_dueto_otherdisease_healthcost_mean = ahle_dueto_healthcost_mean - ahle_dueto_ppr_healthcost_mean - ahle_dueto_bruc_healthcost_mean
    # ahle_dueto_otherdisease_productionloss_mean = ahle_dueto_otherdisease_total_mean - ahle_dueto_otherdisease_mortality_mean - ahle_dueto_otherdisease_healthcost_mean
    # '''
    # Scenarios applied to specific age/sex groups
    '''
    ahle_when_af_repro_imp25_mean = mean_current_repro_25_imp_gross_margin - mean_current_gross_margin
    ahle_when_af_repro_imp50_mean = mean_current_repro_50_imp_gross_margin - mean_current_gross_margin
    ahle_when_af_repro_imp75_mean = mean_current_repro_75_imp_gross_margin - mean_current_gross_margin
    ahle_when_af_repro_imp100_mean = mean_current_repro_100_imp_gross_margin - mean_current_gross_margin

    ahle_when_af_ideal_mean = mean_ideal_af_gross_margin - mean_current_gross_margin
    ahle_when_am_ideal_mean = mean_ideal_am_gross_margin - mean_current_gross_margin
    ahle_when_jf_ideal_mean = mean_ideal_jf_gross_margin - mean_current_gross_margin
    ahle_when_jm_ideal_mean = mean_ideal_jm_gross_margin - mean_current_gross_margin
    ahle_when_nf_ideal_mean = mean_ideal_nf_gross_margin - mean_current_gross_margin
    ahle_when_nm_ideal_mean = mean_ideal_nm_gross_margin - mean_current_gross_margin

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
    # These apply to poultry
    '''
    ahle_when_a_ideal_mean = mean_ideal_a_gross_margin - mean_current_gross_margin
    ahle_when_j_ideal_mean = mean_ideal_j_gross_margin - mean_current_gross_margin
    ahle_when_n_ideal_mean = mean_ideal_n_gross_margin - mean_current_gross_margin

    ahle_when_a_mort_imp100_mean = mean_mortality_zero_a_gross_margin - mean_current_gross_margin
    ahle_when_j_mort_imp100_mean = mean_mortality_zero_j_gross_margin - mean_current_gross_margin
    ahle_when_n_mort_imp100_mean = mean_mortality_zero_n_gross_margin - mean_current_gross_margin
    '''
    ,inplace=True
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

# Other disease
# ahle_combo_withahle['ahle_dueto_otherdisease_total_stdev'] = np.sqrt(
#     ahle_combo_withahle['ahle_total_stdev']**2 + ahle_combo_withahle['ahle_dueto_ppr_total_stdev']**2 + ahle_combo_withahle['ahle_dueto_bruc_total_stdev']**2
#     )
# ahle_combo_withahle['ahle_dueto_otherdisease_mortality_stdev'] = np.sqrt(
#     ahle_combo_withahle['ahle_dueto_mortality_stdev']**2 + ahle_combo_withahle['ahle_dueto_ppr_mortality_stdev']**2 + ahle_combo_withahle['ahle_dueto_bruc_mortality_stdev']**2
#     )
# ahle_combo_withahle['ahle_dueto_otherdisease_healthcost_stdev'] = np.sqrt(
#     ahle_combo_withahle['ahle_dueto_healthcost_stdev']**2 + ahle_combo_withahle['ahle_dueto_ppr_healthcost_stdev']**2 + ahle_combo_withahle['ahle_dueto_bruc_healthcost_stdev']**2
#     )
# ahle_combo_withahle['ahle_dueto_otherdisease_productionloss_stdev'] = np.sqrt(
#     ahle_combo_withahle['ahle_dueto_otherdisease_total_stdev']**2 + ahle_combo_withahle['ahle_dueto_otherdisease_mortality_stdev']**2 + ahle_combo_withahle['ahle_dueto_otherdisease_healthcost_stdev']**2
#     )

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
# ahle_combo_withahle.to_pickle(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_intermediate_calcs.pkl.gz'))

# Keep only key columns and AHLE calcs
_ahle_cols = [i for i in list(ahle_combo_withahle) if 'ahle' in i]
_cols_for_summary = ['region' ,'species' ,'production_system' ,'group' ,'year'] + _ahle_cols

ahle_combo_withahle_smry = ahle_combo_withahle[_cols_for_summary].reset_index(drop=True)
datainfo(ahle_combo_withahle_smry)

ahle_combo_withahle_smry.to_csv(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_summary2.csv') ,index=False)
# ahle_combo_withahle_smry.to_pickle(os.path.join(ETHIOPIA_OUTPUT_FOLDER ,'ahle_all_summary2.pkl.gz'))

# Output for Dash
ahle_combo_withahle_smry.to_csv(os.path.join(DASH_DATA_FOLDER ,'ahle_all_summary2.csv') ,index=False)

#%% Checks on calculated AHLE

check_ahle_combo_withahle = ahle_combo_withahle.copy()

# =============================================================================
#### Sum of agesex AHLE compared to overall AHLE
# =============================================================================
check_ahle_combo_withahle.eval(
    '''
    sum_ahle_individual = ahle_when_af_ideal_mean + ahle_when_am_ideal_mean \
        + ahle_when_jf_ideal_mean + ahle_when_jm_ideal_mean \
        + ahle_when_nf_ideal_mean + ahle_when_nm_ideal_mean

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

#%% Create scenario summary table
'''
This produces a summary data frame with a different structure than before.
This uses only the total system value for each item. It then creates a row
for each scenario. Scenarios set either specific age/sex groups to ideal
conditions or all age/sex groups simultaneously.

For example, the ideal_AF scenario sets adult females to ideal conditions while
leaving other age/sex groups at their current conditions; the resulting values are
interpreted as the total system values of gross margin, health cost, etc., when
adult females are at their ideal.

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

#%% Add calcs to scenario summary table

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

#%% Summarize AHLE

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
#### Process columns
# =============================================================================
'''
Most of the AHLE components are item-level (gross margin) differences between
scenarios that have already been calculated.
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
# AHLE due to Other Diseases
# -----------------------------------------------------------------------------
# Will depend on which diseases were estimated for each species
# Fill missing values of disease-specific AHLE with zero for species where they do not apply

# PPR only impacts small ruminants
ahle_dueto_ppr_cols = [i for i in list(ahle_combo_scensmry_diffs_p) if 'ahle_dueto_ppr' in i]
_ppr_applies = (ahle_combo_scensmry_diffs_p['species'].str.upper().isin(['SHEEP' ,'GOAT' ,'ALL SMALL RUMINANTS']))
for COL in ahle_dueto_ppr_cols:
    ahle_combo_scensmry_diffs_p.loc[~ _ppr_applies ,COL] = \
        ahle_combo_scensmry_diffs_p.loc[~ _ppr_applies ,COL].fillna(0)

# Brucellosis only impacts small ruminants and cattle
ahle_dueto_bruc_cols = [i for i in list(ahle_combo_scensmry_diffs_p) if 'ahle_dueto_bruc' in i]
_bruc_applies = (ahle_combo_scensmry_diffs_p['species'].str.upper().isin(['SHEEP' ,'GOAT' ,'ALL SMALL RUMINANTS' ,'CATTLE']))
for COL in ahle_dueto_bruc_cols:
    ahle_combo_scensmry_diffs_p.loc[~ _bruc_applies ,COL] = \
        ahle_combo_scensmry_diffs_p.loc[~ _bruc_applies ,COL].fillna(0)

# Calculate ahle due to other disease
ahle_combo_scensmry_diffs_p.eval(
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
    ,inplace=True
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

#%% Checks on AHLE using scenario summaries

check_ahle_combo_scensmry_diffs_p = ahle_combo_scensmry_diffs_p.copy()

# =============================================================================
#### Change in Gross Margin overall vs. individual ideal scenarios
# =============================================================================
print('\n> Checking the change in Gross Margin for ideal overall vs. individual ideal scenarios')
print(check_ahle_combo_scensmry_diffs_p[['region' ,'species' ,'production_system' ,'year' ,'agesex_scenario' ,'ahle_total_mean']])
