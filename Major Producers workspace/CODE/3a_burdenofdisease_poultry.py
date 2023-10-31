#%% About

#!!! Note: BOD calculation functions used in dashboard are stored in Dash folder /lib/bod_calcs.py

#%% Load data and production BOD calcs

# Main table
gbads_chickens_merged_fordash = pd.read_pickle(os.path.join(DASH_DATA_FOLDER ,'gbads_chickens_merged_fordash.pkl.gz'))

# Breed Standards
poultrybreedstd_ross308 = pd.read_pickle(os.path.join(DASH_DATA_FOLDER ,'poultrybreedstd_ross308.pkl.gz'))
poultrybreedstd_ross708 = pd.read_pickle(os.path.join(DASH_DATA_FOLDER ,'poultrybreedstd_ross708.pkl.gz'))
poultrybreedstd_cobb500 = pd.read_pickle(os.path.join(DASH_DATA_FOLDER ,'poultrybreedstd_cobb500.pkl.gz'))
poultrybreedstd_vencobb400 = pd.read_pickle(os.path.join(DASH_DATA_FOLDER ,'poultrybreedstd_vencobb400.pkl.gz'))

dfdev = gbads_chickens_merged_fordash.copy()

DASH_LIB_FOLDER = os.path.join(PARENT_FOLDER, 'Dashboard' ,'Dev' ,'lib')
exec(open(os.path.join(DASH_LIB_FOLDER ,'bod_calcs.py')).read())

#%% DEV Burden of disease calcs

# =============================================================================
#### Wrapper function
# =============================================================================
def calc_bod_master(
      INPUT_DF
      ,ACHIEVABLE_PRPN_MASTER=98       # Integer [0, 100]: proportion of ideal production that is achievable without disease, i.e. efficiency of feed, medications, and practices
      ,BREED_MASTER='COBB500'          # String: Breed to use for estimating ideal production. One of 'COBB500' ,'ROSS308' ,'ROSS708'. Needs work before 'VENCOBB' can be used.
      ,AVG_DOF_MASTER=35               # Integer (0, 60]: Average days on feed. Will lookup breed standard weight for this day on feed.
      ,AVG_CARC_YIELD_MASTER=0.695     # Float [0, 1]: average carcass yield in kg meat per kg live weight
      ):
   # Create copy of input data frame
   OUTPUT_DF = INPUT_DF.copy()

   # Apply BOD calculations. Each one adds a column to the data frame.
   #!!! Ordering matters as some rely on variables created by others!!!
   OUTPUT_DF['bod_idealproduction_tonnes'] = OUTPUT_DF.apply(
      calc_bod_idealproduction_tonnes ,axis=1
      ,BREED=BREED_MASTER
      ,AVG_DOF=AVG_DOF_MASTER
      ,AVG_CARC_YIELD=AVG_CARC_YIELD_MASTER
   )
   OUTPUT_DF['bod_subopt_tonnes'] = OUTPUT_DF.apply(
      calc_bod_subopt_tonnes ,axis=1
      ,ACHIEVABLE_PRPN=ACHIEVABLE_PRPN_MASTER
   )
   OUTPUT_DF['bod_gmax_tonnes'] = OUTPUT_DF.apply(
      calc_bod_gmax_tonnes ,axis=1
   )
   OUTPUT_DF['bod_realizedproduction_tonnes'] = OUTPUT_DF.apply(
      calc_bod_realizedproduction_tonnes ,axis=1
   )
   OUTPUT_DF['bod_deathloss_tonnes'] = OUTPUT_DF.apply(
      calc_bod_deathloss_tonnes ,axis=1
   )
   OUTPUT_DF['bod_totalburden_tonnes'] = OUTPUT_DF.apply(
      calc_bod_totalburden_tonnes ,axis=1
   )
   OUTPUT_DF['bod_reducedgrowth_tonnes'] = OUTPUT_DF.apply(
      calc_bod_reducedgrowth_tonnes ,axis=1
   )

   return OUTPUT_DF

# =============================================================================
#### Functions for calculating each component
# =============================================================================
def calc_bod_idealproduction_tonnes(
      INPUT_ROW
      ,BREED                  # Breed to use. One of 'COBB500' ,'ROSS308' ,'ROSS708'. Needs work before 'VENCOBB' can be used.
      ,AVG_DOF                # Integer (0, 60]: Average days on feed. Will lookup breed standard weight for this day on feed.
      ,AVG_CARC_YIELD         # Float [0, 1]: average carcass yield in kg meat per kg live weight
      ):
   # (Birds placed) x (Breed Std. Live Weight @ Country Avg. Days on Feed) x (Carcass Yield)
   # Look up breed standard weight at AVG_DOF
   # Allowing user to specify breed
   # Could limit breed choice based on country
      # India: Vencobb400
      # Others: Cobb500, Ross308, or Ross708
   funcname = inspect.currentframe().f_code.co_name

   if BREED == 'COBB500':
      breed_lookup = poultrybreedstd_cobb500
   elif BREED == 'ROSS308':
      breed_lookup = poultrybreedstd_ross308
   elif BREED == 'ROSS708':
      breed_lookup = poultrybreedstd_ross708
   elif BREED == 'VENCOBB':
      #!!! Vencobb standard is weekly, not daily, and only goes up to day 42.
      breed_lookup = poultrybreedstd_vencobb400
   else:
      print(f"<{funcname}> Invalid breed specified. Using default COBB500.")
      breed_lookup = poultrybreedstd_cobb500

   _select_dof = (breed_lookup['dayonfeed'] == AVG_DOF)
   breedstdwt_kg = breed_lookup.loc[_select_dof ,'bodyweight_g'] / 1000

   OUTPUT = INPUT_ROW['acc_headplaced'] * breedstdwt_kg * AVG_CARC_YIELD / 1000
   return OUTPUT

def calc_bod_subopt_tonnes(
      INPUT_ROW
      ,ACHIEVABLE_PRPN   # Integer [0, 100]: proportion of ideal production that is achievable without disease, i.e. efficiency of feed, medications, and practices
      ):
   OUTPUT = INPUT_ROW['bod_idealproduction_tonnes'] * (1 - (ACHIEVABLE_PRPN/100))
   return OUTPUT

def calc_bod_gmax_tonnes(INPUT_ROW):
    OUTPUT = INPUT_ROW['bod_idealproduction_tonnes'] - INPUT_ROW['bod_subopt_tonnes']
    return OUTPUT

def calc_bod_realizedproduction_tonnes(INPUT_ROW):
   OUTPUT = INPUT_ROW['acc_totalcarcweight_tonnes']
   return OUTPUT

def calc_bod_deathloss_tonnes(INPUT_ROW):
   # Pooling mortality and condemns
   #!!! (Head Placed) minus (Head Slaughtered), assuming those have been adjusted for import/export of live birds
   # Convert to weight by multiplying by avg. carcass weight of those that lived
   OUTPUT = (INPUT_ROW['acc_headplaced'] - INPUT_ROW['acc_headslaughtered']) * INPUT_ROW['acc_avgcarcweight_kg'] / 1000
   return OUTPUT

def calc_bod_totalburden_tonnes(INPUT_ROW):
   OUTPUT = INPUT_ROW['bod_gmax_tonnes'] - INPUT_ROW['bod_realizedproduction_tonnes']
   return OUTPUT

def calc_bod_reducedgrowth_tonnes(INPUT_ROW):
   OUTPUT = INPUT_ROW['bod_totalburden_tonnes'] - INPUT_ROW['bod_deathloss_tonnes']
   return OUTPUT

#!!! Achievable proportion is constrained by data
# Total Burden must be >= Death loss
# Algebra gives Achievable Prpn >= 1 - ((ideal - deathloss - realized)/ideal)
def calc_min_achievable_prpn(INPUT_ROW):
   OUTPUT = 1 - ((INPUT_ROW['bod_idealproduction_tonnes'] - INPUT_ROW['bod_deathloss_tonnes'] - INPUT_ROW['bod_realizedproduction_tonnes']) / INPUT_ROW['bod_idealproduction_tonnes'])
   return

# =============================================================================
#### Call functions
# =============================================================================
gbads_chickens_bod = calc_bod_master(
   gbads_chickens_merged_fordash
   ,ACHIEVABLE_PRPN_MASTER=92       # Integer [0, 100]: proportion of ideal production that is achievable without disease, i.e. efficiency of feed, medications, and practices
   ,BREED_MASTER='COBB500'          # String: Breed to use for estimating ideal production. One of 'COBB500' ,'ROSS308' ,'ROSS708'. Needs work before 'VENCOBB' can be used.
   ,AVG_DOF_MASTER=35               # Integer (0, 60]: Average days on feed. Will lookup breed standard weight for this day on feed.
   ,AVG_CARC_YIELD_MASTER=0.695     # Float [0, 1]: average carcass yield in kg meat per kg live weight
)
datainfo(gbads_chickens_bod)

# Export to CSV
gbads_chickens_bod.to_csv(os.path.join(EXPDATA_FOLDER ,'gbads_chickens_bod.csv') ,index=False)

#%% Prep data for plots DEV

# Same for PowerBI and Dash
# Basic structure the same for both Sankey and Waterfall chart
# Waterfall uses fewer columns and rows
# Sankey needs
   # 3 columns: Source, Amount, Destination
   # 1 row for each node (intermediate or end)
# Waterfall needs
   # 2 columns: Amount, Destination
   # 1 row for each end node (no intermediate nodes)
# Each country and year will become multiple rows: 1 for each node

# -----------------------------------------------------------------------------
# Approach 1: loop through countries and years, manually building data
# Most flexible. Can build structure for multiple sources feeding a single
# destination.
# -----------------------------------------------------------------------------
# bod_plotdata_aslist = []
# for COUNTRY in gbads_chickens_bod['country'].unique():
#     _country = (gbads_chickens_bod['country'] == COUNTRY)
#     for YEAR in gbads_chickens_bod.loc[_country ,'year'].unique():
#       _country_year = (gbads_chickens_bod['country'] == COUNTRY) & (gbads_chickens_bod['year'] == YEAR)
#       row = [{                   # Create row as a dictionary inside a single-element list
#           "country":COUNTRY
#           ,"year":YEAR
#           ,"sankey_source":"bod_idealproduction_tonnes"
#           ,"value":gbads_chickens_bod.loc[_country_year ,'bod_gmax_tonnes'].values[0]
#           ,"bod_component":"bod_gmax_tonnes"
#       }]
#       bod_plotdata_aslist.extend(row)               # Add row to aggregate list
# dev_bod_plotsankey = pd.DataFrame.from_dict(bod_plotdata_aslist ,orient='columns')    # Convert list of dictionaries into data frame
# del row ,bod_plotdata_aslist

# -----------------------------------------------------------------------------
# Approach 2: Pivot. Each BOD component column becomes a row.
# This works if each BOD component defines a destination in the Sankey, and
# each destination has only one source.
# -----------------------------------------------------------------------------
# cols_bod = [i for i in list(gbads_chickens_bod) if 'bod_' in i]
# gbads_chickens_bod_plotdata = gbads_chickens_bod.melt(
#    id_vars=['country' ,'year']
#    ,value_vars=cols_bod          # Columns to "unpivot" to rows. Use all columns related to burden of disease.
#    ,var_name='bod_component'
#    ,value_name='value'
# )

# For Sankey, treat the pivoted data as Destinations. Add Source for each.
# sankey_source_lookup = {
#    "bod_idealproduction_tonnes":""
#    ,"bod_gmax_tonnes":"bod_idealproduction_tonnes"
#    ,"bod_subopt_tonnes":"bod_idealproduction_tonnes"
#    ,"bod_realizedproduction_tonnes":"bod_gmax_tonnes"
#    ,"bod_totalburden_tonnes":"bod_gmax_tonnes"
#    ,"bod_deathloss_tonnes":"bod_totalburden_tonnes"
#    ,"bod_reducedgrowth_tonnes":"bod_totalburden_tonnes"
# }
# gbads_chickens_bod_plotdata['sankey_source'] = gbads_chickens_bod_plotdata['bod_component'].replace(sankey_source_lookup)

# datainfo(gbads_chickens_bod_plotdata)

# =============================================================================
# Make Pretty and Output
# =============================================================================
# BOD Component Names
# rename_bod_components = {
#    "bod_idealproduction_tonnes":"Ideal Production"
#    ,"bod_gmax_tonnes":"Achievable Without Disease (gmax)"
#    ,"bod_subopt_tonnes":"Suboptimal Feed & Practices"
#    ,"bod_realizedproduction_tonnes":"Realized Production"
#    ,"bod_totalburden_tonnes":"Burden of Disease"
#    ,"bod_deathloss_tonnes":"Mortality and Condemns"
#    ,"bod_reducedgrowth_tonnes":"Reduced Growth"
# }
# gbads_chickens_bod_plotdata['bod_component'] = gbads_chickens_bod_plotdata['bod_component'].replace(rename_bod_components)
# gbads_chickens_bod_plotdata['sankey_source'] = gbads_chickens_bod_plotdata['sankey_source'].replace(rename_bod_components)

# Column Names
# rename_cols = {
#    "country":"Country"
#    ,"year":"Year"
#    ,"bod_component":"Component"
#    ,"value":"Tonnes"
#    ,"sankey_source":"Component Source"
# }
# gbads_chickens_bod_plotdata = gbads_chickens_bod_plotdata.rename(columns=rename_cols)

# datainfo(gbads_chickens_bod_plotdata)

# gbads_chickens_bod_plotdata.to_csv(os.path.join(EXPDATA_FOLDER ,'gbads_chickens_bod_plotdata.csv') ,index=False)

#%% Prep data for plots

# =============================================================================
#### Define functions
# =============================================================================
# Create labels for BOD components
pretty_bod_component_names = {
   "bod_idealproduction_tonnes":"Breed Standard Potential"
   ,"bod_gmax_tonnes":"Achievable Without Disease (gmax)"
   ,"bod_subopt_tonnes":"Effect of Feed and Practices"
   ,"bod_realizedproduction_tonnes":"Realised Production"
   ,"bod_totalburden_tonnes":"Burden of Disease"
   ,"bod_deathloss_tonnes":"Mortality and Condemns"
   ,"bod_reducedgrowth_tonnes":"Morbidity"
}

def prep_bod_forsankey(
      INPUT_DF
      ,BOD_COMP_NAMES=pretty_bod_component_names   # Dictionary: defining names for each component of burden of disease that will appear in plots
      ):
   OUTPUT_DF = INPUT_DF.copy()

   # If achievable proportion is too low, reduced growth (morbidity) will be negative for some countries and years
   # However, morbidity cannot be < 0
   # For these, set reduced growth (morbidity) = 0 and add reduced growth to effect of feed
   rows_with_negative_morbidity = (OUTPUT_DF['bod_reducedgrowth_tonnes'] < 0)
   OUTPUT_DF.loc[rows_with_negative_morbidity] = OUTPUT_DF.loc[rows_with_negative_morbidity].eval(
       '''
       bod_subopt_tonnes = bod_subopt_tonnes + bod_reducedgrowth_tonnes
       bod_reducedgrowth_tonnes = 0
       '''
   )

   # Burden of Disease is at least equal to death loss
   # If total burden < death loss, set equal to death loss
   rows_with_burden_lt_deathloss = (OUTPUT_DF['bod_totalburden_tonnes'] < OUTPUT_DF['bod_deathloss_tonnes'])
   OUTPUT_DF.loc[rows_with_burden_lt_deathloss] = OUTPUT_DF.loc[rows_with_burden_lt_deathloss].eval(
      '''
      bod_totalburden_tonnes = bod_deathloss_tonnes
      '''
   )

   # Pivot each BOD component column into a row
   cols_tomelt = [
      # 'bod_idealproduction_tonnes'      # Starting node is implicit. See sankey_source_lookup.
      'bod_gmax_tonnes'
      ,'bod_subopt_tonnes'
      ,'bod_realizedproduction_tonnes'
      ,'bod_totalburden_tonnes'
      ,'bod_deathloss_tonnes'
      ,'bod_reducedgrowth_tonnes'
   ]
   OUTPUT_DF = OUTPUT_DF.melt(
      id_vars=['country' ,'year']
      ,value_vars=cols_tomelt
      ,var_name='bod_component'
      ,value_name='value'
   )

   # Sankey requires a Source for each Destination
   sankey_source_lookup = {
      "bod_idealproduction_tonnes":""
      ,"bod_gmax_tonnes":"bod_idealproduction_tonnes"
      ,"bod_subopt_tonnes":"bod_idealproduction_tonnes"
      ,"bod_realizedproduction_tonnes":"bod_gmax_tonnes"
      ,"bod_totalburden_tonnes":"bod_gmax_tonnes"
      ,"bod_deathloss_tonnes":"bod_totalburden_tonnes"
      ,"bod_reducedgrowth_tonnes":"bod_totalburden_tonnes"
   }
   OUTPUT_DF['sankey_source'] = OUTPUT_DF['bod_component'].replace(sankey_source_lookup)

   # If subopt is negative, it becomes a Source for Gmax
   rows_with_negative_subopt = ((OUTPUT_DF['bod_component'] == 'bod_subopt_tonnes') & (OUTPUT_DF['value'] < 0))
   OUTPUT_DF.loc[rows_with_negative_subopt ,'bod_component'] = 'bod_gmax_tonnes'
   OUTPUT_DF.loc[rows_with_negative_subopt ,'sankey_source'] = 'bod_subopt_tonnes'
   OUTPUT_DF.loc[rows_with_negative_subopt ,'value'] = -1 * OUTPUT_DF['value']

   # Give BOD components pretty names
   OUTPUT_DF['bod_component'] = OUTPUT_DF['bod_component'].replace(BOD_COMP_NAMES)
   OUTPUT_DF['sankey_source'] = OUTPUT_DF['sankey_source'].replace(BOD_COMP_NAMES)

   # Define pretty column names
   rename_cols = {
      "country":"Country"
      ,"year":"Year"
      ,"bod_component":"Component"
      ,"value":"Tonnes"
      ,"sankey_source":"Component Source"
   }
   OUTPUT_DF = OUTPUT_DF.rename(columns=rename_cols)

   return OUTPUT_DF

# Waterfall chart uses same structure as Sankey but fewer rows.
# It does not display interior nodes of Sankey, only end nodes.
def prep_bod_forwaterfall(
      INPUT_DF
      ,BOD_COMP_NAMES=pretty_bod_component_names   # Dictionary: defining names for each component of burden of disease that will appear in plots
      ):
   OUTPUT_DF = INPUT_DF.copy()

   # If user selected an achievable proportion too low, reduced growth (morbidity) will be negative for some countries and years
   # However, morbidity cannot be < 0
   # For these, set reduced growth (morbidity) = 0 and add reduced growth to effect of feed
   rows_with_negative_morbidity = (OUTPUT_DF['bod_reducedgrowth_tonnes'] < 0)
   OUTPUT_DF.loc[rows_with_negative_morbidity] = OUTPUT_DF.loc[rows_with_negative_morbidity].eval(
       '''
       bod_subopt_tonnes = bod_subopt_tonnes + bod_reducedgrowth_tonnes
       bod_reducedgrowth_tonnes = 0
       '''
   )

   # Components that detract from potential must be negative
   OUTPUT_DF['bod_subopt_tonnes'] = OUTPUT_DF['bod_subopt_tonnes'] * -1
   OUTPUT_DF['bod_reducedgrowth_tonnes'] = OUTPUT_DF['bod_reducedgrowth_tonnes'] * -1
   OUTPUT_DF['bod_deathloss_tonnes'] = OUTPUT_DF['bod_deathloss_tonnes'] * -1

   # Melt BOD component columns into rows
   #!!! Ordering here determines order in plot!!!
   cols_tomelt = [
      'bod_idealproduction_tonnes'
      ,'bod_subopt_tonnes'
      ,'bod_reducedgrowth_tonnes'
      ,'bod_deathloss_tonnes'
      ,'bod_realizedproduction_tonnes'
   ]
   OUTPUT_DF = OUTPUT_DF.melt(
      id_vars=['country' ,'year']
      ,value_vars=cols_tomelt
      ,var_name='bod_component'
      ,value_name='value'
   )
   # Give BOD components pretty names
   OUTPUT_DF['bod_component'] = OUTPUT_DF['bod_component'].replace(BOD_COMP_NAMES)

   # Define pretty column names
   rename_cols = {
      "country":"Country"
      ,"year":"Year"
      ,"bod_component":"Component"
      ,"value":"Tonnes"
   }
   OUTPUT_DF = OUTPUT_DF.rename(columns=rename_cols)

   return OUTPUT_DF

# =============================================================================
#### Call functions
# =============================================================================
gbads_chickens_bod_plotsankey = prep_bod_forsankey(gbads_chickens_bod)
gbads_chickens_bod_plotwaterfall = prep_bod_forwaterfall(gbads_chickens_bod)

# Export to CSV
gbads_chickens_bod_plotsankey.to_csv(os.path.join(EXPDATA_FOLDER ,'gbads_chickens_bod_plotsankey.csv') ,index=False)
gbads_chickens_bod_plotwaterfall.to_csv(os.path.join(EXPDATA_FOLDER ,'gbads_chickens_bod_plotwaterfall.csv') ,index=False)

#%% Prepare to show user

# Create a pared down version of bod data to make viewable to the user in dashboard
collist_forviewers = [
   'country'
   ,'year'
   ,'acc_headplaced'
   ,'acc_avgdaysonfeed'
   ,'acc_headslaughtered'
   ,'acc_totalcarcweight_tonnes'
   ,'acc_avgcarcweight_kg'
   ,'acc_avgliveweight_kg'
   ,'bod_idealproduction_tonnes'
   ,'bod_subopt_tonnes'
   ,'bod_gmax_tonnes'
   ,'bod_realizedproduction_tonnes'
   ,'bod_totalburden_tonnes'
   ,'bod_deathloss_tonnes'
   ,'bod_reducedgrowth_tonnes'
]
gbads_chickens_bod_toview = gbads_chickens_bod.loc[
   (gbads_chickens_bod['country'] == 'United Kingdom') & (gbads_chickens_bod['year'] == 2015)
   ,collist_forviewers
]

#%% DEV Burden of disease in increased costs

dfdev_withbod = calc_bod_master_poultry(
   dfdev
   ,ACHIEVABLE_PCT_MASTER=100        # Integer [0, 120]: proportion of ideal production that is achievable without disease, i.e. efficiency of feed, medications, and practices
   ,BREED_DF_MASTER=poultrybreedstd_cobb500              # Data frame with breed reference information. Must contain columns 'dayonfeed' and 'bodyweight_g'.
   ,AVG_DOF_MASTER=40               # Integer (0, 63]: Average days on feed. Will lookup breed standard weight for this day on feed.
   ,AVG_CARC_YIELD_MASTER=None   # Float [0, 1]: average carcass yield as proportion of live weight. If blank, will use column 'bod_breedstdyield_prpn'.
)
