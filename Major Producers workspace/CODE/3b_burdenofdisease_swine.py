#%% About

#!!! Note: BOD calculation functions used in dashboard are stored in Dash folder /lib/bod_calcs.py

#%% Load data and production BOD calcs

# Main table
gbads_pigs_merged_fordash = pd.read_pickle(os.path.join(DASH_DATA_FOLDER ,'gbads_pigs_merged_fordash.pkl.gz'))

# Breed Standards
swinebreedstd_pic_growthandfeed = pd.read_pickle(os.path.join(DASH_DATA_FOLDER ,'swinebreedstd_pic_growthandfeed.pkl.gz'))
swinebreedstd_liverpool_model = pd.read_pickle(os.path.join(DASH_DATA_FOLDER ,'swinebreedstd_liverpool_model.pkl.gz'))
swinebreedstd_liverpool_model2 = pd.read_pickle(os.path.join(DASH_DATA_FOLDER ,'swinebreedstd_liverpool_model2.pkl.gz'))

dfdev = gbads_pigs_merged_fordash.copy()

DASH_LIB_FOLDER = os.path.join(PARENT_FOLDER, 'Dashboard' ,'Dev' ,'lib')
exec(open(os.path.join(DASH_LIB_FOLDER ,'bod_calcs.py')).read())

#%% DEV Breed standard growth based on feed intake

# ----------------------------------------------------------------------------
# Lookup breed weight corresponding to actual average feed intake
# ----------------------------------------------------------------------------
# Plot
snplt = sns.relplot(
   data=swinebreedstd_pic_growthandfeed
   ,x='cml_feedintake_kg'
   ,y='bodyweight_kg'
   ,kind='line'
   ,color='blue'        # Fixed color rather than color by variable
   ,marker='o'          # Add markers
   ,aspect=1.5          # Width of figure as multiplier on height. If paneled, width of each panel.
)

# Create interpolator
interp_weight_from_feed = sp.interpolate.interp1d(
   swinebreedstd_pic_growthandfeed['cml_feedintake_kg']
   ,swinebreedstd_pic_growthandfeed['bodyweight_kg']
)
interp_feed_from_weight = sp.interpolate.interp1d(
   swinebreedstd_pic_growthandfeed['bodyweight_kg']
   ,swinebreedstd_pic_growthandfeed['cml_feedintake_kg']
)

def calc_bod_breedstdwt_fromfeed_kg(
      INPUT_ROW
      ,BREED_DF       # Data frame with breed reference information. Must contain columns 'dayonfeed', 'bodyweight_kg', and 'cml_feedintake_kg'.
      ):
   # Create interpolator
   interp_weight_from_feed = sp.interpolate.interp1d(
      BREED_DF['cml_feedintake_kg']
      ,BREED_DF['bodyweight_kg']
   )
   # Apply interpolator
   OUTPUT = interp_weight_from_feed(INPUT_ROW['acc_avgfeedintake_kgperhd']) * 1   # Multiply by one: trick to convert array to number
   return OUTPUT

dfdev['bod_breedstdwt_kg'] = dfdev.apply(calc_bod_breedstdwt_fromfeed_kg ,'swinebreedstd_pic_growthandfeed' ,axis=1)

#%% DEV BOD for given animal weight
# Burden measured in actual vs. optimal inputs (translated to $ cost)
# First: feed consumption
# Then: chick cost, vet & med cost, labor, land & facilities

def calc_bod_infeed_swine(
      INPUT_DF
      ,BREED_DF_MASTER           # Data frame with breed reference information. Must contain columns 'dayonfeed', 'bodyweight_g', and 'cml_feedintake_kg'.
      ,AVG_CARC_YIELD_MASTER     # Float [0, 1]: average carcass yield in kg meat per kg live weight

      #!!! Instead of slider, could use actual avg. carcass weight achieved
      ,TARGET_CARCASSWT_KG_MASTER       # Float: target carcass weight per animal
      ):
   funcname = inspect.currentframe().f_code.co_name

   OUTPUT_DF = INPUT_DF.copy()     # Create copy of input data frame

   # Apply BOD calculations. Each one adds a column to the data frame.

   # 'bod_breedstdfeedintake_kg'    # Breed standard feed intake to reach TARGET_CARCASSWT_KG @ AVG_CARC_YIELD
   # 'bod_referencefeeduse_tonnes'  # Total feed use for bod_breedstdfeedintake_kg @ head slaughtered (maybe adjusted for mortality)

   return OUTPUT_DF

# -----------------------------------------------------------------------------
# For feed, chicks/piglets, and maybe land area, calculate amounts first and then multiply by price
# -----------------------------------------------------------------------------
acc_feedconsumption_tonnes
opt_feedconsumption_tonnes
# Multiply by ration price

acc_headplaced
opt_headplaced  # Equal to head slaughtered (optimal means no mortality)
# Multiply by cost of chicks/weaned pigs

acc_landarea_sqmeters
opt_landarea_sqmeters  # For poultry, use William's function to calc reduction in necessary area for fewer birds
# Multiply by cost of land

# -----------------------------------------------------------------------------
# For other inputs, we might not have amounts, only costs
# -----------------------------------------------------------------------------
acc_vetmed_cost
opt_vetmed_cost = 0

acc_labor_cost
opt_labor_cost  # Set based on reduction in landarea. Any separate impact of disease?

#%% DEV Burden of disease in increased costs

dfdev_withbod = calc_bod_master_swine(
      dfdev
      ,ACHIEVABLE_WT_KG_MASTER=120             # Float: achievable weight without disease
      ,AVG_DOF_MASTER=147                            # Integer [1, 176]: Average days on feed. Will lookup breed standard weight for this day on feed.
      ,BREED_DF_MASTER=swinebreedstd_pic_growthandfeed   # Data frame with breed reference information. Must contain columns 'dayonfeed' and 'bodyweight_g'.
      ,AVG_CARC_YIELD_MASTER=0.75                        # Float [0, 1]: average carcass yield in kg meat per kg live weight
)

# def calc_cost_feed_swine():
#       INPUT_ROW
#       ,FEEDPRICE_USDPERTONNE
#       ):
#    # actual_feedcost_usd = INPUT_ROW['acc_feedconsumption_tonnes'] * INPUT_ROW['acc_feedprice_usdpertonne']
#    actual_feedcost_usd = INPUT_ROW['acc_feedconsumption_tonnes'] * FEEDPRICE_USDPERTONNE
#    actual_feedcost_usdperkg = actual_feedcost_usd / acc_totalcarcweight_tonnes * 1000
#    return OUTPUT

# Zero mortality and zero morbidity means all head placed would survive to slaughter and
# achieve breed standard weight PLUS effect of feed and practices for given country.
# Calculate number of head needed under these conditions to get same total production.
# TRICK: Gmax already includes logic for breed standard weight and effect of feed and
# practices to tell us what could be achieved by head placed. So find head placed that
# makes gmax = realized production.
def calc_ideal_headplaced(INPUT_ROW):
   # Calculate actual production as proporion of gmax
   realized_prpn_gmax = INPUT_ROW['bod_realizedproduction_tonnes'] / INPUT_ROW['bod_gmax_tonnes']

   # Reduce head placed by this proportion
   ideal_headplaced = INPUT_ROW['acc_headplaced'] * realized_prpn_gmax

   OUTPUT = ideal_headplaced

   return OUTPUT
dfdev_withbod['ideal_headplaced'] = dfdev_withbod.apply(calc_ideal_headplaced ,axis=1)

# Calculate feed required to reach same production under ideal FCR
# Note: because FCR is feed per weight, we can calculate this directly without using head placed
def calc_ideal_feedcost_usdperkgcarc(
      INPUT_ROW
      ,FEEDPRICE_USDPERTONNE=350
      ,IDEAL_FCR_LIVE=2.1   # Ideal FCR per kg live weight
      ):
   # Back-calculate live weight from production and carcass yield
   required_live_weight_tonnes = INPUT_ROW['bod_realizedproduction_tonnes'] / INPUT_ROW['bod_breedstdyield_prpn']

   # Calculate ideal feed required
   ideal_feed_tonnes = required_live_weight_tonnes * IDEAL_FCR_LIVE

   # Calculate feed cost
   ideal_feedcost_usdperkgcarc = (ideal_feed_tonnes * INPUT_ROW['acc_feedprice_usdpertonne']) / (INPUT_ROW['bod_realizedproduction_tonnes'] * 1000)
   ideal_feedcost_whatif_usdperkgcarc = (ideal_feed_tonnes * FEEDPRICE_USDPERTONNE) / (INPUT_ROW['bod_realizedproduction_tonnes'] * 1000)

   OUTPUT = pd.Series([ideal_feedcost_usdperkgcarc ,ideal_feedcost_whatif_usdperkgcarc])
   return OUTPUT
dfdev_withbod[['ideal_feedcost_usdperkgcarc' ,'ideal_feedcost_whatif_usdperkgcarc']] = \
   dfdev_withbod.apply(calc_ideal_feedcost_usdperkgcarc ,axis=1)

def calc_ideal_nonfeedvariablecost_usdperkgcarc(INPUT_ROW):
   # Reduce non-feed variable costs in proportion to head placed
   ideal_headplaced_prpn = INPUT_ROW['ideal_headplaced'] / INPUT_ROW['acc_headplaced']
   ideal_nonfeedvariablecost_usdperkgcarc = INPUT_ROW['acc_nonfeedvariablecost_usdperkgcarc'] * ideal_headplaced_prpn
   OUTPUT = ideal_nonfeedvariablecost_usdperkgcarc
   return OUTPUT
dfdev_withbod['ideal_nonfeedvariablecost_usdperkgcarc'] = dfdev_withbod.apply(calc_ideal_nonfeedvariablecost_usdperkgcarc ,axis=1)

def calc_ideal_financecost_usdperkgcarc(INPUT_ROW):
   # Reduce finance (land & facilities) costs in proportion to head placed
   ideal_headplaced_prpn = INPUT_ROW['ideal_headplaced'] / INPUT_ROW['acc_headplaced']
   ideal_financecost_usdperkgcarc = INPUT_ROW['acc_financecost_usdperkgcarc'] * ideal_headplaced_prpn
   OUTPUT = ideal_financecost_usdperkgcarc
   return OUTPUT
dfdev_withbod['ideal_financecost_usdperkgcarc'] = dfdev_withbod.apply(calc_ideal_financecost_usdperkgcarc ,axis=1)

def calc_ideal_laborcost_usdperkgcarc(INPUT_ROW):
   # Reduce labor costs in proportion to land & facilities costs
   ideal_financecost_prpn = INPUT_ROW['ideal_financecost_usdperkgcarc'] / INPUT_ROW['acc_financecost_usdperkgcarc']
   ideal_laborcost_usdperkgcarc = INPUT_ROW['acc_laborcost_usdperkgcarc'] * ideal_financecost_prpn
   OUTPUT = ideal_laborcost_usdperkgcarc
   return OUTPUT
dfdev_withbod['ideal_laborcost_usdperkgcarc'] = dfdev_withbod.apply(calc_ideal_laborcost_usdperkgcarc ,axis=1)
