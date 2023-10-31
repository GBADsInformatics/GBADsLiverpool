# USDA Quickstats
# https://quickstats.nass.usda.gov/

#%% Broiler placements and production

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
usda_broilers = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'USDA_broilers_2011_2021.csv'))
cleancolnames(usda_broilers)

data_items = usda_broilers[['commodity' ,'data_item' ,'domain' ,'domain_category']].value_counts()

datainfo(usda_broilers)

# ----------------------------------------------------------------------------
# Pivot each data item into a column
# ----------------------------------------------------------------------------
usda_broilers_p = usda_broilers.pivot(
   index='year'           # Column(s) to make new index. If blank, uses existing index.
   ,columns='data_item'        # Column(s) to make new columns
   ,values='value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
   ).reset_index()
datainfo(usda_broilers_p)

# Rename
rename_cols = {
   'CHICKENS, BROILERS - PLACEMENTS, MEASURED IN HEAD':'chicksplaced_broilers_hd'
   ,'CHICKENS, BROILERS - PRICE RECEIVED, MEASURED IN $ / LB':'pricereceived_broilers_dollarsperlb'
   ,'CHICKENS, BROILERS - PRODUCTION, MEASURED IN $':'production_broilers_dollars'
   ,'CHICKENS, BROILERS - PRODUCTION, MEASURED IN HEAD':'production_broilers_hd'
   ,'CHICKENS, BROILERS - PRODUCTION, MEASURED IN LB':'production_broilers_lbs'
   ,'CHICKENS, BROILERS - SALES, MEASURED IN HEAD':'sales_broilers_hd'
   }
usda_broilers_p.rename(columns=rename_cols ,inplace=True)

# Remove commas and convert to numeric
for COL in list(rename_cols.values()):
   usda_broilers_p[COL] = usda_broilers_p[COL].str.replace(',','')
   usda_broilers_p[COL] = pd.to_numeric(usda_broilers_p[COL]
                                        ,errors='coerce'
                                        ,downcast='integer'
                                        )

# ----------------------------------------------------------------------------
# Add calcs
# ----------------------------------------------------------------------------
# Convert lbs to kg
# Convert head to thousands
# Calculate all-cause Mortality
usda_broilers_p.eval(
   f'''
   chicksplaced_broilers_thsdhd = chicksplaced_broilers_hd / 1000
   production_broilers_thsdhd = production_broilers_hd / 1000

   production_broilers_kg = production_broilers_lbs / {uc.lbs_per_kg}
   production_broilers_thsdtonnes = production_broilers_kg / 1000000
   pricereceived_broilers_dollarsperkg = pricereceived_broilers_dollarsperlb * {uc.lbs_per_kg}

   mortality_calc_broilers_thsdhd = chicksplaced_broilers_thsdhd - production_broilers_thsdhd
   mortality_calc_broilers_pctplaced = mortality_calc_broilers_thsdhd / chicksplaced_broilers_thsdhd
   '''
   ,inplace=True
)

# ----------------------------------------------------------------------------
# Checks
# ----------------------------------------------------------------------------
# Total production dollars should equal total production kg * dollars per kg
usda_broilers_p.eval(
   '''
   check_total_dollars = production_broilers_kg * pricereceived_broilers_dollarsperkg
   check_total_dollars_diff = check_total_dollars - production_broilers_dollars
   check_total_dollars_pctdiff = check_total_dollars_diff / production_broilers_dollars
   '''
   ,inplace=True
)

# ----------------------------------------------------------------------------
# Drop
# ----------------------------------------------------------------------------
# Drop base columns that have been converted
usda_broilers_p = usda_broilers_p.drop(
   columns=[
      'chicksplaced_broilers_hd'
      ,'production_broilers_hd'
      ,'production_broilers_lbs'
      ,'production_broilers_kg'
      ,'pricereceived_broilers_dollarsperlb'
      ,'check_total_dollars'
      ,'check_total_dollars_diff'
      ,'check_total_dollars_pctdiff'
   ]
)

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(usda_broilers_p)
datadesc(usda_broilers_p ,CHARACTERIZE_FOLDER)
usda_broilers_p.to_pickle(os.path.join(PRODATA_FOLDER ,'usda_broilers_p.pkl.gz'))

profile = usda_broilers_p.profile_report()
profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'usda_broilers_p_profile.html'))

#%% Chicken Condemns

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
usda_condemn = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'USDA_chickens_condemns_2011_2021.csv'))
cleancolnames(usda_condemn)

data_items_cdm = usda_condemn[['commodity' ,'data_item' ,'domain' ,'domain_category']].value_counts()

datainfo(usda_condemn)

# ----------------------------------------------------------------------------
# Pivot each data item into a column
# ----------------------------------------------------------------------------
usda_condemn_p = usda_condemn.pivot(
   index='year'           # Column(s) to make new index. If blank, uses existing index.
   ,columns='data_item'        # Column(s) to make new columns
   ,values='value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
   ).reset_index()

# Rename
rename_cols = {
   'CHICKENS, SLAUGHTER, FI - CONDEMNED, ANTE-MORTEM, MEASURED IN LB, LIVE BASIS':'condemn_chicken_antemortem_lbslive'
   ,'CHICKENS, SLAUGHTER, FI - CONDEMNED, ANTE-MORTEM, MEASURED IN PCT OF SLAUGHTER':'condemn_chicken_antemortem_pctofslaughter'
   ,'CHICKENS, SLAUGHTER, FI - CONDEMNED, POST-MORTEM, ALL CAUSES, MEASURED IN HEAD':'condemn_chicken_postmortem_hd'
   }
usda_condemn_p.rename(columns=rename_cols ,inplace=True)

# Remove commas and convert to numeric
for COL in list(rename_cols.values()):
   usda_condemn_p[COL] = usda_condemn_p[COL].str.replace(',','')
   usda_condemn_p[COL] = pd.to_numeric(usda_condemn_p[COL]
                                        ,errors='coerce'
                                        ,downcast='integer'
                                        )

datainfo(usda_condemn_p)

#%% Swine Inventory

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
usda_swine_inventory = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'usda_swine_inventory_2011_2022.csv'))
cleancolnames(usda_swine_inventory)
datainfo(usda_swine_inventory)

# ----------------------------------------------------------------------------
# Basic cleanup
# ----------------------------------------------------------------------------
# Convert Value to numeric
usda_swine_inventory['value'] = usda_swine_inventory['value'].str.replace(',','')
usda_swine_inventory['value'] = pd.to_numeric(
   usda_swine_inventory['value']
   ,errors='coerce'
)

# ----------------------------------------------------------------------------
# Pivot each data item into a column
# ----------------------------------------------------------------------------
check_index = usda_swine_inventory[['year' ,'period' ,'data_item']].value_counts()

# Data includes CENSUS and SURVEY results
# If a data_item appears for the same year and period multiple times, keep the CENSUS value
usda_swine_inventory = usda_swine_inventory.sort_values(
   by=['year' ,'period' ,'data_item' ,'program']
   ,ignore_index=True      # Otherwise will keep original index numbers
)
usda_swine_inventory = usda_swine_inventory.drop_duplicates(
   subset=['year' ,'period' ,'data_item']    # List (opt): only consider these columns when identifying duplicates. If None, consider all columns.
   ,keep='first'        # After sorting, CENSUS will appear first
)

usda_swine_inventory_p = usda_swine_inventory.pivot(
   index=['year' ,'period']           # Column(s) to make new index. If blank, uses existing index.
   ,columns='data_item'        # Column(s) to make new columns
   ,values='value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
   ).reset_index()
cleancolnames(usda_swine_inventory_p)

# ----------------------------------------------------------------------------
# Subset and rename columns
# ----------------------------------------------------------------------------
# Add country
usda_swine_inventory_p['country'] = 'United States of America'

keep_rename_cols_thisorder = {
   'country':'country'
   ,'year':'year'
   ,'period':'period'
   ,'hogs___inventory':'hogs_inventory'
   ,'hogs__breeding___inventory':'hogs_breeding_inventory'
   ,'hogs__market___inventory':'hogs_market_inventory'
   ,'hogs__market__120_to_179_lbs___inventory':'hogs_market_inventory_120to179lbs'
   ,'hogs__market__50_to_119_lbs___inventory':'hogs_market_inventory_50to119lbs'
   ,'hogs__market__ge_180_lbs___inventory':'hogs_market_inventory_gte180lbs'
   ,'hogs__market__lt_50_lbs___inventory':'hogs_market_inventory_lt50lbs'
}
usda_swine_inventory_p = usda_swine_inventory_p[list(keep_rename_cols_thisorder)].rename(columns=keep_rename_cols_thisorder)
datainfo(usda_swine_inventory_p)

# ----------------------------------------------------------------------------
# Pivot each period into a column
# ----------------------------------------------------------------------------
usda_swine_inventory_p2 = usda_swine_inventory_p.pivot(
   index=['country' ,'year']           # Column(s) to make new index. If blank, uses existing index.
   ,columns='period'        # Column(s) to make new columns
   # ,values='value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
   ).reset_index()
usda_swine_inventory_p2 = colnames_from_index(usda_swine_inventory_p2)
cleancolnames(usda_swine_inventory_p2)
datainfo(usda_swine_inventory_p2)

usda_swine_inventory_p2 = usda_swine_inventory_p2.rename(columns={"year_":"year" ,"country_":"country"})

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(usda_swine_inventory_p2)
# datadesc(usda_swine_inventory_p2 ,CHARACTERIZE_FOLDER)

usda_swine_inventory_p2.to_pickle(os.path.join(PRODATA_FOLDER ,'usda_swine_inventory_p2.pkl.gz'))

#%% Swine Other metrics
'''
Includes most swine metrics besides inventory: litter rate, death loss, production, producer prices, etc.
'''
# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
usda_swine_othermetrics = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'usda_swine_allmetrics_2011_2021.csv'))
cleancolnames(usda_swine_othermetrics)
datainfo(usda_swine_othermetrics)

# ----------------------------------------------------------------------------
# Basic cleanup
# ----------------------------------------------------------------------------
# Convert Value to numeric
usda_swine_othermetrics['value'] = usda_swine_othermetrics['value'].str.replace(',','')
usda_swine_othermetrics['value'] = pd.to_numeric(
   usda_swine_othermetrics['value']
   ,errors='coerce'
)

# ----------------------------------------------------------------------------
# Pivot each data item into a column
# ----------------------------------------------------------------------------
# Note some items are reported for period YEAR, others for MARKETING YEAR
# From https://quickstats.nass.usda.gov/src/glossary.pdf:
'''
Year Generally refers to calendar year. For Prices Received data, refers to
an unweighted average (by month) for the calendar year.

Marketing Year Definition varies by commodity; see Agricultural Prices publications
for definitions by commodity. For Prices Received data, refers to a
weighted average for the marketing year.
'''
# If a data_item appears for both, keep the MARKETING YEAR value
usda_swine_othermetrics = usda_swine_othermetrics.sort_values(
   by=['year' ,'data_item','period' ]
   ,ignore_index=True      # Otherwise will keep original index numbers
)
usda_swine_othermetrics = usda_swine_othermetrics.drop_duplicates(
   subset=['year' ,'data_item']    # List (opt): only consider these columns when identifying duplicates. If None, consider all columns.
   ,keep='first'        # After sorting, MARKETING YEAR will appear first
)

usda_swine_othermetrics_p = usda_swine_othermetrics.pivot(
   index='year'           # Column(s) to make new index. If blank, uses existing index.
   ,columns='data_item'        # Column(s) to make new columns
   ,values='value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
   ).reset_index()
cleancolnames(usda_swine_othermetrics_p)
datainfo(usda_swine_othermetrics_p)

# ----------------------------------------------------------------------------
# Subset and rename columns
# ----------------------------------------------------------------------------
# Add country
usda_swine_othermetrics_p['country'] = 'United States of America'

keep_rename_cols_thisorder = {
   'country':'country'
   ,'year':'year'
   ,'hogs___litter_rate__measured_in_pigs___litter':'hogs_pigsperlitter'
   ,'hogs___loss__death__measured_in_head':'hogs_deathloss_hd'
   ,'hogs___pig_crop__measured_in_head':'hogs_pigcrop_hd'
   ,'hogs___price_received__measured_in__dol____cwt':'hogs_pricerecvd_dolpercwt'
   ,'hogs___production__measured_in__dol_':'hogs_production_dol'
   ,'hogs___production__measured_in_lb':'hogs_production_lb'
   ,'hogs___shipments_in__measured_in_head':'hogs_shipments_hd'
   ,'hogs___slaughtered__measured_in_head':'hogs_slaughter_hd'
   # ,'hogs___excl_inter_farm_in_state____sales__measured_in__dol_':'hogs_'
   # ,'hogs___excl_inter_farm_in_state____sales__measured_in_head':'hogs_'
   # ,'hogs___excl_inter_farm_in_state____sales__measured_in_lb':'hogs_'
   # ,'hogs__barrows__and__gilts___price_received__measured_in__dol____cwt':'hogs_'
   # ,'hogs__barrows__and__gilts__slaughter__commercial__fi___slaughtered__measured_in_head':'hogs_'
   # ,'hogs__barrows__and__gilts__slaughter__commercial__fi___slaughtered__measured_in_lb___head__dressed_basis':'hogs_'
   # ,'hogs__boars__slaughter__commercial__fi___slaughtered__measured_in_head':'hogs_'
   # ,'hogs__boars__slaughter__commercial__fi___slaughtered__measured_in_lb___head__dressed_basis':'hogs_'
   # ,'hogs__home_consumption___value__measured_in__dol_':'hogs_'
   ,'hogs__slaughter__commercial___slaughtered__measured_in_head':'hogs_slaughter_commercial_hd'
   ,'hogs__slaughter__commercial___slaughtered__measured_in_lb___head__live_basis':'hogs_slaughter_commercial_live_lbperhd'
   ,'hogs__slaughter__commercial___slaughtered__measured_in_lb__live_basis':'hogs_slaughter_commercial_live_lb'
   ,'hogs__slaughter__commercial__fi___slaughtered__measured_in_head':'hogs_slaughter_commercial_fedinsp_hd'
   ,'hogs__slaughter__commercial__fi___slaughtered__measured_in_lb___head__dressed_basis':'hogs_slaughter_commercial_fedinsp_carc_lbperhd'
   ,'hogs__slaughter__commercial__fi___slaughtered__measured_in_lb___head__live_basis':'hogs_slaughter_commercial_fedinsp_live_lbperhd'
   ,'hogs__slaughter__commercial__nfi___slaughtered__measured_in_head':'hogs_slaughter_commercial_notfedinsp_hd'
   ,'hogs__slaughter__commercial__nfi___slaughtered__measured_in_lb___head__live_basis':'hogs_slaughter_commercial_notfedinsp_live_lbperhd'
   ,'hogs__slaughter__on_farm___slaughtered__measured_in_head':'hogs_slaughter_onfarm_hd'
   ,'hogs__sows___price_received__measured_in__dol____cwt':'hogs_sows_pricerecvd_dolpercwt'
   ,'hogs__sows__slaughter__commercial__fi___slaughtered__measured_in_head':'hogs_sows_slaughter_commercial_fedinsp_hd'
   ,'hogs__sows__slaughter__commercial__fi___slaughtered__measured_in_lb___head__dressed_basis':'hogs_sows_slaughter_commercial_fedinsp_carc_lbperhd'
}
usda_swine_othermetrics_p = usda_swine_othermetrics_p[list(keep_rename_cols_thisorder)].rename(columns=keep_rename_cols_thisorder)
datainfo(usda_swine_othermetrics_p)

# ----------------------------------------------------------------------------
# Add calcs
# ----------------------------------------------------------------------------
# Convert lbs to kg
usda_swine_othermetrics_p.eval(
   f'''
   hogs_production_kg = hogs_production_lb / {uc.lbs_per_kg}
   hogs_slaughter_commercial_live_kgperhd = hogs_slaughter_commercial_live_lbperhd / {uc.lbs_per_kg}
   hogs_slaughter_commercial_live_kg = hogs_slaughter_commercial_live_lb / {uc.lbs_per_kg}
   hogs_slaughter_commercial_fedinsp_carc_kgperhd = hogs_slaughter_commercial_fedinsp_carc_lbperhd / {uc.lbs_per_kg}
   hogs_slaughter_commercial_fedinsp_live_kgperhd = hogs_slaughter_commercial_fedinsp_live_lbperhd / {uc.lbs_per_kg}
   hogs_sows_slaughter_commercial_fedinsp_carc_kgperhd = hogs_sows_slaughter_commercial_fedinsp_carc_lbperhd / {uc.lbs_per_kg}
   '''
   ,inplace=True
)

# ----------------------------------------------------------------------------
# Checks
# ----------------------------------------------------------------------------
# How do these compare or sum:
   # hogs_slaughter_hd
   # hogs_slaughter_commercial_hd
   # hogs_slaughter_commercial_fedinsp_hd
   # hogs_slaughter_commercial_notfedinsp_hd
   # hogs_slaughter_onfarm_hd
   # hogs_sows_slaughter_commercial_fedinsp_hd

usda_swine_othermetrics_p_chk = usda_swine_othermetrics_p.copy()
usda_swine_othermetrics_p_chk = usda_swine_othermetrics_p_chk.eval(
    '''
    check_sl_totalvscom = hogs_slaughter_hd / hogs_slaughter_commercial_hd
    check_sl_totalvscomplusfarm = hogs_slaughter_hd / (hogs_slaughter_commercial_hd + hogs_slaughter_onfarm_hd)
    check_sl_comvsfiplusnfi = hogs_slaughter_commercial_hd / (hogs_slaughter_commercial_fedinsp_hd + hogs_slaughter_commercial_notfedinsp_hd)
    '''
)

plot_histogram_withinset(usda_swine_othermetrics_p_chk ,'check_sl_totalvscom')   # Equals ~1
plot_histogram_withinset(usda_swine_othermetrics_p_chk ,'check_sl_totalvscomplusfarm')   # Equals exactly 1
plot_histogram_withinset(usda_swine_othermetrics_p_chk ,'hogs_slaughter_commercial_hd')
plot_histogram_withinset(usda_swine_othermetrics_p_chk ,'hogs_slaughter_onfarm_hd')   # Very small compared to commercial

plot_histogram_withinset(usda_swine_othermetrics_p_chk ,'check_sl_comvsfiplusnfi')   # Equals exactly 1 apart from single outlier
plot_histogram_withinset(usda_swine_othermetrics_p_chk ,'hogs_slaughter_commercial_fedinsp_hd')
plot_histogram_withinset(usda_swine_othermetrics_p_chk ,'hogs_slaughter_commercial_notfedinsp_hd')   # Small compared to fed insp (~1%)

plot_histogram_withinset(usda_swine_othermetrics_p_chk ,'hogs_sows_slaughter_commercial_fedinsp_hd')   # Small compared to commercial (~2%)

# ----------------------------------------------------------------------------
# Drop
# ----------------------------------------------------------------------------
# Drop base columns that have been converted and check columns
usda_swine_othermetrics_p = usda_swine_othermetrics_p.drop(
   columns=[
      # These have been converted
      'hogs_production_lb'
      ,'hogs_slaughter_commercial_live_lbperhd'
      ,'hogs_slaughter_commercial_live_lb'
      ,'hogs_slaughter_commercial_fedinsp_carc_lbperhd'
      ,'hogs_slaughter_commercial_fedinsp_live_lbperhd'
      ,'hogs_sows_slaughter_commercial_fedinsp_carc_lbperhd'

      # After checks, these are no longer needed
      ,'hogs_slaughter_commercial_notfedinsp_hd'
      ,'hogs_slaughter_commercial_notfedinsp_live_lbperhd'
      ,'hogs_slaughter_onfarm_hd'
   ]
)

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(usda_swine_othermetrics_p)
# datadesc(usda_swine_othermetrics_p ,CHARACTERIZE_FOLDER)

usda_swine_othermetrics_p.to_pickle(os.path.join(PRODATA_FOLDER ,'usda_swine_othermetrics_p.pkl.gz'))
