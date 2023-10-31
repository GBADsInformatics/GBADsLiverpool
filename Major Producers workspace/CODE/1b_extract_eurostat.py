#%% About

# Exploring Eurostat data (R12 in Liverpool's spreadsheet)
# https://ec.europa.eu/eurostat/databrowser/explore/all/agric?lang=en

# Note about metadata
# The downloaded tables contain the following columns
# I'm dropping these for analysis, but they may be useful for documentation
# 'dataflow' with information about the parent data
# 'last update' with date of last update

# Note about recoding variables:
# The online data browser shows both the name and the code for each variable.
# Downloaded data contains only codes, not names.
# I cannot find a lookup table on the Eurostat website, so I'm doing this translation manually by defining dictionaries.

# Note about countries/geographies:
# The online data browser shows full country names.
# Downloaded data shows only 2-letter abbreviations, which are not always obvious.

#%% Slaughtered Poultry and Pig Meat
# https://ec.europa.eu/eurostat/databrowser/view/APRO_MT_PANN__custom_2005150/default/table?lang=en

# ----------------------------------------------------------------------------
# Import and intitial prep
# ----------------------------------------------------------------------------
euro_slaughter = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'EUROSTAT_slaughtered_poultry_pig_2011_2021.csv.gz'))
cleancolnames(euro_slaughter)

# Replace codes with names
recode_meat = {
   'B3100':'sl_pigmeat'
   ,'B7000':'sl_poultrymeat'
   ,'B7100':'sl_chicken'
}
euro_slaughter['meat'].replace(to_replace=recode_meat ,inplace=True)

recode_unit = {
   'THS_T':'thsdtonne'
   ,'THS_HD':'thsdhd'
}
euro_slaughter['unit'].replace(to_replace=recode_unit ,inplace=True)

check_meat_item_unit = euro_slaughter[['meat' ,'meatitem' ,'unit']].value_counts()

datainfo(euro_slaughter)

# ----------------------------------------------------------------------------
# Reshape
# ----------------------------------------------------------------------------
# Create metrics by combining meat and unit
euro_slaughter['meat_unit'] = euro_slaughter['meat'] + '_' + euro_slaughter['unit']

# Pivot so each metric is a column
euro_slaughter_p = euro_slaughter.pivot(
   index=['geo' ,'time_period']           # Column(s) to make new index. If blank, uses existing index.
   ,columns='meat_unit'        # Column(s) to make new columns
   ,values='obs_value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
   )
euro_slaughter_p = indextocolumns(euro_slaughter_p)
datainfo(euro_slaughter_p)

# ----------------------------------------------------------------------------
# Write basic description
# ----------------------------------------------------------------------------
datadesc(euro_slaughter_p ,CHARACTERIZE_FOLDER)

#%% Producer prices
# https://ec.europa.eu/eurostat/databrowser/view/APRI_AP_ANOUTA__custom_2155948/default/table?lang=en

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
euro_producerprice = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'EUROSTAT_animalproducts_sellingprices_2011_2020.csv.gz'))
cleancolnames(euro_producerprice)

# Replace codes with names
euro_producerprice_items = list(euro_producerprice['prod_ani'].unique())
recode_items = {
   11210000:'pigs_light_live_priceper100kg_euros'
   ,11220000:'pigs_grade2_carcass_priceper100kg_euros'
   ,11230000:'pigs_grade1_carcass_priceper100kg_euros'
   ,11240000:'piglets_live_priceper100kg_euros'
   ,11510000:'chickens_live1stchoice_priceper100kg_euros'
   ,11511000:'broilingfowl_carcass_priceper100kg_euros'
}
euro_producerprice['item'] = euro_producerprice['prod_ani'].replace(to_replace=recode_items)

datainfo(euro_producerprice)

# ----------------------------------------------------------------------------
# Reshape
# ----------------------------------------------------------------------------
# Pivot so each item is a column
euro_producerprice_p = euro_producerprice.pivot(
   index=['geo' ,'time_period']           # Column(s) to make new index. If blank, uses existing index.
   ,columns='item'        # Column(s) to make new columns
   ,values='obs_value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
   )
euro_producerprice_p = euro_producerprice_p.reset_index()
datainfo(euro_producerprice_p)

# ----------------------------------------------------------------------------
# Write basic description
# ----------------------------------------------------------------------------
datadesc(euro_producerprice_p ,CHARACTERIZE_FOLDER)

#%% Prices for Means of Production
# https://ec.europa.eu/eurostat/databrowser/view/APRI_AP_INA__custom_2612861/default/table?lang=en

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
euro_costofproduction = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'EUROSTAT_prices_meansofproduction_2011_2020.csv.gz'))
cleancolnames(euro_costofproduction)

# Replace codes with names
euro_costofproduction_items = list(euro_costofproduction['prod_inp'].unique())
recode_items = {
   20210000:'electricity_priceper100kwh'
   ,20221000:'heatingoil_priceper100litres'
   ,20222000:'residualfueloil_priceper100kg'
   ,20231000:'motorspirit_priceper100litres'
   ,20232000:'dieseloil_priceper100litres'

   ,20623101:'feed_sacks_rearingpigs_priceper100kg'
   ,20623102:'feed_bulk_rearingpigs_priceper100kg'

   ,20623201:'feed_sacks_fatteningpigs_priceper100kg'
   ,20623202:'feed_bulk_fatteningpigs_priceper100kg'

   ,20623301:'feed_sacks_sows_priceper100kg'
   ,20623302:'feed_bulk_sows_priceper100kg'

   ,20624101:'feed_sacks_chicks_priceper100kg'
   ,20624102:'feed_bulk_chicks_priceper100kg'

   ,20624201:'feed_sacks_rearingpullet_priceper100kg'
   ,20624202:'feed_bulk_rearingpullet_priceper100kg'

   ,20624301:'feed_sacks_layinghens_priceper100kg'
   ,20624302:'feed_bulk_layinghens_priceper100kg'

   ,20624501:'feed_sacks_broilers_priceper100kg'
   ,20624502:'feed_bulk_broilers_priceper100kg'
}
euro_costofproduction['item'] = euro_costofproduction['prod_inp'].replace(to_replace=recode_items)

datainfo(euro_costofproduction)

# ----------------------------------------------------------------------------
# Reshape
# ----------------------------------------------------------------------------
# Concatenate item with currency
recode_currency = {
   "EUR":"euros"
   ,"NAC":"localcrncy"
}
euro_costofproduction['currency_recode'] = euro_costofproduction['currency'].replace(recode_currency)
euro_costofproduction['item_currency'] = euro_costofproduction['item'] + '_' + euro_costofproduction['currency_recode']

# Pivot so each item is a column
euro_costofproduction_p = euro_costofproduction.pivot(
   index=['geo' ,'time_period']           # Column(s) to make new index. If blank, uses existing index.
   ,columns='item_currency'        # Column(s) to make new columns
   ,values='obs_value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
   )
euro_costofproduction_p = euro_costofproduction_p.reset_index()
cleancolnames(euro_costofproduction_p)
datainfo(euro_costofproduction_p)

# ----------------------------------------------------------------------------
# Write basic description
# ----------------------------------------------------------------------------
datadesc(euro_costofproduction_p ,CHARACTERIZE_FOLDER)

#%% Chicks hatched
# https://ec.europa.eu/eurostat/databrowser/view/APRO_EC_POULA__custom_2021635/default/table?lang=en

# ----------------------------------------------------------------------------
# Import and intitial prep
# ----------------------------------------------------------------------------
euro_chicks = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'EUROSTAT_chickshatched_layers_broilers_2011_2021.csv.gz'))
cleancolnames(euro_chicks)

# Replace codes with names
recode_animals = {
   'A5130O':'chickshatched_layers_thsdhd'
   ,'A5130ON':'chickshatched_layers_slct_thsdhd'
   ,'A5130P':'chickshatched_broilers_thsdhd'
   ,'A5130PN':'chickshatched_broilers_slct_thsdhd'
   }
euro_chicks['animals'].replace(to_replace=recode_animals ,inplace=True)
check_animal_item = euro_chicks[['animals' ,'hatchitm']].value_counts()

check_geo = list(euro_chicks['geo'].unique())

datainfo(euro_chicks)

# ----------------------------------------------------------------------------
# Reshape
# ----------------------------------------------------------------------------
# Pivot so each metric is a column
euro_chicks_p = euro_chicks.pivot(
   index=['geo' ,'time_period']           # Column(s) to make new index. If blank, uses existing index.
   ,columns='animals'        # Column(s) to make new columns
   ,values='obs_value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
   )
euro_chicks_p = indextocolumns(euro_chicks_p)
datainfo(euro_chicks_p)

#!!! Fill in missings with zero
# Either missing to begin with or because there was no obs_value for that year
euro_chicks_p.replace(to_replace=np.nan ,value=0 ,inplace=True)

# ----------------------------------------------------------------------------
# Write basic description
# ----------------------------------------------------------------------------
datadesc(euro_chicks_p ,CHARACTERIZE_FOLDER)

#%% Create Chicken Combo

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
# Starting with chicks hatched
# Merge slaughter data regarding chickens
euro_chickencombo = pd.merge(
   left=euro_chicks_p
   ,right=euro_slaughter_p[[
      'geo'
      ,'time_period'
      ,'sl_chicken_thsdhd'
      ,'sl_chicken_thsdtonne'
      ]]
   ,on=['geo' ,'time_period']
   ,how='outer'
   ,indicator='_merge_slaughter'
   )
euro_chickencombo['_merge_slaughter'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Merge producer price data regarding chickens
euro_chickencombo = pd.merge(
   left=euro_chickencombo
   ,right=euro_producerprice_p[[
      'geo'
      ,'time_period'
      ,'broilingfowl_carcass_priceper100kg_euros'
      ,'chickens_live1stchoice_priceper100kg_euros'
      ]]
   ,on=['geo' ,'time_period']
   ,how='outer'
   ,indicator='_merge_prodprice'
   )
euro_chickencombo['_merge_prodprice'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Merge cost of production data regarding chickens
cost_columnbase_chickens = [
   'geo'
   ,'time_period'

   ,'electricity_priceper100kwh'
   ,'heatingoil_priceper100litres'
   ,'residualfueloil_priceper100kg'
   ,'motorspirit_priceper100litres'
   ,'dieseloil_priceper100litres'

   ,'feed_sacks_chicks_priceper100kg'
   ,'feed_bulk_chicks_priceper100kg'

   ,'feed_sacks_rearingpullet_priceper100kg'
   ,'feed_bulk_rearingpullet_priceper100kg'

   ,'feed_sacks_layinghens_priceper100kg'
   ,'feed_bulk_layinghens_priceper100kg'

   ,'feed_sacks_broilers_priceper100kg'
   ,'feed_bulk_broilers_priceper100kg'
]
cost_columns_chickens = []
for STR in cost_columnbase_chickens:
   cost_columns_chickens = cost_columns_chickens + [item for item in list(euro_costofproduction_p) if STR in item]

euro_chickencombo = pd.merge(
   left=euro_chickencombo
   ,right=euro_costofproduction_p[cost_columns_chickens]
   ,on=['geo' ,'time_period']
   ,how='outer'
   ,indicator='_merge_costofprod'
   )
euro_chickencombo['_merge_costofprod'].value_counts()          # Check number of rows from each table (requires indicator=True)

datainfo(euro_chickencombo)

# ----------------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------------
# List of country codes in scope
# This list can be any case. Will be converted to uppercase as needed.
eurostat_countries_inscope = [
   'FR'    # France
   ,'PL'   # Poland
   ,'ES'   # Spain
   ,'IT'   # Italy
   ,'NL'   # Netherlands
   ,'DE'   # Germany
   ,'UK'   # United Kingdom
   ]

eurostat_countries_eu = [
   'AT'
   ,'BE'
   ,'BG'
   ,'CY'
   ,'CZ'
   ,'DE'
   ,'DK'
   ,'EE'
   ,'ES'
   ,'FI'
   ,'FR'
   ,'HR'
   ,'HU'
   ,'IE'
   ,'IT'
   ,'LT'
   ,'LU'
   ,'LV'
   ,'MT'
   ,'NL'
   ,'PL'
   ,'PT'
   ,'RO'
   ,'SE'
   ,'SI'
   ,'SK'
   ]

# Create a filter for countries in scope
eurostat_countries_inscope_upcase = [x.upper() for x in eurostat_countries_inscope]
_euro_chickencombo_inscope = (euro_chickencombo['geo'].str.upper().isin(eurostat_countries_inscope_upcase))

eurostat_countries_eu_upcase = [x.upper() for x in eurostat_countries_eu]
_euro_chickencombo_eu = (euro_chickencombo['geo'].str.upper().isin(eurostat_countries_eu_upcase))

check_scope = euro_chickencombo.loc[_euro_chickencombo_inscope]

# Add columns to data
euro_chickencombo['geo_inscope'] = _euro_chickencombo_inscope
euro_chickencombo['geo_eu27'] = _euro_chickencombo_eu

# ----------------------------------------------------------------------------
# Estimate breakout of slaughter into layers and broilers based on proportion
# of chicks hatched. From data team discussion.
# ----------------------------------------------------------------------------
euro_chickencombo.eval(
   '''
   chickshatched_alltypes_thsdhd = \
      chickshatched_layers_thsdhd \
      + chickshatched_layers_slct_thsdhd \
      + chickshatched_broilers_thsdhd \
      + chickshatched_broilers_slct_thsdhd
   chickshatched_layersandbreeders_thsdhd = \
      chickshatched_layers_thsdhd \
      + chickshatched_layers_slct_thsdhd \
      + chickshatched_broilers_slct_thsdhd
   chickshatched_broilers_prpn = chickshatched_broilers_thsdhd / chickshatched_alltypes_thsdhd
   sl_est_broilers_thsdhd = sl_chicken_thsdhd * chickshatched_broilers_prpn
   sl_est_broilers_thsdtonne = sl_chicken_thsdtonne * chickshatched_broilers_prpn
   '''
   ,inplace=True
)
datainfo(euro_chickencombo)

# ----------------------------------------------------------------------------
# Add calcs
# ----------------------------------------------------------------------------
# All-cause Mortality, including pre-slaughter condemns
# Note: more complete mortality calcs (accounting for imoprts/exports) are done after merging with FAO data
euro_chickencombo.eval(
   '''
   mortality_calc_broilers_thsdhd = chickshatched_broilers_thsdhd - sl_est_broilers_thsdhd
   mortality_calc_broilers_pctplaced = mortality_calc_broilers_thsdhd / chickshatched_broilers_thsdhd
   '''
   ,inplace=True
)
datainfo(euro_chickencombo)

# ----------------------------------------------------------------------------
# Checks
# ----------------------------------------------------------------------------
# Proportion of chicks that are broilers
euro_chickencombo['chickshatched_broilers_prpn'].describe()
plt.figure(figsize=[6 ,2])       # Set [width, height] in inches
sns.boxplot(data=euro_chickencombo.loc[_euro_chickencombo_inscope] ,x='chickshatched_broilers_prpn')

# Broiler Mortality
euro_chickencombo['mortality_calc_broilers_thsdhd'].describe()
plt.figure(figsize=[6 ,2])       # Set [width, height] in inches
sns.boxplot(data=euro_chickencombo.loc[_euro_chickencombo_inscope] ,x='mortality_calc_broilers_thsdhd')

## Separate boxplots by country
snplt = sns.catplot(
   data=euro_chickencombo.loc[_euro_chickencombo_inscope]
   ,x='mortality_calc_broilers_thsdhd'    # Categorical
   ,kind='box'          # 'bar': bars with height determined by y=. 'count': bars showing count of observations. 'strip':. 'swarm':. 'box':. 'violin':. 'boxen':. 'point':.
   ,ci=None             # 'sd': show standard deviation within each category. Float: create bootstrap estimate of variation at this confidence level
   ,row='geo'           # Variables to panel by
   ,sharex=True         # True: facets share x-axis limits
   ,sharey=True         # True: facets share y-axis limits
   ,legend=True         # True: add a legend
   ,legend_out=True     # True: draw legend outside the plot
   ,height=1.5          # Height of figure in inches. If paneled, height of each panel.
   ,aspect=4            # Width of each panel as multiplier on height
)

# ----------------------------------------------------------------------------
# Create groups and sums
# ----------------------------------------------------------------------------
euro_chickencombo_sum_inscope = euro_chickencombo.pivot_table(
   index=['geo_inscope' ,'time_period']           # Column(s) to make new index
   ,values=['chickshatched_broilers_thsdhd' ,'sl_est_broilers_thsdhd' ,'mortality_calc_broilers_thsdhd']                         # Column to aggregate
   ,aggfunc='sum'                  # Aggregate function to use. Can pass list or dictionary {'colname':'function'}. See numpy functions https://docs.scipy.org/doc/numpy/reference/routines.statistics.html
   )
euro_chickencombo_sum_eu27 = euro_chickencombo.pivot_table(
   index=['geo_eu27' ,'time_period']           # Column(s) to make new index
   ,values=['chickshatched_broilers_thsdhd' ,'sl_est_broilers_thsdhd' ,'mortality_calc_broilers_thsdhd']                         # Column to aggregate
   ,aggfunc='sum'                  # Aggregate function to use. Can pass list or dictionary {'colname':'function'}. See numpy functions https://docs.scipy.org/doc/numpy/reference/routines.statistics.html
   )

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(euro_chickencombo)
datadesc(euro_chickencombo ,CHARACTERIZE_FOLDER)
euro_chickencombo.to_pickle(os.path.join(PRODATA_FOLDER ,'euro_chickencombo.pkl.gz'))

# profile = euro_chickencombo.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'euro_chickencombo_profile.html'))

# profile = euro_chickencombo.loc[_euro_chickencombo_inscope].profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'_euro_chickencombo_inscope_profile.html'))

#%% Pig Population
# https://ec.europa.eu/eurostat/databrowser/view/APRO_MT_LSPIG__custom_2012590/default/table?lang=en

# ----------------------------------------------------------------------------
# Import and intitial prep
# ----------------------------------------------------------------------------
euro_pigpop = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'EUROSTAT_pigpopulation_2011_2021.csv.gz'))
cleancolnames(euro_pigpop)

# Replace codes with names
recode_animals = {
   'A3120':'breedingsows_gte50kg'
   ,'A3110':'pigs_lt20kg'
   ,'A3131':'pigs_20to50kg'
   ,'A3132X':'pigs_50to80kg'
   ,'A3132Y':'pigs_80to110kg'
   ,'A3132Z':'pigs_gte110kg'
}
euro_pigpop['animals'].replace(to_replace=recode_animals ,inplace=True)

recode_unit = {'THS_HD':'thsdhd'}
euro_pigpop['unit'].replace(to_replace=recode_unit ,inplace=True)

check_animal_unit = euro_pigpop[['animals' ,'unit']].value_counts()

check_month = euro_pigpop['month'].value_counts()
recode_month = {
   'M12':'dec'
   ,'M05_M06':'jun'
}
euro_pigpop['month'].replace(to_replace=recode_month ,inplace=True)

datainfo(euro_pigpop)

# ----------------------------------------------------------------------------
# Reshape
# ----------------------------------------------------------------------------
# Create metrics by combining animal, unit, and time of year of snapshot
euro_pigpop['animal_unit_snapshot'] = euro_pigpop['animals'] + '_' + euro_pigpop['month'] + '_' + euro_pigpop['unit']

# Pivot so each metric is a column
euro_pigpop_p = euro_pigpop.pivot(
   index=['geo' ,'time_period']           # Column(s) to make new index. If blank, uses existing index.
   ,columns='animal_unit_snapshot'        # Column(s) to make new columns
   ,values='obs_value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
   )
euro_pigpop_p = euro_pigpop_p.reset_index()
datainfo(euro_pigpop_p)

# ----------------------------------------------------------------------------
# Write basic description
# ----------------------------------------------------------------------------
datadesc(euro_pigpop_p ,CHARACTERIZE_FOLDER)

#%% Create Pig Combo

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
# Starting with pig population
# Merge slaughter data regarding pigs
euro_pigcombo = pd.merge(
   left=euro_pigpop_p
   ,right=euro_slaughter_p[[
      'geo'
      ,'time_period'
      ,'sl_pigmeat_thsdhd'
      ,'sl_pigmeat_thsdtonne'
      ]]
   ,on=['geo' ,'time_period']
   ,how='outer'
   ,indicator='_merge_slaughter'
   )
euro_pigcombo['_merge_slaughter'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Merge producer price data regarding pigs
euro_pigcombo = pd.merge(
   left=euro_pigcombo
   ,right=euro_producerprice_p[[
      'geo'
      ,'time_period'
      ,'piglets_live_priceper100kg_euros'
      ,'pigs_grade1_carcass_priceper100kg_euros'
      ,'pigs_grade2_carcass_priceper100kg_euros'
      ,'pigs_light_live_priceper100kg_euros'
      ]]
   ,on=['geo' ,'time_period']
   ,how='outer'
   ,indicator='_merge_prodprice'
   )
euro_pigcombo['_merge_prodprice'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Merge cost of production data regarding pigs
cost_columnbase_pigs = [
   'geo'
   ,'time_period'

   ,'electricity_priceper100kwh'
   ,'heatingoil_priceper100litres'
   ,'residualfueloil_priceper100kg'
   ,'motorspirit_priceper100litres'
   ,'dieseloil_priceper100litres'

   ,'feed_sacks_rearingpigs_priceper100kg'
   ,'feed_bulk_rearingpigs_priceper100kg'

   ,'feed_sacks_fatteningpigs_priceper100kg'
   ,'feed_bulk_fatteningpigs_priceper100kg'

   ,'feed_sacks_sows_priceper100kg'
   ,'feed_bulk_sows_priceper100kg'
]
cost_columns_pigs = []
for STR in cost_columnbase_pigs:
   cost_columns_pigs = cost_columns_pigs + [item for item in list(euro_costofproduction_p) if STR in item]

euro_pigcombo = pd.merge(
   left=euro_pigcombo
   ,right=euro_costofproduction_p[cost_columns_pigs]
   ,on=['geo' ,'time_period']
   ,how='outer'
   ,indicator='_merge_costofprod'
   )
euro_pigcombo['_merge_costofprod'].value_counts()          # Check number of rows from each table (requires indicator=True)

datainfo(euro_pigcombo)

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(euro_pigcombo)
datadesc(euro_pigcombo ,CHARACTERIZE_FOLDER)
euro_pigcombo.to_pickle(os.path.join(PRODATA_FOLDER ,'euro_pigcombo.pkl.gz'))

# profile = euro_pigcombo.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'euro_pigcombo_profile.html'))
