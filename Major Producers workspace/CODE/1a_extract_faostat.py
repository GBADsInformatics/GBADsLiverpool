#%% About

# Exploring FAOSTAT data (R18 in Liverpool's spreadsheet)
# Crops and Livestock:
# https://www.fao.org/faostat/en/#data/QCL

# Producer Prices:
# https://www.fao.org/faostat/en/#data/PP

#%% FAO Common
# Country lists and other things that will be used for all FAO data

# This list can be any case. Will be converted to uppercase as needed.
fao_countries_inscope = [
   # European 6
   'France'
   ,'Poland'
   ,'Spain'
   ,'Italy'
   ,'Netherlands'
   ,'Germany'

   ,'United Kingdom of Great Britain and Northern Ireland'
   ,'United States of America'

   ,'China'
   # Note sub-China measures add up to 'China' measures
   ,'China, Hong Kong SAR'
   ,'China, Macao SAR'
   ,'China, Taiwan Province of'
   ,'China, mainland'

   ,'India'
   ,'Brazil'
]
fao_countries_inscope_upcase = [x.upper() for x in fao_countries_inscope]

#%% Producer prices

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
fao_producerprice_imp = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'FAOSTAT_chickens_pigs_producerprices_2011_2020.csv'))
cleancolnames(fao_producerprice_imp)
datainfo(fao_producerprice_imp)

# ----------------------------------------------------------------------------
# Reshape so each element_item_unit is its own column
# ----------------------------------------------------------------------------
fao_producerprice_element_items = fao_producerprice_imp[['element' ,'item' ,'unit']].value_counts()

fao_producerprice_p = fao_producerprice_imp.pivot(
    index=['area' ,'year']          # Column(s) to make new index. If blank, uses existing index.
    ,columns=['element' ,'item' ,'unit']       # Column(s) to make new columns
    ,values='value'        # Column to populate rows. Can pass a list, but will create multi-indexed columns.
    )
fao_producerprice_p = fao_producerprice_p.reset_index(drop=False)
fao_producerprice_p = colnames_from_index(fao_producerprice_p)
cleancolnames(fao_producerprice_p)

rename_reorder_columns = {
   'area__':'country'
   ,'year__':'year'
   ,'producer_price_index__2014_2016__eq__100__meat__chicken_nan':'producerprice_chickens_carcass_index'
   ,'producer_price_index__2014_2016__eq__100__meat__pig_nan':'producerprice_pigs_carcass_index'
   ,'producer_price_index__2014_2016__eq__100__meat_live_weight__chicken_nan':'producerprice_chickens_live_index'
   ,'producer_price_index__2014_2016__eq__100__meat_live_weight__pig_nan':'producerprice_pigs_live_index'

   ,'producer_price__lcu_tonne__meat__chicken_lcu':'producerprice_chickens_carcass_lcupertonne'
   ,'producer_price__lcu_tonne__meat__pig_lcu':'producerprice_pigs_carcass_lcupertonne'
   ,'producer_price__lcu_tonne__meat_live_weight__chicken_lcu':'producerprice_chickens_live_lcupertonne'
   ,'producer_price__lcu_tonne__meat_live_weight__pig_lcu':'producerprice_pigs_live_lcupertonne'

   ,'producer_price__slc_tonne__meat__chicken_slc':'producerprice_chickens_carcass_slcpertonne'
   ,'producer_price__slc_tonne__meat__pig_slc':'producerprice_pigs_carcass_slcpertonne'
   ,'producer_price__slc_tonne__meat_live_weight__chicken_slc':'producerprice_chickens_live_slcpertonne'
   ,'producer_price__slc_tonne__meat_live_weight__pig_slc':'producerprice_pigs_live_slcpertonne'

   ,'producer_price__usd_tonne__meat__chicken_usd':'producerprice_chickens_carcass_usdpertonne'
   ,'producer_price__usd_tonne__meat__pig_usd':'producerprice_pigs_carcass_usdpertonne'
   ,'producer_price__usd_tonne__meat_live_weight__chicken_usd':'producerprice_chickens_live_usdpertonne'
   ,'producer_price__usd_tonne__meat_live_weight__pig_usd':'producerprice_pigs_live_usdpertonne'
}
fao_producerprice_p = fao_producerprice_p[list(rename_reorder_columns)].rename(columns=rename_reorder_columns)

datainfo(fao_producerprice_p)

# ----------------------------------------------------------------------------
# Create a column for countries in scope
# ----------------------------------------------------------------------------
fao_producerprice_countries = list(fao_producerprice_p['country'].unique())

_fao_producerprice_p_inscope = (fao_producerprice_p['country'].str.upper().isin(fao_countries_inscope_upcase))

# Add column to data
fao_producerprice_p['country_inscope'] = _fao_producerprice_p_inscope
check_fao_producerprice_p_scope = list(fao_producerprice_p.loc[_fao_producerprice_p_inscope ,'country'].unique())

datainfo(fao_producerprice_p)

# ----------------------------------------------------------------------------
# Add calcs
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(fao_producerprice_p)
datadesc(fao_producerprice_p ,CHARACTERIZE_FOLDER)
fao_producerprice_p.to_pickle(os.path.join(PRODATA_FOLDER ,'fao_producerprice_p.pkl.gz'))

profile = fao_producerprice_p.profile_report()
profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'fao_producerprice_p_profile.html'))

# ----------------------------------------------------------------------------
# Checks
# ----------------------------------------------------------------------------

#%% Crops and Livestock: Chickens, Chicken Meat

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
fao_chickens_meat = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'FAOSTAT_chickens_meatchicken_2011_2020.csv'))
cleancolnames(fao_chickens_meat)
datainfo(fao_chickens_meat)

# ----------------------------------------------------------------------------
# Reshape so each element_item_unit is its own column
# ----------------------------------------------------------------------------
fao_chicken_element_items = fao_chickens_meat[['element' ,'item' ,'unit']].value_counts()

fao_chickens_meat_p = fao_chickens_meat.pivot(
    index=['area' ,'year']          # Column(s) to make new index. If blank, uses existing index.
    ,columns=['element' ,'item' ,'unit']       # Column(s) to make new columns
    ,values='value'        # Column to populate rows. Can pass a list, but will create multi-indexed columns.
)
fao_chickens_meat_p = indextocolumns(fao_chickens_meat_p)
fao_chickens_meat_p = colnames_from_index(fao_chickens_meat_p)
cleancolnames(fao_chickens_meat_p)

rename_columns = {
    'area':'country'
    ,'year':'year'
    ,'stocks_chickens_1000_head':'stocks_chickens_thsdhd'
    ,'yield_carcass_weight_meat__chicken_0_1g_an':'yield_chickens_tenthgramperhd'
    ,'production_meat__chicken_tonnes':'production_chickens_tonnes'
    ,'producing_animals_slaughtered_meat__chicken_1000_head':'slaughtered_chickens_thsdhd'
}
fao_chickens_meat_p.rename(columns=rename_columns ,inplace=True)

datainfo(fao_chickens_meat_p)

# ----------------------------------------------------------------------------
# Create a column for countries in scope
# ----------------------------------------------------------------------------
fao_chickens_countries = list(fao_chickens_meat_p['country'].unique())

_fao_chickens_meat_p_inscope = (fao_chickens_meat_p['country'].str.upper().isin(fao_countries_inscope_upcase))

# Add column to data for filtering
fao_chickens_meat_p['country_inscope'] = _fao_chickens_meat_p_inscope

check_fao_chickens_meat_p_scope = list(fao_chickens_meat_p.loc[_fao_chickens_meat_p_inscope ,'country'].unique())

# ----------------------------------------------------------------------------
# Add calcs
# ----------------------------------------------------------------------------
# Change in stocks year over year
fao_chickens_meat_p.sort_values(by=['country' ,'year'] ,ignore_index=True ,inplace=True)   # Ensure sorted
fao_chickens_meat_p['stocks_chickens_thsdhd_lag'] = fao_chickens_meat_p.groupby('country' ,sort=False)['stocks_chickens_thsdhd'].shift(periods=1)
fao_chickens_meat_p.eval(
   '''
   stockschange_chickens_thsdhd = stocks_chickens_thsdhd - stocks_chickens_thsdhd_lag
   '''
   ,inplace=True
)

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(fao_chickens_meat_p)
datadesc(fao_chickens_meat_p ,CHARACTERIZE_FOLDER)
fao_chickens_meat_p.to_pickle(os.path.join(PRODATA_FOLDER ,'fao_chickens_meat_p.pkl.gz'))

profile = fao_chickens_meat_p.profile_report()
profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'fao_chickens_meat_p_profile.html'))

# ----------------------------------------------------------------------------
# Checks
# ----------------------------------------------------------------------------
# Production tonnes of meat should equal (animals slaughtered)*(Yield per animal)
# Thousand head * 1000 to get head
# Tenth gram per animal / 10 to get grams per animal
# Result will be grams. Divide by 1 million to get tonnes
fao_chickens_meat_p_chk = fao_chickens_meat_p.eval(
   '''
   chk_prod = (slaughtered_chickens_thsdhd * 1000) * (yield_chickens_tenthgramperhd / 10) / 1000000
   chk_prod_pctdiff = (chk_prod - production_chickens_tonnes) / production_chickens_tonnes
   '''
)

sns.histplot(
    data=fao_chickens_meat_p_chk
    ,x='chk_prod_pctdiff'
    ,stat='count'
    ,bins=20
)

# Is Stocks the same as number placed? No.
# How does it compare to animals slaughtered? It's smaller.

# Check variations of China
# Does 'China' equal sum of all sub-Chinas? Yes.

#%% Crops and Livestock: Pigs, Pig Meat

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
fao_pigs_meat = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'FAOSTAT_pigs_meatpigs_2011_2020.csv'))
cleancolnames(fao_pigs_meat)
datainfo(fao_pigs_meat)

# ----------------------------------------------------------------------------
# Get lists of countries, items, etc.
# ----------------------------------------------------------------------------
countries = list(fao_pigs_meat['area'].unique())
element_items = fao_pigs_meat[['element' ,'item' ,'unit']].value_counts()

# ----------------------------------------------------------------------------
# Reshape so each element_item_unit is its own column
# ----------------------------------------------------------------------------
fao_pigs_meat_p = fao_pigs_meat.pivot(
   index=['area' ,'year']          # Column(s) to make new index. If blank, uses existing index.
   ,columns=['element' ,'item' ,'unit']       # Column(s) to make new columns
   ,values='value'        # Column to populate rows. Can pass a list, but will create multi-indexed columns.
)
fao_pigs_meat_p = indextocolumns(fao_pigs_meat_p)
fao_pigs_meat_p = colnames_from_index(fao_pigs_meat_p)
cleancolnames(fao_pigs_meat_p)
datainfo(fao_pigs_meat_p)

rename_columns = {
    'area':'country'
    ,'year':'year'
    ,'yield_carcass_weight_meat__pig_hg_an':'yield_pigs_hgperhd'
    ,'production_meat__pig_tonnes':'production_pigs_tonnes'
    ,'producing_animals_slaughtered_meat__pig_head':'slaughtered_pigs_hd'
    ,'stocks_pigs_head':'stocks_pigs_hd'
}
fao_pigs_meat_p.rename(columns=rename_columns ,inplace=True)
datainfo(fao_pigs_meat_p)

datadesc(fao_pigs_meat_p ,CHARACTERIZE_FOLDER)

# ----------------------------------------------------------------------------
# Create a column for countries in scope
# ----------------------------------------------------------------------------
fao_pigs_countries = list(fao_pigs_meat_p['country'].unique())

_fao_pigs_meat_p_inscope = (fao_pigs_meat_p['country'].str.upper().isin(fao_countries_inscope_upcase))

# Add column to data for filtering
fao_pigs_meat_p['country_inscope'] = _fao_pigs_meat_p_inscope

check_fao_pigs_meat_p_scope = list(fao_pigs_meat_p.loc[_fao_pigs_meat_p_inscope ,'country'].unique())

# ----------------------------------------------------------------------------
# Add calcs
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(fao_pigs_meat_p)
datadesc(fao_pigs_meat_p ,CHARACTERIZE_FOLDER)
fao_pigs_meat_p.to_pickle(os.path.join(PRODATA_FOLDER ,'fao_pigs_meat_p.pkl.gz'))

profile = fao_pigs_meat_p.profile_report()
profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'fao_pigs_meat_p_profile.html'))

# ----------------------------------------------------------------------------
# Checks
# ----------------------------------------------------------------------------

#%% Create Chicken Combo

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
# Starting with production data
# Add producer price columns regarding chicken
fao_chickencombo = pd.merge(
   left=fao_chickens_meat_p
   ,right=fao_producerprice_p[[
      'country'
      ,'year'
      ,'producerprice_chickens_carcass_lcupertonne'
      ,'producerprice_chickens_live_lcupertonne'
      ,'producerprice_chickens_carcass_slcpertonne'
      ,'producerprice_chickens_live_slcpertonne'
      ,'producerprice_chickens_carcass_usdpertonne'
      ,'producerprice_chickens_live_usdpertonne'
      ,'producerprice_chickens_carcass_index'
      ,'producerprice_chickens_live_index'
   ]]
   ,on=['country' ,'year']
   ,how='left'
   ,indicator='_merge_prodprice'
)
fao_chickencombo['_merge_prodprice'].value_counts()          # Check number of rows from each table (requires indicator=True)
datainfo(fao_chickencombo)

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(fao_chickencombo)
datadesc(fao_chickencombo ,CHARACTERIZE_FOLDER)
fao_chickencombo.to_pickle(os.path.join(PRODATA_FOLDER ,'fao_chickencombo.pkl.gz'))

profile = fao_chickencombo.profile_report()
profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'fao_chickencombo_profile.html'))

#%% Create Pig Combo

# ----------------------------------------------------------------------------
# Merge
# ----------------------------------------------------------------------------
fao_pigcombo = pd.merge(
   left=fao_pigs_meat_p
   ,right=fao_producerprice_p[[
      'country'
      ,'year'
      ,'producerprice_pigs_carcass_lcupertonne'
      ,'producerprice_pigs_live_lcupertonne'
      ,'producerprice_pigs_carcass_slcpertonne'
      ,'producerprice_pigs_live_slcpertonne'
      ,'producerprice_pigs_carcass_usdpertonne'
      ,'producerprice_pigs_live_usdpertonne'
      ,'producerprice_pigs_carcass_index'
      ,'producerprice_pigs_live_index'
   ]]
   ,on=['country' ,'year']
   ,how='left'
   ,indicator='_merge_prodprice'
)
fao_pigcombo['_merge_prodprice'].value_counts()          # Check number of rows from each table (requires indicator=True)
datainfo(fao_pigcombo)

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(fao_pigcombo)
datadesc(fao_pigcombo ,CHARACTERIZE_FOLDER)
fao_pigcombo.to_pickle(os.path.join(PRODATA_FOLDER ,'fao_pigcombo.pkl.gz'))

profile = fao_pigcombo.profile_report()
profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'fao_pigcombo_profile.html'))
