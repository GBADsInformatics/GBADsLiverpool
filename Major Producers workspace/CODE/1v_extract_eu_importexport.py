#%% About

# EUROSTAT Import/Export data
# Received from William Gilbert

# Eurostat Trade Main Page:
# https://ec.europa.eu/eurostat/web/international-trade-in-goods/data/database

# Data browser with filters applied for Poultry:
   # Note these filters require some adjustment
# https://appsso.eurostat.ec.europa.eu/nui/show.do?query=BOOKMARK_DS-645593_QID_4FECA14_UID_-3F171EB0&layout=PERIOD,L,X,0;REPORTER,L,Y,0;PARTNER,C,Z,0;PRODUCT,L,Z,1;FLOW,L,Z,2;INDICATORS,C,Z,3;&zSelection=DS-645593INDICATORS,VALUE_IN_EUROS;DS-645593PARTNER,EU27_2020_EXTRA;DS-645593PRODUCT,TOTAL;DS-645593FLOW,1;&rankName1=PARTNER_1_2_-1_2&rankName2=INDICATORS_1_2_-1_2&rankName3=FLOW_1_2_-1_2&rankName4=PRODUCT_1_2_-1_2&rankName5=PERIOD_1_0_0_0&rankName6=REPORTER_1_2_0_1&sortC=ASC_-1_FIRST&rStp=&cStp=&rDCh=&cDCh=&rDM=true&cDM=true&footnes=false&empty=true&wai=false&time_mode=NONE&time_most_recent=false&lang=EN&cfo=%23%23%23%2C%23%23%23.%23%23%23&cxt_bm=1&lang=en

# Data browser with filters applied for Swine:
   # https://appsso.eurostat.ec.europa.eu/nui/show.do?query=BOOKMARK_DS-645593_QID_-1147A110_UID_-3F171EB0&layout=PERIOD,L,X,0;REPORTER,L,Y,0;PARTNER,C,Z,0;PRODUCT,B,Z,1;FLOW,L,Z,2;INDICATORS,C,Z,3;&zSelection=DS-645593INDICATORS,VALUE_IN_EUROS;DS-645593PARTNER,WORLD;DS-645593FLOW,1;DS-645593PRODUCT,0103;&rankName1=PARTNER_1_2_-1_2&rankName2=INDICATORS_1_2_-1_2&rankName3=FLOW_1_2_-1_2&rankName4=PRODUCT_1_2_-1_2&rankName5=PERIOD_1_0_0_0&rankName6=REPORTER_1_2_0_1&sortC=ASC_-1_FIRST&rStp=&cStp=&rDCh=&cDCh=&rDM=true&cDM=true&footnes=false&empty=true&wai=false&time_mode=NONE&time_most_recent=false&lang=EN&cfo=%23%23%23%2C%23%23%23.%23%23%23

# Commodity codes are CN codes:
   # Note data might cutoff leading zeros
# https://ec.europa.eu/taxation_customs/business/calculation-customs-duties/customs-tariff/combined-nomenclature_en
# https://eur-lex.europa.eu/legal-content/EN/TXT/PDF/?uri=OJ:L:2021:414:FULL&from=EN

#%% Poultry

# =============================================================================
#### Initial cleanup
# =============================================================================
# ----------------------------------------------------------------------------
# Import and intitial prep
# ----------------------------------------------------------------------------
# Poultry
euro_impexp = pd.read_csv(
   os.path.join(RAWDATA_FOLDER ,'eurostat_impexp_poultry_2011_2021' ,'DS-645593_1_Data.csv')
   ,encoding='ansi'       # Default gave encoding error. 'ansi' worked after using Notepad++ to convert file to ANSI.
)
cleancolnames(euro_impexp)
datainfo(euro_impexp)

products = list(euro_impexp['product_label'].unique())
partners = euro_impexp[['partner' ,'partner_label']].drop_duplicates()
reporters = euro_impexp[['reporter' ,'reporter_label']].drop_duplicates()
periods = list(euro_impexp['period_label'].unique())

# ----------------------------------------------------------------------------
# Pivot to columns
# ----------------------------------------------------------------------------
# Create Year column
euro_impexp[['month_range' ,'year']] = euro_impexp['period_label'].str.split(' ' ,expand=True)
euro_impexp['year'] = euro_impexp['year'].astype('int')

# Change values to numeric
euro_impexp['value'] = euro_impexp['value'].str.replace(',' ,'')  # First remove commas
euro_impexp['value'] = pd.to_numeric(euro_impexp['value'] ,errors='coerce')    # Use to_numeric to handle remaining values like ':'

# Get Product ID lookup
product_desc = euro_impexp[['product' ,'product_label']].drop_duplicates()

# Recode Product Description
#!!! Note this data only contains the 8-digit commodity code, which does not distinguish breeders from fattening birds!
recode_productid = {
   '01051199':'live_gallusdom_lte185g'
   ,'01059400':'live_gallusdom_gt185g'
}
euro_impexp['product_shortname'] = euro_impexp['product'].replace(recode_productid)

# Recode indicators
recode_indicators = {
   'QUANTITY_IN_100KG':'100KG'
   ,'SUPPLEMENTARY_QUANTITY':'HEAD'   # Supplementary quantity is head count. See checks below.
   ,'VALUE_IN_EUROS':'VALUE_IN_EUROS'
}
euro_impexp['indicators_recode'] = euro_impexp['indicators'].replace(recode_indicators)

# Combine product, flow, and indicator
euro_impexp['item'] = euro_impexp['flow_label'] + '_' + euro_impexp['product_shortname'] + '_' + euro_impexp['indicators_recode']

# Pivot
euro_impexp_p = euro_impexp.pivot(
   index=[           # Column(s) to make new index
          'partner'
          ,'partner_label'
          ,'reporter'
          ,'reporter_label'
          ,'year'
          ]
   ,columns='item'        # Column(s) to make new columns
   ,values='value'        # Column to populate row
).reset_index()
cleancolnames(euro_impexp_p)
datainfo(euro_impexp_p)

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(euro_impexp_p)
datadesc(euro_impexp_p ,CHARACTERIZE_FOLDER)

euro_impexp_p.to_pickle(os.path.join(PRODATA_FOLDER ,'euro_impexp_p.pkl.gz'))

# =============================================================================
#### Restructure
# =============================================================================
# Plan: Keep one record per country and year with each country as Reporter
# Add columns to indicate imports/exports to each Partner

import_columns = [i for i in list(euro_impexp_p) if 'import' in i]
export_columns = [i for i in list(euro_impexp_p) if 'export' in i]

# ----------------------------------------------------------------------------
# Imports and exports for each country as REPORTER
# ----------------------------------------------------------------------------
# To partner WORLD
euro_impexp_p_partner_world = euro_impexp_p.loc[euro_impexp_p['partner'] == 'WORLD'].copy()
euro_impexp_p_partner_world = euro_impexp_p_partner_world.drop(columns=['partner' ,'partner_label'])
euro_impexp_p_partner_world = euro_impexp_p_partner_world.add_prefix('world_')
euro_impexp_p_partner_world = euro_impexp_p_partner_world.rename(
   columns={
      "world_reporter":"reporter"
      ,"world_reporter_label":"reporter_label"
      ,"world_year":"year"
   }
)
datainfo(euro_impexp_p_partner_world)

# To partner INTRA-EU
euro_impexp_p_partner_intraeu = euro_impexp_p.loc[euro_impexp_p['partner'] == 'EU_INTRA'].copy()
euro_impexp_p_partner_intraeu = euro_impexp_p_partner_intraeu.drop(columns=['partner' ,'partner_label'])
euro_impexp_p_partner_intraeu = euro_impexp_p_partner_intraeu.add_prefix('intraeu_')
euro_impexp_p_partner_intraeu = euro_impexp_p_partner_intraeu.rename(
   columns={
      "intraeu_reporter":"reporter"
      ,"intraeu_reporter_label":"reporter_label"
      ,"intraeu_year":"year"
   }
)
datainfo(euro_impexp_p_partner_intraeu)

# To partner EXTRA-EU
euro_impexp_p_partner_extraeu = euro_impexp_p.loc[euro_impexp_p['partner'] == 'EU_EXTRA'].copy()
euro_impexp_p_partner_extraeu = euro_impexp_p_partner_extraeu.drop(columns=['partner' ,'partner_label'])
euro_impexp_p_partner_extraeu = euro_impexp_p_partner_extraeu.add_prefix('extraeu_')
euro_impexp_p_partner_extraeu = euro_impexp_p_partner_extraeu.rename(
   columns={
      "extraeu_reporter":"reporter"
      ,"extraeu_reporter_label":"reporter_label"
      ,"extraeu_year":"year"
   }
)
datainfo(euro_impexp_p_partner_extraeu)

# Merge
euro_impexp_p_reporter_perspective = pd.merge(
   left=euro_impexp_p_partner_intraeu
   ,right=euro_impexp_p_partner_extraeu
   ,on=['reporter' ,'reporter_label' ,'year']
   ,how='outer'
   ,indicator='_intra_extra'
)
euro_impexp_p_reporter_perspective['_intra_extra'].value_counts()          # Check number of rows from each table (requires indicator=True)

euro_impexp_p_reporter_perspective = pd.merge(
   left=euro_impexp_p_reporter_perspective
   ,right=euro_impexp_p_partner_world
   ,on=['reporter' ,'reporter_label' ,'year']
   ,how='outer'
   ,indicator='_intra_extra_world'
)
euro_impexp_p_reporter_perspective['_intra_extra_world'].value_counts()          # Check number of rows from each table (requires indicator=True)

datainfo(euro_impexp_p_reporter_perspective)

# ----------------------------------------------------------------------------
# Imports and exports for each country as PARTNER
# Note: since we only have Reporters in the EU, we can only produce intra-EU
# sums for each country as Partner (not extra-EU or World).
# ----------------------------------------------------------------------------
# From individual reporters
_reporter_eutotal = (euro_impexp_p['reporter_label'].str.contains('European Union' ,case=False ,na=False))
print(f"> Selected {_reporter_eutotal.sum() :,} rows.")

euro_impexp_p_reporter_intraeu_indiv = euro_impexp_p.loc[~ _reporter_eutotal].pivot_table(   # Do not include EU total reporter
    index=['partner' ,'partner_label' ,'year']           # Column(s) to make new index
    ,values=[                                            # Column(s) to aggregate
      'export_live_gallusdom_gt185g_100kg'
      ,'export_live_gallusdom_gt185g_head'
      ,'export_live_gallusdom_gt185g_value_in_euros'
      ,'export_live_gallusdom_lte185g_100kg'
      ,'export_live_gallusdom_lte185g_head'
      ,'export_live_gallusdom_lte185g_value_in_euros'
      ,'import_live_gallusdom_gt185g_100kg'
      ,'import_live_gallusdom_gt185g_head'
      ,'import_live_gallusdom_gt185g_value_in_euros'
      ,'import_live_gallusdom_lte185g_100kg'
      ,'import_live_gallusdom_lte185g_head'
      ,'import_live_gallusdom_lte185g_value_in_euros'
      ]
    ,aggfunc='sum'                  # Aggregate function to use. Can pass list or dictionary {'colname':'function'}. See numpy functions https://docs.scipy.org/doc/numpy/reference/routines.statistics.html
    ,fill_value=0                     # Replace missing values with this
)
euro_impexp_p_reporter_intraeu_indiv = euro_impexp_p_reporter_intraeu_indiv.add_prefix('intraeu_indiv_')    # Add prefix to column names (all columns except index columns)
euro_impexp_p_reporter_intraeu_indiv = euro_impexp_p_reporter_intraeu_indiv.reset_index()           # Pivoting will change columns to indexes. Change them back.
datainfo(euro_impexp_p_reporter_intraeu_indiv)

# From Total EU reporter
euro_impexp_p_reporter_intraeu_total = euro_impexp_p.loc[euro_impexp_p['reporter'] == 'EU'].pivot_table(
    index=['partner' ,'partner_label' ,'year']           # Column(s) to make new index
    ,values=[                                            # Column(s) to aggregate
      'export_live_gallusdom_gt185g_100kg'
      ,'export_live_gallusdom_gt185g_head'
      ,'export_live_gallusdom_gt185g_value_in_euros'
      ,'export_live_gallusdom_lte185g_100kg'
      ,'export_live_gallusdom_lte185g_head'
      ,'export_live_gallusdom_lte185g_value_in_euros'
      ,'import_live_gallusdom_gt185g_100kg'
      ,'import_live_gallusdom_gt185g_head'
      ,'import_live_gallusdom_gt185g_value_in_euros'
      ,'import_live_gallusdom_lte185g_100kg'
      ,'import_live_gallusdom_lte185g_head'
      ,'import_live_gallusdom_lte185g_value_in_euros'
      ]
    ,aggfunc='sum'                  # Aggregate function to use. Can pass list or dictionary {'colname':'function'}. See numpy functions https://docs.scipy.org/doc/numpy/reference/routines.statistics.html
    ,fill_value=0                     # Replace missing values with this
)
euro_impexp_p_reporter_intraeu_total = euro_impexp_p_reporter_intraeu_total.add_prefix('intraeu_total_')    # Add prefix to column names (all columns except index columns)
euro_impexp_p_reporter_intraeu_total = euro_impexp_p_reporter_intraeu_total.reset_index()           # Pivoting will change columns to indexes. Change them back.
datainfo(euro_impexp_p_reporter_intraeu_total)

# Merge
euro_impexp_p_partner_perspective = pd.merge(
   left=euro_impexp_p_reporter_intraeu_indiv
   ,right=euro_impexp_p_reporter_intraeu_total
   ,on=['partner' ,'partner_label' ,'year']
   ,how='outer'
   ,indicator='_merge'
)
euro_impexp_p_partner_perspective['_merge'].value_counts()          # Check number of rows from each table (requires indicator=True)
datainfo(euro_impexp_p_partner_perspective)

# =============================================================================
#### Describe and output
# =============================================================================
datainfo(euro_impexp_p_reporter_perspective)
datadesc(euro_impexp_p_reporter_perspective ,CHARACTERIZE_FOLDER)

euro_impexp_p_reporter_perspective.to_pickle(os.path.join(PRODATA_FOLDER ,'euro_impexp_p_reporter_perspective.pkl.gz'))

# =============================================================================
#### Checks
# =============================================================================

# ----------------------------------------------------------------------------
# Is "supplementary quantity" head count?  Yes.
# ----------------------------------------------------------------------------
# Check kg per head
euro_impexp_p.eval(
   '''
   check_export_gt185g_kgperhead = export_live_gallusdom_gt185g_100kg * 100 / export_live_gallusdom_gt185g_head
   check_import_gt185g_kgperhead = import_live_gallusdom_gt185g_100kg * 100 / import_live_gallusdom_gt185g_head

   check_export_lte185g_kgperhead = export_live_gallusdom_lte185g_100kg * 100 / export_live_gallusdom_lte185g_head
   check_import_lte185g_kgperhead = import_live_gallusdom_lte185g_100kg * 100 / import_live_gallusdom_lte185g_head
   '''
   ,inplace=True
)
plot_histogram_withinset(euro_impexp_p ,'check_export_gt185g_kgperhead' ,NBINS=20)
# plot_histogram_withinset(euro_impexp_p ,'check_import_gt185g_kgperhead' ,NBINS=20)
# plot_histogram_withinset(euro_impexp_p ,'check_export_lte185g_kgperhead' ,NBINS=20)
# plot_histogram_withinset(euro_impexp_p ,'check_import_lte185g_kgperhead' ,NBINS=20)

# Excluding outliers
plot_histogram_withinset(euro_impexp_p.loc[euro_impexp_p['check_export_gt185g_kgperhead'] < 10]
               ,'check_export_gt185g_kgperhead' ,NBINS=20)
plot_histogram_withinset(euro_impexp_p.loc[euro_impexp_p['check_import_gt185g_kgperhead'] < 10]
               ,'check_import_gt185g_kgperhead' ,NBINS=20)
plot_histogram_withinset(euro_impexp_p.loc[euro_impexp_p['check_export_lte185g_kgperhead'] < 10]
               ,'check_export_lte185g_kgperhead' ,NBINS=20)
plot_histogram_withinset(euro_impexp_p.loc[euro_impexp_p['check_import_lte185g_kgperhead'] < 10]
               ,'check_import_lte185g_kgperhead' ,NBINS=20)

# ----------------------------------------------------------------------------
# Add check columns from Partner perspective to Reporter perspective
# ----------------------------------------------------------------------------
euro_impexp_p_partner_perspective_tomerge = euro_impexp_p_partner_perspective.add_prefix('aspartner_')
datainfo(euro_impexp_p_partner_perspective_tomerge)

euro_impexp_p_reporter_perspective_checkpartner = pd.merge(
   left=euro_impexp_p_reporter_perspective
   ,right=euro_impexp_p_partner_perspective_tomerge
   ,left_on=['reporter' ,'year']
   ,right_on=['aspartner_partner' ,'aspartner_year']
   ,how='left'
   ,indicator='_merge'
)
euro_impexp_p_reporter_perspective_checkpartner['_merge'].value_counts()          # Check number of rows from each table (requires indicator=True)

datainfo(euro_impexp_p_reporter_perspective_checkpartner)

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(euro_impexp_p_reporter_perspective_checkpartner)
datadesc(euro_impexp_p_reporter_perspective_checkpartner ,CHARACTERIZE_FOLDER)

euro_impexp_p_reporter_perspective_checkpartner.to_pickle(os.path.join(PRODATA_FOLDER ,'euro_impexp_p_reporter_perspective_checkpartner.pkl.gz'))

# ----------------------------------------------------------------------------
# Calcs and checks
# ----------------------------------------------------------------------------
# Compare intra-EU exports as Reporter to imports as Partner
euro_impexp_p_reporter_perspective_checkpartner.eval(
    '''
    check_ratio_maturebirds_kg = intraeu_export_live_gallusdom_gt185g_100kg / aspartner_intraeu_indiv_import_live_gallusdom_gt185g_100kg
    check_ratio_maturebirds_hd = intraeu_export_live_gallusdom_gt185g_head / aspartner_intraeu_indiv_import_live_gallusdom_gt185g_head

    check_ratio_chicks_kg = intraeu_export_live_gallusdom_lte185g_100kg / aspartner_intraeu_indiv_import_live_gallusdom_lte185g_100kg
    check_ratio_chicks_hd = intraeu_export_live_gallusdom_lte185g_head / aspartner_intraeu_indiv_import_live_gallusdom_lte185g_head
    '''
    ,inplace=True
)

# Plot Mature birds
plot_histogram_withinset(euro_impexp_p_reporter_perspective_checkpartner ,'check_ratio_maturebirds_kg')
plot_histogram_withinset(
   euro_impexp_p_reporter_perspective_checkpartner
   ,'check_ratio_maturebirds_kg'
   ,WHERE_VAR_LTE=np.inf      # Note how this reduces N
)
plot_histogram_withinset(
   euro_impexp_p_reporter_perspective_checkpartner
   ,'check_ratio_maturebirds_kg'
   ,WHERE_VAR_LTE=1000
)

plot_histogram_withinset(euro_impexp_p_reporter_perspective_checkpartner ,'check_ratio_maturebirds_hd')
plot_histogram_withinset(
   euro_impexp_p_reporter_perspective_checkpartner
   ,'check_ratio_maturebirds_hd'
   ,WHERE_VAR_LTE=np.inf      # Note how this reduces N
)
plot_histogram_withinset(
   euro_impexp_p_reporter_perspective_checkpartner
   ,'check_ratio_maturebirds_hd'
   ,WHERE_VAR_LTE=40
)

# Plot chicks
plot_histogram_withinset(euro_impexp_p_reporter_perspective_checkpartner ,'check_ratio_chicks_kg')
plot_histogram_withinset(
   euro_impexp_p_reporter_perspective_checkpartner
   ,'check_ratio_chicks_kg'
   ,WHERE_VAR_LTE=np.inf      # Note how this reduces N
)
plot_histogram_withinset(
   euro_impexp_p_reporter_perspective_checkpartner
   ,'check_ratio_chicks_kg'
   ,WHERE_VAR_LTE=40
)

plot_histogram_withinset(euro_impexp_p_reporter_perspective_checkpartner ,'check_ratio_chicks_hd')
plot_histogram_withinset(
   euro_impexp_p_reporter_perspective_checkpartner
   ,'check_ratio_chicks_hd'
   ,WHERE_VAR_LTE=np.inf      # Note how this reduces N
)
plot_histogram_withinset(
   euro_impexp_p_reporter_perspective_checkpartner
   ,'check_ratio_chicks_hd'
   ,WHERE_VAR_LTE=70
)

#%% Swine

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
euro_impexp_swine = pd.read_csv(
    os.path.join(RAWDATA_FOLDER ,'eurostat_impexp_swine_2011_2021' ,'DS-645593_1_Data.csv')
    ,encoding='ansi'       # Default gave encoding error. 'ansi' worked.
)
cleancolnames(euro_impexp_swine)
datainfo(euro_impexp_swine)

# ----------------------------------------------------------------------------
# Basic cleanup
# ----------------------------------------------------------------------------
# Create Year column
euro_impexp_swine[['month_range' ,'year']] = euro_impexp_swine['period'].str.split(' ' ,expand=True)
euro_impexp_swine['year'] = euro_impexp_swine['year'].astype('int')

# Change values to numeric
euro_impexp_swine['value'] = euro_impexp_swine['value'].str.replace(',' ,'')  # First remove commas
euro_impexp_swine['value'] = pd.to_numeric(euro_impexp_swine['value'] ,errors='coerce')    # Use to_numeric to handle remaining values like ':'

# Get Product ID lookup
product_desc_swine = euro_impexp_swine[['product' ,'product_label']].drop_duplicates()

# Recode Product Description
recode_productid = {
   103:'live_swine'
   ,10310:'live_swine_purebred_breeding'
   ,1031000:'live_swine_purebred_breeding'
   ,10391:'live_swine_purebred_lt50kg'
   ,1039110:'live_swine_dom_lt50kg'
   ,1039190:'live_swine_nondom_lt50kg'
   ,10392:'live_swine_purebred_gte50kg'
   ,1039211:'live_sows_dom_nongilt_gte160kg'
   ,1039219:'live_swine_dom_gte50kg'
   ,1039290:'live_swine_nondom_gte50kg'
}
euro_impexp_swine['product_shortname'] = euro_impexp_swine['product'].replace(recode_productid)

# Drop redundant products: 10310 (same as 10310000)
_droprows = (euro_impexp_swine['product'] == 10310)
print(f"> Dropping {_droprows.sum() :,} rows.")
euro_impexp_swine = euro_impexp_swine.drop(euro_impexp_swine.loc[_droprows].index).reset_index(drop=True)

# Recode indicators
recode_indicators = {
   'QUANTITY_IN_100KG':'100KG'
   ,'SUPPLEMENTARY_QUANTITY':'HEAD'
   ,'VALUE_IN_EUROS':'VALUE_IN_EUROS'
}
euro_impexp_swine['indicators_recode'] = euro_impexp_swine['indicators'].replace(recode_indicators)

# ----------------------------------------------------------------------------
# Pivot to columns
# ----------------------------------------------------------------------------
# Combine product, flow, and indicator
euro_impexp_swine['item'] = euro_impexp_swine['flow'] + '_' + euro_impexp_swine['product_shortname'] + '_' + euro_impexp_swine['indicators_recode']

# Pivot
check_pivot = euro_impexp_swine[['partner' ,'reporter' ,'year' ,'item' ,'value']].value_counts()
euro_impexp_swine_p = euro_impexp_swine.pivot(
   index=[           # Column(s) to make new index
          'partner'
          ,'reporter'
          ,'year'
          ]
   ,columns='item'        # Column(s) to make new columns
   ,values='value'        # Column to populate row
).reset_index()
cleancolnames(euro_impexp_swine_p)
datainfo(euro_impexp_swine_p)

# Fill nan with zero
euro_impexp_swine_p = euro_impexp_swine_p.replace(np.nan ,0)

# ----------------------------------------------------------------------------
# Calcs
# ----------------------------------------------------------------------------
# =============================================================================
# # UPDATE: not using the purebred non-breeding category as that appears to be mislabeled
# # CN codes 010391 and 010392 are higher-level categories for <50kg and >50kg
# # We only want domestic and non-domestic animals, codes 01039110, 01039190, 01039219, and 01039290
# # Also not using my estimated head_touse because domestic and nondomestic variables have nonzero head counts

# # Purebred import/export columns have data for total weight (_100kg) while corresponding head count is zero
# # Estimate head counts from total weight by assuming an average weight
# # LT50kg is a large interval for young pigs, spanning the Nursery phase and 3-4 weeks of the grow-finish phase
# # Assuming these are generally imported/exported at the end of the Nursery phase
# swine_lt50kg_avgweight_kg = 28.6   # Weight at end of Nursery phase from PIC standard
# swine_gte50kg_avgweight_kg = 100   # Medium weight for fully grown pigs (147 days) from PIC standard
#
# # Exports GTE50kg
# euro_impexp_swine_p['export_live_swine_dom_gte50kg_esthead'] =\
#    np.floor(euro_impexp_swine_p['export_live_swine_dom_gte50kg_100kg'] * 100 / swine_gte50kg_avgweight_kg)
# euro_impexp_swine_p['export_live_swine_nondom_gte50kg_esthead'] =\
#    np.floor(euro_impexp_swine_p['export_live_swine_nondom_gte50kg_100kg'] * 100 / swine_gte50kg_avgweight_kg)
# euro_impexp_swine_p['export_live_swine_purebred_gte50kg_esthead'] =\
#    np.floor(euro_impexp_swine_p['export_live_swine_purebred_gte50kg_100kg'] * 100 / swine_gte50kg_avgweight_kg)
#
# # Exports LT50kg
# euro_impexp_swine_p['export_live_swine_dom_lt50kg_esthead'] =\
#    np.floor(euro_impexp_swine_p['export_live_swine_dom_lt50kg_100kg'] * 100 / swine_lt50kg_avgweight_kg)
# euro_impexp_swine_p['export_live_swine_nondom_lt50kg_esthead'] =\
#    np.floor(euro_impexp_swine_p['export_live_swine_nondom_lt50kg_100kg'] * 100 / swine_lt50kg_avgweight_kg)
# euro_impexp_swine_p['export_live_swine_purebred_lt50kg_esthead'] =\
#    np.floor(euro_impexp_swine_p['export_live_swine_purebred_lt50kg_100kg'] * 100 / swine_lt50kg_avgweight_kg)
#
# # Imports GTE50kg
# euro_impexp_swine_p['import_live_swine_dom_gte50kg_esthead'] =\
#    np.floor(euro_impexp_swine_p['import_live_swine_dom_gte50kg_100kg'] * 100 / swine_gte50kg_avgweight_kg)
# euro_impexp_swine_p['import_live_swine_nondom_gte50kg_esthead'] =\
#    np.floor(euro_impexp_swine_p['import_live_swine_nondom_gte50kg_100kg'] * 100 / swine_gte50kg_avgweight_kg)
# euro_impexp_swine_p['import_live_swine_purebred_gte50kg_esthead'] =\
#    np.floor(euro_impexp_swine_p['import_live_swine_purebred_gte50kg_100kg'] * 100 / swine_gte50kg_avgweight_kg)
#
# # Imports LT50kg
# euro_impexp_swine_p['import_live_swine_dom_lt50kg_esthead'] =\
#    np.floor(euro_impexp_swine_p['import_live_swine_dom_lt50kg_100kg'] * 100 / swine_lt50kg_avgweight_kg)
# euro_impexp_swine_p['import_live_swine_nondom_lt50kg_esthead'] =\
#    np.floor(euro_impexp_swine_p['import_live_swine_nondom_lt50kg_100kg'] * 100 / swine_lt50kg_avgweight_kg)
# euro_impexp_swine_p['import_live_swine_purebred_lt50kg_esthead'] =\
#    np.floor(euro_impexp_swine_p['import_live_swine_purebred_lt50kg_100kg'] * 100 / swine_lt50kg_avgweight_kg)
#
# # If regular _head variable is not zero, use it instead of _esthead
# def take_first_nonmissingorzero(
#       INPUT_DF
#       ,CANDIDATE_COLS      # List of strings: columns to search for non-missing value, in this order
#    ):
#    # Initialize new column with first candidate column
#    OUTPUT_SERIES = INPUT_DF[CANDIDATE_COLS[0]].copy()
#
#    for CANDIDATE in CANDIDATE_COLS:       # For each candidate column...
#       newcol_null = (OUTPUT_SERIES.isnull()) | (OUTPUT_SERIES == 0)      # ...where new column is missing or zero...
#       OUTPUT_SERIES.loc[newcol_null] = INPUT_DF.loc[newcol_null ,CANDIDATE]    # ...fill with candidate
#
#    return OUTPUT_SERIES
#
# # Exports GTE50kg
# candidates = ['export_live_swine_dom_gte50kg_head' ,'export_live_swine_dom_gte50kg_esthead']
# euro_impexp_swine_p['export_live_swine_dom_gte50kg_head_touse'] = take_first_nonmissingorzero(euro_impexp_swine_p ,candidates)
#
# candidates = ['export_live_swine_nondom_gte50kg_head' ,'export_live_swine_nondom_gte50kg_esthead']
# euro_impexp_swine_p['export_live_swine_nondom_gte50kg_head_touse'] = take_first_nonmissingorzero(euro_impexp_swine_p ,candidates)
#
# candidates = ['export_live_swine_purebred_gte50kg_head' ,'export_live_swine_purebred_gte50kg_esthead']
# euro_impexp_swine_p['export_live_swine_purebred_gte50kg_head_touse'] = take_first_nonmissingorzero(euro_impexp_swine_p ,candidates)
#
# # Exports LT50kg
# candidates = ['export_live_swine_dom_lt50kg_head' ,'export_live_swine_dom_lt50kg_esthead']
# euro_impexp_swine_p['export_live_swine_dom_lt50kg_head_touse'] = take_first_nonmissingorzero(euro_impexp_swine_p ,candidates)
#
# candidates = ['export_live_swine_nondom_lt50kg_head' ,'export_live_swine_nondom_lt50kg_esthead']
# euro_impexp_swine_p['export_live_swine_nondom_lt50kg_head_touse'] = take_first_nonmissingorzero(euro_impexp_swine_p ,candidates)
#
# candidates = ['export_live_swine_purebred_lt50kg_head' ,'export_live_swine_purebred_lt50kg_esthead']
# euro_impexp_swine_p['export_live_swine_purebred_lt50kg_head_touse'] = take_first_nonmissingorzero(euro_impexp_swine_p ,candidates)
#
# # Imports GTE50kg
# candidates = ['import_live_swine_dom_gte50kg_head' ,'import_live_swine_dom_gte50kg_esthead']
# euro_impexp_swine_p['import_live_swine_dom_gte50kg_head_touse'] = take_first_nonmissingorzero(euro_impexp_swine_p ,candidates)
#
# candidates = ['import_live_swine_nondom_gte50kg_head' ,'import_live_swine_nondom_gte50kg_esthead']
# euro_impexp_swine_p['import_live_swine_nondom_gte50kg_head_touse'] = take_first_nonmissingorzero(euro_impexp_swine_p ,candidates)
#
# candidates = ['import_live_swine_purebred_gte50kg_head' ,'import_live_swine_purebred_gte50kg_esthead']
# euro_impexp_swine_p['import_live_swine_purebred_gte50kg_head_touse'] = take_first_nonmissingorzero(euro_impexp_swine_p ,candidates)
#
# # Imports LT50kg
# candidates = ['import_live_swine_dom_lt50kg_head' ,'import_live_swine_dom_lt50kg_esthead']
# euro_impexp_swine_p['import_live_swine_dom_lt50kg_head_touse'] = take_first_nonmissingorzero(euro_impexp_swine_p ,candidates)
#
# candidates = ['import_live_swine_nondom_lt50kg_head' ,'import_live_swine_nondom_lt50kg_esthead']
# euro_impexp_swine_p['import_live_swine_nondom_lt50kg_head_touse'] = take_first_nonmissingorzero(euro_impexp_swine_p ,candidates)
#
# candidates = ['import_live_swine_purebred_lt50kg_head' ,'import_live_swine_purebred_lt50kg_esthead']
# euro_impexp_swine_p['import_live_swine_purebred_lt50kg_head_touse'] = take_first_nonmissingorzero(euro_impexp_swine_p ,candidates)
#
# =============================================================================
# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(euro_impexp_swine_p)
datadesc(euro_impexp_swine_p ,CHARACTERIZE_FOLDER)

euro_impexp_swine_p.to_pickle(os.path.join(PRODATA_FOLDER ,'euro_impexp_swine_p.pkl.gz'))

# =============================================================================
#### Checks
# =============================================================================
# ----------------------------------------------------------------------------
# UK import_live_swine_100kg shows values, but _head is zero
# This is true for all countries!!
# ----------------------------------------------------------------------------
check_uk_toworld = euro_impexp_swine_p.query("(reporter == 'United Kingdom') & (partner == 'WORLD')")

# ----------------------------------------------------------------------------
# Estimated head vs. reported head
# ----------------------------------------------------------------------------
plotdata = euro_impexp_swine_p.loc[euro_impexp_swine_p['partner'] == 'WORLD']
snplt = sns.relplot(
   data=plotdata
   ,x='import_live_swine_dom_gte50kg_head'
   ,y='import_live_swine_dom_gte50kg_esthead'
   ,kind='scatter'      # 'scatter' (default): scatterplot. 'line': line chart.
   ,hue='reporter'             # Variable to color by. If numeric, will create heat coloring.
   ,height=6         # Height of figure in inches. If paneled, height of each panel.
   ,aspect=1.5         # Width of figure as multiplier on height. If paneled, width of each panel.
)
sns.lineplot(
   data=plotdata
   ,x='import_live_swine_dom_gte50kg_head'
   ,y='import_live_swine_dom_gte50kg_head'
   ,ax=snplt.ax            # Use axes (both X and Y) associated with plot object (above)
   ,linestyle='--' ,linewidth=2 ,color='gray'          # Other arguments that function on base matplotlib plot
   ,ci=None
   ,legend=False     # To disable legend so not duplicated from base plot
)
snplt = sns.relplot(
   data=plotdata
   ,x='import_live_swine_nondom_gte50kg_head'
   ,y='import_live_swine_nondom_gte50kg_esthead'
   ,kind='scatter'      # 'scatter' (default): scatterplot. 'line': line chart.
   ,hue='reporter'             # Variable to color by. If numeric, will create heat coloring.
   ,height=6         # Height of figure in inches. If paneled, height of each panel.
   ,aspect=1.5         # Width of figure as multiplier on height. If paneled, width of each panel.
)
sns.lineplot(
   data=plotdata
   ,x='import_live_swine_nondom_gte50kg_head'
   ,y='import_live_swine_nondom_gte50kg_head'
   ,ax=snplt.ax            # Use axes (both X and Y) associated with plot object (above)
   ,linestyle='--' ,linewidth=2 ,color='gray'          # Other arguments that function on base matplotlib plot
   ,ci=None
   ,legend=False     # To disable legend so not duplicated from base plot
)
snplt = sns.relplot(
   data=plotdata
   ,x='import_live_swine_purebred_gte50kg_head'
   ,y='import_live_swine_purebred_gte50kg_esthead'
   ,kind='scatter'      # 'scatter' (default): scatterplot. 'line': line chart.
   ,hue='reporter'             # Variable to color by. If numeric, will create heat coloring.
   ,height=6         # Height of figure in inches. If paneled, height of each panel.
   ,aspect=1.5         # Width of figure as multiplier on height. If paneled, width of each panel.
)
sns.lineplot(
   data=plotdata
   ,x='import_live_swine_purebred_gte50kg_head'
   ,y='import_live_swine_purebred_gte50kg_head'
   ,ax=snplt.ax            # Use axes (both X and Y) associated with plot object (above)
   ,linestyle='--' ,linewidth=2 ,color='gray'          # Other arguments that function on base matplotlib plot
   ,ci=None
   ,legend=False     # To disable legend so not duplicated from base plot
)

# ----------------------------------------------------------------------------
# Can I use total exports? How large are components?
# ----------------------------------------------------------------------------
plotdata = euro_impexp_swine_p.loc[euro_impexp_swine_p['reporter'].isin(['United Kingdom' ,'Netherlands' ,'Poland'])]
snplt = sns.relplot(
   data=plotdata
   ,x='year'
   ,y='export_live_swine_dom_gte50kg_head'
   ,kind='line'      # 'scatter' (default): scatterplot. 'line': line chart.
   ,color='blue'     # Fixed color rather than color by variable
   ,marker='o'       # Add markers
   ,label='Dom GTE50kg'  # Label for this line in legend
   ,ci=None				# None: don't draw confidence interval. Integer (0,100): draw confidence interval at this level. Default: 95.
   ,col='reporter'       # Variables to panel by
   ,col_wrap=2        # Integer: draw this many plots as column facets before moving to next row
   ,facet_kws={
      'sharey':False      # True: facets share y-axis limits
   }
   ,height=4         # Height of figure in inches. If paneled, height of each panel.
   ,aspect=1.5         # Width of figure as multiplier on height. If paneled, width of each panel.
)
# sns.lineplot(  # Overlay is not working
#    data=plotdata
#    ,x='year'
#    ,y='export_live_swine_nondom_gte50kg_head'
#    ,ax=snplt.axes            # Use axes (both X and Y) associated with plot object (above)
#    ,color='green'          # Other arguments that function on base matplotlib plot
#    ,marker='o'       # Add markers
#    ,label='Non-Dom GTE50kg'  # Label for this line in legend
#    ,ci=None
#    ,col='reporter'       # Variables to panel by
#    ,col_wrap=2        # Integer: draw this many plots as column facets before moving to next row
#    ,facet_kws={
#        'sharey':False      # True: facets share y-axis limits
#    }
# )



plotdata = euro_impexp_swine_p.query("(reporter == 'United Kingdom') & (partner == 'WORLD')")
plt.plot('year' ,'export_live_swine_dom_gte50kg_head'
         ,data=plotdata
         ,label='Dom' ,color='blue'     # Labels will be used in legend
         )
plt.plot('year' ,'export_live_swine_nondom_gte50kg_head'
         ,data=plotdata
         ,label='Non-Dom' ,color='green'     # Labels will be used in legend
         )
plt.plot('year' ,'export_live_swine_purebred_gte50kg_head'
         ,data=plotdata
         ,label='Purebred' ,color='red'     # Labels will be used in legend
         )
plt.legend(loc='upper right')
plt.title('UK Exports GTE 50kg (head)')
plt.show()

plt.plot('year' ,'export_live_swine_dom_lt50kg_head'
         ,data=plotdata
         ,label='Dom' ,color='blue'     # Labels will be used in legend
         )
plt.plot('year' ,'export_live_swine_nondom_lt50kg_head'
         ,data=plotdata
         ,label='Non-Dom' ,color='green'     # Labels will be used in legend
         )
plt.plot('year' ,'export_live_swine_purebred_lt50kg_head'
         ,data=plotdata
         ,label='Purebred' ,color='red'     # Labels will be used in legend
         )
plt.legend(loc='upper right')
plt.title('UK Exports LT 50kg (head)')
plt.show()

plt.plot('year' ,'import_live_swine_dom_gte50kg_head'
         ,data=plotdata
         ,label='Dom' ,color='blue'     # Labels will be used in legend
         )
plt.plot('year' ,'import_live_swine_nondom_gte50kg_head'
         ,data=plotdata
         ,label='Non-Dom' ,color='green'     # Labels will be used in legend
         )
plt.plot('year' ,'import_live_swine_purebred_gte50kg_head'
         ,data=plotdata
         ,label='Purebred' ,color='red'     # Labels will be used in legend
         )
plt.legend(loc='upper right')
plt.title('UK imports GTE 50kg (head)')
plt.show()

plt.plot('year' ,'import_live_swine_dom_lt50kg_head'
         ,data=plotdata
         ,label='Dom' ,color='blue'     # Labels will be used in legend
         )
plt.plot('year' ,'import_live_swine_nondom_lt50kg_head'
         ,data=plotdata
         ,label='Non-Dom' ,color='green'     # Labels will be used in legend
         )
plt.plot('year' ,'import_live_swine_purebred_lt50kg_head'
         ,data=plotdata
         ,label='Purebred' ,color='red'     # Labels will be used in legend
         )
plt.legend(loc='upper right')
plt.title('UK imports LT 50kg (head)')
plt.show()
