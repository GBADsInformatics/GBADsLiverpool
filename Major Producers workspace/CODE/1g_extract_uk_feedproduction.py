# UK AHDB Feed production (satisfies R6, but updated to include latest years)
# https://ahdb.org.uk/cereals-oilseeds/cereal-use-in-gb-animal-feed-production

# The structure of this file is difficult to work with
# Rather than import the whole thing, I am importing the segments for the feed
# types I care about from specific rows.
def import_feedproduction_rowset(
      FILENAME
      ,SHEETNAME
      ,STARTROW
      ,NROWS=12
      ,ITEM_SUFFIX=''   # String: Optional suffix to add to all Item lables. Use to distinguish sources if needed.
      ):
   # Import
   OUTPUT_DF = pd.read_excel(FILENAME
                             ,sheet_name=SHEETNAME
                             ,skiprows=STARTROW - 1
                             ,nrows=NROWS
                             )

   # Get feed item as its own column
   feed_item = list(OUTPUT_DF)[0]   # First column is name of item
   OUTPUT_DF['item'] = feed_item + ITEM_SUFFIX

   # Rename first column
   OUTPUT_DF.rename(columns={feed_item:'month'} ,inplace=True)

   return OUTPUT_DF

#%% Feed Production - Poultry

uk_feedproduction_filename = os.path.join(RAWDATA_FOLDER ,'GB_AFproduction_Dec21.xlsx')

# =============================================================================
#### Import desired pieces
# =============================================================================
# ----------------------------------------------------------------------------
# Retail
# ----------------------------------------------------------------------------
# uk_feedproduction_rtl_chickrearing = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
#                                                                    ,ITEM_SUFFIX=' RTL'
#                                                               ,STARTROW=245)
uk_feedproduction_rtl_layers = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
                                                            ,ITEM_SUFFIX=' RTL'
                                                            ,STARTROW=262)
uk_feedproduction_rtl_broilers = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
                                                              ,ITEM_SUFFIX=' RTL'
                                                              ,STARTROW=279)
# uk_feedproduction_rtl_poultrybreeding = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
#                                                                    ,ITEM_SUFFIX=' RTL'
#                                                               ,STARTROW=296)
# uk_feedproduction_rtl_turkey = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
#                                                                    ,ITEM_SUFFIX=' RTL'
#                                                               ,STARTROW=313)
# uk_feedproduction_rtl_otherpoultry = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
#                                                                    ,ITEM_SUFFIX=' RTL'
#                                                               ,STARTROW=330)
# uk_feedproduction_rtl_poultryprotein = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
#                                                                    ,ITEM_SUFFIX=' RTL'
#                                                               ,STARTROW=347)
uk_feedproduction_rtl_totalpoultry = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
                                                                  ,ITEM_SUFFIX=' RTL'
                                                                  ,STARTROW=364)

# ----------------------------------------------------------------------------
# Integrated Poultry Units
# ----------------------------------------------------------------------------
uk_feedproduction_ipu_layers = import_feedproduction_rowset(uk_feedproduction_filename ,'Integrated poultry units'
                                                            ,ITEM_SUFFIX=' IPU'
                                                            ,STARTROW=109
                                                            )
uk_feedproduction_ipu_broilers = import_feedproduction_rowset(uk_feedproduction_filename ,'Integrated poultry units'
                                                              ,ITEM_SUFFIX=' IPU'
                                                              ,STARTROW=75
                                                              )
uk_feedproduction_ipu_totalpoultry = import_feedproduction_rowset(uk_feedproduction_filename ,'Integrated poultry units'
                                                                  ,ITEM_SUFFIX=' IPU'
                                                                  ,STARTROW=160
                                                                  )

# ----------------------------------------------------------------------------
# Stack
# ----------------------------------------------------------------------------
uk_feedproduction_selectpoultry = pd.DataFrame()   # Initialize

uk_feedproduction_selectpoultry = uk_feedproduction_selectpoultry.append(uk_feedproduction_rtl_layers)
uk_feedproduction_selectpoultry = uk_feedproduction_selectpoultry.append(uk_feedproduction_rtl_broilers)
uk_feedproduction_selectpoultry = uk_feedproduction_selectpoultry.append(uk_feedproduction_rtl_totalpoultry)

uk_feedproduction_selectpoultry = uk_feedproduction_selectpoultry.append(uk_feedproduction_ipu_layers)
uk_feedproduction_selectpoultry = uk_feedproduction_selectpoultry.append(uk_feedproduction_ipu_broilers)
uk_feedproduction_selectpoultry = uk_feedproduction_selectpoultry.append(uk_feedproduction_ipu_totalpoultry)

datainfo(uk_feedproduction_selectpoultry)

uk_feedproduction_selectpoultry_items = list(uk_feedproduction_selectpoultry['item'].unique())

# =============================================================================
#### Clean up
# =============================================================================
# Drop difference columns
dropcols = [
   '% difference on the year'
   ,'Actual difference'
   ,'Unnamed: 30'
   ,'Unnamed: 31'
   ,'Unnamed: 32'
   ,'Unnamed: 33'
]
uk_feedproduction_selectpoultry.drop(columns=dropcols ,inplace=True ,errors='ignore')

# Melt year range into single column
uk_feedproduction_selectpoultry_m = uk_feedproduction_selectpoultry.melt(
   id_vars=['item' ,'month']         # Columns to use as ID variables
   # ,value_vars=['col3' ,'col4']     # Columns to "unpivot" to rows. If blank, will use all columns not listed in id_vars.
   ,var_name='year_range'             # Name for new "variable" column
   ,value_name='thsdtonnes'              # Name for new "value" column
)
datainfo(uk_feedproduction_selectpoultry_m)

# Separate years in range
uk_feedproduction_selectpoultry_m[['year_range_1' ,'year_range_2']] = \
   uk_feedproduction_selectpoultry_m['year_range'].str.split('-', expand=True).astype(int)

# Add correct year based on months
# Note these must be ALL UPPERCASE
first_semester_months_upcase = [
   'JAN'
   ,'FEB'
   ,'MAR'
   ,'APR'
   ,'MAY'
   ,'JUN'
]
last_semester_months_upcase = [
   'JUL'
   ,'AUG'
   ,'SEP'
   ,'OCT'
   ,'NOV'
   ,'DEC'
]
_uk_feedproduction_selectpoultry_m_firstsem = (uk_feedproduction_selectpoultry_m['month'].str.upper().isin(first_semester_months_upcase))
_uk_feedproduction_selectpoultry_m_lastsem = (uk_feedproduction_selectpoultry_m['month'].str.upper().isin(last_semester_months_upcase))

uk_feedproduction_selectpoultry_m.loc[_uk_feedproduction_selectpoultry_m_firstsem ,'year'] = uk_feedproduction_selectpoultry_m['year_range_1'] + 1
uk_feedproduction_selectpoultry_m.loc[_uk_feedproduction_selectpoultry_m_lastsem ,'year'] = uk_feedproduction_selectpoultry_m['year_range_1']

datainfo(uk_feedproduction_selectpoultry_m)

# Drop intermediate columns
uk_feedproduction_selectpoultry_m.drop(columns=['year_range' ,'year_range_1' ,'year_range_2'] ,inplace=True)

# Pivot each item to its own column
uk_feedproduction_selectpoultry_m_p = uk_feedproduction_selectpoultry_m.pivot(
   index=['year' ,'month']           # Column(s) to make new index. If blank, uses existing index.
   ,columns='item'        # Column(s) to make new columns
   ,values='thsdtonnes'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
)
cleancolnames(uk_feedproduction_selectpoultry_m_p)

# Add units to column names
uk_feedproduction_selectpoultry_m_p = uk_feedproduction_selectpoultry_m_p.add_prefix('production_')   # To a subset of columns
uk_feedproduction_selectpoultry_m_p = uk_feedproduction_selectpoultry_m_p.add_suffix('_thsdtonnes')

# Reset index to columns
uk_feedproduction_selectpoultry_m_p.reset_index(inplace=True)

datainfo(uk_feedproduction_selectpoultry_m_p)

# =============================================================================
#### Calcs
# =============================================================================
# Add Total production as IPU + RTL
uk_feedproduction_selectpoultry_m_p.eval(
   '''
   production_broiler_chicken_compounds_thsdtonnes = production_broiler_chicken_compounds_ipu_thsdtonnes + production_broiler_chicken_compounds_rtl_thsdtonnes
   production_layers_compounds_thsdtonnes = production_layers_compounds_ipu_thsdtonnes + production_layers_compounds_rtl_thsdtonnes
   production_total_poultry_feed_thsdtonnes = production_total_poultry_feed_rtl_thsdtonnes + production_total_production_ipu_thsdtonnes
   '''
   ,inplace=True
)

# =============================================================================
#### Describe and output
# =============================================================================
datainfo(uk_feedproduction_selectpoultry_m_p)
datadesc(uk_feedproduction_selectpoultry_m_p ,CHARACTERIZE_FOLDER)
uk_feedproduction_selectpoultry_m_p.to_pickle(os.path.join(PRODATA_FOLDER ,'uk_feedproduction_selectpoultry_m_p.pkl.gz'))

# profile = uk_feedproduction_selectpoultry_m_p.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'uk_feedproduction_selectpoultry_m_p_profile.html'))

#%% Feed Production - Swine

uk_feedproduction_filename = os.path.join(RAWDATA_FOLDER ,'GB_AFproduction_Dec21.xlsx')

# =============================================================================
#### Import desired pieces
# =============================================================================
uk_feedproduction_pigstarter = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
                                                              ,STARTROW=126)
uk_feedproduction_piglink = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
                                                              ,STARTROW=143)
uk_feedproduction_piggrower = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
                                                              ,STARTROW=160)
uk_feedproduction_pigfinishing = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
                                                              ,STARTROW=177)
uk_feedproduction_pigbreeding = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
                                                              ,STARTROW=194)
uk_feedproduction_pigproteinconc = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
                                                              ,STARTROW=211)
uk_feedproduction_totalpigfeed = import_feedproduction_rowset(uk_feedproduction_filename ,'Production GB animal feed'
                                                              ,STARTROW=228)

# ----------------------------------------------------------------------------
# Stack
# ----------------------------------------------------------------------------
uk_feedproduction_selectpig = pd.DataFrame()   # Initialize

uk_feedproduction_selectpig = uk_feedproduction_selectpig.append(uk_feedproduction_pigstarter)
uk_feedproduction_selectpig = uk_feedproduction_selectpig.append(uk_feedproduction_piglink)
uk_feedproduction_selectpig = uk_feedproduction_selectpig.append(uk_feedproduction_piggrower)
uk_feedproduction_selectpig = uk_feedproduction_selectpig.append(uk_feedproduction_pigfinishing)
uk_feedproduction_selectpig = uk_feedproduction_selectpig.append(uk_feedproduction_pigbreeding)
uk_feedproduction_selectpig = uk_feedproduction_selectpig.append(uk_feedproduction_pigproteinconc)
uk_feedproduction_selectpig = uk_feedproduction_selectpig.append(uk_feedproduction_totalpigfeed)

datainfo(uk_feedproduction_selectpig)

uk_feedproduction_selectpig_items = list(uk_feedproduction_selectpig['item'].unique())

# =============================================================================
#### Clean up
# =============================================================================
# Drop difference columns
dropcols = [
   '% difference on the year'
   ,'Actual difference'
]
uk_feedproduction_selectpig.drop(columns=dropcols ,inplace=True ,errors='ignore')

# Melt year range into single column
uk_feedproduction_selectpig_m = uk_feedproduction_selectpig.melt(
   id_vars=['item' ,'month']         # Columns to use as ID variables
   # ,value_vars=['col3' ,'col4']     # Columns to "unpivot" to rows. If blank, will use all columns not listed in id_vars.
   ,var_name='year_range'             # Name for new "variable" column
   ,value_name='thsdtonnes'              # Name for new "value" column
)
datainfo(uk_feedproduction_selectpig_m)

# Change type to numeric
uk_feedproduction_selectpig_m['thsdtonnes'] = pd.to_numeric(uk_feedproduction_selectpig_m['thsdtonnes'] ,errors='coerce')

# Separate years in range
uk_feedproduction_selectpig_m[['year_range_1' ,'year_range_2']] = \
   uk_feedproduction_selectpig_m['year_range'].str.split('-', expand=True).astype(int)

# Add correct year based on months
# Note these must be ALL UPPERCASE
first_semester_months_upcase = [
   'JAN'
   ,'FEB'
   ,'MAR'
   ,'APR'
   ,'MAY'
   ,'JUN'
]
last_semester_months_upcase = [
   'JUL'
   ,'AUG'
   ,'SEP'
   ,'OCT'
   ,'NOV'
   ,'DEC'
]
_uk_feedproduction_selectpig_m_firstsem = (uk_feedproduction_selectpig_m['month'].str.upper().isin(first_semester_months_upcase))
_uk_feedproduction_selectpig_m_lastsem = (uk_feedproduction_selectpig_m['month'].str.upper().isin(last_semester_months_upcase))

uk_feedproduction_selectpig_m.loc[_uk_feedproduction_selectpig_m_firstsem ,'year'] = uk_feedproduction_selectpig_m['year_range_1'] + 1
uk_feedproduction_selectpig_m.loc[_uk_feedproduction_selectpig_m_lastsem ,'year'] = uk_feedproduction_selectpig_m['year_range_1']

datainfo(uk_feedproduction_selectpig_m)

# Drop intermediate columns
uk_feedproduction_selectpig_m.drop(columns=['year_range' ,'year_range_1' ,'year_range_2'] ,inplace=True)

# Pivot each item to its own column
uk_feedproduction_selectpig_m_p = uk_feedproduction_selectpig_m.pivot(
   index=['year' ,'month']           # Column(s) to make new index. If blank, uses existing index.
   ,columns='item'        # Column(s) to make new columns
   ,values='thsdtonnes'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
)
cleancolnames(uk_feedproduction_selectpig_m_p)

# Add units to column names
uk_feedproduction_selectpig_m_p = uk_feedproduction_selectpig_m_p.add_prefix('production_')   # To a subset of columns
uk_feedproduction_selectpig_m_p = uk_feedproduction_selectpig_m_p.add_suffix('_thsdtonnes')

# Reset index to columns
uk_feedproduction_selectpig_m_p.reset_index(inplace=True)

datainfo(uk_feedproduction_selectpig_m_p)

# =============================================================================
#### Describe and output
# =============================================================================
datainfo(uk_feedproduction_selectpig_m_p)
datadesc(uk_feedproduction_selectpig_m_p ,CHARACTERIZE_FOLDER)
uk_feedproduction_selectpig_m_p.to_pickle(os.path.join(PRODATA_FOLDER ,'uk_feedproduction_selectpig_m_p.pkl.gz'))

# profile = uk_feedproduction_selectpig_m_p.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'uk_feedproduction_selectpig_m_p_profile.html'))

# =============================================================================
#### Checks
# =============================================================================
uk_feedproduction_selectpig_m_p.eval(
   '''
   check_totalprod = production_link_early_grower_feed_thsdtonnes + production_pig_breeding_compounds_thsdtonnes + production_pig_finishing_compounds_thsdtonnes + production_pig_growing_compounds_thsdtonnes + production_pig_protein_concentrates_thsdtonnes + production_pig_starters_and_creep_feed_thsdtonnes
   '''
   ,inplace=True
)

#%% Feed Stocks

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
# Yearly numbers measured in June
uk_feedstocks = import_feedproduction_rowset(uk_feedproduction_filename ,'GB animal feed stocks'
                                             ,STARTROW=7 ,NROWS=4)
uk_feedstocks_ipu = import_feedproduction_rowset(uk_feedproduction_filename ,'GB animal feed stocks'
                                                 ,STARTROW=15 ,NROWS=3)
