# UK government feed data (R6 & R7)

#%% Compound Feed Price
# https://www.gov.uk/government/statistical-data-sets/animal-feed-prices#:~:text=Compound%20feed%20prices%2C%20quarterly

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
uk_feedprice_imp = pd.read_excel(os.path.join(RAWDATA_FOLDER ,'uk-commodityprices-compounds-03feb22.ods')
                                 ,sheet_name='Compound_Feed_Prices'
                                 ,skiprows=list(range(8))   # Exclude descriptive rows and footnotes
                                 )
cleancolnames(uk_feedprice_imp)
datainfo(uk_feedprice_imp)

rename_columns = {
   'unnamed:_0':'time_period'
   ,'cattle_and_calf_feed__1_':'feedprice_cattle_calf_gbppertonne'
   ,'pig_feed__2_':'feedprice_pig_gbppertonne'
   ,'poultry_feed__3_':'feedprice_poultry_gbppertonne'
   ,'sheep_feed__4_':'feedprice_sheep_gbppertonne'
   }
uk_feedprice = uk_feedprice_imp.rename(columns=rename_columns)
datainfo(uk_feedprice)

# Drop blank rows at end
uk_feedprice.dropna(
   axis=0                        # 0 = drop rows, 1 = drop columns
   ,subset=[                     # List (opt): if dropping rows, only consider these columns in NA check
      'feedprice_cattle_calf_gbppertonne'
      ,'feedprice_pig_gbppertonne'
      ,'feedprice_poultry_gbppertonne'
      ,'feedprice_sheep_gbppertonne'
      ]
   ,how='all'                    # String: 'all' = drop rows that have all missing values. 'any' = drop rows that have any missing values.
   ,inplace=True                 # True: modify data in place
)

# ----------------------------------------------------------------------------
# Get Yearly
# ----------------------------------------------------------------------------
# Get Year and quarter
time_periods = list(uk_feedprice['time_period'].unique())
uk_feedprice['year'] = 2000 + uk_feedprice['time_period'].str[-2:].astype(int)       # Last 2 characters
datainfo(uk_feedprice)

uk_feedprice.loc[uk_feedprice['time_period'].str.contains('jan' ,case=False ,na=False) ,'quarter'] = 1
uk_feedprice.loc[uk_feedprice['time_period'].str.contains('apr' ,case=False ,na=False) ,'quarter'] = 2
uk_feedprice.loc[uk_feedprice['time_period'].str.contains('jul' ,case=False ,na=False) ,'quarter'] = 3
uk_feedprice.loc[uk_feedprice['time_period'].str.contains('oct' ,case=False ,na=False) ,'quarter'] = 4

#!!! Don't SUM price! Want AVERAGE price for the year. Preferably weighting each quarter by the amount used.
# Do this in the Assembly code after bringing together price and production data.
# uk_feedprice_p = uk_feedprice.pivot_table(
#    index='year'                     # Column(s) to make new index
#    ,values=[         # Column(s) to aggregate
#       'feedprice_cattle_calf_gbppertonne'
#       ,'feedprice_pig_gbppertonne'
#       ,'feedprice_poultry_gbppertonne'
#       ,'feedprice_sheep_gbppertonne'
#       ]
#    ,aggfunc='sum'                  # Aggregate function to use. Can pass list or dictionary {'colname':'function'}. See numpy functions https://docs.scipy.org/doc/numpy/reference/routines.statistics.html
# )
# uk_feedprice_p.reset_index(inplace=True)
# datainfo(uk_feedprice_p)

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(uk_feedprice)
datadesc(uk_feedprice ,CHARACTERIZE_FOLDER)
uk_feedprice.to_pickle(os.path.join(PRODATA_FOLDER ,'uk_feedprice.pkl.gz'))

profile = uk_feedprice.profile_report()
profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'uk_feedprice_profile.html'))

#%% Straights (Commodity) Feed Price
# https://www.gov.uk/government/statistical-data-sets/animal-feed-prices#:~:text=pounds%20(%C2%A3)%20per%20tonne.-,Straights%20feed%20prices%2C%20monthly,-(ODS%2C%2069.8%20KB

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
# Create list for naming month columns
monthlist = [
   'jan'
   ,'feb'
   ,'mar'
   ,'apr'
   ,'may'
   ,'jun'
   ,'jul'
   ,'aug'
   ,'sep'
   ,'oct'
   ,'nov'
   ,'dec'
]

# Loop over all tabs in spreadsheet
uk_feedcmdtyprice_imp = pd.DataFrame()   # Initialize data to hold all years
for YEAR in range(2011 ,2022):
   # Import single sheet (year)
   uk_feedcmdtyprice_sheet_imp = pd.read_excel(os.path.join(RAWDATA_FOLDER ,'uk-commodityprices-straights-20jan22.ods')
                                               ,sheet_name=str(YEAR)
                                               ,skiprows=list(range(8))   # Exclude descriptive rows at top
                                               )

   # Assign column names
   #!!! Note this assumes the same column ordering for all tabs
   uk_feedcmdtyprice_sheet_imp.columns = ['commodity'] + monthlist

   # Add Year
   uk_feedcmdtyprice_sheet_imp['year'] = YEAR

   # Drop empty rows (including descriptive footer)
   uk_feedcmdtyprice_sheet_imp.dropna(
      axis=0                        # 0 = drop rows, 1 = drop columns
      ,subset=monthlist             # List (opt): if dropping rows, only consider these columns in NA check
      ,how='all'                    # String: 'all' = drop rows that have all missing values. 'any' = drop rows that have any missing values.
      ,inplace=True                 # True: modify data in place
   )

   # Append
   uk_feedcmdtyprice_imp = uk_feedcmdtyprice_imp.append(uk_feedcmdtyprice_sheet_imp)

datainfo(uk_feedcmdtyprice_imp)

# Pivot month to a single column
uk_feedcmdtyprice_imp_m = uk_feedcmdtyprice_imp.melt(
   id_vars=['commodity' ,'year']         # Columns to use as ID variables
   ,value_vars=monthlist     # Columns to "unpivot" to rows
   ,var_name='month'    # Name for new "variable" column
   ,value_name='price_gbppertonne'    # Name for new "value" column
)

uk_feedcmdtyprice_imp_m['price_gbppertonne'] = pd.to_numeric(
   uk_feedcmdtyprice_imp_m['price_gbppertonne']
   ,errors='coerce'                       # 'coerce': errors become nan. 'ignore': errors will return input. 'raise': errors with throw an error.
)

commodities_years = uk_feedcmdtyprice_imp_m[['commodity' ,'year']].value_counts()

# ----------------------------------------------------------------------------
# Get Yearly
# ----------------------------------------------------------------------------
# Want AVERAGE price for the year. Preferably weighting each month by the amount used.
# Do this in the Assembly code after bringing together price and production data.

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(uk_feedcmdtyprice_imp_m)
datadesc(uk_feedcmdtyprice_imp_m ,CHARACTERIZE_FOLDER)
uk_feedcmdtyprice_imp_m.to_pickle(os.path.join(PRODATA_FOLDER ,'uk_feedcmdtyprice_imp_m.pkl.gz'))

profile = uk_feedcmdtyprice_imp_m.profile_report()
profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'uk_feedcmdtyprice_imp_m_profile.html'))
