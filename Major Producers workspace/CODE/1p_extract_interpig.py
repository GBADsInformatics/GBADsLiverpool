#%% About
'''
InterPIG is an association of countries that share data about pig production and costs.

They publish yearly reports as PDFs, which I accessed from the UK's AHDB website here:
https://ahdb.org.uk/cost-of-production-in-selected-countries

17 Countries participate:
- Great Britain – Agriculture and Horticulture Development Board (AHDB)
- Austria – VLV Upper Austria and Chamber of Agriculture Lower Austria
- Flemish Government and Boerenbond Belgie
- Brazil – Embrapa Swine and Poultry
	Brazil submits data for two regions: Mato Grosso (MT) and Santa Catarina (SC)
- Canada – Canadian Pork Council
- Czech Republic – Institute of Agricultural Economics and Information (UZEI)
- Denmark – SEGES
- Finland – Atria
- France – Institute of Pork (IFIP)
- Germany – Thuenen Institute of Farm Economics and Interessengemeinschaft der Schweinehalter (ISN)
- Hungary – AKI Research Institute of Agricultural Economics
- Ireland – Teagasc
- Italy – Research Centre for Animal Production (CRPA)
- Netherlands – Wageningen Economic Research
- Spain – SIP Consultors
- Sweden – Farm and Animal Health
- USA – Iowa State University
'''
#%% Define file locations

interpig_pdf_location = os.path.join(RAWDATA_FOLDER ,'interPIG')

interpig_pdf_2015 = os.path.join(interpig_pdf_location ,'2015-pig-cost-of-production-in-selected-countries.pdf')
interpig_pdf_2016 = os.path.join(interpig_pdf_location ,'2016-pig-cost-of-production-in-selected-countries.pdf')
interpig_pdf_2017 = os.path.join(interpig_pdf_location ,'2017-pig-cost-of-production-in-selected-countries.pdf')
interpig_pdf_2018 = os.path.join(interpig_pdf_location ,'CostofPigProduction2018_200302_WEB.pdf')
interpig_pdf_2019 = os.path.join(interpig_pdf_location ,'CostOfPigProduction_20193995-WEB2.pdf')
interpig_pdf_2020 = os.path.join(interpig_pdf_location ,'CostOfPigProduction_2020_4568_161121_WEB.pdf')

#%% Define functions

def read_interpig_singleyear(
      PDF_FILE
      ,PDF_PAGE
      ,PDF_TABLENUM                 # If there are multiple tables on the page, use this one (zero indexed!).
      ,UNIT_DESC                    # String: Units of measure. Will be added to column names.
      ,FIRST_TABLE_STARTROW         # Start row for first table (zero indexed!)
      ,FIRST_TABLE_ENDROW
      ,SECOND_TABLE_STARTROW=None   # If there are multiple tables read as one, specify start row for second table
      ):
   funcname = inspect.currentframe().f_code.co_name

   df_list = tabula.read_pdf(PDF_FILE
                             ,pages=PDF_PAGE
                             ,pandas_options={'header':None}
                             ,lattice=True 	    # True: use lattice mode (if there are ruling lines separating cells)
                             )
   df = df_list[PDF_TABLENUM]

   # Get rows for first table
   df_a = df.iloc[FIRST_TABLE_STARTROW:FIRST_TABLE_ENDROW + 1]

   # Drop duplicate rows, using the first column as key
   df_a = df_a.drop_duplicates(subset=[0] ,keep='first')

   if SECOND_TABLE_STARTROW:
      # Get rows for second table
      df_b = df.iloc[SECOND_TABLE_STARTROW:]

      # Drop duplicate rows, using the first column as key
      df_b = df_b.drop_duplicates(subset=[0] ,keep='first')

      # Merge pieces, using the first column as key
      df_c = pd.merge(left=df_a ,right=df_b ,on=0 ,how='outer')

   else:
      df_c = df

   # Drop empty columns
   df_c = df_c.dropna(axis=1 ,how='all')

   # Transpose
   df_c_t = df_c.transpose()

   # Promote first row to column names
   colnames = list(df_c_t.iloc[0])
   colnames_cln = [re.sub('[^A-Za-z0-9]+', '', str(name)) for name in colnames]  # Remove regular expressions from names
   df_c_t.columns = colnames_cln
   df_c_t = df_c_t.drop(index=0).reset_index(drop=True)  # Remove row used
   cleancolnames(df_c_t)

   # Add units suffix
   df_c_t = df_c_t.add_suffix(f'_{UNIT_DESC}')

   # Rename columns
   rename_columns = {
      f'nan_{UNIT_DESC}':'country'
   }
   df_c_t = df_c_t.rename(columns=rename_columns)
   # print(f"\n<{funcname}> After renaming, columns are: {list(df_c_t)}")

   # Convert to numeric
   numeric_cols = list(df_c_t)
   numeric_cols.remove('country')
   for COL in numeric_cols:
      df_c_t[COL] = df_c_t[COL].str.replace(',' ,'').astype('float')

   return df_c_t

def read_interpig_multiyear(
      PDF_FILE
      ,PDF_PAGE
      ,PDF_TABLENUM                 # If there are multiple tables on the page, use this one (zero indexed!).
      ,FIRST_TABLE_STARTROW         # Start row for first table (zero indexed!)
      ,FIRST_TABLE_ENDROW
      ,SECOND_TABLE_STARTROW=None   # If there are multiple tables read as one, specify start row for second table
      ,SECOND_TABLE_ENDROW=None
      ,THIRD_TABLE_STARTROW=None    # If there are multiple tables read as one, specify start row for third table
      ,THIRD_TABLE_ENDROW=None
      ):
   funcname = inspect.currentframe().f_code.co_name

   df_list = tabula.read_pdf(PDF_FILE
                             ,pages=PDF_PAGE
                             ,pandas_options={'header':None}
                             ,lattice=True 	    # True: use lattice mode (if there are ruling lines separating cells)
                             )
   df = df_list[PDF_TABLENUM]

   # Get rows for first table
   df_a = df.iloc[FIRST_TABLE_STARTROW:FIRST_TABLE_ENDROW + 1]

   # Get country list
   countrylist_a = list(df_a.iloc[0])
   countrylist_a = [item for item in countrylist_a if isinstance(item, str)]  # Drop nan
   print(f"\n<{funcname}> Country list from first section: {countrylist_a}")

   # Create list with each country repeated for the number of years
   countrylist = []
   for country in countrylist_a:
      countrylist = countrylist + [country, country, country]

   # Drop duplicate rows, using the first column as key
   df_a = df_a.drop_duplicates(subset=[0] ,keep='last')  # Keep='last' to keep year row, drop country row

   if SECOND_TABLE_STARTROW:
      # Get rows for second table
      df_b = df.iloc[SECOND_TABLE_STARTROW:SECOND_TABLE_ENDROW + 1]

      # Get country list
      countrylist_b = list(df_b.iloc[0])
      countrylist_b = [item for item in countrylist_b if isinstance(item, str)]  # Drop nan
      print(f"\n<{funcname}> Country list from second section: {countrylist_b}")

      # Add to combined country list
      for country in countrylist_b:
         countrylist = countrylist + [country, country, country]

      # Drop duplicate rows, using the first column as key
      df_b = df_b.drop_duplicates(subset=[0] ,keep='last')  # Keep='last' to keep year row, drop country row

      # Merge pieces, using the first column as key
      df_merged = pd.merge(left=df_a ,right=df_b ,on=0 ,how='outer')

      if THIRD_TABLE_STARTROW:
         # Get rows for second table
         df_c = df.iloc[THIRD_TABLE_STARTROW:THIRD_TABLE_ENDROW + 1]

         # Get country list
         countrylist_c = list(df_c.iloc[0])
         countrylist_c = [item for item in countrylist_c if isinstance(item, str)]  # Drop nan
         print(f"\n<{funcname}> Country list from third section: {countrylist_c}")

         # Add to combined country list
         for country in countrylist_c:
            countrylist = countrylist + [country, country, country]

         # Drop duplicate rows, using the first column as key
         df_c = df_c.drop_duplicates(subset=[0] ,keep='last')  # Keep='last' to keep year row, drop country row

         # Merge pieces, using the first column as key
         df_merged = pd.merge(left=df_merged ,right=df_c ,on=0 ,how='outer')

   else:
      df_merged = df_a

   # Drop empty columns
   df_merged = df_merged.dropna(axis=1 ,how='all')

   # Transpose
   df_merged_t = df_merged.transpose()

   # Promote first row to column names
   colnames = list(df_merged_t.iloc[0])
   colnames_cln = [re.sub('[^A-Za-z0-9]+', '', str(name)) for name in colnames]  # Remove regular expressions from names
   df_merged_t.columns = colnames_cln
   df_merged_t = df_merged_t.drop(index=0).reset_index(drop=True)  # Remove row used
   cleancolnames(df_merged_t)

   # Change columns to numeric
   for COL in list(df_merged_t):
      df_merged_t[COL] = df_merged_t[COL].str.replace(',' ,'').astype('float')

   # Rename columns
   rename_columns = {
      'nan':'year'
   }
   df_merged_t = df_merged_t.rename(columns=rename_columns)

   # Add country names
   df_merged_t['country'] = countrylist

   return df_merged_t

#%% Read Financial Performance Summary

# =============================================================================
#### DEV Read 2020
# =============================================================================
interpig_fps_2020_list = tabula.read_pdf(interpig_pdf_2020
                                          ,pages=9
                                          ,pandas_options={'header':None}
                                          ,lattice=True 	    # True: use lattice mode (if there are ruling lines separating cells)
                                          )
interpig_fps_2020 = interpig_fps_2020_list[0]

# Split table and merge
interpig_fps_2020_a = interpig_fps_2020.iloc[0:8]
interpig_fps_2020_b = interpig_fps_2020.iloc[8:]
interpig_fps_2020_c = pd.merge(
    left=interpig_fps_2020_a
    ,right=interpig_fps_2020_b
    ,on=0
    ,how='outer'
)

# Drop empty columns
interpig_fps_2020_c = interpig_fps_2020_c.dropna(
    axis=1                        # 0 = drop rows, 1 = drop columns
    ,how='all'                    # String: 'all' = drop rows / columns that have ALL missing values. 'any' = drop rows / columns that have ANY missing values.
)

# Transpose
interpig_fps_2020_c_t = interpig_fps_2020_c.transpose()

# Promote first row to column names
colnames = list(interpig_fps_2020_c_t.iloc[0])
colnames_cln = [re.sub('[^A-Za-z0-9]+', '', str(name)) for name in colnames]  # Remove regular expressions from names
interpig_fps_2020_c_t.columns = colnames_cln
interpig_fps_2020_c_t = interpig_fps_2020_c_t.drop(index=0).reset_index(drop=True)

cleancolnames(interpig_fps_2020_c_t)

# Add units suffix
interpig_fps_2020_c_t = interpig_fps_2020_c_t.add_suffix('_gbpperkgcarc')

# Rename columns
rename_columns = {
    'nan_gbpperkgcarc':'country'
}
interpig_fps_2020_c_t = interpig_fps_2020_c_t.rename(columns=rename_columns)

# Convert to numeric
numeric_cols = list(interpig_fps_2020_c_t)
numeric_cols.remove('country')
for COL in numeric_cols:
    interpig_fps_2020_c_t[COL] = interpig_fps_2020_c_t[COL].astype('float')

datainfo(interpig_fps_2020_c_t)

# =============================================================================
#### Read 2015-2020 with function
# =============================================================================
interpig_fps_2020 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2020
   ,PDF_PAGE=9
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='gbpperkgcarc'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=7
   ,SECOND_TABLE_STARTROW=8
)
interpig_fps_2019 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2019
   ,PDF_PAGE=9
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='gbpperkgcarc'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=1
   ,FIRST_TABLE_ENDROW=8
   ,SECOND_TABLE_STARTROW=9
)
interpig_fps_2018 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2018
   ,PDF_PAGE=8
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='gbpperkgcarc'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=1
   ,FIRST_TABLE_ENDROW=8
   ,SECOND_TABLE_STARTROW=9
)
interpig_fps_2017 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2017
   ,PDF_PAGE=8
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='gbpperkgcarc'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=7
   ,SECOND_TABLE_STARTROW=8
)
interpig_fps_2016 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2016
   ,PDF_PAGE=7
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='gbpperkgcarc'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=1
   ,FIRST_TABLE_ENDROW=8
)
interpig_fps_2015 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2015
   ,PDF_PAGE=10
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='gbpperkgcarc'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=7
   ,SECOND_TABLE_STARTROW=9
)

# =============================================================================
#### Read 3-year table from 2015
# =============================================================================
# From 2015 file, read alternate table that includes data back to 2013
interpig_fps_2013to2015_list = tabula.read_pdf(interpig_pdf_2015
                                                ,pages=13
                                                ,pandas_options={'header':None}
                                                ,lattice=True 	    # True: use lattice mode (if there are ruling lines separating cells)
                                                )

# =============================================================================
#### Stack
# =============================================================================
# Add year
interpig_fps_2015['year'] = 2015
interpig_fps_2016['year'] = 2016
interpig_fps_2017['year'] = 2017
interpig_fps_2018['year'] = 2018
interpig_fps_2019['year'] = 2019
interpig_fps_2020['year'] = 2020

# Stack
interpig_financial_2015to2020 = pd.concat(
   [interpig_fps_2015
    ,interpig_fps_2016
    ,interpig_fps_2017
    ,interpig_fps_2018
    ,interpig_fps_2019
    ,interpig_fps_2020
    ]
   ,axis=0
   ,join='outer'
)

# Drop missing column
del interpig_financial_2015to2020['notetotalsmaynotaddupduetorounding_gbpperkgcarc']

# -----------------------------------------------------------------------------
# Fix country
# -----------------------------------------------------------------------------
# Remove special characters
interpig_financial_2015to2020['country'] = interpig_financial_2015to2020['country'].apply(lambda x: re.sub('[^A-Za-z0-9 ]+', '', x))

countries_financial = list(interpig_financial_2015to2020['country'].unique())

# Fix Brazil in 2017: missing labels for state (MT or SC)
# Judging from other years, MT is the one with the lower values
_fix_brazil = (interpig_financial_2015to2020['country'] == 'BRA') & (interpig_financial_2015to2020['year'] == 2017)
interpig_financial_2015to2020.loc[(_fix_brazil) & (interpig_financial_2015to2020['feed_gbpperkgcarc'] == 0.58)
                                  , 'country'] = 'BRAMT'
interpig_financial_2015to2020.loc[(_fix_brazil) & (interpig_financial_2015to2020['feed_gbpperkgcarc'] != 0.58)
                                  , 'country'] = 'BRASC'

# Recode
recode_country = {
   # 'AUS':''
   'AVEEU':'EU AVERAGE'
   # ,'BEL':''
   ,'BRAMT':'BRA_MT'
   ,'BRASC':'BRA_SC'
   # ,'CAN':''
   # ,'DEN':''
   ,'EU AVERAGE':'EU AVERAGE'
   ,'EUAVE':'EU AVERAGE'
   ,'EUAVERAGE':'EU AVERAGE'
   # ,'FIN':''
   # ,'FRA':''
   # ,'GB':''    # For 2015-2017 GB is not broken out by indoor/outdoor
   ,'GBIN':'GB_IN'
   ,'GBOUT':'GB_OUT'
   # ,'GER':''
   # ,'HUN':''
   # ,'IRE':''
   # ,'ITA':''
   # ,'NL':''
   # ,'SPA':''
   # ,'SWE':''
   # ,'USA':''
}
interpig_financial_2015to2020['country'] = interpig_financial_2015to2020['country'].replace(recode_country)
interpig_financial_2015to2020['country_upcase'] = interpig_financial_2015to2020['country'].str.upper()

datainfo(interpig_financial_2015to2020)

#%% Read Feed Prices

# =============================================================================
#### DEV Read single file
# =============================================================================
interpig_feedprice_2016_list = tabula.read_pdf(interpig_pdf_2016
                                               ,pages=8
                                               ,pandas_options={'header':None}
                                               ,lattice=True 	    # True: use lattice mode (if there are ruling lines separating cells)
                                               )

# =============================================================================
#### Read 2016-2020 tables in GBP
# =============================================================================
interpig_feedprice_gbp_2020 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2020
   ,PDF_PAGE=11
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='gbppertonne'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=4
   ,SECOND_TABLE_STARTROW=5
)
interpig_feedprice_gbp_2019 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2019
   ,PDF_PAGE=11
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='gbppertonne'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=1
   ,FIRST_TABLE_ENDROW=5
   ,SECOND_TABLE_STARTROW=6
)
interpig_feedprice_gbp_2018 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2018
   ,PDF_PAGE=10
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='gbppertonne'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=1
   ,FIRST_TABLE_ENDROW=5
   ,SECOND_TABLE_STARTROW=6
)
interpig_feedprice_gbp_2017 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2017
   ,PDF_PAGE=9
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='gbppertonne'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=1
   ,FIRST_TABLE_ENDROW=5
   ,SECOND_TABLE_STARTROW=7
)
interpig_feedprice_gbp_2016 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2016
   ,PDF_PAGE=8
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='gbppertonne'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=5
   ,SECOND_TABLE_STARTROW=6
)
# 2015 file only has multiyear table, but it is in a different format from other multiyear tables.

# =============================================================================
#### Read 2016-2020 tables in Euros
# =============================================================================
interpig_feedprice_euro_2020 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2020
   ,PDF_PAGE=11
   ,PDF_TABLENUM=1                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='europertonne'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=4
   ,SECOND_TABLE_STARTROW=5
)
interpig_feedprice_euro_2019 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2019
   ,PDF_PAGE=11
   ,PDF_TABLENUM=1                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='europertonne'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=1
   ,FIRST_TABLE_ENDROW=5
   ,SECOND_TABLE_STARTROW=6
)
interpig_feedprice_euro_2018 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2018
   ,PDF_PAGE=10
   ,PDF_TABLENUM=1                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='europertonne'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=1
   ,FIRST_TABLE_ENDROW=5
   ,SECOND_TABLE_STARTROW=6
)
interpig_feedprice_euro_2017 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2017
   ,PDF_PAGE=9
   ,PDF_TABLENUM=1                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='europertonne'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=1
   ,FIRST_TABLE_ENDROW=5
   ,SECOND_TABLE_STARTROW=7
)
interpig_feedprice_euro_2016 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2016
   ,PDF_PAGE=8
   ,PDF_TABLENUM=1                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC='europertonne'        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=5
   ,SECOND_TABLE_STARTROW=6
)

# =============================================================================
#### Merge Euro and GBP columns
# =============================================================================
interpig_feedprice_2016 = pd.merge(
   left=interpig_feedprice_gbp_2016
   ,right=interpig_feedprice_euro_2016
   ,on='country'
   ,how='outer'
)
interpig_feedprice_2017 = pd.merge(
   left=interpig_feedprice_gbp_2017
   ,right=interpig_feedprice_euro_2017
   ,on='country'
   ,how='outer'
)
interpig_feedprice_2018 = pd.merge(
   left=interpig_feedprice_gbp_2018
   ,right=interpig_feedprice_euro_2018
   ,on='country'
   ,how='outer'
)
interpig_feedprice_2019 = pd.merge(
   left=interpig_feedprice_gbp_2019
   ,right=interpig_feedprice_euro_2019
   ,on='country'
   ,how='outer'
)
interpig_feedprice_2020 = pd.merge(
   left=interpig_feedprice_gbp_2020
   ,right=interpig_feedprice_euro_2020
   ,on='country'
   ,how='outer'
)

# =============================================================================
#### Stack
# =============================================================================
# Add year
interpig_feedprice_2016['year'] = 2016
interpig_feedprice_2017['year'] = 2017
interpig_feedprice_2018['year'] = 2018
interpig_feedprice_2019['year'] = 2019
interpig_feedprice_2020['year'] = 2020

# Stack
interpig_feedprice_2016to2020 = pd.concat(
   [interpig_feedprice_2016
    ,interpig_feedprice_2017
    ,interpig_feedprice_2018
    ,interpig_feedprice_2019
    ,interpig_feedprice_2020
    ]
   ,axis=0
   ,join='outer'
)

# Fill average farm feed price column
affp_null = (interpig_feedprice_2016to2020['averagefarmfeedprice_gbppertonne'].isnull())     # Where col1 is missing...
interpig_feedprice_2016to2020.loc[affp_null ,'averagefarmfeedprice_gbppertonne'] = \
   interpig_feedprice_2016to2020.loc[affp_null ,'averagefarmfeedpricepertonne_gbppertonne']    # ...fill with col2
del interpig_feedprice_2016to2020['averagefarmfeedpricepertonne_gbppertonne']

affp_null = (interpig_feedprice_2016to2020['averagefarmfeedprice_europertonne'].isnull())     # Where col1 is missing...
interpig_feedprice_2016to2020.loc[affp_null ,'averagefarmfeedprice_europertonne'] = \
   interpig_feedprice_2016to2020.loc[affp_null ,'averagefarmfeedpricepertonne_europertonne']    # ...fill with col2
del interpig_feedprice_2016to2020['averagefarmfeedpricepertonne_europertonne']

# Drop cols
del interpig_feedprice_2016to2020['tonne_gbppertonne']
del interpig_feedprice_2016to2020['tonne_europertonne']

# -----------------------------------------------------------------------------
# Fix country
# -----------------------------------------------------------------------------
# Remove special characters
interpig_feedprice_2016to2020['country'] = interpig_feedprice_2016to2020['country'].apply(lambda x: re.sub('[^A-Za-z0-9 ]+', '', x))

countries_feedprice = list(interpig_feedprice_2016to2020['country'].unique())

# Fix Brazil in 2017: missing labels for state (MT or SC)
# Judging from other years, MT is the one with the lower values
_fix_brazil = (interpig_feedprice_2016to2020['country'] == 'BRA') & (interpig_feedprice_2016to2020['year'] == 2017)
interpig_feedprice_2016to2020.loc[(_fix_brazil) & (interpig_feedprice_2016to2020['sow_gbppertonne'] == 147.56)
                                  , 'country'] = 'BRAMT'
interpig_feedprice_2016to2020.loc[(_fix_brazil) & (interpig_feedprice_2016to2020['sow_gbppertonne'] != 147.56)
                                  , 'country'] = 'BRASC'

# Recode
recode_country = {
   # 'AUS':''
   # ,'BEL':''
   'BRA MT':'BRA_MT'
   ,'BRA SC':'BRA_SC'
   ,'BRAMT':'BRA_MT'
   ,'BRASC':'BRA_SC'
   # ,'CAN':''
   # ,'DEN':''
   ,'EU AVE':'EU AVERAGE'
   ,'EU AVERAGE':'EU AVERAGE'
   ,'EUAVERAGE':'EU AVERAGE'
   # ,'FIN':''
   # ,'FRA':''
   # ,'GB':''
   ,'GBIN':'GB_IN'
   ,'GBOUT':'GB_OUT'
   # ,'GER':''
   # ,'HUN':''
   # ,'IRE':''
   # ,'ITA':''
   # ,'NL':''
   # ,'SPA':''
   # ,'SWE':''
   # ,'USA':''
}
interpig_feedprice_2016to2020['country'] = interpig_feedprice_2016to2020['country'].replace(recode_country)
interpig_feedprice_2016to2020['country_upcase'] = interpig_feedprice_2016to2020['country'].str.upper()

datainfo(interpig_feedprice_2016to2020)

#%% Read Physical Performance

# =============================================================================
#### DEV Read single file
# =============================================================================
interpig_physical_2015to2017_list = tabula.read_pdf(interpig_pdf_2017
                                                    ,pages=13
                                                    ,pandas_options={'header':None}
                                                    ,lattice=True 	    # True: use lattice mode (if there are ruling lines separating cells)
                                                    )

# =============================================================================
#### Read 2017 and 2020 files with function
# =============================================================================
# -----------------------------------------------------------------------------
# 2020 file: read years 2018-2020
# -----------------------------------------------------------------------------
# Read each page
interpig_physical_2018to2020_1of4 = read_interpig_multiyear(
   PDF_FILE=interpig_pdf_2020
   ,PDF_PAGE=17
   ,PDF_TABLENUM=0              # If there are multiple tables on the page, use this one (zero-indexed).
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=12
   ,SECOND_TABLE_STARTROW=13
   ,SECOND_TABLE_ENDROW=26
)
interpig_physical_2018to2020_2of4 = read_interpig_multiyear(
   PDF_FILE=interpig_pdf_2020
   ,PDF_PAGE=18
   ,PDF_TABLENUM=0              # If there are multiple tables on the page, use this one (zero-indexed).
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=12
   ,SECOND_TABLE_STARTROW=13
   ,SECOND_TABLE_ENDROW=26
)
interpig_physical_2018to2020_3of4 = read_interpig_multiyear(
   PDF_FILE=interpig_pdf_2020
   ,PDF_PAGE=19
   ,PDF_TABLENUM=0              # If there are multiple tables on the page, use this one (zero-indexed).
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=12
   ,SECOND_TABLE_STARTROW=13
   ,SECOND_TABLE_ENDROW=26
)
interpig_physical_2018to2020_4of4 = read_interpig_multiyear(
   PDF_FILE=interpig_pdf_2020
   ,PDF_PAGE=20
   ,PDF_TABLENUM=0              # If there are multiple tables on the page, use this one (zero-indexed).
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=12
)

# -----------------------------------------------------------------------------
# 2017 file: read years 2015-2017
# -----------------------------------------------------------------------------
# Read each page
interpig_physical_2015to2017_1of2 = read_interpig_multiyear(
   PDF_FILE=interpig_pdf_2017
   ,PDF_PAGE=13
   ,PDF_TABLENUM=0              # If there are multiple tables on the page, use this one (zero-indexed).
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=12
   ,SECOND_TABLE_STARTROW=13
   ,SECOND_TABLE_ENDROW=25
   ,THIRD_TABLE_STARTROW=26
   ,THIRD_TABLE_ENDROW=38
)
interpig_physical_2015to2017_2of2 = read_interpig_multiyear(
   PDF_FILE=interpig_pdf_2017
   ,PDF_PAGE=14
   ,PDF_TABLENUM=0              # If there are multiple tables on the page, use this one (zero-indexed).
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=12
   ,SECOND_TABLE_STARTROW=13
   ,SECOND_TABLE_ENDROW=25
   ,THIRD_TABLE_STARTROW=26
   ,THIRD_TABLE_ENDROW=38
)

# =============================================================================
#### Stack
# =============================================================================
interpig_physical_2015to2020 = pd.concat(
   [interpig_physical_2018to2020_1of4
    ,interpig_physical_2018to2020_2of4
    ,interpig_physical_2018to2020_3of4
    ,interpig_physical_2018to2020_4of4
    ,interpig_physical_2015to2017_1of2
    ,interpig_physical_2015to2017_2of2
    ]
   ,axis=0
   ,join='outer'
)

# Drop blank rows (dunno why they're here)
interpig_physical_2015to2020 = interpig_physical_2015to2020.dropna(
   axis=0                        # 0 = drop rows, 1 = drop columns
   ,subset=[         # List (opt): if dropping rows, only consider these columns in NA check
      'pigsweanedsowyear'
      ,'pigsrearedsowyear'
      ,'pigssoldsowyear'
      ,'litterssowyear'
      ,'rearingmortality'
      ,'finishingmortality'
      ,'finishingdailyliveweightgaingday'
      ,'finishingfeedconversionratio'
      ,'averageliveweightatslaughterkg'
      ,'averagecarcaseweightcoldkg'
      ,'carcasemeatproductionsowyearkg'
   ]
   ,how='all'                    # String: 'all' = drop rows / columns that have ALL missing values. 'any' = drop rows / columns that have ANY missing values.
).reset_index(drop=True)         # If dropping rows, reset index

# -----------------------------------------------------------------------------
# Fix country
# -----------------------------------------------------------------------------
# Remove special characters
interpig_physical_2015to2020['country'] = interpig_physical_2015to2020['country'].apply(lambda x: re.sub('[^A-Za-z0-9 ]+', '', x))

countries_physical = list(interpig_physical_2015to2020['country'].unique())

# Recode
recode_country = {
   # 'AUS':''
   # ,'BEL':''
   'BRA MT':'BRA_MT'
   ,'BRA SC':'BRA_SC'
   # ,'CAN':''
   # ,'DEN':''
   # ,'EU AVERAGE':''
   # ,'EU average':''
   # ,'FIN':''
   # ,'FRA':''
   # ,'GB':''
   ,'GB IN':'GB_IN'
   ,'GB OUT':'GB_OUT'
   # ,'GER':''
   # ,'HUN':''
   # ,'IRE':''
   # ,'ITA':''
   # ,'NL':''
   # ,'SPA':''
   # ,'SWE':''
   # ,'USA':''
}
interpig_physical_2015to2020['country'] = interpig_physical_2015to2020['country'].replace(recode_country)
interpig_physical_2015to2020['country_upcase'] = interpig_physical_2015to2020['country'].str.upper()

datainfo(interpig_physical_2015to2020)

#%% Read Industry Trends
# Contains Breeding Sow numbers for each country

# =============================================================================
#### DEV Read single file
# =============================================================================
interpig_industrytrends_2020_list = tabula.read_pdf(interpig_pdf_2020
                                                    ,pages=22
                                                    ,pandas_options={'header':None}
                                                    ,lattice=True 	    # True: use lattice mode (if there are ruling lines separating cells)
                                                    )

# =============================================================================
#### Read 2015-2020
# =============================================================================
interpig_industrytrends_2020 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2020
   ,PDF_PAGE=22
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC=''        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=7
   ,SECOND_TABLE_STARTROW=8
)
interpig_industrytrends_2019 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2019
   ,PDF_PAGE=22
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC=''        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=1
   ,FIRST_TABLE_ENDROW=8
   ,SECOND_TABLE_STARTROW=9
)
interpig_industrytrends_2018 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2018
   ,PDF_PAGE=21
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC=''        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=1
   ,FIRST_TABLE_ENDROW=8
   ,SECOND_TABLE_STARTROW=9
)
interpig_industrytrends_2017a = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2017
   ,PDF_PAGE=17
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC=''        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=7
   # ,SECOND_TABLE_STARTROW=8    # Doesn't work. Must consider them separate tables.
)
interpig_industrytrends_2017b = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2017
   ,PDF_PAGE=17
   ,PDF_TABLENUM=1                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC=''        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=7
)
interpig_industrytrends_2016a = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2016
   ,PDF_PAGE=16
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC=''        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=7
   # ,SECOND_TABLE_STARTROW=8    # Doesn't work. Must consider them separate tables.
)
interpig_industrytrends_2016b = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2016
   ,PDF_PAGE=16
   ,PDF_TABLENUM=1                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC=''        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=7
)
interpig_industrytrends_2015 = read_interpig_singleyear(
   PDF_FILE=interpig_pdf_2015
   ,PDF_PAGE=21
   ,PDF_TABLENUM=0                  # If there are multiple tables on the page, use this one (zero-indexed).
   ,UNIT_DESC=''        # String: Units of measure. Will be added to column names.
   ,FIRST_TABLE_STARTROW=0
   ,FIRST_TABLE_ENDROW=7
   ,SECOND_TABLE_STARTROW=8
)

# =============================================================================
#### Stack
# =============================================================================
# Add year
interpig_industrytrends_2015['year'] = 2015
interpig_industrytrends_2016a['year'] = 2016
interpig_industrytrends_2016b['year'] = 2016
interpig_industrytrends_2017a['year'] = 2017
interpig_industrytrends_2017b['year'] = 2017
interpig_industrytrends_2018['year'] = 2018
interpig_industrytrends_2019['year'] = 2019
interpig_industrytrends_2020['year'] = 2020

# Stack
interpig_industrytrends_2015to2020 = pd.concat(
   [interpig_industrytrends_2015
    ,interpig_industrytrends_2016a
    ,interpig_industrytrends_2016b
    ,interpig_industrytrends_2017a
    ,interpig_industrytrends_2017b
    ,interpig_industrytrends_2018
    ,interpig_industrytrends_2019
    ,interpig_industrytrends_2020
    ]
   ,axis=0
   ,join='outer'
)

# Fill pig meat consumption per head column
pmc_null = (interpig_industrytrends_2015to2020['pigmeatconsumptionkgperhead_'].isnull())     # Where col1 is missing...
interpig_industrytrends_2015to2020.loc[pmc_null ,'pigmeatconsumptionkgperhead_'] = \
   interpig_industrytrends_2015to2020.loc[pmc_null ,'pigmeatconsumptionkghead_']    # ...fill with col2
del interpig_industrytrends_2015to2020['pigmeatconsumptionkghead_']

# Recode country to match other interPIG tables
countries_indtrends = list(interpig_industrytrends_2015to2020['country'].unique())
recode_country = {
#    'AUS':''
#    ,'BEL':''
#    ,'BRA':''
#    ,'CAN':''
#    ,'CZE':''
#    ,'DEN':''
#    ,'FIN':''
#    ,'FRA':''
#    ,'GER':''
#    ,'HUN':''
#    ,'IRE':''
#    ,'ITA':''
#    ,'NL':''
#    ,'POL':''
    'SP':'SPA'
#    ,'SPA':''
#    ,'SWE':''
    ,'UK':'GB'
#    ,'USA':''
}
interpig_industrytrends_2015to2020['country'] = interpig_industrytrends_2015to2020['country'].replace(recode_country)
interpig_industrytrends_2015to2020['country_upcase'] = interpig_industrytrends_2015to2020['country'].str.upper()

datainfo(interpig_industrytrends_2015to2020)

#%% Merge all and export

countries_financial = list(interpig_financial_2015to2020['country_upcase'].unique())
countries_feedprice = list(interpig_feedprice_2016to2020['country_upcase'].unique())
countries_physical = list(interpig_physical_2015to2020['country_upcase'].unique())
countries_indtrends = list(interpig_industrytrends_2015to2020['country_upcase'].unique())

interpig_combo = pd.merge(
   left=interpig_financial_2015to2020
   ,right=interpig_feedprice_2016to2020
   ,on=['country_upcase' ,'year']
   ,how='outer'
   ,indicator='_merge_1'
)
interpig_combo['_merge_1'].value_counts()

interpig_combo = pd.merge(
   left=interpig_combo
   ,right=interpig_physical_2015to2020
   ,on=['country_upcase' ,'year']
   ,how='outer'
   ,indicator='_merge_2'
)
interpig_combo['_merge_2'].value_counts()

interpig_combo = pd.merge(
   left=interpig_combo
   ,right=interpig_industrytrends_2015to2020
   ,on=['country_upcase' ,'year']
   ,how='outer'
   ,indicator='_merge_3'
)
interpig_combo['_merge_3'].value_counts()

# Drop extraneous columns
interpig_combo = interpig_combo.drop(columns=['country_x' ,'country_y'])

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(interpig_combo)
datadesc(interpig_combo ,CHARACTERIZE_FOLDER)
interpig_combo.to_pickle(os.path.join(PRODATA_FOLDER ,'interpig_combo.pkl.gz'))

# profile = interpig_combo.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'interpig_combo_profile.html'))
