# Exploring UK government data (R3 and R4)
# https://www.gov.uk/government/statistics/poultry-and-poultry-meat-statistics

#%% Chicks placed

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
uk_chicksplaced = pd.read_excel(os.path.join(RAWDATA_FOLDER ,'uk-poultry-placings-20jan22.ods')
                                ,sheet_name='UK_Placings_Annual'
                                ,skiprows=list(range(7)) + list(range(37,43))   # Exclude descriptive rows and footnotes
                                )
cleancolnames(uk_chicksplaced)

# ----------------------------------------------------------------------------
# Change year to type integer
# ----------------------------------------------------------------------------
# Split number from asterisks
uk_chicksplaced[ ['year1' ,'year2'] ] = uk_chicksplaced['year'].str.split('*', 1, expand=True)  # Splitting at asterisk (*)
uk_chicksplaced['year1'] = pd.to_numeric(
   uk_chicksplaced['year1']
   ,errors='coerce'                       # 'coerce': errors become nan. 'ignore': errors will return input. 'raise': errors with throw an error.
   ,downcast='integer'                    # Default None. 'integer': convert numbers to integer
   )

# Convert base column. Those with asterisks will be nan.
uk_chicksplaced['year'] = pd.to_numeric(
   uk_chicksplaced['year']
   ,errors='coerce'                       # 'coerce': errors become nan. 'ignore': errors will return input. 'raise': errors with throw an error.
   ,downcast='integer'                    # Default None. 'integer': convert numbers to integer
   )

# Fill in nans with year1
uk_chicksplaced.loc[ uk_chicksplaced['year'].isnull() ,'year' ] = uk_chicksplaced['year1']

datainfo(uk_chicksplaced)

#%% Poultry Slaughter

#%%% Number of birds

# ----------------------------------------------------------------------------
# Import and basics
# ----------------------------------------------------------------------------
uk_slaughter_birds = pd.read_excel(os.path.join(RAWDATA_FOLDER ,'uk-poultry-slaughter-20jan22.ods')
                                ,sheet_name='Slaughterings_Annual_'
                                ,skiprows=list(range(8))   # Exclude descriptive rows
                                )
cleancolnames(uk_slaughter_birds)

# Drop footnotes and blank rows
uk_slaughter_birds.dropna(
   axis=0                        # 0 = drop rows, 1 = drop columns
   ,subset=['year' ,'broilers_uk']      # List (opt): if dropping rows, only consider these columns in NA check
   ,how='any'                    # String: 'all' = drop rows that have all missing values. 'any' = drop rows that have any missing values.
   ,inplace=True                 # True: modify data in place
)

# ----------------------------------------------------------------------------
# Change year to type integer
# ----------------------------------------------------------------------------
# Split number from asterisks
uk_slaughter_birds[ ['year1' ,'year2'] ] = uk_slaughter_birds['year'].str.split('*', 1, expand=True)  # Splitting at asterisk (*)
uk_slaughter_birds['year1'] = pd.to_numeric(
   uk_slaughter_birds['year1']
   ,errors='coerce'                       # 'coerce': errors become nan. 'ignore': errors will return input. 'raise': errors with throw an error.
   ,downcast='integer'                    # Default None. 'integer': convert numbers to integer
   )

# Convert base column. Those with asterisks will be nan.
uk_slaughter_birds['year'] = pd.to_numeric(
   uk_slaughter_birds['year']
   ,errors='coerce'                       # 'coerce': errors become nan. 'ignore': errors will return input. 'raise': errors with throw an error.
   ,downcast='integer'                    # Default None. 'integer': convert numbers to integer
   )

# Fill in nans with year1
uk_slaughter_birds.loc[ uk_slaughter_birds['year'].isnull() ,'year' ] = uk_slaughter_birds['year1']

datainfo(uk_slaughter_birds)

#%%% Liveweight

# ----------------------------------------------------------------------------
# Import and basics
# ----------------------------------------------------------------------------
uk_slaughter_liveweight = pd.read_excel(os.path.join(RAWDATA_FOLDER ,'uk-poultry-slaughter-20jan22.ods')
                                ,sheet_name='Liveweights_Annual'
                                ,skiprows=list(range(7))   # Exclude descriptive rows
                                )
cleancolnames(uk_slaughter_liveweight)

# Drop footnotes and blank rows
uk_slaughter_liveweight.dropna(
   axis=0                        # 0 = drop rows, 1 = drop columns
   ,subset=['year' ,'broilers']      # List (opt): if dropping rows, only consider these columns in NA check
   ,how='any'                    # String: 'all' = drop rows that have all missing values. 'any' = drop rows that have any missing values.
   ,inplace=True                 # True: modify data in place
)

# Change datatype of broilers
uk_slaughter_liveweight['broilers'] = pd.to_numeric(
   uk_slaughter_liveweight['broilers']
   ,errors='coerce'                       # 'coerce': errors become nan. 'ignore': errors will return input. 'raise': errors with throw an error.
   ,downcast='integer'                    # Default None. 'integer': convert numbers to integer
   )

# ----------------------------------------------------------------------------
# Change year to type integer
# ----------------------------------------------------------------------------
# Split number from asterisks
uk_slaughter_liveweight[ ['year1' ,'year2'] ] = uk_slaughter_liveweight['year'].str.split('*', 1, expand=True)  # Splitting at asterisk (*)
uk_slaughter_liveweight['year1'] = pd.to_numeric(
   uk_slaughter_liveweight['year1']
   ,errors='coerce'                       # 'coerce': errors become nan. 'ignore': errors will return input. 'raise': errors with throw an error.
   ,downcast='integer'                    # Default None. 'integer': convert numbers to integer
   )

# Convert base column. Those with asterisks will be nan.
uk_slaughter_liveweight['year'] = pd.to_numeric(
   uk_slaughter_liveweight['year']
   ,errors='coerce'                       # 'coerce': errors become nan. 'ignore': errors will return input. 'raise': errors with throw an error.
   ,downcast='integer'                    # Default None. 'integer': convert numbers to integer
   )

# Fill in nans with year1
uk_slaughter_liveweight.loc[ uk_slaughter_liveweight['year'].isnull() ,'year' ] = uk_slaughter_liveweight['year1']

datainfo(uk_slaughter_liveweight)

#%%% Carcass Weight

# ----------------------------------------------------------------------------
# Import and basics
# ----------------------------------------------------------------------------
uk_slaughter_carcassweight = pd.read_excel(os.path.join(RAWDATA_FOLDER ,'uk-poultry-slaughter-20jan22.ods')
                                ,sheet_name='Production_Annual'
                                ,skiprows=list(range(7))   # Exclude descriptive rows
                                )
cleancolnames(uk_slaughter_carcassweight)
uk_slaughter_carcassweight.rename(columns={'total_for_year':'year'} ,inplace=True)

# Drop footnotes and blank rows
uk_slaughter_carcassweight.dropna(
   axis=0                        # 0 = drop rows, 1 = drop columns
   ,subset=['year' ,'broilers']      # List (opt): if dropping rows, only consider these columns in NA check
   ,how='any'                    # String: 'all' = drop rows that have all missing values. 'any' = drop rows that have any missing values.
   ,inplace=True                 # True: modify data in place
)

# Change datatype of broilers
uk_slaughter_carcassweight['broilers'] = pd.to_numeric(
   uk_slaughter_carcassweight['broilers']
   ,errors='coerce'                       # 'coerce': errors become nan. 'ignore': errors will return input. 'raise': errors with throw an error.
   ,downcast='integer'                    # Default None. 'integer': convert numbers to integer
   )

# ----------------------------------------------------------------------------
# Change year to type integer
# ----------------------------------------------------------------------------
# Split number from asterisks
uk_slaughter_carcassweight[ ['year1' ,'year2'] ] = uk_slaughter_carcassweight['year'].str.split('*', 1, expand=True)  # Splitting at asterisk (*)
uk_slaughter_carcassweight['year1'] = pd.to_numeric(
   uk_slaughter_carcassweight['year1']
   ,errors='coerce'                       # 'coerce': errors become nan. 'ignore': errors will return input. 'raise': errors with throw an error.
   ,downcast='integer'                    # Default None. 'integer': convert numbers to integer
   )

# Convert base column. Those with asterisks will be nan.
uk_slaughter_carcassweight['year'] = pd.to_numeric(
   uk_slaughter_carcassweight['year']
   ,errors='coerce'                       # 'coerce': errors become nan. 'ignore': errors will return input. 'raise': errors with throw an error.
   ,downcast='integer'                    # Default None. 'integer': convert numbers to integer
   )

# Fill in nans with year1
uk_slaughter_carcassweight.loc[ uk_slaughter_carcassweight['year'].isnull() ,'year' ] = uk_slaughter_carcassweight['year1']

datainfo(uk_slaughter_carcassweight)

#%% Combine placements and slaughter

# ----------------------------------------------------------------------------
# Rename and subset columns
# ----------------------------------------------------------------------------
datainfo(uk_chicksplaced)
keep_rename_cols = {
   'year':'year'
   ,'commercial_broilers_uk':'chicksplaced_broilers_mlln'
}
uk_chicksplaced_tomerge = uk_chicksplaced[list(keep_rename_cols)].rename(columns=keep_rename_cols)
datainfo(uk_chicksplaced_tomerge)

datainfo(uk_slaughter_birds)
keep_rename_cols = {
   'year':'year'
   ,'broilers_uk':'slaughter_broilers_mlln'
}
uk_slaughter_birds_tomerge = uk_slaughter_birds[list(keep_rename_cols)].rename(columns=keep_rename_cols)
datainfo(uk_slaughter_birds_tomerge)

datainfo(uk_slaughter_liveweight)
keep_rename_cols = {
   'year':'year'
   ,'broilers':'slaughter_broilers_avglivewt_kg'
}
uk_slaughter_liveweight_tomerge = uk_slaughter_liveweight[list(keep_rename_cols)].rename(columns=keep_rename_cols)
datainfo(uk_slaughter_liveweight_tomerge)

datainfo(uk_slaughter_carcassweight)
keep_rename_cols = {
   'year':'year'
   ,'broilers':'slaughter_broilers_carcwt_thsdtonnes'
}
uk_slaughter_carcassweight_tomerge = uk_slaughter_carcassweight[list(keep_rename_cols)].rename(columns=keep_rename_cols)
datainfo(uk_slaughter_carcassweight_tomerge)

# ----------------------------------------------------------------------------
# Merge on year
# ----------------------------------------------------------------------------
merge1 = pd.merge(
   left=uk_chicksplaced_tomerge
   ,right=uk_slaughter_birds_tomerge
   ,on='year'
   ,how='outer'
   ,indicator='_merge1'
   )
merge1['_merge1'].value_counts()          # Check number of rows from each table (requires indicator=True)

merge2 = pd.merge(
   left=merge1
   ,right=uk_slaughter_liveweight_tomerge
   ,on='year'
   ,how='outer'
   ,indicator='_merge2'
   )
merge2['_merge2'].value_counts()          # Check number of rows from each table (requires indicator=True)

merge3 = pd.merge(
   left=merge2
   ,right=uk_slaughter_carcassweight_tomerge
   ,on='year'
   ,how='outer'
   ,indicator='_merge3'
   )
merge3['_merge3'].value_counts()          # Check number of rows from each table (requires indicator=True)

# Drop merge indicators
uk_broilercombo = merge3.drop(
   columns=['_merge1' ,'_merge2' ,'_merge3']
   ,errors='ignore'      # If any columns are not found (e.g. they were dropped previously), suppress error.
   )

# Add column for country
uk_broilercombo['country'] = 'uk'

# ----------------------------------------------------------------------------
# Add calcs
# ----------------------------------------------------------------------------
# Convert head to thousands
# Calculate all-cause Mortality
uk_broilercombo.eval('''
                     chicksplaced_broilers_thsdhd = chicksplaced_broilers_mlln * 1000
                     slaughter_broilers_thsdhd = slaughter_broilers_mlln * 1000

                     mortality_calc_broilers_thsdhd = chicksplaced_broilers_thsdhd - slaughter_broilers_thsdhd
                     mortality_calc_broilers_pctplaced = mortality_calc_broilers_thsdhd / chicksplaced_broilers_thsdhd
                     '''
                     ,inplace=True
                     )

# ----------------------------------------------------------------------------
# Checks
# ----------------------------------------------------------------------------
# Number of birds * Avg Live weight compared to total production should give sensible yield %
uk_broilercombo.eval('''
                     check_total_livewt_thsdtonnes = slaughter_broilers_mlln * slaughter_broilers_avglivewt_kg
                     check_yield = slaughter_broilers_carcwt_thsdtonnes / check_total_livewt_thsdtonnes
                     '''
                     ,inplace=True
                     )

# ----------------------------------------------------------------------------
# Drop and reorder
# ----------------------------------------------------------------------------
# Drop base columns that have been converted
uk_broilercombo.drop(columns=[
   'chicksplaced_broilers_mlln'
   ,'slaughter_broilers_mlln'
   ,'check_total_livewt_thsdtonnes'
   ,'check_yield'
   ]
   ,inplace=True
   )

# Idea for getting post-mortem condemns:
#	Calculate kg that should have been produced.  The difference between this and actual kg produced is weight of PM condemns.
#	Note: on the flip side, calculating avg. carcass weight from total kg produced will underestimate because some weight is lost to PM condemns
#	If we have Avg. Liveweight at slaughter, we can calculate avg. carcass weight that should have been produced by assuming a standard yield %

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(uk_broilercombo)
datadesc(uk_broilercombo ,CHARACTERIZE_FOLDER)
uk_broilercombo.to_pickle(os.path.join(PRODATA_FOLDER ,'uk_broilercombo.pkl.gz'))

profile = uk_broilercombo.profile_report()
profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'uk_broilercombo_profile.html'))
