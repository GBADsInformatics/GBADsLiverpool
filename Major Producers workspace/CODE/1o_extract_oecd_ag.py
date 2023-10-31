#%% About

# Exploring FAO OECD iLibrary data
# Agriculture Statistics:
# https://www.oecd-ilibrary.org/agriculture-and-food/data/oecd-agriculture-statistics_agr-data-en

#%% Import and explore

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
oecd_ag_imp = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'FAO_OECD_Ag_2010_2021.csv'))

# Some column names appear twice, once in CAPS and another in Propcase, and contain different data
rename_cols = {
   'LOCATION':'location'
   ,'Country':'country'
   ,'COMMODITY':'commodity_code'
   ,'Commodity':'commodity'
   ,'VARIABLE':'variable_code'
   ,'Variable':'variable'
   ,'TIME':'year'
   ,'Time':'year2'
   ,'Unit Code':'base_unit_code'
   ,'Unit':'base_unit'
   ,'PowerCode Code':'unit_power'
   ,'PowerCode':'unit_multiplier'
   ,'Reference Period Code':'reference_period_code'
   ,'Reference Period':'reference_period'
   ,'Value':'value'
   ,'Flag Codes':'flag_codes'
   ,'Flags':'flags'
}

# oecd_ag_imp = oecd_ag_imp.rename(columms=rename_cols)   # Doesn't recognize keyword columns= !?
oecd_ag_imp = oecd_ag_imp.rename(rename_cols ,axis='columns')

oecd_ag_imp = oecd_ag_imp.drop(columns=['year2'])

oecd_ag_imp_hd = oecd_ag_imp.head()

datainfo(oecd_ag_imp)

# ----------------------------------------------------------------------------
# Describe
# ----------------------------------------------------------------------------
oecd_countries = list(oecd_ag_imp['country'].unique())
oecd_commodities = list(oecd_ag_imp['commodity'].unique())
oecd_variables = list(oecd_ag_imp['variable'].unique())
oecd_years = list(oecd_ag_imp['year'].unique())
oecd_flags = list(oecd_ag_imp['flags'].unique())

# ----------------------------------------------------------------------------
# Explore variables
# ----------------------------------------------------------------------------
# Producer Price
oecd_var_producerprice = oecd_ag_imp.loc[oecd_ag_imp['variable'] == 'Producer price']

# Production
oecd_var_production = oecd_ag_imp.loc[oecd_ag_imp['variable'] == 'Production']

# Feed?
oecd_var_feed = oecd_ag_imp.loc[oecd_ag_imp['variable'] == 'Feed']

# ----------------------------------------------------------------------------
# Export full data
# ----------------------------------------------------------------------------
datainfo(oecd_ag_imp)
datadesc(oecd_ag_imp ,CHARACTERIZE_FOLDER)
oecd_ag_imp.to_pickle(os.path.join(PRODATA_FOLDER ,'oecd_ag_imp.pkl.gz'))

#%% Reshape

# Concatenate variable and unit
oecd_ag_imp['variable_with_unit'] = \
   oecd_ag_imp['variable'].astype(str) + \
      ' ' + oecd_ag_imp['base_unit'].astype(str) + \
         ' ' + oecd_ag_imp['unit_multiplier'].astype(str)
oecd_var_with_unit = list(oecd_ag_imp['variable_with_unit'].unique())

# Pivot each variable to a column
oecd_ag_p = oecd_ag_imp.pivot(
   index=['country' ,'year' ,'commodity']           # Column(s) to make new index. If blank, uses existing index.
   ,columns='variable_with_unit'        # Column(s) to make new columns
   ,values='value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
).reset_index()           # Pivoting will change columns to indexes. Change them back.
cleancolnames(oecd_ag_p)

# ----------------------------------------------------------------------------
# Export
# ----------------------------------------------------------------------------
datainfo(oecd_ag_p)
datadesc(oecd_ag_p ,CHARACTERIZE_FOLDER)
oecd_ag_p.to_pickle(os.path.join(PRODATA_FOLDER ,'oecd_ag_p.pkl.gz'))

#%% Subset

# Subset to commodities of interest
_row_selection = (oecd_ag_p['commodity'].isin(['Pigmeat' ,'Poultry meat']))
print(f"> Selected {_row_selection.sum() :,} rows.")
oecd_ag_pigandpoultry = oecd_ag_p.loc[_row_selection].copy().reset_index(drop=True)

# Some variables are not reported for all commodities. Drop columns that are all missing.
oecd_ag_pigandpoultry = oecd_ag_pigandpoultry.dropna(
   axis=1                        # 0 = drop rows, 1 = drop columns
   ,how='all'                    # String: 'all' = drop rows that have all missing values. 'any' = drop rows that have any missing values.
)

# ----------------------------------------------------------------------------
# Export
# ----------------------------------------------------------------------------
datainfo(oecd_ag_pigandpoultry)
datadesc(oecd_ag_pigandpoultry ,CHARACTERIZE_FOLDER)
oecd_ag_pigandpoultry.to_pickle(os.path.join(PRODATA_FOLDER ,'oecd_ag_pigandpoultry.pkl.gz'))
