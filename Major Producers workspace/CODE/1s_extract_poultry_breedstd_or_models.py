#%% About
'''
Reading tables from breed standard PDFs.
'''

#%% Ross

# Two breeds Ross308 and Ross708 have the same column names so I am processing them together.
breedstd_pdf_ross308 = os.path.join(RAWDATA_FOLDER ,'Poultry Standards' ,'Ross308-308FF-BroilerPO2019-EN.pdf')
breedstd_pdf_ross708 = os.path.join(RAWDATA_FOLDER ,'Poultry Standards' ,'Ross-708-BroilerPO2019-EN.pdf')

# =============================================================================
#### Import and cleanup
# =============================================================================
# ----------------------------------------------------------------------------
# Weight by Day
# ----------------------------------------------------------------------------
# Read PDFs
ross308_list = tabula.read_pdf(breedstd_pdf_ross308
                               ,pages=4    # From a specific page. Note result is a list even if there is only 1 table. If there are multiple tables, each will be stored as an element.
                               )
ross308 = ross308_list[0]
cleancolnames(ross308)
datainfo(ross308)

ross708_list = tabula.read_pdf(breedstd_pdf_ross708
                               ,pages=4    # From a specific page. Note result is a list even if there is only 1 table. If there are multiple tables, each will be stored as an element.
                               )
ross708 = ross708_list[0]
cleancolnames(ross708)
datainfo(ross708)

# Rename columns and drop unnecessary
rename_columns = {
   'day':'dayonfeed'
   ,'body_weight':'bodyweight_g'
   ,'daily_gain':'dailygain_g'
   ,'av__daily_gain_week':'drop_1'
   ,'daily_intake':'dailyfeedintake_g'
   ,'cum__intake':'cmlfeedintake_g'
   ,'fcr3':'fcr'
   ,'unnamed:_0':'drop_2'
   }
ross308 = ross308.rename(columns=rename_columns)
ross308 = ross308.drop(columns=['drop_1' ,'drop_2'])
datainfo(ross308)

ross708 = ross708.rename(columns=rename_columns)
ross708 = ross708.drop(columns=['drop_1' ,'drop_2'])
datainfo(ross708)

# Drop rows corresponding to comments
ross308 = ross308.dropna(
   axis=0                        # 0 = drop rows, 1 = drop columns
   ,subset=['dayonfeed']      # List (opt): if dropping rows, only consider these columns in NA check
   ,how='all'                    # String: 'all' = drop rows that have all missing values. 'any' = drop rows that have any missing values.
).reset_index(drop=True)

ross708 = ross708.dropna(
   axis=0                        # 0 = drop rows, 1 = drop columns
   ,subset=['dayonfeed']      # List (opt): if dropping rows, only consider these columns in NA check
   ,how='all'                    # String: 'all' = drop rows that have all missing values. 'any' = drop rows that have any missing values.
).reset_index(drop=True)

# Change all columns to numeric
ross308 = ross308.astype('float64')
ross708 = ross708.astype('float64')

# ----------------------------------------------------------------------------
# Carcass Yield by Weight
# ----------------------------------------------------------------------------
# Read Excel
# These tables are not machine-readable in the PDFs, so I have manually copied them into Excel files
ross308_yield = pd.read_excel(os.path.join(RAWDATA_FOLDER ,'Poultry Standards' ,'Ross 308 Yield table.xlsx')
                              ,sheet_name='Sheet1'
                              )
cleancolnames(ross308_yield)
datainfo(ross308_yield)

ross708_yield = pd.read_excel(os.path.join(RAWDATA_FOLDER ,'Poultry Standards' ,'Ross 708 Yield table.xlsx')
                              ,sheet_name='Sheet1'
                              )
cleancolnames(ross708_yield)
datainfo(ross708_yield)

# Add calcs
ross308_yield.eval(
   '''
   bodyweight_g = bodyweight_kg * 1000
   eviscerated_pct_avg = (eviscerated_pct_male + eviscerated_pct_female) / 2
   '''
   ,inplace=True
)
ross708_yield.eval(
   '''
   bodyweight_g = bodyweight_kg * 1000
   eviscerated_pct_avg = (eviscerated_pct_male + eviscerated_pct_female) / 2
   '''
   ,inplace=True
)

# ----------------------------------------------------------------------------
# Add Carcass Yield to Weight table
# ----------------------------------------------------------------------------
# Create interpolator from yield table
min_yield = ross308_yield['eviscerated_pct_avg'].min()
max_yield = ross308_yield['eviscerated_pct_avg'].max()
interp_yield_from_weight = sp.interpolate.interp1d(
   ross308_yield['bodyweight_g']
   ,ross308_yield['eviscerated_pct_avg']
   ,bounds_error=False	            # False: out of bounds values are assigned fill_value, or nan if none given.
   ,fill_value=(min_yield ,max_yield)      # If weight is out of bounds, set to minimum or maximum yield
)
# Apply interpolator
ross308['pct_yield'] = interp_yield_from_weight(ross308['bodyweight_g']) * 1   # Multiply by one: trick to convert array to number

# Create interpolator from yield table
min_yield = ross708_yield['eviscerated_pct_avg'].min()
max_yield = ross708_yield['eviscerated_pct_avg'].max()
interp_yield_from_weight = sp.interpolate.interp1d(
   ross708_yield['bodyweight_g']
   ,ross708_yield['eviscerated_pct_avg']
   ,bounds_error=False	            # False: out of bounds values are assigned fill_value, or nan if none given.
   ,fill_value=(min_yield ,max_yield)      # If weight is out of bounds, set to minimum or maximum yield
)
# Apply interpolator
ross708['pct_yield'] = interp_yield_from_weight(ross708['bodyweight_g']) * 1   # Multiply by one: trick to convert array to number

# =============================================================================
#### Export
# =============================================================================
poultrybreedstd_ross308 = ross308
datadesc(poultrybreedstd_ross308 ,CHARACTERIZE_FOLDER)

# To pickle
poultrybreedstd_ross308.to_pickle(os.path.join(PRODATA_FOLDER ,'poultrybreedstd_ross308.pkl.gz'))
poultrybreedstd_ross308.to_pickle(os.path.join(DASH_DATA_FOLDER ,'poultrybreedstd_ross308.pkl.gz'))

# To CSV
poultrybreedstd_ross308.to_csv(os.path.join(EXPDATA_FOLDER ,'poultrybreedstd_ross308.csv') ,index=False)

poultrybreedstd_ross708 = ross708
datadesc(poultrybreedstd_ross708 ,CHARACTERIZE_FOLDER)

# To pickle
poultrybreedstd_ross708.to_pickle(os.path.join(PRODATA_FOLDER ,'poultrybreedstd_ross708.pkl.gz'))
poultrybreedstd_ross708.to_pickle(os.path.join(DASH_DATA_FOLDER ,'poultrybreedstd_ross708.pkl.gz'))

# To CSV
poultrybreedstd_ross708.to_csv(os.path.join(EXPDATA_FOLDER ,'poultrybreedstd_ross708.csv') ,index=False)

# =============================================================================
#### Plot
# =============================================================================
snplt = sns.relplot(
   data=poultrybreedstd_ross708
   ,x='dayonfeed'
   ,y='bodyweight_g'
   ,kind='line'
   ,color='blue'     # Fixed color rather than color by variable
   ,marker='o'       # Add markers
   ,aspect=1.5         # Width of figure as multiplier on height. If paneled, width of each panel.
)
plt.title('Poultry reference growth\n Based on Ross 708 breed standard' ,fontsize=12)

#%% Cobb

breedstd_pdf_cobb500 = os.path.join(RAWDATA_FOLDER ,'Poultry Standards' ,'Cobb 500 Nutrition and Performance Guide-2018.pdf')

# =============================================================================
#### Import and cleanup
# =============================================================================
# ----------------------------------------------------------------------------
# Weight by Day
# ----------------------------------------------------------------------------
# Read PDF
cobb_list = tabula.read_pdf(breedstd_pdf_cobb500
                            ,pages=3    # From a specific page. Note result is a list even if there is only 1 table. If there are multiple tables, each will be stored as an element.
                            )
cobb = cobb_list[0]
cleancolnames(cobb)
datainfo(cobb)

# Rename cols
rename_columns = {
   'unnamed:_0':'dayonfeed'
   ,'unnamed:_1':'bodyweight_g'
   ,'unnamed:_2':'dailygain_g'
   ,'as_hatched':'adg_and_fcr'
   ,'unnamed:_3':'dailyfeedintake_g'
   ,'unnamed:_4':'cmlfeedintake_g'
   }
cobb = cobb.rename(columns=rename_columns)

# Drop rows corresponding to comments
cobb = cobb.drop(index=[0, 1]).reset_index(drop=True)             # By row number

# Separate ADG and FCR which were read into same column for some reason
cobb[ ['adg' ,'fcr'] ] = cobb['adg_and_fcr'].str.split(' ', expand=True)
cobb = cobb.drop(columns=['adg_and_fcr'])

# Change all columns to numeric
cobb = cobb.astype('float64')

datainfo(cobb)

# ----------------------------------------------------------------------------
# Carcass Yield by Weight
# ----------------------------------------------------------------------------
# Read table
cobb_yield_list = tabula.read_pdf(breedstd_pdf_cobb500
                                  ,pages=13    # From a specific page. Note result is a list even if there is only 1 table. If there are multiple tables, each will be stored as an element.
                                  )
cobb_yield = cobb_yield_list[0]   # Use AS HATCHED table
cleancolnames(cobb_yield)
datainfo(cobb_yield)

# Rename cols
rename_columns = {
   'unnamed:_0':'bodyweight_g'
   ,'unnamed:_1':'bodyweight_lb'
   ,'unnamed:_2':'_drop1'
   ,'unnamed:_3':'pct_evis'
   ,'as_hatched':'pct_breast'
   ,'unnamed:_4':'pct_leg'
   ,'unnamed:_5':'pct_wing'
   }
cobb_yield = cobb_yield.rename(columns=rename_columns)
cobb_yield = cobb_yield.drop(columns='_drop1')

# Drop rows corresponding to comments
cobb_yield = cobb_yield.drop(index=[0, 1]).reset_index(drop=True)             # By row number

# Change all columns to numeric
cobb_yield = cobb_yield.astype('float64')

datainfo(cobb_yield)

# ----------------------------------------------------------------------------
# Add Carcass Yield to Weight table
# ----------------------------------------------------------------------------
# Create interpolator from yield table
min_yield = cobb_yield['pct_evis'].min()
max_yield = cobb_yield['pct_evis'].max()
interp_yield_from_weight = sp.interpolate.interp1d(
   cobb_yield['bodyweight_g']
   ,cobb_yield['pct_evis']
   ,bounds_error=False	            # False: out of bounds values are assigned fill_value, or nan if none given.
   ,fill_value=(min_yield ,max_yield)      # If weight is out of bounds, set to minimum or maximum yield
)
# Apply interpolator
cobb['pct_yield'] = interp_yield_from_weight(cobb['bodyweight_g']) * 1   # Multiply by one: trick to convert array to number

# =============================================================================
#### Export
# =============================================================================
poultrybreedstd_cobb500 = cobb.copy()
datadesc(poultrybreedstd_cobb500 ,CHARACTERIZE_FOLDER)

# To pickle
poultrybreedstd_cobb500.to_pickle(os.path.join(PRODATA_FOLDER ,'poultrybreedstd_cobb500.pkl.gz'))
poultrybreedstd_cobb500.to_pickle(os.path.join(DASH_DATA_FOLDER ,'poultrybreedstd_cobb500.pkl.gz'))

# To CSV
poultrybreedstd_cobb500.to_csv(os.path.join(EXPDATA_FOLDER ,'poultrybreedstd_cobb500.csv') ,index=False)

# =============================================================================
#### Plot
# =============================================================================
snplt = sns.relplot(
   data=poultrybreedstd_cobb500
   ,x='dayonfeed'
   ,y='bodyweight_g'
   ,kind='line'
   ,color='blue'     # Fixed color rather than color by variable
   ,marker='o'       # Add markers
   ,aspect=1.5         # Width of figure as multiplier on height. If paneled, width of each panel.
)
plt.title('Poultry reference growth\n Based on Cobb 500 breed standard' ,fontsize=12)

#%% Vencobb 400
# Breed used in India
# Does not include yield targets

# =============================================================================
#### Import and cleanup
# =============================================================================
# ----------------------------------------------------------------------------
# Read table
# ----------------------------------------------------------------------------
vencobb_imp = pd.read_excel(
   os.path.join(RAWDATA_FOLDER ,'Poultry Standards' ,'Vencobb400 Broiler manual - Performance Goals.xlsx')
   ,sheet_name='Vencobb 400'
)
cleancolnames(vencobb_imp)
datainfo(vencobb_imp)

# ----------------------------------------------------------------------------
# Cleanup
# ----------------------------------------------------------------------------
rename_columns = {
   'age_in_days':'dayonfeed'
   ,'body_wt___gms_':'bodyweight_g'
   ,'mortality__pct_':'mortality_pct'
   ,'cumulative_feed_consumption__gms_':'cmlfeedintake_g'
   ,'cumulative_fcr__pelleted_feed_':'fcr'
   }
vencobb_imp = vencobb_imp.rename(columns=rename_columns)
datainfo(vencobb_imp)

# ----------------------------------------------------------------------------
# Data is weekly. Interpolate days between.
# ----------------------------------------------------------------------------
# Build interpolator
interp_weight_from_days = sp.interpolate.interp1d(
   vencobb_imp['dayonfeed']
   ,vencobb_imp['bodyweight_g']
   ,bounds_error=False	            # False: out of bounds values are assigned fill_value, or nan if none given.
)
interp_feed_from_days = sp.interpolate.interp1d(
   vencobb_imp['dayonfeed']
   ,vencobb_imp['cmlfeedintake_g']
   ,bounds_error=False	            # False: out of bounds values are assigned fill_value, or nan if none given.
)
interp_fcr_from_days = sp.interpolate.interp1d(
   vencobb_imp['dayonfeed']
   ,vencobb_imp['fcr']
   ,bounds_error=False	            # False: out of bounds values are assigned fill_value, or nan if none given.
)

# Add rows for days
days_min = vencobb_imp['dayonfeed'].min()
days_max = vencobb_imp['dayonfeed'].max()
days_range_df = pd.DataFrame()
days_range_df['dayonfeed'] = np.array(range(days_min ,days_max + 1))
vencobb_expanded = pd.merge(
   left=vencobb_imp
   ,right=days_range_df
   ,on='dayonfeed'
   ,how='outer'
)
vencobb_expanded = vencobb_expanded.sort_values(by='dayonfeed')

# Apply interpolator
vencobb_expanded['bodyweight_g_interp'] = interp_weight_from_days(vencobb_expanded['dayonfeed']) * 1   # Multiply by one: trick to convert array to number
_null_rows = (vencobb_expanded['bodyweight_g'].isnull())                        # Where col1 is missing...
vencobb_expanded.loc[_null_rows ,'bodyweight_g'] = vencobb_expanded.loc[_null_rows ,'bodyweight_g_interp']    # ...fill with col2
del vencobb_expanded['bodyweight_g_interp']

vencobb_expanded['cmlfeedintake_g_interp'] = interp_feed_from_days(vencobb_expanded['dayonfeed']) * 1   # Multiply by one: trick to convert array to number
_null_rows = (vencobb_expanded['cmlfeedintake_g'].isnull())                        # Where col1 is missing...
vencobb_expanded.loc[_null_rows ,'cmlfeedintake_g'] = vencobb_expanded.loc[_null_rows ,'cmlfeedintake_g_interp']    # ...fill with col2
del vencobb_expanded['cmlfeedintake_g_interp']

vencobb_expanded['fcr_interp'] = interp_fcr_from_days(vencobb_expanded['dayonfeed']) * 1   # Multiply by one: trick to convert array to number
_null_rows = (vencobb_expanded['fcr'].isnull())                        # Where col1 is missing...
vencobb_expanded.loc[_null_rows ,'fcr'] = vencobb_expanded.loc[_null_rows ,'fcr_interp']    # ...fill with col2
del vencobb_expanded['fcr_interp']

# ----------------------------------------------------------------------------
# Data does not include yield. Assume same as Cobb.
# ----------------------------------------------------------------------------
vencobb_expanded = pd.merge(
   left=vencobb_expanded
   ,right=cobb[['dayonfeed' ,'pct_yield']]
   ,on='dayonfeed'
   ,how='left'
)

# =============================================================================
#### Export
# =============================================================================
poultrybreedstd_vencobb400 = vencobb_expanded.copy()
datadesc(poultrybreedstd_vencobb400 ,CHARACTERIZE_FOLDER)

# To pickle
poultrybreedstd_vencobb400.to_pickle(os.path.join(PRODATA_FOLDER ,'poultrybreedstd_vencobb400.pkl.gz'))
poultrybreedstd_vencobb400.to_pickle(os.path.join(DASH_DATA_FOLDER ,'poultrybreedstd_vencobb400.pkl.gz'))

# To CSV
poultrybreedstd_vencobb400.to_csv(os.path.join(EXPDATA_FOLDER ,'poultrybreedstd_vencobb400.csv') ,index=False)

# =============================================================================
#### Plot
# =============================================================================
snplt = sns.relplot(
   data=poultrybreedstd_vencobb400
   ,x='dayonfeed'
   ,y='bodyweight_g'
   ,kind='line'
   ,color='blue'     # Fixed color rather than color by variable
   ,marker='o'       # Add markers
   ,aspect=1.5         # Width of figure as multiplier on height. If paneled, width of each panel.
)
plt.title('Poultry reference growth\n Based on Vencobb 400 breed standard' ,fontsize=12)

#%% Gemma's model

# Import
poultrybreedstd_liverpool_model = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'Poultry standards' ,'poultry_dat_for_justin.csv'))
cleancolnames(poultrybreedstd_liverpool_model)
datainfo(poultrybreedstd_liverpool_model)

# Drop cols
poultrybreedstd_liverpool_model = poultrybreedstd_liverpool_model.drop(columns=['unnamed:_0'])

# Rename cols
rename_cols = {
   "days_growing":"dayonfeed"
}
poultrybreedstd_liverpool_model = poultrybreedstd_liverpool_model.rename(columns=rename_cols)

# Add columns for finish weight
poultrybreedstd_liverpool_model.eval(
   f'''
   pred_finishwt_mean_lb = (adg_mean * dayonfeed)
   pred_finishwt_min_lb = (adg_min * dayonfeed)
   pred_finishwt_max_lb = (adg_max * dayonfeed)

   pred_finishwt_mean_kg = pred_finishwt_mean_lb / {uc.lbs_per_kg}
   pred_finishwt_min_kg = pred_finishwt_min_lb / {uc.lbs_per_kg}
   pred_finishwt_max_kg = pred_finishwt_max_lb / {uc.lbs_per_kg}

   pred_finishwt_mean_sim_lb = (newdata_adg_mean_ab_fcr_agethin_parents * dayonfeed)
   pred_finishwt_min_sim_lb = (newdata_adg_min_ab_fcr_agethin_parents * dayonfeed)
   pred_finishwt_max_sim_lb = (newdata_adg_max_ab_fcr_agethin_parents * dayonfeed)

   pred_finishwt_mean_sim_kg = pred_finishwt_mean_sim_lb / {uc.lbs_per_kg}
   pred_finishwt_min_sim_kg = pred_finishwt_min_sim_lb / {uc.lbs_per_kg}
   pred_finishwt_max_sim_kg = pred_finishwt_max_sim_lb / {uc.lbs_per_kg}
   '''
   ,inplace=True
)
datainfo(poultrybreedstd_liverpool_model)

# Dashboard will look for column 'bodyweight_g' to determine potential growth of animals
# Create this column based on the weight you want to use
# Currently using maximum predicted weight for a given dayonfeed
poultrybreedstd_liverpool_model['bodyweight_kg'] = poultrybreedstd_liverpool_model['pred_finishwt_max_sim_kg']
poultrybreedstd_liverpool_model['bodyweight_g'] = poultrybreedstd_liverpool_model['bodyweight_kg'] * 1000

# -----------------------------------------------------------------------------
# Plot
# -----------------------------------------------------------------------------
plt.plot('dayonfeed' ,'pred_finishwt_max_lb' ,data=poultrybreedstd_liverpool_model)
plt.plot('dayonfeed' ,'pred_finishwt_max_sim_lb' ,data=poultrybreedstd_liverpool_model)
plt.legend()
plt.xlabel('Day on Feed')

# -----------------------------------------------------------------------------
# Export
# -----------------------------------------------------------------------------
# To PRODATA
poultrybreedstd_liverpool_model.to_pickle(os.path.join(PRODATA_FOLDER ,'poultrybreedstd_liverpool_model.pkl.gz'))

# To DASH data
poultrybreedstd_liverpool_model.to_pickle(os.path.join(DASH_DATA_FOLDER ,'poultrybreedstd_liverpool_model.pkl.gz'))
