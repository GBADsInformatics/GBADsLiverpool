#%% About
'''
USDA Foreign Agricultural Service PSD database
Production, import/export, and consumption of meat (not live animals)
https://apps.fas.usda.gov/psdonline/app/index.html#/app/advQuery
'''

#%% Chicken Meat

# Import
usda_psd_chickenmeat = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'usda_fas_psd_chickenmeat_2010_2021.csv'))
cleancolnames(usda_psd_chickenmeat)
datainfo(usda_psd_chickenmeat)

# Melt years to a single column
usda_psd_chickenmeat_m = usda_psd_chickenmeat.melt(
   id_vars=['country' ,'commodity' ,'attribute' ,'unit_description']         # Optional: column(s) to use as ID variables
   # ,value_vars=['col3' ,'col4']     # Columns to "unpivot" to rows. If blank, will use all columns not listed in id_vars.
   ,var_name='year'             # Name for new "variable" column
   ,value_name='value'              # Name for new "value" column
)

# Concatenate attributes with units
usda_psd_chickenmeat_m['metric'] = usda_psd_chickenmeat_m['attribute'] + ' ' + usda_psd_chickenmeat_m['unit_description']

# Pivot attributes to columns
usda_psd_chickenmeat_p = usda_psd_chickenmeat_m.pivot(
   index=['country' ,'commodity' ,'year']           # Column(s) to make new index. If blank, uses existing index.
   ,columns=['metric']        # Column(s) to make new columns
   ,values='value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
).reset_index()                     # Pivoting will change columns to indexes. Change them back.
cleancolnames(usda_psd_chickenmeat_p)
datainfo(usda_psd_chickenmeat_p)

# Change columns to numeric
convert_cols_to_numeric = [
   'year'
   ,'beginning_stocks__1000_mt_'
   ,'domestic_consumption__1000_mt_'
   ,'ending_stocks__1000_mt_'
   ,'exports__1000_mt_'
   ,'imports__1000_mt_'
   ,'production__1000_mt_'
   ,'total_distribution__1000_mt_'
   ,'total_supply__1000_mt_'
   ,'total_use__1000_mt_'
]
for COL in convert_cols_to_numeric:
   usda_psd_chickenmeat_p[COL] = usda_psd_chickenmeat_p[COL].str.replace(',' ,'')             # First remove commas
   usda_psd_chickenmeat_p[COL] = pd.to_numeric(usda_psd_chickenmeat_p[COL] ,errors='coerce')  # Use to_numeric to handle remaining values like ':'
datainfo(usda_psd_chickenmeat_p)

# Save to Pickle
usda_psd_chickenmeat_p.to_pickle(os.path.join(PRODATA_FOLDER ,'usda_psd_chickenmeat_p.pkl.gz'))

#%% Swine Meat and Animal Numbers

usda_psd_swinemeat = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'usda_fas_psd_swinemeat_animalnumbers_2010_2021.csv'))
cleancolnames(usda_psd_swinemeat)
datainfo(usda_psd_swinemeat)

# Melt years to a single column
usda_psd_swinemeat_m = usda_psd_swinemeat.melt(
   id_vars=['country' ,'commodity' ,'attribute' ,'unit_description']         # Optional: column(s) to use as ID variables
   # ,value_vars=    # Columns to "unpivot" to rows. If blank, will use all columns not listed in id_vars.
   ,var_name='year'             # Name for new "variable" column
   ,value_name='value'              # Name for new "value" column
)

# Concatenate attributes with units
usda_psd_swinemeat_m['metric'] = usda_psd_swinemeat_m['attribute'] + ' ' + usda_psd_swinemeat_m['unit_description']

# Pivot attributes to columns
usda_psd_swinemeat_p = usda_psd_swinemeat_m.pivot(
   index=['country' ,'year']           # Column(s) to make new index. If blank, uses existing index.
   ,columns=['metric']        # Column(s) to make new columns
   ,values='value'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
).reset_index()                     # Pivoting will change columns to indexes. Change them back.
cleancolnames(usda_psd_swinemeat_p)
datainfo(usda_psd_swinemeat_p)

# Change columns to numeric
convert_cols_to_numeric = list(usda_psd_swinemeat_p)
convert_cols_to_numeric.remove('country')
for COL in convert_cols_to_numeric:
   usda_psd_swinemeat_p[COL] = usda_psd_swinemeat_p[COL].str.replace(',' ,'')             # First remove commas
   usda_psd_swinemeat_p[COL] = pd.to_numeric(usda_psd_swinemeat_p[COL] ,errors='coerce')  # Use to_numeric to handle remaining values like ':'
datainfo(usda_psd_swinemeat_p)

# Save to Pickle
usda_psd_swinemeat_p.to_pickle(os.path.join(PRODATA_FOLDER ,'usda_psd_swinemeat_p.pkl.gz'))
