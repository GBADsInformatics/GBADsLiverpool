#%% About
'''
USDA Foreign Agricultural Service GATS database
Imports & exports, including live animals, meat, and feed
https://apps.fas.usda.gov/gats/default.aspx

Lookup 10-digit HS codes here
Note Imported data might drop leading zero
Lookups have decimal points in this pattern: xxxx.xx.xx.xx
https://www.aphis.usda.gov/aphis/ourfocus/importexport/ace/hts-master-correlation-table
'''
#%% Define HS code lookup

# Define lookup for HS codes
# Note data has dropped leading zero
describe_hscode = {
   "105110040":"live_chickens_lte185g_nonbreeding"
   ,"105110020":"live_chickens_lte185g_breeding_broiler"
   ,"105110010":"live_chickens_lte185g_breeding_layer"
   ,"105940000":"live_chickens_other"

   ,"103100000":"live_swine_purebred_breeding"

   ,"103910000":"live_swine_lt50kg"
   ,"103910030":"live_swine_other_23to50kg"
   ,"103910020":"live_swine_other_7to23kg"
   ,"103910010":"live_swine_other_lte7kg"

   ,"103920000":"live_swine_gte50kg"
   ,"103920091":"live_swine_other_gte50kg"
   ,"103920020":"live_swine_nonpb_breeding_gte50kg"
   ,"103920010":"live_swine_immslaughter_gte50kg"
}

#%% Chicken Imports

# Read data
us_imports_chickens = pd.read_csv(
   os.path.join(RAWDATA_FOLDER ,'usda_gats_us_imports_chickens_2011_2021_corrected.csv')
   ,skiprows=3
   ,skipfooter=3
)
cleancolnames(us_imports_chickens)
datainfo(us_imports_chickens)

# Create description based on HS codes
us_imports_chickens['desc'] = us_imports_chickens['unnamed:_3'].replace(describe_hscode)

# Melt years to rows
us_imports_chickens_m = us_imports_chickens.melt(
   id_vars='desc'         # Optional: column(s) to use as ID variables
   ,value_vars=[     # Columns to "unpivot" to rows. If blank, will use all columns not listed in id_vars.
               '2011_1'
               ,'2012_1'
               ,'2013_1'
               ,'2014_1'
               ,'2015_1'
               ,'2016_1'
               ,'2017_1'
               ,'2018_1'
               ,'2019_1'
               ,'2020_1'
               ,'2021_1'
               ]
   ,var_name='orig_col'             # Name for new "variable" column
   ,value_name='head'              # Name for new "value" column
)

# Drop unneeded rows
_droprows = (us_imports_chickens_m['desc'] == 'HS Code')
print(f"> Dropping {_droprows.sum() :,} rows.")
us_imports_chickens_m = us_imports_chickens_m.drop(us_imports_chickens_m.loc[_droprows].index).reset_index(drop=True)

# Create Year column
us_imports_chickens_m['year'] = us_imports_chickens_m['orig_col'].str[:4].astype(int)

# Change to numeric
us_imports_chickens_m['head'] = us_imports_chickens_m['head'].str.replace(',' ,'').astype('float')

# Pivot metrics to columns
us_imports_chickens_m_p = us_imports_chickens_m.pivot(
   index='year'           # Column(s) to make new index. If blank, uses existing index.
   ,columns='desc'        # Column(s) to make new columns
   ,values='head'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
)
us_imports_chickens_m_p = us_imports_chickens_m_p.add_prefix('import_')    # Add suffix to column names (all columns except index columns)
us_imports_chickens_m_p = us_imports_chickens_m_p.add_suffix('_hd')    # Add suffix to column names (all columns except index columns)
us_imports_chickens_m_p = us_imports_chickens_m_p.reset_index()           # Pivoting will change columns to indexes. Change them back.

#%% Chicken Exports

# Read data
us_exports_chickens = pd.read_csv(
   os.path.join(RAWDATA_FOLDER ,'usda_gats_us_exports_chickens_2011_2021_corrected.csv')
   ,skiprows=3
   ,skipfooter=3
)
cleancolnames(us_exports_chickens)
datainfo(us_exports_chickens)

# Create description based on HS codes
us_exports_chickens['desc'] = us_exports_chickens['unnamed:_3'].replace(describe_hscode)

# Melt years to rows
us_exports_chickens_m = us_exports_chickens.melt(
   id_vars='desc'         # Optional: column(s) to use as ID variables
   ,value_vars=[     # Columns to "unpivot" to rows. If blank, will use all columns not listed in id_vars.
               '2011_1'
               ,'2012_1'
               ,'2013_1'
               ,'2014_1'
               ,'2015_1'
               ,'2016_1'
               ,'2017_1'
               ,'2018_1'
               ,'2019_1'
               ,'2020_1'
               ,'2021_1'
               ]
   ,var_name='orig_col'             # Name for new "variable" column
   ,value_name='head'              # Name for new "value" column
)

# Drop unneeded rows
_droprows = (us_exports_chickens_m['desc'] == 'HS Code')
print(f"> Dropping {_droprows.sum() :,} rows.")
us_exports_chickens_m = us_exports_chickens_m.drop(us_exports_chickens_m.loc[_droprows].index).reset_index(drop=True)

# Create Year column
us_exports_chickens_m['year'] = us_exports_chickens_m['orig_col'].str[:4].astype(int)

# Change to numeric
us_exports_chickens_m['head'] = us_exports_chickens_m['head'].str.replace(',' ,'').astype('float')

# Pivot metrics to columns
us_exports_chickens_m_p = us_exports_chickens_m.pivot(
   index='year'           # Column(s) to make new index. If blank, uses existing index.
   ,columns='desc'        # Column(s) to make new columns
   ,values='head'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
)
us_exports_chickens_m_p = us_exports_chickens_m_p.add_prefix('export_')    # Add suffix to column names (all columns except index columns)
us_exports_chickens_m_p = us_exports_chickens_m_p.add_suffix('_hd')    # Add suffix to column names (all columns except index columns)
us_exports_chickens_m_p = us_exports_chickens_m_p.reset_index()           # Pivoting will change columns to indexes. Change them back.

#%% Swine Imports

# Read data
us_imports_swine = pd.read_csv(
   os.path.join(RAWDATA_FOLDER ,'usda_gats_us_imports_swine_2011_2021_corrected.csv')
   ,skiprows=3
   ,skipfooter=3
)
cleancolnames(us_imports_swine)
datainfo(us_imports_swine)

# Create description based on HS codes
us_imports_swine['desc'] = us_imports_swine['unnamed:_3'].replace(describe_hscode)

# Melt years to rows
us_imports_swine_m = us_imports_swine.melt(
   id_vars='desc'         # Optional: column(s) to use as ID variables
   ,value_vars=[     # Columns to "unpivot" to rows. If blank, will use all columns not listed in id_vars.
               '2011_1'
               ,'2012_1'
               ,'2013_1'
               ,'2014_1'
               ,'2015_1'
               ,'2016_1'
               ,'2017_1'
               ,'2018_1'
               ,'2019_1'
               ,'2020_1'
               ,'2021_1'
               ]
   ,var_name='orig_col'             # Name for new "variable" column
   ,value_name='head'              # Name for new "value" column
)

# Drop unneeded rows
_droprows = (us_imports_swine_m['desc'] == 'HS Code')
print(f"> Dropping {_droprows.sum() :,} rows.")
us_imports_swine_m = us_imports_swine_m.drop(us_imports_swine_m.loc[_droprows].index).reset_index(drop=True)

# Create Year column
us_imports_swine_m['year'] = us_imports_swine_m['orig_col'].str[:4].astype(int)

# Change to numeric
us_imports_swine_m['head'] = us_imports_swine_m['head'].str.replace(',' ,'').astype('float')

# Pivot metrics to columns
us_imports_swine_m_p = us_imports_swine_m.pivot(
   index='year'           # Column(s) to make new index. If blank, uses existing index.
   ,columns='desc'        # Column(s) to make new columns
   ,values='head'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
)
us_imports_swine_m_p = us_imports_swine_m_p.add_prefix('import_')    # Add suffix to column names (all columns except index columns)
us_imports_swine_m_p = us_imports_swine_m_p.add_suffix('_hd')    # Add suffix to column names (all columns except index columns)
us_imports_swine_m_p = us_imports_swine_m_p.reset_index()           # Pivoting will change columns to indexes. Change them back.

#%% Swine Exports

# Read data
us_exports_swine = pd.read_csv(
   os.path.join(RAWDATA_FOLDER ,'usda_gats_us_exports_swine_2011_2021_corrected.csv')
   ,skiprows=3
   ,skipfooter=3
)
cleancolnames(us_exports_swine)
datainfo(us_exports_swine)

# Create description based on HS codes
us_exports_swine['desc'] = us_exports_swine['unnamed:_3'].replace(describe_hscode)

# Melt years to rows
us_exports_swine_m = us_exports_swine.melt(
   id_vars='desc'         # Optional: column(s) to use as ID variables
   ,value_vars=[     # Columns to "unpivot" to rows. If blank, will use all columns not listed in id_vars.
               '2011_1'
               ,'2012_1'
               ,'2013_1'
               ,'2014_1'
               ,'2015_1'
               ,'2016_1'
               ,'2017_1'
               ,'2018_1'
               ,'2019_1'
               ,'2020_1'
               ,'2021_1'
               ]
   ,var_name='orig_col'             # Name for new "variable" column
   ,value_name='head'              # Name for new "value" column
)

# Drop unneeded rows
_droprows = (us_exports_swine_m['desc'] == 'HS Code')
print(f"> Dropping {_droprows.sum() :,} rows.")
us_exports_swine_m = us_exports_swine_m.drop(us_exports_swine_m.loc[_droprows].index).reset_index(drop=True)

# Create Year column
us_exports_swine_m['year'] = us_exports_swine_m['orig_col'].str[:4].astype(int)

# Change to numeric
us_exports_swine_m['head'] = us_exports_swine_m['head'].str.replace(',' ,'').astype('float')

# Pivot metrics to columns
us_exports_swine_m_p = us_exports_swine_m.pivot(
   index='year'           # Column(s) to make new index. If blank, uses existing index.
   ,columns='desc'        # Column(s) to make new columns
   ,values='head'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
)
us_exports_swine_m_p = us_exports_swine_m_p.add_prefix('export_')    # Add suffix to column names (all columns except index columns)
us_exports_swine_m_p = us_exports_swine_m_p.add_suffix('_hd')    # Add suffix to column names (all columns except index columns)
us_exports_swine_m_p = us_exports_swine_m_p.reset_index()           # Pivoting will change columns to indexes. Change them back.

#%% Export

us_imports_chickens_m_p.to_pickle(os.path.join(PRODATA_FOLDER ,'us_imports_chickens_m_p.pkl.gz'))
us_exports_chickens_m_p.to_pickle(os.path.join(PRODATA_FOLDER ,'us_exports_chickens_m_p.pkl.gz'))
us_imports_swine_m_p.to_pickle(os.path.join(PRODATA_FOLDER ,'us_imports_swine_m_p.pkl.gz'))
us_exports_swine_m_p.to_pickle(os.path.join(PRODATA_FOLDER ,'us_exports_swine_m_p.pkl.gz'))
