# UK Food Standards Agency condemns (conditions)
# https://data.food.gov.uk/catalog/datasets?search=conditions#results

#%% Poultry

ukfsa_poultry_folder = os.path.join(RAWDATA_FOLDER ,'UK FSA Poultry Conditions')

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
# Get list of files
poultry_file_list = os.listdir(ukfsa_poultry_folder)

# Loop through all files, importing and appending
ukfsa_poultry = pd.DataFrame()
for FILE in poultry_file_list:
   file_i = pd.read_csv(os.path.join(ukfsa_poultry_folder ,FILE))
   ukfsa_poultry = ukfsa_poultry.append(file_i)

cleancolnames(ukfsa_poultry)

# Cleanup species
ukfsa_poultry_fillspecies = (ukfsa_poultry['species'].isnull())
ukfsa_poultry.loc[ukfsa_poultry_fillspecies ,'species'] = ukfsa_poultry.loc[ukfsa_poultry_fillspecies ,'___species']
ukfsa_poultry.drop(columns='___species' ,inplace=True)

species = list(ukfsa_poultry['species'].unique())
conditions = list(ukfsa_poultry['condition'].unique())
countries = list(ukfsa_poultry['country'].unique())

datainfo(ukfsa_poultry)

# ----------------------------------------------------------------------------
# Sum yearly
# ----------------------------------------------------------------------------
# Create year
ukfsa_poultry[['year' ,'month']] = ukfsa_poultry['yearmonth'].str.split('-' ,expand=True)
ukfsa_poultry['year'] = ukfsa_poultry['year'].astype('int64')
ukfsa_poultry['month'] = ukfsa_poultry['month'].astype('int64')

# Sum total condemns yearly, separately for each species
ukfsa_poultry_sum = ukfsa_poultry.pivot_table(
   index=['year' ,'species']           # Column(s) to make new index
   ,values=['numberofconditions' ,'throughput']         # Column(s) to aggregate
   ,aggfunc='sum'                  # Aggregate function to use. Can pass list or dictionary {'colname':'function'}. See numpy functions https://docs.scipy.org/doc/numpy/reference/routines.statistics.html
).reset_index()

rename_cols = {
   'numberofconditions':'condemned_hd'
   ,'throughput':'totalproduction_hd'
}
ukfsa_poultry_sum = ukfsa_poultry_sum.rename(columns=rename_cols)

# Pivot so each species is a column
ukfsa_poultry_sum_p = ukfsa_poultry_sum.pivot(
   index='year'          # Column(s) to make new index. If blank, uses existing index.
   ,columns='species'        # Column(s) to make new columns
   ,values=['condemned_hd' ,'totalproduction_hd']                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
).reset_index()
ukfsa_poultry_sum_p = colnames_from_index(ukfsa_poultry_sum_p)
cleancolnames(ukfsa_poultry_sum_p)
datainfo(ukfsa_poultry_sum_p)

ukfsa_poultry_sum_p['year'] = ukfsa_poultry_sum_p['year_']

# Exclude 2016 because it's a partial year
_droprows = (ukfsa_poultry_sum_p['year'] == 2016)
print(f"> Dropping {_droprows.sum() :,} rows.")
ukfsa_poultry_sum_p = ukfsa_poultry_sum_p.drop(ukfsa_poultry_sum_p.loc[_droprows].index)

# ----------------------------------------------------------------------------
# Describe and Output
# ----------------------------------------------------------------------------
ukfsa_poultry_sum_p.to_pickle(os.path.join(PRODATA_FOLDER ,'ukfsa_poultry_sum_p.pkl.gz'))

#%% Pigs

ukfsa_pig_folder = os.path.join(RAWDATA_FOLDER ,'UK FSA Pig Conditions')

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
# Get list of files
pig_file_list = os.listdir(ukfsa_pig_folder)

# Loop through all files, importing and appending
ukfsa_pigs = pd.DataFrame()
for FILE in pig_file_list:
   file_i = pd.read_csv(os.path.join(ukfsa_pig_folder ,FILE))
   ukfsa_pigs = ukfsa_pigs.append(file_i)

cleancolnames(ukfsa_pigs)
datainfo(ukfsa_pigs)
