#%% About
'''
UN Comtrade database
Import/Exports
https://comtrade.un.org/data/

This data uses HS commodity codes like the UDSA GATS data, but does not go to the same level of detail,
as William pointed out.

For example, this data contains code 010511 - live fowls of species Gallus Domesticus weighing LTE 185g,
but does not specify how many are breeders vs. fattening birds, which is coded in the last 4 digits of
the 10-digit code:
   0105110040:"live fowls of species Gallus Domesticus weighing LTE 185g, nonbreeding"
   0105110020:"live fowls of species Gallus Domesticus weighing LTE 185g, breeding_broiler"
   0105110010:"live fowls of species Gallus Domesticus weighing LTE 185g, breeding_layer"

Lookup commodity codes here:
   Note Imported data drops leading zero
   https://www.aphis.usda.gov/aphis/ourfocus/importexport/ace/hts-master-correlation-table

'''
#%% Define HS code lookup

# Define lookup for HS codes
# Note data has dropped leading zero
describe_hscode = {
   10511:"live_chickens_lte185g_alluses"
   ,10591:"live_chickens_gt185g_alluses"
   ,10594:"live_chickens_other"

   ,10391:"live_swine_lt50kg"
   ,10392:"live_swine_gte50kg"
}

#%% Import and stack

uncomtrade_2009_2011 = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'uncomtrade_impexp_swine_poultry_2009_2011.csv'))
uncomtrade_2012_2016 = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'uncomtrade_impexp_swine_poultry_2012_2016.csv'))
uncomtrade_2017_2021 = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'uncomtrade_impexp_swine_poultry_2017_2021.csv'))

uncomtrade_stack = pd.concat(
   [uncomtrade_2009_2011 ,uncomtrade_2012_2016 ,uncomtrade_2017_2021]      # List of dataframes to concatenate
   ,axis=0              # Axis=0: concatenate rows, axis=1: concatenate columns (merge)
   ,ignore_index=True   # True: do not keep index values on concatenation axis
)

#%% Cleanup

cleancolnames(uncomtrade_stack)
datainfo(uncomtrade_stack)

list_codes = list(uncomtrade_stack['commodity_code'].unique())

# Create description based on HS codes
uncomtrade_stack['commodity_desc'] = uncomtrade_stack['commodity_code'].replace(describe_hscode)
list_codes_desc = uncomtrade_stack[['commodity_code' ,'commodity_desc']].value_counts()

# Combine description and flow
uncomtrade_stack['commodity_desc_flow'] = \
   uncomtrade_stack['trade_flow'] +\
      '_' + uncomtrade_stack['commodity_desc'] +\
         '_' + uncomtrade_stack['qty_unit']

# Pivot
uncomtrade_stack_p = uncomtrade_stack.pivot(
   index=['reporter' ,'year']           # Column(s) to make new index. If blank, uses existing index.
   ,columns='commodity_desc_flow'        # Column(s) to make new columns
   ,values='qty'                   # Column to populate rows. Can pass a list, but will create multi-indexed columns.
).reset_index()           # Pivoting will change columns to indexes. Change them back.
cleancolnames(uncomtrade_stack_p)
datainfo(uncomtrade_stack_p)

# Many combinations of commodity and unit create blanks. Fill in blanks with zero.
uncomtrade_stack_p = uncomtrade_stack_p.replace(np.nan ,0)

# Drop columns that are all zero
col_allzero = (uncomtrade_stack_p == 0).all(axis=0)                # Check whether each column contains all zeros. Creates boolean series.
col_allzero_count = col_allzero.sum()              # Count of columns that are all zero
uncomtrade_stack_p_nz = uncomtrade_stack_p.loc[:, ~ col_allzero]                      # Keep only columns that are not (~) all zeros
datainfo(uncomtrade_stack_p_nz)

#%% Export

uncomtrade_stack_p_nz.to_pickle(os.path.join(PRODATA_FOLDER ,'uncomtrade_stack_p_nz.pkl.gz'))
