#%% About

# Pulling in data for Brazil
# https://www.avisite.com.br/estatisticas-precos/

#%% Brazil

# Import
brazil_chicksplaced = pd.read_excel(
   os.path.join(RAWDATA_FOLDER ,'Brazil_AviSite_ChicksPlaced.xlsx')
   ,sheet_name='Sheet1'
   ,skiprows=3                 # List: row numbers to skip. Integer: count of rows to skip at start of file
)
# cleancolnames(brazil_chicksplaced)
datainfo(brazil_chicksplaced)

# Pivot
brazil_chicksplaced_m = brazil_chicksplaced.melt(
   id_vars='Unnamed: 0'         # Columns to use as ID variables
   # ,value_vars=            # Columns to "unpivot" to rows. If blank, will use all columns not listed in id_vars.
   ,var_name='year_chr'             # Name for new "variable" column
   ,value_name='value'              # Name for new "value" column
)
datainfo(brazil_chicksplaced_m)

# Change data types
brazil_chicksplaced_m['year'] = brazil_chicksplaced_m['year_chr'].astype('int')

# Data is reported as million head, but note the European style numbers:
# commas are decimal separators, periods are thousands separators
# Unknown: are these chicks hatched or chicks placed??
brazil_chicksplaced_m['chicksplaced_mllnhd'] = brazil_chicksplaced_m['value'].str.replace('.' ,'').str.replace(',' ,'.').astype('float64')
brazil_chicksplaced_m['chicksplaced_thsdhd'] = brazil_chicksplaced_m['chicksplaced_mllnhd'] * 1000

# Subset columns
brazil_chicksplaced_m = brazil_chicksplaced_m[['year' ,'chicksplaced_thsdhd']]

# ----------------------------------------------------------------------------
# Describe and output
# ----------------------------------------------------------------------------
datainfo(brazil_chicksplaced_m)
brazil_chicksplaced_m.to_pickle(os.path.join(PRODATA_FOLDER ,'brazil_chicksplaced_m.pkl.gz'))
