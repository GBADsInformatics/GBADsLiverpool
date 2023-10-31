#%% About
'''
This program brings in variables for the UK that are not in databases, but rather
estimates from studies or expert opinion.
'''
#%% Average DOF & Downtime

ukmisc_poultry = pd.DataFrame(
   {'country':'United Kingdom'
   ,'avgdaysonfeed':35
   ,'avgdowntime_days':7
   ,'cyclesperyear':7
   }
   ,index=[0]                    # If creating a single-row dataframe, must specify index=[0]
)

#%% Describe and Output

datainfo(ukmisc_poultry)
datadesc(ukmisc_poultry ,CHARACTERIZE_FOLDER)

ukmisc_poultry.to_pickle(os.path.join(PRODATA_FOLDER ,'ukmisc_poultry.pkl.gz'))
