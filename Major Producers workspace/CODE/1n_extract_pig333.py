#%% About
'''
Using a free account created for jreplogle@firstanalytics.com.
Manually copying data from pig333.com because data cannot be downloaded, only viewed.
This code is organized into sections matching the organization on the pig333 website.

According to the site, data for European countries comes from Eurostat, which we are already using.
So I am only bringing in Pig333 data for the non-European countries in scope (Brazil, China, and Russia).
'''
#%% Production data

pig333_production = pd.read_excel(os.path.join(RAWDATA_FOLDER ,'pig333_production_brazil_china_russia_2010_2021.xlsx') ,sheet_name='Data')
datainfo(pig333_production)

#%% Producer Prices

pig333_producerprice = pd.read_excel(os.path.join(RAWDATA_FOLDER ,'pig333_producerprice_brazil_china_russia_2010_2021.xlsx') ,sheet_name='Data')
datainfo(pig333_producerprice)

#%% Merge and export

pig333_production_price = pd.merge(
   left=pig333_production
   ,right=pig333_producerprice
   ,on=['country' ,'year']
   ,how='outer'
)

# Export
pig333_production_price.to_pickle(os.path.join(PRODATA_FOLDER ,'pig333_production_price.pkl.gz'))
