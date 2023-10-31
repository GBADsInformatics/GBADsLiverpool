# For documentation see http://gbadske.org:9000/dataportal/

import requests as req         # For sending HTTP requests

#%% View tables and field names

# Get list of all available tables
gbadske_tablelist_uri = 'http://gbadske.org:9000/GBADsTables/public?format=text'
gbadske_tablelist_resp = req.get(gbadske_tablelist_uri)
gbadske_tablelist = gbadske_tablelist_resp.text.split(',')   # Convert to list

# ----------------------------------------------------------------------------
# This command returns the column/field names in the given table
# ----------------------------------------------------------------------------
gbadske_fieldnames_uri = 'http://gbadske.org:9000/GBADsTable/public'
gbadske_fieldnames_params = {
   'table_name':'livestock_production_faostat'
   ,'format':'text'
   }
gbadske_fieldnames_resp = req.get(gbadske_fieldnames_uri , params=gbadske_fieldnames_params)
print(gbadske_fieldnames_resp.text)

#%% Livestock population

# This commands retrieves data from one of the livestock population tables in the public database
# Specify table that will be common to both FAO and OIE
gbadske_table_params = {
   'year':'*'
   ,'country':'*'
   ,'species':'Poultry'   # FAO can use more specific "Chickens", but OIE cannot
   ,'format':'file'
   }

# ----------------------------------------------------------------------------
# FAOStat
# ----------------------------------------------------------------------------
gbadske_table_fao_uri = 'http://gbadske.org:9000/GBADsLivestockPopulation/faostat'
gbadske_table_fao_resp = req.get(gbadske_table_fao_uri , params=gbadske_table_params)
# print(gbadske_table_fao_resp.text)

# Save to CSV
tofile = os.path.join(EXPDATA_FOLDER ,'gbadske_pop_poultry_fao.csv')
with open(tofile, 'w') as f:
   f.write(gbadske_table_fao_resp.text)

# ----------------------------------------------------------------------------
# OIE
# ----------------------------------------------------------------------------
gbadske_table_oie_uri = 'http://gbadske.org:9000/GBADsLivestockPopulation/oie'
gbadske_table_oie_resp = req.get(gbadske_table_oie_uri , params=gbadske_table_params)
# print(gbadske_table_oie_resp.text)

# Save to CSV
tofile = os.path.join(EXPDATA_FOLDER ,'gbadske_pop_poultry_oie.csv')
with open(tofile, 'w') as f:
   f.write(gbadske_table_oie_resp.text)

#%% Biomass

# Use general table query
# Test call copied directly from documentation
test_req = 'http://gbadske.org:9000/GBADsPublicQuery/livestock_production_faostat?fields=country,year,species,population&query=year=2017%20AND%20species=%27Goats%27&format=file'
test_resp = req.get(test_req)
print(test_resp.text)

gbadske_table_biomass_uri = 'http://gbadske.org:9000/GBADsPublicQuery/'

# ----------------------------------------------------------------------------
# FAOStat
# ----------------------------------------------------------------------------
gbadske_table_biomass_fao = 'livestock_national_population_biomass_faostat'

# Get field names
gbadske_fieldnames_params = {
   'table_name':gbadske_table_biomass_fao
   ,'format':'text'
   }
gbadske_fieldnames_resp = req.get(gbadske_fieldnames_uri , params=gbadske_fieldnames_params)
print(gbadske_fieldnames_resp.text)

# Get data
# This approach is giving an error. Manually build the url instead.
# gbadske_table_biomass_fao_params = {
#    'fields':gbadske_fieldnames_resp.text
#    ,'query':'year=2017ANDspecies=Goats'
#    ,'format':'file'
#    }
# gbadske_table_biomass_fao_resp = req.get(gbadske_table_biomass_uri + gbadske_table_biomass_fao , params=gbadske_table_biomass_fao_params)
# print(gbadske_table_biomass_fao_resp.text)

gbadske_table_biomass_fields = gbadske_fieldnames_resp.text
# gbadske_table_biomass_query = 'year=2017%20AND%20species=%27Chickens%27'
gbadske_table_biomass_query = 'species=%27Chickens%27'   # All years
gbadske_table_biomass_format = 'file'

gbadske_table_biomass_fao_req = gbadske_table_biomass_uri + gbadske_table_biomass_fao +\
   '?fields=' + gbadske_table_biomass_fields +\
      '&query=' + gbadske_table_biomass_query +\
         '&format=' + gbadske_table_biomass_format

gbadske_table_biomass_fao_resp = req.get(gbadske_table_biomass_fao_req)
print(gbadske_table_biomass_fao_resp.text)

# Save to CSV
tofile = os.path.join(EXPDATA_FOLDER ,'gbadske_biomass_fao_chickens.csv')
with open(tofile, 'w') as f:
   f.write(gbadske_table_biomass_fao_resp.text)

# ----------------------------------------------------------------------------
# OIE
# ----------------------------------------------------------------------------
gbadske_table_biomass_oie = 'livestock_national_population_biomass_oie'

# Get field names
gbadske_fieldnames_params = {
   'table_name':gbadske_table_biomass_oie
   ,'format':'text'
   }
gbadske_fieldnames_resp = req.get(gbadske_fieldnames_uri , params=gbadske_fieldnames_params)
print(gbadske_fieldnames_resp.text)

# Get data
gbadske_table_biomass_fields = gbadske_fieldnames_resp.text
# gbadske_table_biomass_query = 'year=2017%20AND%20species=%27Chickens%27'
gbadske_table_biomass_query = 'species=%27Birds%27'   # All years
gbadske_table_biomass_format = 'file'

gbadske_table_biomass_oie_req = gbadske_table_biomass_uri + gbadske_table_biomass_oie +\
   '?fields=' + gbadske_table_biomass_fields +\
      '&query=' + gbadske_table_biomass_query +\
         '&format=' + gbadske_table_biomass_format

gbadske_table_biomass_oie_resp = req.get(gbadske_table_biomass_oie_req)
print(gbadske_table_biomass_oie_resp.text)

# Save to CSV
tofile = os.path.join(EXPDATA_FOLDER ,'gbadske_biomass_oie_birds.csv')
with open(tofile, 'w') as f:
   f.write(gbadske_table_biomass_fao_resp.text)
