# Exploring OIE WAHIS Disease data

#%% Birds

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
wahis_birds_imp = pd.read_excel(os.path.join(RAWDATA_FOLDER, 'wahis_birds_2011_2021.xlsx') ,sheet_name='Sheet1')
cleancolnames(wahis_birds_imp)
datainfo(wahis_birds_imp)

wahis_birds_countries = wahis_birds_imp['country'].unique()

# ----------------------------------------------------------------------------
# Calcs
# ----------------------------------------------------------------------------
# Add each metric in thousands of head
wahis_birds_imp.eval('''
                     susceptible_thsdhd = susceptible / 1000
                     cases_thsdhd = cases / 1000
                     killed_and_disposed_of_thsdhd = killed_and_disposed_of / 1000
                     slaughtered_thsdhd = slaughtered / 1000
                     deaths_thsdhd = deaths / 1000
                     vaccinated_thsdhd = vaccinated / 1000
                     '''
                     ,inplace=True
                     )

# Sum to country yearly level
wahis_birds_agg = wahis_birds_imp.pivot_table(
   index=['year' ,'country' ,'disease' ,'serotype_subtype_genotype' ,'animal_category' ,'species']      # Column(s) to make new index
   ,values=[
      'new_outbreaks'
      ,'susceptible_thsdhd'
      ,'cases_thsdhd'
      ,'killed_and_disposed_of_thsdhd'
      ,'slaughtered_thsdhd'
      ,'deaths_thsdhd'
      ,'vaccinated_thsdhd'
      ]
   ,aggfunc='sum'
   ,fill_value=0          # Replace missing values with this
   )
wahis_birds_agg = indextocolumns(wahis_birds_agg)
datainfo(wahis_birds_agg)

# ----------------------------------------------------------------------------
# Filter to reportable diseases
# https://www.oie.int/en/what-we-do/animal-health-and-welfare/animal-diseases/?_tax_animal=terrestrials%2Cavian
# https://www.oie.int/en/what-we-do/animal-health-and-welfare/animal-diseases/?_tax_animal=terrestrials%2Cmultiple-species
# ----------------------------------------------------------------------------
wahis_birds_diseases = list(wahis_birds_agg['disease'].unique())
wahis_birds_diseases_subtypes = wahis_birds_agg[['disease' ,'serotype_subtype_genotype']].value_counts()

# Full list is all bird diseases. Non-reportables are commented out.
# Can be any case. Will be converted to uppercase as needed.
# Create abbreviations while you're at it
wahis_birds_reportables_and_abbreviations = {
   # In Birds reportable list:
   'Avian chlamydiosis':'chla'
   ,'Avian infectious bronchitis':'aib'
   ,'Avian infectious laryngotracheitis':'ail'
   ,'High pathogenicity avian influenza viruses (poultry) (Inf. with)':'hpai'
   ,'Low pathogenic avian influenza (poultry) (2006-2021)':'lpai'
   ,'Avian mycoplasmosis (M.synoviae) (2006-)':'msyn'
   ,'Mycoplasma gallisepticum (Avian mycoplasmosis) (Inf. with)':'mgal'
   ,'Duck virus hepatitis':'dvh'
   ,'Fowl typhoid':'typh'
   ,'Influenza A viruses of high pathogenicity (Inf. with) (non-poultry including wild birds) (2017-)':'flua'
   ,'Infectious bursal disease (Gumboro disease)':'ibd'
   ,'Newcastle disease virus (Inf. with)':'ndv'
   ,'Pullorum disease':'pul'
   ,'Turkey rhinotracheitis (2006-)':'trh'

   # In Multispecies reportable list:
   ,'Rabies virus (Inf. with)':'rab'

   # ,'Bovine babesiosis':'bov'
   # ,'Fowl cholera (-2011)':'chol'
   # ,"Marek's disease (-2011)":"mrk"
   # ,'Salmonellosis (S. abortusovis)':'sal'
   # ,'Sheep pox and goat pox':'pox'
   }

birds_reportable_diseases_upcase = [x.upper() for x in list(wahis_birds_reportables_and_abbreviations)]
_wahis_birds_agg_reportable = (wahis_birds_agg['disease'].str.upper().isin(birds_reportable_diseases_upcase))
wahis_birds_agg_rptbl = wahis_birds_agg.loc[_wahis_birds_agg_reportable].copy()

#!!! Why doesn't Mycoplasma gallisepticum appear here?
check_wahis_birds_reportable = list(wahis_birds_agg_rptbl['disease'].unique())

# ----------------------------------------------------------------------------
# Pivot each disease to a column
# ----------------------------------------------------------------------------
# Add column with disease abbreviations
wahis_birds_agg_rptbl['disease_abbrev'] = wahis_birds_agg_rptbl['disease'].replace(wahis_birds_reportables_and_abbreviations)
wahis_birds_diseases_abb = list(wahis_birds_agg_rptbl['disease_abbrev'].unique())

# Sum over subtypes
wahis_birds_agg2 = wahis_birds_agg_rptbl.pivot_table(
   index=['year' ,'country' ,'disease' ,'disease_abbrev' ,'animal_category' ,'species']      # Column(s) to make new index
   ,values=[
      'new_outbreaks'
      ,'susceptible_thsdhd'
      ,'cases_thsdhd'
      ,'killed_and_disposed_of_thsdhd'
      ,'slaughtered_thsdhd'
      ,'deaths_thsdhd'
      ,'vaccinated_thsdhd'
      ]
   ,aggfunc='sum'
   ,fill_value=0          # Replace missing values with this
   )
wahis_birds_agg2 = indextocolumns(wahis_birds_agg2)
datainfo(wahis_birds_agg2)

# Pivot
wahis_birds_agg2_p = wahis_birds_agg2.pivot(
   index=['year' ,'country' ,'animal_category' ,'species']
   ,columns='disease_abbrev'        # Column(s) to make new columns
   ,values=[
      'new_outbreaks'
      ,'susceptible_thsdhd'
      ,'cases_thsdhd'
      ,'killed_and_disposed_of_thsdhd'
      ,'slaughtered_thsdhd'
      ,'deaths_thsdhd'
      ,'vaccinated_thsdhd'
      ]
   )
wahis_birds_agg2_p = indextocolumns(wahis_birds_agg2_p)
wahis_birds_agg2_p = colnames_from_index(wahis_birds_agg2_p)
datainfo(wahis_birds_agg2_p)

#!!! Fill missing with zero
# Either missing to begin with or because no data for that disease and year
wahis_birds_agg2_p.replace(np.nan ,0 ,inplace=True)

# ----------------------------------------------------------------------------
# Calcs
# ----------------------------------------------------------------------------
# Sum of each metric over all diseases
cols_new_outbreaks = [colname for colname in list(wahis_birds_agg2_p) if 'new_outbreaks' in colname]
cols_susceptible = [colname for colname in list(wahis_birds_agg2_p) if 'susceptible' in colname]
cols_cases = [colname for colname in list(wahis_birds_agg2_p) if 'cases' in colname]
cols_killed_and_disposed_of = [colname for colname in list(wahis_birds_agg2_p) if 'killed_and_disposed_of' in colname]
cols_slaughtered = [colname for colname in list(wahis_birds_agg2_p) if 'slaughtered' in colname]
cols_deaths = [colname for colname in list(wahis_birds_agg2_p) if 'deaths' in colname]
cols_vaccinated = [colname for colname in list(wahis_birds_agg2_p) if 'vaccinated' in colname]

wahis_birds_agg2_p['new_outbreaks_anydisease'] = wahis_birds_agg2_p[cols_new_outbreaks].sum(axis=1)
wahis_birds_agg2_p['susceptible_thsdhd_anydisease'] = wahis_birds_agg2_p[cols_susceptible].sum(axis=1)
wahis_birds_agg2_p['cases_thsdhd_anydisease'] = wahis_birds_agg2_p[cols_cases].sum(axis=1)
wahis_birds_agg2_p['killed_and_disposed_of_thsdhd_anydisease'] = wahis_birds_agg2_p[cols_killed_and_disposed_of].sum(axis=1)
wahis_birds_agg2_p['slaughtered_thsdhd_anydisease'] = wahis_birds_agg2_p[cols_slaughtered].sum(axis=1)
wahis_birds_agg2_p['deaths_thsdhd_anydisease'] = wahis_birds_agg2_p[cols_deaths].sum(axis=1)
wahis_birds_agg2_p['vaccinated_thsdhd_anydisease'] = wahis_birds_agg2_p[cols_vaccinated].sum(axis=1)

datainfo(wahis_birds_agg2_p)

#%% Describe and output

datadesc(wahis_birds_agg ,CHARACTERIZE_FOLDER)
wahis_birds_agg.to_pickle(os.path.join(PRODATA_FOLDER ,'wahis_birds_agg.pkl.gz'))
# profile = wahis_birds_agg.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'wahis_birds_agg_profile.html'))

datadesc(wahis_birds_agg2_p ,CHARACTERIZE_FOLDER)
wahis_birds_agg2_p.to_pickle(os.path.join(PRODATA_FOLDER ,'wahis_birds_agg2_p.pkl.gz'))
# profile = wahis_birds_agg2_p.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'wahis_birds_agg2_p_profile.html'))

#%% Swine

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
wahis_swine_imp = pd.read_excel(os.path.join(RAWDATA_FOLDER, 'wahis_swine_2011_2021.xlsx') ,sheet_name='Sheet1')
cleancolnames(wahis_swine_imp)
datainfo(wahis_swine_imp)

wahis_swine_countries = wahis_swine_imp['country'].unique()

# ----------------------------------------------------------------------------
# Calcs
# ----------------------------------------------------------------------------
# Add each metric in thousands of head
wahis_swine_imp.eval('''
                     susceptible_thsdhd = susceptible / 1000
                     cases_thsdhd = cases / 1000
                     killed_and_disposed_of_thsdhd = killed_and_disposed_of / 1000
                     slaughtered_thsdhd = slaughtered / 1000
                     deaths_thsdhd = deaths / 1000
                     vaccinated_thsdhd = vaccinated / 1000
                     '''
                     ,inplace=True
                     )

# Sum to country yearly level
wahis_swine_agg = wahis_swine_imp.pivot_table(
   index=['year' ,'country' ,'disease' ,'serotype_subtype_genotype' ,'animal_category' ,'species']      # Column(s) to make new index
   ,values=[
      'new_outbreaks'
      ,'susceptible_thsdhd'
      ,'cases_thsdhd'
      ,'killed_and_disposed_of_thsdhd'
      ,'slaughtered_thsdhd'
      ,'deaths_thsdhd'
      ,'vaccinated_thsdhd'
      ]
   ,aggfunc='sum'
   ,fill_value=0          # Replace missing values with this
   )
wahis_swine_agg = indextocolumns(wahis_swine_agg)
datainfo(wahis_swine_agg)

# ----------------------------------------------------------------------------
# Filter to reportable diseases
# https://www.oie.int/en/what-we-do/animal-health-and-welfare/animal-diseases/?_tax_animal=terrestrials%2Cswine
# https://www.oie.int/en/what-we-do/animal-health-and-welfare/animal-diseases/?_tax_animal=terrestrials%2Cmultiple-species
# ----------------------------------------------------------------------------
wahis_swine_diseases = list(wahis_swine_agg['disease'].unique())
wahis_swine_diseases_subtypes = wahis_swine_agg[['disease' ,'serotype_subtype_genotype']].value_counts()

# Full list is all swine diseases. Non-reportables are commented out.
# Can be any case. Will be converted to uppercase as needed.
# Create abbreviations while you're at it
wahis_swine_reportables_and_abbreviations = {
   # In Swine reportable list:
   'African swine fever virus (Inf. with)':'asf'
   ,'Classical swine fever virus (Inf. with)':'csf'
   ,'Porcine reproductive and respiratory syndrome virus (Inf. with)':'prrs'
   ,'Transmissible gastroenteritis':'tgas'
   ,'Taenia solium (Inf. with) (Porcine cysticercosis)':'tsol'

   # Swine reportable diseases that don't appear in data:
   # ,'nipah virus':''
   # ,'old world screwworm':''

   # In Multispecies reportable list:
   ,'Anthrax':'antx'
   ,"Aujeszky's disease virus (Inf. with)":"adv"
   ,'Brucella abortus (Inf. with)':'babo'
   ,'Brucella suis (Inf. with)':'bsui'
   ,'Echinococcosis/hydatidosis':'ehyd'
   ,'Echinococcus granulosus (Inf. with) (2014-)':'egra'
   ,'Echinococcus multilocularis (Inf. with) (2014-)':'emul'
   ,'Foot and mouth disease virus (Inf. with)':'fmd'
   ,'Japanese encephalitis':'jenc'
   ,'New world screwworm (Cochliomyia hominivorax)':'nws'
   ,'Rabies virus (Inf. with)':'rab'
   ,'Trichinella spp. (Inf. with)':'tric'

   # ,'Bovine tuberculosis (-2018)':'bvtb'
   # ,'Leptospirosis':'lept'
   # ,'Porcine epidemic diarrhoea virus (Inf. with)':'ped'
   # ,'SARS-CoV-2 in animals (Inf. with)':'cov2'
   # ,'Swine vesicular disease (-2014)':'svd'
   # ,'Vesicular stomatitis (-2014)':'vsto'
   }

swine_reportable_diseases_upcase = [x.upper() for x in list(wahis_swine_reportables_and_abbreviations)]
_wahis_swine_agg_reportable = (wahis_swine_agg['disease'].str.upper().isin(swine_reportable_diseases_upcase))
wahis_swine_agg_rptbl = wahis_swine_agg.loc[_wahis_swine_agg_reportable].copy()
datainfo(wahis_swine_agg_rptbl)

check_wahis_swine_reportable = list(wahis_swine_agg_rptbl['disease'].unique())

# ----------------------------------------------------------------------------
# Pivot each disease to a column
# ----------------------------------------------------------------------------
# Add column with disease abbreviations
wahis_swine_agg_rptbl['disease_abbrev'] = wahis_swine_agg_rptbl['disease'].replace(wahis_swine_reportables_and_abbreviations)
wahis_swine_diseases_abb = list(wahis_swine_agg_rptbl['disease_abbrev'].unique())

# Sum over subtypes
wahis_swine_agg2 = wahis_swine_agg_rptbl.pivot_table(
   index=['year' ,'country' ,'disease' ,'disease_abbrev' ,'animal_category' ,'species']      # Column(s) to make new index
   ,values=[
      'new_outbreaks'
      ,'susceptible_thsdhd'
      ,'cases_thsdhd'
      ,'killed_and_disposed_of_thsdhd'
      ,'slaughtered_thsdhd'
      ,'deaths_thsdhd'
      ,'vaccinated_thsdhd'
      ]
   ,aggfunc='sum'
   ,fill_value=0          # Replace missing values with this
   )
wahis_swine_agg2 = indextocolumns(wahis_swine_agg2)
datainfo(wahis_swine_agg2)

# Pivot
wahis_swine_agg2_p = wahis_swine_agg2.pivot(
   index=['year' ,'country' ,'animal_category' ,'species']
   ,columns='disease_abbrev'        # Column(s) to make new columns
   ,values=[
      'new_outbreaks'
      ,'susceptible_thsdhd'
      ,'cases_thsdhd'
      ,'killed_and_disposed_of_thsdhd'
      ,'slaughtered_thsdhd'
      ,'deaths_thsdhd'
      ,'vaccinated_thsdhd'
      ]
   )
wahis_swine_agg2_p = indextocolumns(wahis_swine_agg2_p)
wahis_swine_agg2_p = colnames_from_index(wahis_swine_agg2_p)
datainfo(wahis_swine_agg2_p)

#!!! Fill missing with zero
# Either missing to begin with or because no data for that disease and year
wahis_swine_agg2_p.replace(np.nan ,0 ,inplace=True)

# ----------------------------------------------------------------------------
# Calcs
# ----------------------------------------------------------------------------
# Sum of each metric over all diseases
cols_new_outbreaks = [colname for colname in list(wahis_swine_agg2_p) if 'new_outbreaks' in colname]
cols_susceptible = [colname for colname in list(wahis_swine_agg2_p) if 'susceptible' in colname]
cols_cases = [colname for colname in list(wahis_swine_agg2_p) if 'cases' in colname]
cols_killed_and_disposed_of = [colname for colname in list(wahis_swine_agg2_p) if 'killed_and_disposed_of' in colname]
cols_slaughtered = [colname for colname in list(wahis_swine_agg2_p) if 'slaughtered' in colname]
cols_deaths = [colname for colname in list(wahis_swine_agg2_p) if 'deaths' in colname]
cols_vaccinated = [colname for colname in list(wahis_swine_agg2_p) if 'vaccinated' in colname]

wahis_swine_agg2_p['new_outbreaks_anydisease'] = wahis_swine_agg2_p[cols_new_outbreaks].sum(axis=1)
wahis_swine_agg2_p['susceptible_thsdhd_anydisease'] = wahis_swine_agg2_p[cols_susceptible].sum(axis=1)
wahis_swine_agg2_p['cases_thsdhd_anydisease'] = wahis_swine_agg2_p[cols_cases].sum(axis=1)
wahis_swine_agg2_p['killed_and_disposed_of_thsdhd_anydisease'] = wahis_swine_agg2_p[cols_killed_and_disposed_of].sum(axis=1)
wahis_swine_agg2_p['slaughtered_thsdhd_anydisease'] = wahis_swine_agg2_p[cols_slaughtered].sum(axis=1)
wahis_swine_agg2_p['deaths_thsdhd_anydisease'] = wahis_swine_agg2_p[cols_deaths].sum(axis=1)
wahis_swine_agg2_p['vaccinated_thsdhd_anydisease'] = wahis_swine_agg2_p[cols_vaccinated].sum(axis=1)

datainfo(wahis_swine_agg2_p)

#%% Describe and output

datadesc(wahis_swine_agg ,CHARACTERIZE_FOLDER)
wahis_swine_agg.to_pickle(os.path.join(PRODATA_FOLDER ,'wahis_swine_agg.pkl.gz'))
# profile = wahis_swine_agg.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'wahis_swine_agg_profile.html'))

datadesc(wahis_swine_agg2_p ,CHARACTERIZE_FOLDER)
wahis_swine_agg2_p.to_pickle(os.path.join(PRODATA_FOLDER ,'wahis_swine_agg2_p.pkl.gz'))
# profile = wahis_swine_agg2_p.profile_report()
# profile.to_file(os.path.join(CHARACTERIZE_FOLDER ,'wahis_swine_agg2_p_profile.html'))
