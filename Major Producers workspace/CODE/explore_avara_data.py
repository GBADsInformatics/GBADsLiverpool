# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------
# Cleaned data
avara_pm = pd.read_excel(os.path.join(RAWDATA_FOLDER, 'Avara', 'Avara PMdata_nopw.xlsx')
                         ,sheet_name='Data'
                         )
cleancolnames(avara_pm)
datainfo(avara_pm)

# Original data (merged tab)
avara_orig = pd.read_excel(os.path.join(RAWDATA_FOLDER, 'Avara', 'Avara_original_nopw.xlsx')
                           ,sheet_name='Merge'
                           )
cleancolnames(avara_orig)
datainfo(avara_orig)

# ----------------------------------------------------------------------------
# Split columns describing shed from those describing sampled birds
# ----------------------------------------------------------------------------
avara_pm_keycols = ['shed_code_old' ,'shedcodenew' ,'date_placed' ,'postingdate']
avara_pm_shedcols = list(avara_pm.iloc[: ,0:116])
avara_pm_birdcols = list(avara_pm.iloc[: ,116:])

# Shed level
avara_pm_shedlevel = avara_pm[avara_pm_shedcols].copy()
avara_pm_shedlevel.drop_duplicates(inplace=True)      # Drop duplicate shed records corresponding to individual sampled birds. Should be 6 per shed.
datainfo(avara_pm_shedlevel)

keyfreq = avara_pm_shedlevel[avara_pm_keycols].value_counts()

# Sampled birds
avara_pm_birdlevel = avara_pm[avara_pm_keycols + avara_pm_birdcols].copy()
datainfo(avara_pm_birdlevel)

# ----------------------------------------------------------------------------
# Picking out variables I like
# ----------------------------------------------------------------------------
