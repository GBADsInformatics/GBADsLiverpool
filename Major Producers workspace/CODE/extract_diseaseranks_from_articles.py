# Extract tables ranking common diseases for each species
# From article An assessment of opportunities to dissect host genetic variation in resistance to infectious diseases in livestock

# ----------------------------------------------------------------------------
# Export to CSV
# ----------------------------------------------------------------------------
# Define folder path to article. Will export CSVs to same place.
article_pdf = os.path.join(RAWDATA_FOLDER ,'An_assessment_of_opportunities_to_dissct_host_gene.pdf')

tabula.convert_into(article_pdf ,os.path.join(RAWDATA_FOLDER ,'disease_ranks_poultry.csv') ,pages=9)
tabula.convert_into(article_pdf ,os.path.join(RAWDATA_FOLDER ,'disease_ranks_cattle.csv') ,pages=12)
tabula.convert_into(article_pdf ,os.path.join(RAWDATA_FOLDER ,'disease_ranks_swine.csv') ,pages=15)
tabula.convert_into(article_pdf ,os.path.join(RAWDATA_FOLDER ,'disease_ranks_salmon.csv') ,pages=18)

# ----------------------------------------------------------------------------
# Import to dataframe
# ----------------------------------------------------------------------------
# Simpler handling of column names than reading directly from PDF
df_poultry = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'disease_ranks_poultry.csv') ,skiprows=[1])
