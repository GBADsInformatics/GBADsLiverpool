#%% About
'''
Reading tables from breed standard PDFs.
'''
#%% PIC
# https://www.pic.com/wp-content/uploads/sites/3/2021/12/PIC-Wean-To-Finish-Manual.pdf

# =============================================================================
#### Read tables into pandas dataframes
# =============================================================================
# Table 1.1: ADG, FCR, etc.
pic_adg_list = tabula.read_pdf(
   os.path.join(RAWDATA_FOLDER ,'Swine standards' ,'PIC-Wean-To-Finish-Manual.pdf')
   ,pages=5    # Integer or List: page number(s) to read from. 'all': read whole PDF.
)
pic_adg = pic_adg_list[0]

# Tables A-1, A-2, and A-3: Weight by Days on Feed for different flooring types
pic_dof_list = tabula.read_pdf(
   os.path.join(RAWDATA_FOLDER ,'Swine standards' ,'PIC-Wean-To-Finish-Manual.pdf')
   ,pages=[45 ,46 ,47]    # Integer or List: page number(s) to read from. 'all': read whole PDF.
)
# Table A-1: Recommendations for Curtain-Sided Barns Utilizing Brooders and No Mats With Slatted Floors
pic_dof_1 = pic_dof_list[0]
cleancolnames(pic_dof_1)
datainfo(pic_dof_1)

# Table A-2: Recommendations for Curtain-Sided Barns Utilizing Brooders and Mats With Concrete Slatted Floors
pic_dof_2 = pic_dof_list[0]
cleancolnames(pic_dof_2)
datainfo(pic_dof_2)

# Table A-3: Recommendations for Solid-Sided Barns With Slated Floors Utilizing Both Brooders and Mats
pic_dof_3 = pic_dof_list[0]
cleancolnames(pic_dof_3)
datainfo(pic_dof_3)

# Table J: Growth and Feed Intake by Day on Feed
# Supercedes tables A-1 thru A-3 as it covers more days on feed and has expected feed intake
pic_growthandfeed_list = tabula.read_pdf(
   os.path.join(RAWDATA_FOLDER ,'Swine standards' ,'PIC-Wean-To-Finish-Manual.pdf')
   ,pages=56    # Integer or List: page number(s) to read from. 'all': read whole PDF.
)
pic_growthandfeed = pic_growthandfeed_list[0]

# Table 1.1 produced by Tabula is hard to work with
# If you need this table, try starting with copy-paste (below)
'''
NURSERY = 12-63 LBS (5.5-28.6 KG) TARGET
AVERAGE
PERFORMANCE
INTERVENTION
GROW-FINISH = 63-277 LBS (27.2-126 KG) LEVEL
Average Daily Gain
Nursery, lbs/day (kg/day) 1.07 (0.487) 1.04 (0.473) 0.84 (0.383)
Grow-Finish, lbs/day (kg/day) 2.10 (0.955) 2.04 (0.927) 1.84 (0.835)
Wean-to-Finish, lbs/day (kg/day) 1.77 (0.805) 1.72 (0.782) 1.55 (0.704)
Feed Conversion (lbs:lbs or kg:kg)
Nursery 1.31 1.46 1.66
Grow-Finish 2.33 2.59 2.80
 1,560 Kcal ME Diet 2.25 2.50 2.70
 1,470 Kcal ME Diet 2.42 2.69 2.91
Wean-to-Finish 2.13 2.37 2.56
Energy Conversion (1,516 Kcal/lb)
Nursery 1,982 2,202 2,356
Grow-Finish 3,539 3,932 4,207
Wean-to-Finish 3,239 3,599 3,851
Losses
Nursery Mortality % 1.5% 2.0% 3.0%
Grow-Finish Mortality % 2.0% 2.5% 4.0%
Wean-to-Finish 3.5% 4.5% 7.0%
Cull Rate 0.5% 1.0% 2.0%
Defects, % of all Pigs
Scrotal Hernias 0.50% 1.00% 1.50%
Rigs (retained testicle) 0.13% 0.25% 0.50%
Umbilical Hernias 0.40% 0.80% 1.50%
Transport Loss, %
DOA’s (Dead on arrival) 0.06% 0.13% 0.20%
NAI/NANI’s (Injured/Fatigued) 0.08% 0.15% 0.25%
'''
# =============================================================================
#### Cleanup
# =============================================================================
# -----------------------------------------------------------------------------
# Day on Feed Tables
# -----------------------------------------------------------------------------
def cleanup_pic_dof_table(INPUT_DF):
   OUTPUT_DF = INPUT_DF.copy()

   # Drop unused header rows
   OUTPUT_DF = OUTPUT_DF.drop(index=[0, 1]).reset_index(drop=True)

   # Rename columns
   rename_cols = {
      'unnamed:_0':'dayonfeed'
      ,'unnamed:_1':'bodyweight'
      ,'desired_room':'desired_room_temperature'
      ,'winter':'winter_setpoint'
      ,'summer':'summer_setpoint'
      ,'unnamed:_2':'winter_cfm'
   }
   OUTPUT_DF = OUTPUT_DF.rename(columns=rename_cols)

   OUTPUT_DF['dayonfeed'] = OUTPUT_DF['dayonfeed'].astype('int')

   # Separate values with different units into their own columns
   OUTPUT_DF[['bodyweight_lb' ,'bodyweight_kg']] = OUTPUT_DF['bodyweight'].str.split('(', expand=True)
   OUTPUT_DF['bodyweight_lb'] = OUTPUT_DF['bodyweight_lb'].str.replace(' lbs' ,'').astype('float')
   OUTPUT_DF['bodyweight_kg'] = OUTPUT_DF['bodyweight_kg'].str.replace(')' ,'').str.replace(' kg' ,'').astype('float')

   # Drop columns we are not using
   keep_cols = ['dayonfeed' ,'bodyweight_lb' ,'bodyweight_kg']
   OUTPUT_DF = OUTPUT_DF[keep_cols]

   return OUTPUT_DF

swinebreedstd_pic_barntype1 = cleanup_pic_dof_table(pic_dof_1)
datainfo(swinebreedstd_pic_barntype1)

swinebreedstd_pic_barntype2 = cleanup_pic_dof_table(pic_dof_2)
datainfo(swinebreedstd_pic_barntype2)

swinebreedstd_pic_barntype3 = cleanup_pic_dof_table(pic_dof_3)
datainfo(swinebreedstd_pic_barntype3)

# -----------------------------------------------------------------------------
# Growth and Feed Intake table
# -----------------------------------------------------------------------------
swinebreedstd_pic_growthandfeed = pic_growthandfeed.copy()

# Fix column names
new_column_names = [
   'age_days_weeks'
   ,'bodyweight_lbs_kg'
   ,'adg_lbs_g'
   ,'weekly_feedintake_lbs_kg'
   ,'cml_adg_lbs_g_perday'
   ,'cml_feedintake_lbs_kg'
   ,'cml_fcr_orig'
]
swinebreedstd_pic_growthandfeed.columns = new_column_names

# Drop rows with broken header info
swinebreedstd_pic_growthandfeed = swinebreedstd_pic_growthandfeed.drop(index=[0, 1, 2 ,3]).reset_index(drop=True)

# Separate values with different units into their own columns
swinebreedstd_pic_growthandfeed[['dayonfeed' ,'weekonfeed']] = swinebreedstd_pic_growthandfeed['age_days_weeks'].str.split('/', expand=True)
swinebreedstd_pic_growthandfeed['dayonfeed'] = swinebreedstd_pic_growthandfeed['dayonfeed'].astype('int')
swinebreedstd_pic_growthandfeed['weekonfeed'] = swinebreedstd_pic_growthandfeed['weekonfeed'].astype('int')
swinebreedstd_pic_growthandfeed = swinebreedstd_pic_growthandfeed.drop(columns='age_days_weeks')

swinebreedstd_pic_growthandfeed[['bodyweight_lb' ,'bodyweight_kg']] = swinebreedstd_pic_growthandfeed['bodyweight_lbs_kg'].str.split('(', expand=True)
swinebreedstd_pic_growthandfeed['bodyweight_lb'] = swinebreedstd_pic_growthandfeed['bodyweight_lb'].astype('float')
swinebreedstd_pic_growthandfeed['bodyweight_kg'] = swinebreedstd_pic_growthandfeed['bodyweight_kg'].str.replace(')' ,'').astype('float')
swinebreedstd_pic_growthandfeed = swinebreedstd_pic_growthandfeed.drop(columns='bodyweight_lbs_kg')

swinebreedstd_pic_growthandfeed[['adg_lbs' ,'adg_g']] = swinebreedstd_pic_growthandfeed['adg_lbs_g'].str.split('(', expand=True)
swinebreedstd_pic_growthandfeed['adg_lbs'] = swinebreedstd_pic_growthandfeed['adg_lbs'].astype('float')
swinebreedstd_pic_growthandfeed['adg_g'] = swinebreedstd_pic_growthandfeed['adg_g'].str.replace(')' ,'').astype('float')
swinebreedstd_pic_growthandfeed = swinebreedstd_pic_growthandfeed.drop(columns='adg_lbs_g')

swinebreedstd_pic_growthandfeed[['weekly_feedintake_lbs' ,'weekly_feedintake_kg']] = swinebreedstd_pic_growthandfeed['weekly_feedintake_lbs_kg'].str.split('(', expand=True)
swinebreedstd_pic_growthandfeed['weekly_feedintake_lbs'] = swinebreedstd_pic_growthandfeed['weekly_feedintake_lbs'].astype('float')
swinebreedstd_pic_growthandfeed['weekly_feedintake_kg'] = swinebreedstd_pic_growthandfeed['weekly_feedintake_kg'].str.replace(')' ,'').astype('float')
swinebreedstd_pic_growthandfeed = swinebreedstd_pic_growthandfeed.drop(columns='weekly_feedintake_lbs_kg')

swinebreedstd_pic_growthandfeed[['cml_adg_lbs_perday' ,'cml_adg_g_perday']] = swinebreedstd_pic_growthandfeed['cml_adg_lbs_g_perday'].str.split('(', expand=True)
swinebreedstd_pic_growthandfeed['cml_adg_lbs_perday'] = swinebreedstd_pic_growthandfeed['cml_adg_lbs_perday'].astype('float')
swinebreedstd_pic_growthandfeed['cml_adg_g_perday'] = swinebreedstd_pic_growthandfeed['cml_adg_g_perday'].str.replace(')' ,'').astype('float')
swinebreedstd_pic_growthandfeed = swinebreedstd_pic_growthandfeed.drop(columns='cml_adg_lbs_g_perday')

swinebreedstd_pic_growthandfeed[['cml_feedintake_lbs' ,'cml_feedintake_kg']] = swinebreedstd_pic_growthandfeed['cml_feedintake_lbs_kg'].str.split('(', expand=True)
swinebreedstd_pic_growthandfeed['cml_feedintake_lbs'] = swinebreedstd_pic_growthandfeed['cml_feedintake_lbs'].astype('float')
swinebreedstd_pic_growthandfeed['cml_feedintake_kg'] = swinebreedstd_pic_growthandfeed['cml_feedintake_kg'].str.replace(')' ,'').astype('float')
swinebreedstd_pic_growthandfeed = swinebreedstd_pic_growthandfeed.drop(columns='cml_feedintake_lbs_kg')

swinebreedstd_pic_growthandfeed['cml_fcr'] = swinebreedstd_pic_growthandfeed['cml_fcr_orig'].astype('float')
swinebreedstd_pic_growthandfeed = swinebreedstd_pic_growthandfeed.drop(columns='cml_fcr_orig')

# =============================================================================
#### Write pickle
# =============================================================================
# To PRODATA
swinebreedstd_pic_barntype1.to_pickle(os.path.join(PRODATA_FOLDER ,'swinebreedstd_pic_barntype1.pkl.gz'))
swinebreedstd_pic_barntype2.to_pickle(os.path.join(PRODATA_FOLDER ,'swinebreedstd_pic_barntype2.pkl.gz'))
swinebreedstd_pic_barntype3.to_pickle(os.path.join(PRODATA_FOLDER ,'swinebreedstd_pic_barntype3.pkl.gz'))

swinebreedstd_pic_growthandfeed.to_pickle(os.path.join(PRODATA_FOLDER ,'swinebreedstd_pic_growthandfeed.pkl.gz'))

# To DASH data
swinebreedstd_pic_barntype1.to_pickle(os.path.join(DASH_DATA_FOLDER ,'swinebreedstd_pic_barntype1.pkl.gz'))
swinebreedstd_pic_barntype2.to_pickle(os.path.join(DASH_DATA_FOLDER ,'swinebreedstd_pic_barntype2.pkl.gz'))
swinebreedstd_pic_barntype3.to_pickle(os.path.join(DASH_DATA_FOLDER ,'swinebreedstd_pic_barntype3.pkl.gz'))

swinebreedstd_pic_growthandfeed.to_pickle(os.path.join(DASH_DATA_FOLDER ,'swinebreedstd_pic_growthandfeed.pkl.gz'))

# =============================================================================
#### Plot
# =============================================================================
snplt = sns.relplot(
   data=swinebreedstd_pic_growthandfeed
   ,x='dayonfeed'
   ,y='bodyweight_kg'
   ,kind='line'
   ,color='blue'        # Fixed color rather than color by variable
   ,marker='o'          # Add markers
   ,label='Live Weight'     # Label for this line in legend
   ,aspect=1.5          # Width of figure as multiplier on height. If paneled, width of each panel.
)
sns.lineplot(     # Overlay feed intake
   data=swinebreedstd_pic_growthandfeed
   ,x='dayonfeed'
   ,y='cml_feedintake_kg'
   ,ax=snplt.ax            # Use axes (both X and Y) associated with plot object (above)
   ,color='orange'
   ,marker='o'          # Add markers
   ,label='Cumulative Feed Intake'        # Label for this line in legend
)
plt.title('Swine reference\n based on PIC breed standard' ,fontsize=12)
plt.ylabel('Kilograms' ,fontsize=12)

#%% Gemma's model

# =============================================================================
#### Single predicted ADG
# =============================================================================
# Import
swinebreedstd_liverpool_model = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'Swine standards' ,'swine_growth_dat.csv'))
cleancolnames(swinebreedstd_liverpool_model)
datainfo(swinebreedstd_liverpool_model)

# Drop cols
swinebreedstd_liverpool_model = swinebreedstd_liverpool_model.drop(columns=['unnamed:_0'])

# Rename cols
rename_cols = {
   "days_growing":"dayonfeed"
   ,"predicted_adg":"pred_adg"
   ,"se_predicted_adg":"se_pred_adg"
   ,"finish_weight":"pred_finishweight_lbs_withoutstartwt"
   ,"changeinadg":"delta_pred_adg"
}
swinebreedstd_liverpool_model = swinebreedstd_liverpool_model.rename(columns=rename_cols)

# Add start weight according to comment from Gemma Chaters
avg_startwt = 14
swinebreedstd_liverpool_model['pred_finishweight_lb'] = swinebreedstd_liverpool_model['pred_finishweight_lbs_withoutstartwt'] + avg_startwt
swinebreedstd_liverpool_model['bodyweight_kg'] = swinebreedstd_liverpool_model['pred_finishweight_lb'] / uc.lbs_per_kg

# -----------------------------------------------------------------------------
# Export
# -----------------------------------------------------------------------------
# To PRODATA
swinebreedstd_liverpool_model.to_pickle(os.path.join(PRODATA_FOLDER ,'swinebreedstd_liverpool_model.pkl.gz'))

# To DASH data
swinebreedstd_liverpool_model.to_pickle(os.path.join(DASH_DATA_FOLDER ,'swinebreedstd_liverpool_model.pkl.gz'))

# =============================================================================
#### Update 4/12: Range of predicted ADG
# =============================================================================
# Import
swinebreedstd_liverpool_model2 = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'Swine standards' ,'predicted_adg_feedintake_daysgrowing.csv'))
cleancolnames(swinebreedstd_liverpool_model2)
datainfo(swinebreedstd_liverpool_model2)

# Drop cols
swinebreedstd_liverpool_model2 = swinebreedstd_liverpool_model2.drop(columns=['unnamed:_0'])

# Rename cols
rename_cols = {
   "days_growing":"dayonfeed"
}
swinebreedstd_liverpool_model2 = swinebreedstd_liverpool_model2.rename(columns=rename_cols)

# Add columns for finish weight
avg_startwt = 14
swinebreedstd_liverpool_model2.eval(
   f'''
   avg_startwt_lb = {avg_startwt}

   finishweight_mean_lb = (adg_mean * dayonfeed) + avg_startwt_lb
   finishweight_min_lb = (adg_min * dayonfeed) + avg_startwt_lb
   finishweight_max_lb = (adg_max * dayonfeed) + avg_startwt_lb

   finishweight_mean_kg = finishweight_mean_lb / {uc.lbs_per_kg}
   finishweight_min_kg = finishweight_min_lb / {uc.lbs_per_kg}
   finishweight_max_kg = finishweight_max_lb / {uc.lbs_per_kg}

   dailyfeedintake_mean_lb = dailyfeedintake_mean * dayonfeed
   dailyfeedintake_min_lb = dailyfeedintake_min * dayonfeed
   dailyfeedintake_max_lb = dailyfeedintake_max * dayonfeed

   dailyfeedintake_mean_kg = dailyfeedintake_mean_lb / {uc.lbs_per_kg}
   dailyfeedintake_min_kg = dailyfeedintake_min_lb / {uc.lbs_per_kg}
   dailyfeedintake_max_kg = dailyfeedintake_max_lb / {uc.lbs_per_kg}
   '''
   ,inplace=True
)
datainfo(swinebreedstd_liverpool_model2)

# Dashboard will look for column 'bodyweight_kg' to determine potential growth of animals
# Create this column based on the weight you want to use
# Currently using maximum predicted weight for a given dayonfeed
swinebreedstd_liverpool_model2['bodyweight_kg'] = swinebreedstd_liverpool_model2['finishweight_max_kg']

# -----------------------------------------------------------------------------
# Plot
# -----------------------------------------------------------------------------
snplt = sns.relplot(
   data=swinebreedstd_liverpool_model2
   ,x='dayonfeed'
   ,y='finishweight_max_kg'
   ,kind='line'
   ,color='blue'     # Fixed color rather than color by variable
   ,marker='o'       # Add markers
   ,aspect=1.5         # Width of figure as multiplier on height. If paneled, width of each panel.
)
plt.title('Swine reference\n based on Liverpool model' ,fontsize=12)

# -----------------------------------------------------------------------------
# Export
# -----------------------------------------------------------------------------
# To PRODATA
swinebreedstd_liverpool_model2.to_pickle(os.path.join(PRODATA_FOLDER ,'swinebreedstd_liverpool_model2.pkl.gz'))

# To DASH data
swinebreedstd_liverpool_model2.to_pickle(os.path.join(DASH_DATA_FOLDER ,'swinebreedstd_liverpool_model2.pkl.gz'))

# =============================================================================
#### Update 4/28: Using simulated inputs (minimum mortality)
# =============================================================================
# Import predicted ADG
swinebreedstd_liverpool_model3_adg = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'Swine standards' ,'dat_for_justin.csv'))

# Import predicted feed intake
swinebreedstd_liverpool_model3_feed = pd.read_csv(os.path.join(RAWDATA_FOLDER ,'Swine standards' ,'dat_for_justin_pigfeedintake.csv'))

# Merge
swinebreedstd_liverpool_model3 = pd.merge(
   left=swinebreedstd_liverpool_model3_adg
   ,right=swinebreedstd_liverpool_model3_feed
   ,on='days_growing'
)
cleancolnames(swinebreedstd_liverpool_model3)

# Drop cols
swinebreedstd_liverpool_model3 = swinebreedstd_liverpool_model3.drop(columns=['unnamed:_0_x' ,'unnamed:_0_y'])

# Rename cols
rename_cols = {
   "days_growing":"dayonfeed"
}
swinebreedstd_liverpool_model3 = swinebreedstd_liverpool_model3.rename(columns=rename_cols)

# Add columns for finish weight
avg_startwt = 14
swinebreedstd_liverpool_model3.eval(
   f'''
   avg_startwt_lb = {avg_startwt}

   pred_finishweight_max_lb = (adg_max * dayonfeed) + avg_startwt_lb
   pred_finishweight_max_kg = pred_finishweight_max_lb / {uc.lbs_per_kg}

   pred_finishweight_max_sim_lb = (adg_max_mort_prrs_sstd_mth * dayonfeed) + avg_startwt_lb
   pred_finishweight_max_sim_kg = pred_finishweight_max_sim_lb / {uc.lbs_per_kg}

   pred_cmlfeedintake_max_lb = feedint_max * dayonfeed
   pred_cmlfeedintake_max_kg = pred_cmlfeedintake_max_lb / {uc.lbs_per_kg}

   pred_cmlfeedintake_max_sim_lb = feedint_max_mort_sstd * dayonfeed
   pred_cmlfeedintake_max_sim_kg = pred_cmlfeedintake_max_sim_lb / {uc.lbs_per_kg}
   '''
   ,inplace=True
)
datainfo(swinebreedstd_liverpool_model3)

# Dashboard will look for column 'bodyweight_kg' to determine potential growth of animals
# Create this column based on the weight you want to use
# Currently using maximum predicted weight for a given dayonfeed
swinebreedstd_liverpool_model3['bodyweight_kg'] = swinebreedstd_liverpool_model3['pred_finishweight_max_sim_kg']

# -----------------------------------------------------------------------------
# Plot
# -----------------------------------------------------------------------------
plt.plot('dayonfeed' ,'pred_finishweight_max_sim_lb' ,data=swinebreedstd_liverpool_model3)
plt.plot('dayonfeed' ,'pred_finishweight_max_lb' ,data=swinebreedstd_liverpool_model3)
plt.legend()
plt.xlabel('Day on Feed')

# -----------------------------------------------------------------------------
# Export
# -----------------------------------------------------------------------------
# To PRODATA
swinebreedstd_liverpool_model3.to_pickle(os.path.join(PRODATA_FOLDER ,'swinebreedstd_liverpool_model3.pkl.gz'))

# To DASH data
swinebreedstd_liverpool_model3.to_pickle(os.path.join(DASH_DATA_FOLDER ,'swinebreedstd_liverpool_model3.pkl.gz'))
