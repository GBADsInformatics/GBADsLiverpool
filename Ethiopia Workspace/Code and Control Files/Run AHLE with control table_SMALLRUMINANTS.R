# =================================================================
# About
# =================================================================
# This code reads an Excel file which defines the scenarios to run.
# If you see the below error, the cause is likely a missing parenthesis
# for one of the parameter values in the Excel file.
#
# Error in parse(text = functioncall_string) : 
#      <text>:2:0: unexpected end of input
#

# =================================================================
# Top-level program parameters
# =================================================================
# -----------------------------------------------------------------
# Set manually
# -----------------------------------------------------------------
# Number of simulation iterations

## > to trial the code and parameter spreadsheet set this to 10, 
## NOTE currently takes about 2 minutes to run each scenario (10,000 runs) and there are 250 scenarios in the example small ruminant spreadsheet
cmd_nruns <- 10000

# Folder location to save outputs
## > make sure you have your own folder path name 
##   here where you want your outputs to store
#cmd_output_directory <- 'F:/First Analytics/Clients/University of Liverpool/GBADs Github/GBADsLiverpool/Ethiopia Workspace/Program outputs'
cmd_output_directory <- '/Users/gemmachaters/Dropbox/Mac/Documents/GitHub/GBADsLiverpool/Ethiopia Workspace/Program outputs'


# Full path to scenario control file
## > and the path name of the scenario spreadsheet you are using
#cmd_scenario_file <- 'F:/First Analytics/Clients/University of Liverpool/GBADs Github/GBADsLiverpool/Ethiopia Workspace/Code and Control Files/AHLE scenario parameters MAJOR SCENARIOS ONLY.xlsx'
cmd_scenario_file <- '/Users/gemmachaters/Dropbox/Mac/Documents/GitHub/GBADsLiverpool/Ethiopia Workspace/Code and Control Files/AHLE scenario parameters-20221202.xlsx'

# -----------------------------------------------------------------
# Get from command line arguments
# -----------------------------------------------------------------
# If this was invoked from the command line, look for command arguments
# Will overwrite manual settings above

## ONLY FOR PYTHON when running this script as part of larger programme ##
if (grepl('Rterm.exe', paste(commandArgs(), collapse=" "), ignore.case = TRUE, fixed = TRUE))
{
  cmd_args <- commandArgs(trailingOnly=TRUE)	# Fetch command line arguments
  cmd_nruns <- as.numeric(cmd_args[1]) 			# Arg 1: number of runs. Convert to numeric.
  cmd_output_directory <- cmd_args[2] 			# Arg 2: folder location to save outputs
  cmd_scenario_file <- cmd_args[3] 				# Arg 3: full path to scenario control file
}

# -----------------------------------------------------------------
# Show in console
# -----------------------------------------------------------------
print('Using the following program parameters:')
print('   Number of simulation runs')
print(cmd_nruns)
print('   Output directory')
print(cmd_output_directory)

# =================================================================
# Libraries
# =================================================================
library(mc2d)
library(truncnorm)
library(readxl)

# =================================================================
# Define functions
# =================================================================

## > This is the pert distribution used in the code, note min, max, mode order of values
rpert <- function( n, x.min, x.max, x.mode, lambda = 4 )
{
  if( x.min > x.max || x.mode > x.max || x.mode < x.min ) stop( "invalid parameters" );
  
  x.range <- x.max - x.min;
  if( x.range == 0 ) return( rep( x.min, n ));
  
  mu <- ( x.min + x.max + lambda * x.mode ) / ( lambda + 2 );
  
  # special case if mu == mode
  if( mu == x.mode ){
    v <- ( lambda / 2 ) + 1
  }
  else {
    v <- (( mu - x.min ) * ( 2 * x.mode - x.min - x.max )) /
      (( x.mode - mu ) * ( x.max - x.min ));
  }
  
  w <- ( v * ( x.max - mu )) / ( mu - x.min );
  return ( rbeta( n, v, w ) * x.range + x.min );
}

## Function to use data from models and build summary data frames that are
## stored in the cmd_output_directory you have assigned above

build_summary_df <- function(
    items_to_summarize 	# Labeled list of matrices to summarize. Matrix names should be WITHOUT SUFFIXES (without _M, _NF_M, etc.). Will iterate through all suffixes. Labels will be used in output data.
)
{
  suffixes = c( 			# Labeled list of matrix name suffixes. Suffixes will be appended to matrix names in items_to_summarize and must match matrices created above. Labels will be used to describe the group summarized.
    'Overall' = '_M'
    ,'Neonatal Female' = '_NF_M'
    ,'Neonatal Male' = '_NM_M'
    ,'Neonatal Combined' = '_N_M'
    ,'Juvenile Female' = '_JF_M'
    ,'Juvenile Male' = '_JM_M'
    ,'Juvenile Combined' = '_J_M'
    ,'Adult Female' = '_AF_M'
    ,'Adult Male' = '_AM_M'
  )
  summary_df_updated <- data.frame()  # Initialize data frame
  for (i in seq(1, length(items_to_summarize))) 	# Loop through items to summarize
  {
    base_matrix <- items_to_summarize[i]
    base_label <- names(base_matrix)
    for (j in seq(1, length(suffixes))) 						# Loop through suffixes
    {
      suffix <- suffixes[j]
      group <- names(suffix)
      
      if (exists(paste(base_matrix, suffix, sep=''), frame=-2)) 	# If matrix with this suffix exists
      {
        matrix_to_summarize <- dynGet(paste(base_matrix, suffix, sep=''))
        vector_to_summarize <- matrix_to_summarize[,12]
        
		  # Print details to console for debugging 
        #print('base label:')
        #print(base_label)
        #print('group label:')
        #print(group)
        #print('matrix to summarize:')
        #print(paste(base_matrix, suffix, sep=''))
        
        item_mean <- mean(vector_to_summarize)
        item_sd <- sd(vector_to_summarize)
        item_min <- min(vector_to_summarize)
        item_q1 <- quantile(vector_to_summarize, 0.25)
        item_median <- median(vector_to_summarize)
        item_q3 <- quantile(vector_to_summarize, 0.75)
        item_max <- max(vector_to_summarize)
        
        onerow_df <- data.frame(Item=base_label ,Group=group ,Mean=item_mean ,StDev=item_sd ,Min=item_min ,Q1=item_q1 ,Median=item_median ,Q3=item_q3 ,Max=item_max)
        summary_df_updated <- rbind(summary_df_updated ,onerow_df)
      }
    }
  }
  return(summary_df_updated)
}

## --------------------------------------------------------------------- ##
##  The compartmental model function created to simulate the population  ##
## --------------------------------------------------------------------- ##

## lines 174 to 317 are all parameters that are read in from the scenario
## spreadsheet. If you want to trial running the model without a scenario file 
## you can define each of these parameters in turn

## Note, a lot of the parameters used in the model are distributions which are informed by data from literature
## and national sources which is combined using meta-analysis (should be a standardised method across the GBADs programme)

## this version of the model samples from the distribution once and generally multiplies that value by the number of animals.
## this process, sampling from means and redoing the sampling and whole simulation 10,000 times relies on central limit therom
## to produce results representative of the population.
## We created and ran a version of the model which sampled each individual from the distribution, so each adult female had their own
## lactation length and avg daily yeild, each neonate their own mortality rate etc, the model took days to run and the outputs
## were very similar (means within 0.01%) so we reverted to the faster version of the model, sampling for the whole poulation for each run.

## If you want to add extra to the model (eg. Antimicrobial_expenditure) then remember the parameters must be defined 
## in this list, in the appropriate place AND in the spreadsheet, using exactly the same name
## and then used in the model loop again with the same name. If you want to see an example search for
## Beta through the code and see where it is used. In this model and scenario file Beta is the same for neonate to juv 
## and juv to adult as animals are in each age group for 6 months, but for cattle and poultry there is a beta_N and beta_J

compartmental_model <- function(
    nruns 			## Number of iterations (duration of simulation) defined at start of script
    ,Num_months 	## NOTE - if you change this to days/weeks/years you must change rates accordingly 
                  ## currently 12 to represent a year with monthly time steps
    # Initial population
    ,N_NF_t0		# Neonatal female
    ,N_NM_t0		# Neonatal male
    ,N_JF_t0		# Juvenile female
    ,N_JM_t0		# Juvenile male
    ,N_AF_t0		# Adult female
    ,N_AM_t0		# Adult male
    
    ## Growth rate N -> J and J-> A
    ## for small ruminants this is 6 months for each transition but for other species
    ## this will be set as a Beta_N and Beta_J if animals spend different durations 
    ## in the neonate and juvenile age groups
    ,Beta

    # Fertility (reproduction rate mu is calculated using these two variables)
    ,part # parturition rate
    ,prolif # litter size/number of animals born per parturition
    
    # lactation
    ,prop_F_milked ## proportion of females that are milked after giving birth
    ,lac_duration # (duration of lactation in days)
    ,avg_daily_yield_ltr # note AVERAGE so total lactation yield is calculated based on lactation length and average daily yield
    ,milk_value_ltr # current value, used to convert milk produced into a financial value
    
    # Offtake rate
    
    ## Currently fixed (from CSA data in Ethiopia scenarios), but, this should probably be dependent on new population size, 
    ## to keep pop size as it was at t0 offtake should probably be = offtake + any population growth that's occured in each 
    ## age-sex group that month OR this can be solved by constraining the model based on metabolisable energy used by the 
    ## system at present (population at t0) and optimizing all outputs based on available ME
    
    # NOTE annual offtake also same for juvs and adults but juvs in age group for half the time (only 6 months) meaning their
    # offtake is effectively 50% less than in the adults BUT this is thought to be representative of reality as offtake in the
    # juveniles is lower than in adults, only starting around 7-9 months depending on animal size
    
    ,GammaF 		# offtake rate female (juv and adult only) 
    ,GammaM 		# offtake rate male
    
    # Mortality ## informed from META analysis of literature and CSA data
    ,AlphaN		# mortality rate neonate ## No distinction between sexes for neonates and juvs
    ,AlphaJ		# mortality rate juvenile ## 
    ,AlphaF		# mortality adult female ##
    ,AlphaM		# mortality adult male ##
    
    # Culls
    ,CullF	 	# cullrate Adult Female ## These are valueless in SR model as SR females are culled age 10 years
    ,CullM		# cullrate Adult Male  ## These will still have a value, in SR culled around age 5
    
    ## Production parameters (kg)
    
    # Liveweight conversion (kg) ## Informed from META analysis
    ,lwNF  		# Liveweight Neonate female  
    ,lwNM  		# Liveweight Neonate male 
    ,lwJF 		# Liveweight Juvenille female # 
    ,lwJM 		# Liveweight Juvenille male # 
    ,lwAF 		# Liveweight Adult # 
    ,lwAM 		# Liveweight Adult # 
    
    # carcase yeild
    ,ccy 			# As a % of Liveweight for all groups used to calculate kg meat out
    
    ## Financial value of live animals - taken from current market data or historic for previous years
    # Ethiopian Birr
    ,fvNF 		## Financial value of neonatal Female
    ,fvJF 		## Financial value of neonatal Male
    ,fvAF			## Financial value of juv Female
    ,fvNM			## Financial value of juv Male
    ,fvJM			## Financial value of adult Female
    ,fvAM			## Financial value of adult Male  
    
    ## skin/hides  
    ## parameters can be updated through expert opinion but adding options for flexibility here
    ## hides_rate is actually excluded in the main code now as the liveweight price includes meat and hides
    ## hides from mortality are still included (50%) of dead animals contribute a hide, because otherwise these 
    ## animals are valueless
    ,hides_rate			# 1 skin per animal offtake for males
    ,hides_rate_mor	# 50% of dead animals contribute to hides count
    
    # 51 ethiopian birr currently used for small ruminants (parameter set in scenario table)
    ,hides_value
    
    # manure rate (kg produced/animal/day)
    ,Man_N		# Manure kg/ day from neonates ## means and Sds  are derived from  body wt
    ,Man_J		# Manure kg/ day from juvenile## means and Sds  are derived from  body wt
    ,Man_A		# Manure kg/ day from adults ## means and Sds  are derived from  body wt
    
    # 0.65 eth birr per kg 2021 price (parameter set in scenario table)
    ,Man_value
    
    ## dry matter requirements as proportion of Liveweight
    ,DM_req_prpn_NF		# Dry matter required by neonates
    ,DM_req_prpn_NM		# Dry matter required by neonates
    ,DM_req_prpn_JF		# Dry matter required by juvenile
    ,DM_req_prpn_JM		# Dry matter required by juvenile
    ,DM_req_prpn_AF		# Dry matter required by adults
    ,DM_req_prpn_AM		# Dry matter required by adults
    
    ## Proportion of livestock keepers that spend any money on feed
    ## Using this in the model equates to the proportion of livestock for which some food is purchased.
    ## NOTE Currently the same for all age*sex groups
    ,prpn_lskeepers_purch_feed
    
    ## For those spending any money on feed, the proportion of feed that is purchased
    ## NOTE Currently the same for all age*sex groups and an estimate centered around 50%
    ,prpn_feed_paid_for
    
    ## Feed cost (example distributions used for now)
    ,Feed_cost_kg		## Ethiopian birr/kg wheat and barley fodder used - NOT concentrate feed
    
    ## Dry matter % in feed 
    ## used to calculate volume of feed purchased based on dry matter needed, proportion of livestock keepers
    ## purchasing any feed for livestock and estimated proportion of feed required which these ls keepers purchase
    ## variable results for the amount of dry matter in wheat and barley and tef in Ethiopia
    ## range 30-90%
    ## taking 90% as mean estimate for this model (pert distribution)
    ,DM_in_feed			## 
    
    ## Labour cost
    ## birr/head/month
    ,Lab_SR ## (pert distribution)
    ## amount of labour time spent on NON-health related tasks (so healthcare labour can be discounted in ideal scenario)
    ,lab_non_health	## 0.86 in ideal this was not used in the current and this may not apply for ideal
    
    ## Helath care costs
    ## birr/head/month
    ## this includes medicines and veterinary care
    ## health care costs select from a uniform distribution as we only had 2 estimates
    ## the two national level estimates(national production and import of vet drugs and vaccines, 
    ## and LFSDP and RPLRP projects) are used as bound for the price and used for unif distribution  
    ,Health_exp	
    
    ## Capital costs
    ## for this we are using bank of Ethiopia inflation rate
    ,Interest_rate ## zero in current model as inflation so high there is 
                   ## no capitol cost associated with having livestock
    
    ## Infrastructure cost associated with keeping livestock and some maintenance over the year 
    ## (though this is considered negligeble in extensive Ethiopian systems),
    ## arbitrary value of 1 birr per head used in current model more to serve as place holder 
    ## for when dealing with systems with higher infrastructure costs
    ,Infrastructure_per_head
)
{
  
  ## Reproduction rate (parturition rate (avg # births per adult female year) * prolificacy rate (litter size))
  Mu <- (sample(part, size = 10000, replace = TRUE) * sample(prolif, size = 10000, replace = TRUE)) / 12 # birth rate (parturition rate * prolificacy rate / 12)
  
  ## Daily dry matter requirements (measured in kg and calculated as a % of Liveweight)
  ## so calculation is liveweight (kg) (pert distribution) * Dry matter requirement per kg of body weight (0.026)
  kg_DM_req_NF = DM_req_prpn_NF * lwNF  	# Dry matter required by neonate females 
  kg_DM_req_NM = DM_req_prpn_NM * lwNM  	# Dry matter required by neonates males
  kg_DM_req_JF = DM_req_prpn_JF * lwJF  	# Dry matter required by juvenile females
  kg_DM_req_JM = DM_req_prpn_JM * lwJM  	# Dry matter required by juvenile males
  kg_DM_req_AF = DM_req_prpn_AF * lwAF  	# Dry matter required by adult females
  kg_DM_req_AM = DM_req_prpn_AM * lwAM  	# Dry matter required by adult males
  
  ## Daily dry matter purchased 
  ## (dry matter required * proportion of LS keepers purchasing feed * proportion of feed paid for, by those purchasing any)
  ## prpn_feed_paid_for = rpert(10000, 0.1, 1, 0.5)) - a pert distribution centred around 50%
  ## prpn_lskeepers_purch_feed <- 0.25 (set as a fixed value in paramaters but could be taken from a distribution)
  ## NOTE in the pastoral system this purchased feed will be 0
  
  DM_purch_NF <- (kg_DM_req_NF * prpn_lskeepers_purch_feed * prpn_feed_paid_for) 
  DM_purch_NM <- (kg_DM_req_NM * prpn_lskeepers_purch_feed * prpn_feed_paid_for) 
  DM_purch_JF <- (kg_DM_req_JF * prpn_lskeepers_purch_feed * prpn_feed_paid_for) 
  DM_purch_JM <- (kg_DM_req_JM * prpn_lskeepers_purch_feed * prpn_feed_paid_for) 
  DM_purch_AF <- (kg_DM_req_AF * prpn_lskeepers_purch_feed * prpn_feed_paid_for) 
  DM_purch_AM <- (kg_DM_req_AM * prpn_lskeepers_purch_feed * prpn_feed_paid_for) 
  
  ## Actual Kg of feed purchased on average per day for an avg animal in each age-sex group
  ## (amount of dry matter needed to be purchased / expected dry matter % of purchased feed)
  ## In this system we only consider the cost of purchasing forage type feed, not concentrate feed
  ## we do not account for the cost (financial or opportunity cost) of grazed feed
  KG_Feed_purchased_NF <- DM_purch_NF / DM_in_feed
  KG_Feed_purchased_NM <- DM_purch_NM / DM_in_feed
  KG_Feed_purchased_JF <- DM_purch_JF / DM_in_feed
  KG_Feed_purchased_JM <- DM_purch_JM / DM_in_feed
  KG_Feed_purchased_AF <- DM_purch_AF / DM_in_feed
  KG_Feed_purchased_AM <- DM_purch_AM / DM_in_feed
  
  ## Expenditure on feed per animal day
  ## amount of feed purchased * feed cost (taken from market information data)
  Expenditure_on_feed_NF <- KG_Feed_purchased_NF * Feed_cost_kg
  Expenditure_on_feed_NM <- KG_Feed_purchased_NM * Feed_cost_kg
  Expenditure_on_feed_JF <- KG_Feed_purchased_JF * Feed_cost_kg
  Expenditure_on_feed_JM <- KG_Feed_purchased_JM * Feed_cost_kg
  Expenditure_on_feed_AF <- KG_Feed_purchased_AF * Feed_cost_kg
  Expenditure_on_feed_AM <- KG_Feed_purchased_AM * Feed_cost_kg
  
  # --------------------------------------------------------------
  # Create vectors to store the model outputs at each time step
  # --------------------------------------------------------------
  # time step is set in scenario code and for now is 12 months (1 year)
  #	Num_months <- 12
  
  ## population size at each time step
  numNF <- rep(0, Num_months)
  numJF <- rep(0, Num_months)
  numAF <- rep(0, Num_months)
  numNM <- rep(0, Num_months)
  numJM <- rep(0, Num_months)
  numAM <- rep(0, Num_months)
  numN <- rep(0, Num_months)
  
  # births
  births <- rep(0, Num_months)
  
  # size of population growth from neonates and juveniles 
  # that will move into juveniles and adults compartments respectively
  growth_NF <- rep(0, Num_months)
  growth_NM <- rep(0, Num_months)
  growth_JF <- rep(0, Num_months)
  growth_JM <- rep(0, Num_months)
  
  # deaths from each each sex group
  deaths_NF <- rep(0, Num_months)
  deaths_NM <- rep(0, Num_months)
  deaths_JF <- rep(0, Num_months)
  deaths_JM <- rep(0, Num_months)
  deaths_AF <- rep(0, Num_months)
  deaths_AM <- rep(0, Num_months)
  
  # culls from each each sex group with any culls (only adults)
  culls_AF <- rep(0, Num_months)
  culls_AM <- rep(0, Num_months)
  
  ## A vector for storing cumulative number of culled animals over the year (or total time steps)
  Cumulative_culls_AM <- rep(0, Num_months)
  
  # number of offtake from each age-sex group with any offtake (juvs and adults)
  offtake_JF <- rep(0, Num_months)
  offtake_JM <- rep(0, Num_months)
  offtake_AF <- rep(0, Num_months)
  offtake_AM <- rep(0, Num_months)
  
  # vector for storing mortalities (total deaths)
  Total_Mortality <- rep(0, Num_months)
  
  Total_Mortality_NF <- rep(0, Num_months)
  Total_Mortality_NM <- rep(0, Num_months)
  Total_Mortality_JF <- rep(0, Num_months)
  Total_Mortality_JM <- rep(0, Num_months)
  Total_Mortality_AF <- rep(0, Num_months)
  Total_Mortality_AM <- rep(0, Num_months)
  
  # Monetary value of mortality
  Value_of_Total_Mortality <- rep(0, Num_months)
  
  Value_of_Total_Mortality_NF <- rep(0, Num_months)
  Value_of_Total_Mortality_NM <- rep(0, Num_months)
  Value_of_Total_Mortality_JF <- rep(0, Num_months)
  Value_of_Total_Mortality_JM <- rep(0, Num_months)
  Value_of_Total_Mortality_AF <- rep(0, Num_months)
  Value_of_Total_Mortality_AM <- rep(0, Num_months)
  
  ## Production ##
  
  # Live weight
  # totals will be the sum of the age sex groups
  Quant_Liveweight_kg_NF <- rep(0, Num_months)
  Quant_Liveweight_kg_NM <- rep(0, Num_months)
  Quant_Liveweight_kg_JF <- rep(0, Num_months)
  Quant_Liveweight_kg_JM <- rep(0, Num_months)
  Quant_Liveweight_kg_AF <- rep(0, Num_months)
  Quant_Liveweight_kg_AM <- rep(0, Num_months)
  
  Quant_Liveweight_kg <- rep(0, Num_months)
  
  # Meat
  # for if you want to turn liveweight offtake into meat, 
  # this is calculated as liveweight offtake * carase yeild
  Quant_Meat_kg <- rep(0, Num_months)
  
  # Offtake
  ## for individual age catagories
  Num_Offtake_NF <- rep(0, Num_months)
  Num_Offtake_NM <- rep(0, Num_months)
  Num_Offtake_JF <- rep(0, Num_months)
  Num_Offtake_JM <- rep(0, Num_months)
  Num_Offtake_AF <- rep(0, Num_months)
  Num_Offtake_AM <- rep(0, Num_months)
  
  Num_Offtake <- rep(0, Num_months) # total
  
  # Offtake Liveweight
  Offtake_Liveweight_kg <- rep(0, Num_months)
  ## and for individual age cats
  Offtake_Liveweight_kg_JF <- rep(0, Num_months)
  Offtake_Liveweight_kg_JM <- rep(0, Num_months)
  Offtake_Liveweight_kg_AF <- rep(0, Num_months)
  Offtake_Liveweight_kg_AM <- rep(0, Num_months)
  
  # Population growth
  Pop_growth <- rep(0, Num_months)
  
  Pop_growth_NF <- rep(0, Num_months)
  Pop_growth_NM <- rep(0, Num_months)
  Pop_growth_JF <- rep(0, Num_months)
  Pop_growth_JM <- rep(0, Num_months)
  Pop_growth_AF <- rep(0, Num_months)
  Pop_growth_AM <- rep(0, Num_months)
  
  
  ## Manure
  Quant_Manure <- rep(0, Num_months)
  Quant_Manure_NF <- rep(0, Num_months)
  Quant_Manure_NM <- rep(0, Num_months)
  Quant_Manure_JF <- rep(0, Num_months)
  Quant_Manure_JM <- rep(0, Num_months)
  Quant_Manure_AF <- rep(0, Num_months)
  Quant_Manure_AM <- rep(0, Num_months)
  
  ## Havent seperated by age cat as only adults
  Quant_Hides <- rep(0, Num_months)
  
  Quant_Hides_JF <- rep(0, Num_months)
  Quant_Hides_JM <- rep(0, Num_months)
  Quant_Hides_AF <- rep(0, Num_months)
  Quant_Hides_AM <- rep(0, Num_months)
  
  # milk and wool
  Quant_Milk <- rep(0, Num_months)
  Quant_Wool <- rep(0, Num_months)
  
  ## dry matter required
  Cumulative_Dry_Matter <- rep(0, Num_months)
  
  Cumulative_Dry_Matter_NF <- rep(0, Num_months)
  Cumulative_Dry_Matter_NM <- rep(0, Num_months)
  Cumulative_Dry_Matter_JF <- rep(0, Num_months)
  Cumulative_Dry_Matter_JM <- rep(0, Num_months)
  Cumulative_Dry_Matter_AF <- rep(0, Num_months)
  Cumulative_Dry_Matter_AM <- rep(0, Num_months)
  
  ## Value of offtake
  Value_Offtake <- rep(0, Num_months)
  
  Value_Offtake_NF <- rep(0, Num_months)
  Value_Offtake_NM <- rep(0, Num_months)
  Value_Offtake_JF <- rep(0, Num_months)
  Value_Offtake_JM <- rep(0, Num_months)
  Value_Offtake_AF <- rep(0, Num_months)
  Value_Offtake_AM <- rep(0, Num_months)
  
  ###################################
  ## Value increase
  Value_Herd_Increase <- rep(0, Num_months)
  
  Value_Herd_Increase_NF <- rep(0, Num_months)
  Value_Herd_Increase_NM <- rep(0, Num_months)
  Value_Herd_Increase_JF <- rep(0, Num_months)
  Value_Herd_Increase_JM <- rep(0, Num_months)
  Value_Herd_Increase_AF <- rep(0, Num_months)
  Value_Herd_Increase_AM <- rep(0, Num_months)
  
  ## Total value increase 
  ## Herd value increase + Offtake value
  Total_Value_increase <- rep(0, Num_months)
  
  Total_Value_increase_NF <- rep(0, Num_months)
  Total_Value_increase_NM <- rep(0, Num_months)
  Total_Value_increase_JF <- rep(0, Num_months)
  Total_Value_increase_JM <- rep(0, Num_months)
  Total_Value_increase_AF <- rep(0, Num_months)
  Total_Value_increase_AM <- rep(0, Num_months)
  
  ## Inputs
  # Feed
  Feed_cost <- rep(0, Num_months)
  
  Feed_cost_NF <- rep(0, Num_months)
  Feed_cost_NM <- rep(0, Num_months)
  Feed_cost_JF <- rep(0, Num_months)
  Feed_cost_JM <- rep(0, Num_months)
  Feed_cost_AF <- rep(0, Num_months)
  Feed_cost_AM <- rep(0, Num_months)
  
  # Labour
  Labour_cost <- rep(0, Num_months)
  
  Labour_cost_NF <- rep(0, Num_months)
  Labour_cost_NM <- rep(0, Num_months)
  Labour_cost_JF <- rep(0, Num_months)
  Labour_cost_JM <- rep(0, Num_months)
  Labour_cost_AF <- rep(0, Num_months)
  Labour_cost_AM <- rep(0, Num_months)
  
  # Health
  Health_cost <- rep(0, Num_months)
  
  Health_cost_NF <- rep(0, Num_months)
  Health_cost_NM <- rep(0, Num_months)
  Health_cost_JF <- rep(0, Num_months)
  Health_cost_JM <- rep(0, Num_months)
  Health_cost_AF <- rep(0, Num_months)
  Health_cost_AM <- rep(0, Num_months)
  
  # Capital
  Capital_cost <- rep(0, Num_months)
  
  Capital_cost_NF <- rep(0, Num_months)
  Capital_cost_NM <- rep(0, Num_months)
  Capital_cost_JF <- rep(0, Num_months)
  Capital_cost_JM <- rep(0, Num_months)
  Capital_cost_AF <- rep(0, Num_months)
  Capital_cost_AM <- rep(0, Num_months)
  
  # Infrastructure
  Infrastructure_cost <- rep(0, Num_months)
  
  Infrastructure_cost_NF <- rep(0, Num_months)
  Infrastructure_cost_NM <- rep(0, Num_months)
  Infrastructure_cost_JF <- rep(0, Num_months)
  Infrastructure_cost_JM <- rep(0, Num_months)
  Infrastructure_cost_AF <- rep(0, Num_months)
  Infrastructure_cost_AM <- rep(0, Num_months)
  
  # Total expenditure sums all summed costs
  Total_expenditure <- rep(0, Num_months)
  
  Total_expenditure_NF <- rep(0, Num_months)
  Total_expenditure_NM <- rep(0, Num_months)
  Total_expenditure_JF <- rep(0, Num_months)
  Total_expenditure_JM <- rep(0, Num_months)
  Total_expenditure_AF <- rep(0, Num_months)
  Total_expenditure_AM <- rep(0, Num_months)
  
  # --------------------------------------------------------------
  # Create matrix to store the model output vectors at each time step
  # --------------------------------------------------------------
  # all vectors created above also have a matrix with as many rows as number of runs required
  # nruns for trial run of code to check and debug maybe 10, for modelling we have used 10,000 runs
  
  # population
  numNF_M <- matrix(, nrow = nruns, ncol = Num_months)
  numJF_M <- matrix(, nrow = nruns, ncol = Num_months)
  numAF_M <- matrix(, nrow = nruns, ncol = Num_months)
  numNM_M <- matrix(, nrow = nruns, ncol = Num_months)
  numJM_M <- matrix(, nrow = nruns, ncol = Num_months)
  numAM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  numN_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # mortality
  Total_Mortality_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Total_Mortality_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_Mortality_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_Mortality_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_Mortality_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_Mortality_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_Mortality_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # Monetary value of mortality
  Value_of_Total_Mortality_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Value_of_Total_Mortality_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_of_Total_Mortality_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_of_Total_Mortality_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_of_Total_Mortality_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_of_Total_Mortality_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_of_Total_Mortality_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ### Production ###
  
  # Live weight
  Quant_Liveweight_kg_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Quant_Liveweight_kg_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Liveweight_kg_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Liveweight_kg_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Liveweight_kg_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Liveweight_kg_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Liveweight_kg_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # Meat
  Quant_Meat_kg_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # Offtake
  Num_Offtake_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Num_Offtake_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Num_Offtake_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Num_Offtake_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Num_Offtake_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Num_Offtake_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Num_Offtake_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## Offtake Live weight
  Offtake_Liveweight_kg_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Offtake_Liveweight_kg_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Offtake_Liveweight_kg_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Offtake_Liveweight_kg_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Offtake_Liveweight_kg_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # Pop growth
  Pop_growth_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Pop_growth_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Pop_growth_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Pop_growth_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Pop_growth_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Pop_growth_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Pop_growth_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## Manure
  Quant_Manure_M <- matrix(, nrow = nruns, ncol = Num_months)
 
  Quant_Manure_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Manure_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Manure_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Manure_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Manure_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Manure_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # Value of manure
  Value_Manure_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Value_Manure_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Manure_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Manure_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Manure_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Manure_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Manure_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## Hides
  Quant_Hides_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Quant_Hides_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Hides_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Hides_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Hides_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # Value of hides
  Value_Hides_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Value_Hides_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Hides_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Hides_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Hides_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # milk quantity and value
  Quant_Milk_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Milk_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # wool quantity
  Quant_Wool_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## dry matter required by system
  Cumulative_Dry_Matter_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Cumulative_Dry_Matter_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Cumulative_Dry_Matter_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Cumulative_Dry_Matter_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Cumulative_Dry_Matter_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Cumulative_Dry_Matter_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Cumulative_Dry_Matter_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## Value of offtake
  Value_Offtake_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Value_Offtake_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Offtake_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Offtake_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Offtake_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Offtake_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Offtake_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ###################################
  ## Value increase
  Value_Herd_Increase_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Value_Herd_Increase_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Herd_Increase_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Herd_Increase_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Herd_Increase_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Herd_Increase_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Herd_Increase_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## Total value increase herd value increase plus offtake value
  Total_Value_increase_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Total_Value_increase_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_Value_increase_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_Value_increase_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_Value_increase_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_Value_increase_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_Value_increase_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## all produce values matrix
  Production_value_herd_offteake_hide_man_M <- matrix(, nrow = nruns, ncol = Num_months)
 
   Production_value_herd_offteake_hide_man_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Production_value_herd_offteake_hide_man_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Production_value_herd_offteake_hide_man_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Production_value_herd_offteake_hide_man_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Production_value_herd_offteake_hide_man_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Production_value_herd_offteake_hide_man_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## Inputs
  # Feed
  Feed_cost_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Feed_cost_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Feed_cost_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Feed_cost_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Feed_cost_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Feed_cost_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Feed_cost_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # Labour
  Labour_cost_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Labour_cost_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Labour_cost_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Labour_cost_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Labour_cost_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Labour_cost_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Labour_cost_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # Health
  Health_cost_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Health_cost_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Health_cost_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Health_cost_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Health_cost_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Health_cost_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Health_cost_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # Capital
  Capital_cost_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Capital_cost_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Capital_cost_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Capital_cost_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Capital_cost_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Capital_cost_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Capital_cost_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # Infrastructure
  Infrastructure_cost_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Infrastructure_cost_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Infrastructure_cost_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Infrastructure_cost_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Infrastructure_cost_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Infrastructure_cost_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Infrastructure_cost_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  # Total expenditure
  Total_expenditure_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## Example of making storage output a matrix
  Total_expenditure_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_expenditure_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_expenditure_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_expenditure_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_expenditure_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_expenditure_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ################################################################
  ## FIND START ##
  for (i in c(1:nruns))
  {
    ## Initial population size
    
    # Total population is sum of age*sex segments
    Nt0 <- N_NF_t0 + N_NM_t0 + N_JF_t0 + N_JM_t0 + N_AF_t0 + N_AM_t0
    
    ## Step 1
    # Define population variables and set initial values from function arguments 
    
    N <- Nt0
    NF <- N_NF_t0
    NM <- N_NM_t0
    JF <- N_JF_t0
    JM <- N_JM_t0
    AF <- N_AF_t0
    AM <- N_AM_t0
    
    ## age sex group prop of pop at t0 - this ratio should stay the same
    ## but is currently unused in the code - but we need to calculate it for 
    # ME requirements down the line so leaving it in for now
    pNF_t0 <- NF/N
    pJF_t0 <- JF/N
    pAF_t0 <- AF/N
    pNM_t0 <- NM/N
    pJM_t0 <- JM/N
    pAM_t0 <- AM/N
    
    Num_dead <- 0
    
    Num_dead_NF <- 0
    Num_dead_NM <- 0
    Num_dead_JF <- 0
    Num_dead_JM <- 0
    Num_dead_AF <- 0
    Num_dead_AM <- 0
    
    ##################################################################
    ##  Create empty variables to be used for calculating production
    
    ## NOTE these MUST always be reset to 0 before running the model which 
    ## is why they are coded here otherwise the starting values will be the 
    ## same as they were at the end of the last model run
    
    # Production quantities and values (at t0)
    Liveweight_kg <- 0
    
    Liveweight_kg_NF <- 0
    Liveweight_kg_NM <- 0
    Liveweight_kg_JF <- 0
    Liveweight_kg_JM <- 0
    Liveweight_kg_AF <- 0
    Liveweight_kg_AM <- 0

    ##
    Offtake <- 0
    
    Offtake_JF <- 0
    Offtake_JM <- 0
    Offtake_AF <- 0
    Offtake_AM <- 0
    
    # Offtake Liveweight
    Offtake_Liveweight_JF <- 0
    Offtake_Liveweight_JM <- 0
    Offtake_Liveweight_AF <- 0
    Offtake_Liveweight_AM <- 0
    
    ##
    Manure_kg <- 0
    
    Manure_kg_NF <- 0
    Manure_kg_NM <- 0
    Manure_kg_JF <- 0
    Manure_kg_JM <- 0
    Manure_kg_AF <- 0
    Manure_kg_AM <- 0
    
    ##
    Hides <- 0
    
    Hides_JF <- 0
    Hides_JM <- 0
    Hides_AF <- 0
    Hides_AM <- 0
    
    Milk <- 0
    Meat_kg <- 0
    Wool <- 0
    
    #################################
    ### System Input requirements ###
    Cumulitive_DM <- 0
    
    Cumulitive_DM_NF <- 0
    Cumulitive_DM_NM <- 0
    Cumulitive_DM_JF <- 0
    Cumulitive_DM_JM <- 0
    Cumulitive_DM_AF <- 0
    Cumulitive_DM_AM <- 0

    
    # Input cost values (Value at t0)

    Feed_NF <- 0
    Feed_NM <- 0
    Feed_JF <- 0
    Feed_JM <- 0
    Feed_AF <- 0
    Feed_AM <- 0
    
    ## Labour cost at t0 
    
    Labour_NF <- 0
    Labour_NM <- 0
    Labour_JF <- 0
    Labour_JM <- 0
    Labour_AF <- 0
    Labour_AM <- 0
    
    ##
    
    Health_NF <- 0
    Health_NM <- 0
    Health_JF <- 0
    Health_JM <- 0
    Health_AF <- 0
    Health_AM <- 0
    
    ##
    Capital <- 0
    
    
    ############################################################
    #############     Simulation model            ##############
    
    ## Gemma editing code here to make individual based sampling rather than a population mean
    
    ####
    
    ## 
    for(month in c(1:Num_months)) ## For each month for as many months as set, do this...
    {
      births[month] <- sample(Mu, 1) * AF # number of adult females * reproduction rate
      
      # calculate number of deaths from each age-sex group
      deaths_NF[month] <- (sample(AlphaN, 1) * NF)
      deaths_JF[month] <- (sample(AlphaJ, 1) * JF) 
      deaths_AF[month] <- (sample(AlphaF, 1) * AF)
      deaths_NM[month] <- (sample(AlphaN, 1) * NM)
      deaths_JM[month] <- (sample(AlphaJ, 1) * JM)
      deaths_AM[month] <- (sample(AlphaM, 1) * AM)
      
      # calculate number of offtake from each age-sex group
      offtake_JF[month] <- (sample(GammaF, 1) * JF)
      offtake_AF[month] <- (sample(GammaF, 1) * AF)
      offtake_JM[month] <- (sample(GammaM, 1) * JM)
      offtake_AM[month] <- (sample(GammaM, 1) * AM)
      
      # calculate size of population growth into next age-sex compartment from each age-sex group
      growth_NF[month] <- (sample(Beta, 1) * NF)
      growth_JF[month] <- (sample(Beta, 1) * JF)
      growth_NM[month] <- (sample(Beta, 1) * NM)
      growth_JM[month] <- (sample(Beta, 1) * JM)
      
      # calculate number of culls from each age-sex group
      culls_AF[month] <- (sample(CullF, 1) * AF)
      culls_AM[month] <- (sample(CullM, 1) * AM)
      
      # now the compartmental population model uses numbers calculated in stochastic equations above
      # to create a new population for each monthly time step
     
       # females
      numNF[month] = NF + (births[month] * 0.5) - deaths_NF[month] - growth_NF[month]
      numJF[month] = JF + growth_NF[month] - growth_JF[month] - offtake_JF[month] - deaths_JF[month]
      numAF[month] = AF + growth_JF[month] - offtake_AF[month] - deaths_AF[month] - culls_AF[month]
      
      # males
      numNM[month] = NM + (births[month] * 0.5) - growth_NM[month] - deaths_NM[month]
      numJM[month] = JM + growth_NM[month] - growth_JM[month] - offtake_JM[month] - deaths_JM[month]
      numAM[month] = AM + growth_JM[month] - offtake_AM[month] - deaths_AM[month] - culls_AM[month]
     
       # total
      numN[month] = numNF[month] + numJF[month] + numAF[month] + numNM[month] + numJM[month] + numAM[month]
      
      # set new monthly population numbers based on above equations
      NF = numNF[month]
      JF = numJF[month]
      AF = numAF[month]
      NM = numNM[month]
      JM = numJM[month]
      AM = numAM[month]
      N = numN[month]
      
      ## Mortality
      ## age group deaths (cumulative within age groups then sum across age-sex groups for total)
      Total_Mortality_NF[month] = Num_dead_NF + deaths_NF[month]
      Total_Mortality_NM[month] = Num_dead_NM + deaths_NM[month]
      Total_Mortality_JF[month] = Num_dead_JF + deaths_JF[month]
      Total_Mortality_JM[month] = Num_dead_JM + deaths_JM[month]
      Total_Mortality_AF[month] = Num_dead_AF + deaths_AF[month]
      Total_Mortality_AM[month] = Num_dead_AM + deaths_AM[month]
      
      Num_dead_NF = Total_Mortality_NF[month]
      Num_dead_NM = Total_Mortality_NM[month]
      Num_dead_JF = Total_Mortality_JF[month]
      Num_dead_JM = Total_Mortality_JM[month]
      Num_dead_AF = Total_Mortality_AF[month]
      Num_dead_AM = Total_Mortality_AM[month]
      
      Total_Mortality[month] = Total_Mortality_NF[month] + Total_Mortality_NM[month] + 
        Total_Mortality_JF[month] + Total_Mortality_JM[month] + 
        Total_Mortality_AF[month] + Total_Mortality_AM[month]
		  
		# Monetary value of mortality
      Value_of_Total_Mortality_NF[month] = Total_Mortality_NF[month] * fvNF
      Value_of_Total_Mortality_NM[month] = Total_Mortality_NM[month] * fvNM
      Value_of_Total_Mortality_JF[month] = Total_Mortality_JF[month] * fvJF
      Value_of_Total_Mortality_JM[month] = Total_Mortality_JM[month] * fvJM
      Value_of_Total_Mortality_AF[month] = Total_Mortality_AF[month] * fvAF
      Value_of_Total_Mortality_AM[month] = Total_Mortality_AM[month] * fvAM
		
		Value_of_Total_Mortality[month] = Value_of_Total_Mortality_NF[month] + Value_of_Total_Mortality_NM[month] + 
        Value_of_Total_Mortality_JF[month] + Value_of_Total_Mortality_JM[month] + 
        Value_of_Total_Mortality_AF[month] + Value_of_Total_Mortality_AM[month]
		  
      # Population growth (total population in month - original population size)
      Pop_growth[month] =  numN[month] - Nt0
      
      Pop_growth_NF[month] =  NF - N_NF_t0
      Pop_growth_NM[month] =  NM - N_NM_t0
      Pop_growth_JF[month] =  JF - N_JF_t0
      Pop_growth_JM[month] =  JM - N_JM_t0
      Pop_growth_AF[month] =  AF - N_AF_t0
      Pop_growth_AM[month] =  AM - N_AM_t0
      
      # whole population as Liveweight (number in each age sex group * Liveweight conversion factor, for each month - NOT Cumulative)
      Quant_Liveweight_kg_NF[month] = (NF * sample(lwNF, 1))
      Quant_Liveweight_kg_NM[month] = (NM * sample(lwNM, 1))
      Quant_Liveweight_kg_JF[month] = (JF * sample(lwJF, 1))
      Quant_Liveweight_kg_JM[month] = (JM * sample(lwJM, 1))
      Quant_Liveweight_kg_AF[month] = (AF * sample(lwAF, 1))
      Quant_Liveweight_kg_AM[month] = (AM * sample(lwAM, 1))
      
      Quant_Liveweight_kg[month] = Quant_Liveweight_kg_NF[month] + Quant_Liveweight_kg_NM[month] +
        Quant_Liveweight_kg_JF[month] + Quant_Liveweight_kg_JM[month] + 
        Quant_Liveweight_kg_AF[month] + Quant_Liveweight_kg_AM[month]
      
      # Offtake (all offtake added + culled adult males)
      ## offtake_ from different age cats. (offtake... (lowercase o) is from original calculation above, 
      ## Offtake_is cumulative sum across months
      ## Num_Offtake is Cumulative sum for that month at time [month]
      Num_Offtake_JF[month] <- Offtake_JF + offtake_JF[month]
      Num_Offtake_JM[month] <- Offtake_JM + offtake_JM[month]
      Num_Offtake_AF[month] <- Offtake_AF + offtake_AF[month]
      Num_Offtake_AM[month] <- Offtake_AM + offtake_AM[month] + culls_AM[month]
      
      ##
      Offtake_JF = Num_Offtake_JF[month]
      Offtake_JM = Num_Offtake_JM[month]
      Offtake_AF = Num_Offtake_AF[month]
      Offtake_AM = Num_Offtake_AM[month]
      
      Num_Offtake[month] = Num_Offtake_JF[month] + Num_Offtake_JM[month] + Num_Offtake_AF[month] + Num_Offtake_AM[month] 
      Offtake = Num_Offtake[month]
      
      ## Off take Liveweight (convert offtake animals into a liveweight)
      Offtake_Liveweight_kg_JF[month] = (sample(lwJF, 1) * Offtake_JF)
      Offtake_Liveweight_kg_JM[month] = (sample(lwJM, 1) * Offtake_JM)
      Offtake_Liveweight_kg_AF[month] = (sample(lwAF, 1) * Offtake_AF)
      Offtake_Liveweight_kg_AM[month] = (sample(lwAM, 1) * Offtake_AM)
      
      Offtake_Liveweight_kg[month] <- Offtake_Liveweight_kg_JF[month] + Offtake_Liveweight_kg_JM[month] +
        Offtake_Liveweight_kg_AF[month] + Offtake_Liveweight_kg_AM[month]
      
      ## Meat production (not currently used by other models as an output as liveweight prices are used)
      ## Offtake live weight converted into meat 
      ## Offtake_JF etc accumulates month on month through the year so converting this to meat each month 
      ## will give total meat for the year by month 12
      
      Quant_Meat_kg[month] = (Offtake_Liveweight_kg_JF[month] * ccy) + 
        (Offtake_Liveweight_kg_JM[month] * ccy) +
        (Offtake_Liveweight_kg_AF[month] * ccy) + 
        (Offtake_Liveweight_kg_AM[month] * ccy) 
      
      ## not actually used as dont need to make this cumulative  
      Meat_kg = Quant_Meat_kg[month]
      
      ## Hides
      ## Quantity of hides in the dif age sex groups
      ## Only adding hides from dead stock as offtake hides value is covered in live animal price
      Quant_Hides_JF[month] = Hides_JF + (deaths_JF[month] * hides_rate_mor)
      Quant_Hides_JM[month] = Hides_JM + (deaths_JM[month] * hides_rate_mor)
      Quant_Hides_AF[month] = Hides_AF + (deaths_AF[month] * hides_rate_mor)
      Quant_Hides_AM[month] = Hides_AM + (deaths_AM[month] * hides_rate_mor) 
      
      Hides_JF = Quant_Hides_JF[month]
      Hides_JM = Quant_Hides_JM[month]
      Hides_AF = Quant_Hides_AF[month]
      Hides_AM = Quant_Hides_AM[month]
      
      # Total sum of hides for total population
      Quant_Hides[month] = Quant_Hides_JF[month] + Quant_Hides_JM[month] + 	Quant_Hides_AF[month] + Quant_Hides_AM[month]
      Hides = Quant_Hides[month]
      
      ## Milk yield 
      # Number of females * monthly avg parturition rate * number that would be milked * lactation duration * avg daily yield

      ## The amount of milk produced each month is the sum of milk from each adult female that gives birth 
      ## full lactation, and each month this is added to the amount of milk produced by animals that give birth 
      ## in the previous months. Some milking animals will die during their lactation and this is not accounted for
      ## in this calculation, but, dead animals wont give birth and produce any milk so I think it evens out.
      ## This is a calculation that can be revisited in time
      
      Quant_Milk[month] = Milk + (AF * (sample(part, 1)) * prop_F_milked * lac_duration * avg_daily_yield_ltr) 
      
      Milk = Quant_Milk[month]
      
      ## Manure 
      ## daily manure produced from all animals in each age-sex group * 30 
      ## to give monthly manure production from the different age-sex groups
      ## cumulative so added
      
      Quant_Manure_NF[month] = Manure_kg_NF + (NF * (sample(Man_N, 1) * 30))  
      Quant_Manure_NM[month] = Manure_kg_NM + (NM * (sample(Man_N, 1) * 30))  
      Quant_Manure_JF[month] = Manure_kg_JF + (JF * (sample(Man_J, 1) * 30))  
      Quant_Manure_JM[month] = Manure_kg_JM + (JM * (sample(Man_J, 1) * 30))  
      Quant_Manure_AF[month] = Manure_kg_AF + (AF * (sample(Man_A, 1) * 30))  
      Quant_Manure_AM[month] = Manure_kg_AM + (AM * (sample(Man_A, 1) * 30))  
      
      Manure_kg_NF = Quant_Manure_NF[month]
      Manure_kg_NM = Quant_Manure_NM[month]
      Manure_kg_JF = Quant_Manure_JF[month]
      Manure_kg_JM = Quant_Manure_JM[month]
      Manure_kg_AF = Quant_Manure_AF[month]
      Manure_kg_AM = Quant_Manure_AM[month]
      
      Quant_Manure[month] = Quant_Manure_NF[month] + Quant_Manure_NM[month] + Quant_Manure_JF[month] + Quant_Manure_JM[month] + Quant_Manure_AF[month] + Quant_Manure_AM[month]
      
      Manure_kg = Quant_Manure[month]
      
      # Cumulative dry matter used by the system (dry matter for now but ME can be calculated similarly)
      
      ## This is number of animals in age-sex group * daily dry matter requirement of those animals * 30 
      ## to give monthly dry matter requirement, which is summed month on month
      
      ## // SIDE note... to calculate ME we will need to estimate animals daily ME requirements for maintenance, 
      ## growth (in neonates and juveniles) and any production outputs (milk, gestation, traction)
      ## for the average sized animal in each age-sex group, and this can be the ME used by the system 
      ## at present //
      
      Cumulative_Dry_Matter_NF[month] = Cumulitive_DM_NF + (NF * (sample(kg_DM_req_NF, 1) * 30)) 
      Cumulative_Dry_Matter_NM[month] = Cumulitive_DM_NM + (NM * (sample(kg_DM_req_NF, 1) * 30)) 
      Cumulative_Dry_Matter_JF[month] = Cumulitive_DM_JF + (JF * (sample(kg_DM_req_NF, 1) * 30)) 
      Cumulative_Dry_Matter_JM[month] = Cumulitive_DM_JM + (JM * (sample(kg_DM_req_NF, 1) * 30)) 
      Cumulative_Dry_Matter_AF[month] = Cumulitive_DM_AF + (AF * (sample(kg_DM_req_NF, 1) * 30)) 
      Cumulative_Dry_Matter_AM[month] = Cumulitive_DM_AM + (AM * (sample(kg_DM_req_NF, 1) * 30)) 
      
      Cumulitive_DM_NF = Cumulative_Dry_Matter_NF[month]
      Cumulitive_DM_NM = Cumulative_Dry_Matter_NM[month]
      Cumulitive_DM_JF = Cumulative_Dry_Matter_JF[month]
      Cumulitive_DM_JM = Cumulative_Dry_Matter_JM[month]
      Cumulitive_DM_AF = Cumulative_Dry_Matter_AF[month]
      Cumulitive_DM_AM = Cumulative_Dry_Matter_AM[month]
      
      ## Total population dry matter requirement
      Cumulative_Dry_Matter[month] = Cumulative_Dry_Matter_NF[month] + Cumulative_Dry_Matter_NM[month] +
        Cumulative_Dry_Matter_JF[month] + Cumulative_Dry_Matter_JM[month] +
        Cumulative_Dry_Matter_AF[month] + Cumulative_Dry_Matter_AM[month]
      
      
      
      ## Financial value of off take 
      ## (all off take * financial value) // Note, for adult males the Offtake_AM includes culled males
      ## Juvenile and adults only
      
      Value_Offtake_JF[month] = (sample(fvJF, 1) * Offtake_JF) 
      Value_Offtake_JM[month] = (sample(fvJM, 1) * Offtake_JM)
      Value_Offtake_AF[month] = (sample(fvAF, 1) * Offtake_AF)
      Value_Offtake_AM[month] = (sample(fvAM, 1) * Offtake_AM)  
      
      ## Total value of off take
      ## Sum value's of offtake for the four age-sex groups 
      Value_Offtake[month] = Value_Offtake_JF[month] + Value_Offtake_JM[month] + Value_Offtake_AF[month] + Value_Offtake_AM[month]
      
      ## Financial value of heard increase 
      ## calculation is change in herd/flock population since t0 * price per head (each month compares to t0)
      ## NOTE important factor here is we use current market price, BUT, if pop size increase 
      ## AND offtake increases price and demand will likely drop so we need to factor that in.
      ## MUST work with wider economic impact theme to adjust this price relationship
      
        Value_Herd_Increase_NF[month] = (NF - N_NF_t0) * (sample(fvNF, 1))
        Value_Herd_Increase_NM[month] = (NM - N_NM_t0) * (sample(fvNM, 1))
        Value_Herd_Increase_JF[month] = (JF - N_JF_t0) * (sample(fvJF, 1))
        Value_Herd_Increase_JM[month] = (JM - N_JM_t0) * (sample(fvJM, 1))
        Value_Herd_Increase_AF[month] = (AF - N_AF_t0) * (sample(fvAF, 1))
        Value_Herd_Increase_AM[month] = (AM - N_AM_t0) * (sample(fvAM, 1))
        
      # total population value of national herd size increase
      Value_Herd_Increase[month] = Value_Herd_Increase_NF[month] + Value_Herd_Increase_NM[month] + Value_Herd_Increase_JF[month] + Value_Herd_Increase_JM[month] + Value_Herd_Increase_AF[month] + Value_Herd_Increase_AM[month]
      
      ## Total value increase (Offtake + herd increase) 
      ## AGAIN this calculation uses current market prices
      ## summing together the two values calculated above
      Total_Value_increase[month] = Value_Herd_Increase[month] + Value_Offtake[month]
      
      Total_Value_increase_NF[month] = Value_Herd_Increase_NF[month] 
      Total_Value_increase_NM[month] = Value_Herd_Increase_NM[month] 
      Total_Value_increase_JF[month] = Value_Herd_Increase_JF[month] + Value_Offtake_JF[month]
      Total_Value_increase_JM[month] = Value_Herd_Increase_JM[month] + Value_Offtake_JM[month]
      Total_Value_increase_AF[month] = Value_Herd_Increase_AF[month] + Value_Offtake_AF[month]
      Total_Value_increase_AM[month] = Value_Herd_Increase_AM[month] + Value_Offtake_AM[month]
      
      ##########################################
      ######     Expenditure in system    ######
      
      ## feed cost is calculated based on current market value cost of feed
      ## which is roughage crop (not concentrate) for Ethiopia scenario
      ## // Note, reiterating point made above re feed calculations...
      ## multiplied by the amount needed by the animals (dry matter requirement) 
      ## multiplied by the total amount that is purchased (proportion of LS keepers 
      ## buying any feed * estimated amount they might buy) 
      
      ## > Feed cost ##
     
      # amount spent calculated each month and added to previous month(s)
      Feed_cost_NF[month] = Feed_NF + (NF * (sample(Expenditure_on_feed_NF, 1)) * 30) 
      Feed_cost_NM[month] = Feed_NM + (NM * (sample(Expenditure_on_feed_NM, 1)) * 30) 
      Feed_cost_JF[month] = Feed_JF + (JF * (sample(Expenditure_on_feed_JF, 1)) * 30)
      Feed_cost_JM[month] = Feed_JM + (JM * (sample(Expenditure_on_feed_JM, 1)) * 30)
      Feed_cost_AF[month] = Feed_AF + (AF * (sample(Expenditure_on_feed_AF, 1)) * 30)
      Feed_cost_AM[month] = Feed_AM + (AM * (sample(Expenditure_on_feed_AM, 1)) * 30) 
      
      Feed_NF = Feed_cost_NF[month]
      Feed_NM = Feed_cost_NM[month]
      Feed_JF = Feed_cost_JF[month]
      Feed_JM = Feed_cost_JM[month]
      Feed_AF = Feed_cost_AF[month]
      Feed_AM = Feed_cost_AM[month]
      
      # Total feed cost (sum all age-sex groups)
      Feed_cost[month] = Feed_cost_NF[month] + Feed_cost_NM[month] + Feed_cost_JF[month] + Feed_cost_JM[month] + Feed_cost_AF[month] + Feed_cost_AM[month]
      
      
      ## > Labour cost ##
      
      ## Labour cost is calculated each month by multiplying the number of animals 
      ## in each age-sex group by the labour cost per head per month and added to previous
      ## months data to get cumulative expenditure on labour through the year
      ## labour cost is also multiplied by the proportio of labour time spent on health 
      ## related activities, as in the ideal scenario overall labour time and thus cost will 
      ## be reduced as less time will be spent on health related activities.
      ## to get this parameter experts were asked what proportion of labour time was spent on 
      ## health related tasks. in the current scenario the value of lab_non_health is = 1 and in 
      ## the ideal scenario the value of lab_non_health is 1 - (prop. time spent on health related tasks)
      
      Labour_cost_NF[month] = Labour_NF + (NF * (sample(Lab_SR, 1)) * lab_non_health) 
      Labour_cost_NM[month] = Labour_NM + (NM * (sample(Lab_SR, 1)) * lab_non_health)  
      Labour_cost_JF[month] = Labour_JF + (JF * (sample(Lab_SR, 1)) * lab_non_health)  
      Labour_cost_JM[month] = Labour_JM + (JM * (sample(Lab_SR, 1)) * lab_non_health)  
      Labour_cost_AF[month] = Labour_AF + (AF * (sample(Lab_SR, 1)) * lab_non_health)  
      Labour_cost_AM[month] = Labour_AM + (AM * (sample(Lab_SR, 1)) * lab_non_health)  
      
      Labour_NF = Labour_cost_NF[month]
      Labour_NM = Labour_cost_NM[month]
      Labour_JF = Labour_cost_JF[month]
      Labour_JM = Labour_cost_JM[month]
      Labour_AF = Labour_cost_AF[month]
      Labour_AM = Labour_cost_AM[month]
      
      ## Total population labour expenditure
      Labour_cost[month] = Labour_cost_NF[month] + Labour_cost_NM[month] + Labour_cost_JF[month] + Labour_cost_JM[month] + Labour_cost_AF[month] + Labour_cost_AM[month]
      

      ## > Health care cost ##
      
      ## This calculation includes medicines and veterinary expenditure
      ## each month is calculated for animals in each age-sex groups and added to previous month(s)
      
      Health_cost_NF[month] = Health_NF + (NF * (sample(Health_exp, 1))) 
      Health_cost_NM[month] = Health_NM + (NM * (sample(Health_exp, 1))) 
      Health_cost_JF[month] = Health_JF + (JF * (sample(Health_exp, 1))) 
      Health_cost_JM[month] = Health_JM + (JM * (sample(Health_exp, 1)))
      Health_cost_AF[month] = Health_AF + (AF * (sample(Health_exp, 1))) 
      Health_cost_AM[month] = Health_AM + (AM * (sample(Health_exp, 1))) 
      
      Health_NF = Health_cost_NF[month]
      Health_NM = Health_cost_NM[month]
      Health_JF = Health_cost_JF[month]
      Health_JM = Health_cost_JM[month]
      Health_AF = Health_cost_AF[month]
      Health_AM = Health_cost_AM[month]
      
      Health_cost[month] = Health_cost_NF[month] + Health_cost_NM[month] + Health_cost_JF[month] + Health_cost_JM[month] + Health_cost_AF[month] + Health_cost_AM[month]

      
      ## > Capital cost ##
      
      Capital_cost_NF[month] = numNF[1] * (sample(fvNF, 1)) * Interest_rate 
      Capital_cost_NM[month] = numNM[1] * (sample(fvNM, 1)) * Interest_rate  
      Capital_cost_JF[month] = numJF[1] * (sample(fvJF, 1)) * Interest_rate  
      Capital_cost_JM[month] = numJM[1] * (sample(fvJM, 1)) * Interest_rate  
      Capital_cost_AF[month] = numAF[1] * (sample(fvAF, 1)) * Interest_rate  
      Capital_cost_AM[month] = numAM[1] * (sample(fvAM, 1)) * Interest_rate  
      
      # total population capital cost
      Capital_cost[month] = Capital_cost_NF[month] + Capital_cost_NM[month] + Capital_cost_JF[month] + 
                            Capital_cost_JM[month] + Capital_cost_AF[month] + Capital_cost_AM[month]
      
      
      ## > Infrastructure cost ##
      
      ## Simple calculation - number of animals at t0 * baseline annual infrastructure cost per head
      ## this is mainly a place holder for other production systems as in Ethiopia infrastructure 
      ## expenditure is very low, we used a value of 1 birr per animal in the example preliminary scenarios
      ## This is calculated monthly for consistency but its the same cost each month as its all the same as the
      ## initial expenduture on onfrastructure in month 1. If you have a growing population, which we actually want to 
      ## avoid, you could change the calculation to add extra infrastructure costs as the population grows, if you
      ## are dealing with a population and production system that needs additional expenditure on infrastructure if
      ## the population changes
      
      Infrastructure_cost_NF[month] <- N_NF_t0 * (sample(Infrastructure_per_head, 1))
      Infrastructure_cost_NM[month] <- N_NM_t0 * (sample(Infrastructure_per_head, 1))
      Infrastructure_cost_JF[month] <- N_JF_t0 * (sample(Infrastructure_per_head, 1))
      Infrastructure_cost_JM[month] <- N_JM_t0 * (sample(Infrastructure_per_head, 1))
      Infrastructure_cost_AF[month] <- N_AF_t0 * (sample(Infrastructure_per_head, 1))
      Infrastructure_cost_AM[month] <- N_AM_t0 * (sample(Infrastructure_per_head, 1))
      
      # total infrastructure cost (per month)
      Infrastructure_cost[month] <- Infrastructure_cost_NF[month] + Infrastructure_cost_NM[month] + Infrastructure_cost_JF[month] + Infrastructure_cost_JM[month] + Infrastructure_cost_AF[month] + Infrastructure_cost_AM[month]
    
      
      ## > Total expenditure 
      
      # This is the expenditure on feed, health, labour, capital and infrastructure
      ## summed for total population each month and for age-sex groups
      
      Total_expenditure[month] =  Feed_cost[month] + Health_cost[month] + Labour_cost[month] + Capital_cost[month] + Infrastructure_cost[month]
      
      Total_expenditure_NF[month] =  Feed_NF + Health_NF + Labour_NF + Capital_cost_NF[month] + Infrastructure_cost_NF[month]
      Total_expenditure_NM[month] =  Feed_NM + Health_NM + Labour_NM + Capital_cost_NM[month] + Infrastructure_cost_NM[month]
      Total_expenditure_JF[month] =  Feed_JF + Health_JF + Labour_JF + Capital_cost_JF[month] + Infrastructure_cost_JF[month]
      Total_expenditure_JM[month] =  Feed_JM + Health_JM + Labour_JM + Capital_cost_JM[month] + Infrastructure_cost_JM[month]
      Total_expenditure_AF[month] =  Feed_AF + Health_AF + Labour_AF + Capital_cost_AF[month] + Infrastructure_cost_AF[month]
      Total_expenditure_AM[month] =  Feed_AM + Health_AM + Labour_AM + Capital_cost_AM[month] + Infrastructure_cost_AM[month]
    }
    
    ### Next step is to fill in each row of the matrices with every scenario run
    ## the above loop then resets to time 0 and another simulation runs
    
    ## Remember n runs will be how many rows the matrices has to fill, n months will be the number of columns
    ## so for a year long simulation number of months = 12 and the 12th month totals are the annual total
    
    # Population Numbers
    
    numNF_M[i, ] <- numNF
    numJF_M[i, ] <- numJF
    numAF_M[i, ] <- numAF
    numNM_M[i, ] <- numNM
    numJM_M[i, ] <- numJM
    numAM_M[i, ] <- numAM
    numN_M[i, ] <- numN
    
    # Mortality
    
    Total_Mortality_M[i, ] <- Total_Mortality
    
    Total_Mortality_NF_M[i, ] <- Total_Mortality_NF
    Total_Mortality_NM_M[i, ] <- Total_Mortality_NM
    Total_Mortality_JF_M[i, ] <- Total_Mortality_JF
    Total_Mortality_JM_M[i, ] <- Total_Mortality_JM
    Total_Mortality_AF_M[i, ] <- Total_Mortality_AF
    Total_Mortality_AM_M[i, ] <- Total_Mortality_AM
	 
	 # Monetary value of mortality
    Value_of_Total_Mortality_M[i, ] <- Value_of_Total_Mortality
    
    Value_of_Total_Mortality_NF_M[i, ] <- Value_of_Total_Mortality_NF
    Value_of_Total_Mortality_NM_M[i, ] <- Value_of_Total_Mortality_NM
    Value_of_Total_Mortality_JF_M[i, ] <- Value_of_Total_Mortality_JF
    Value_of_Total_Mortality_JM_M[i, ] <- Value_of_Total_Mortality_JM
    Value_of_Total_Mortality_AF_M[i, ] <- Value_of_Total_Mortality_AF
    Value_of_Total_Mortality_AM_M[i, ] <- Value_of_Total_Mortality_AM
    
    ###############################################################
    # Production
    
    # Liveweight
    Quant_Liveweight_kg_M[i, ] <- Quant_Liveweight_kg
    
    Quant_Liveweight_kg_NF_M[i, ] <- Quant_Liveweight_kg_NF
    Quant_Liveweight_kg_NM_M[i, ] <- Quant_Liveweight_kg_NM
    Quant_Liveweight_kg_JF_M[i, ] <- Quant_Liveweight_kg_JF
    Quant_Liveweight_kg_JM_M[i, ] <- Quant_Liveweight_kg_JM
    Quant_Liveweight_kg_AF_M[i, ] <- Quant_Liveweight_kg_AF
    Quant_Liveweight_kg_AM_M[i, ] <- Quant_Liveweight_kg_AM
    
    # Meat
    Quant_Meat_kg_M[i, ] <- Quant_Meat_kg
    
    # Offtake
    Num_Offtake_M[i, ] <- Num_Offtake
    
    Num_Offtake_NF_M[i, ] <- Num_Offtake_NF
    Num_Offtake_NM_M[i, ] <- Num_Offtake_NM
    Num_Offtake_JF_M[i, ] <- Num_Offtake_JF
    Num_Offtake_JM_M[i, ] <- Num_Offtake_JM
    Num_Offtake_AF_M[i, ] <- Num_Offtake_AF
    Num_Offtake_AM_M[i, ] <- Num_Offtake_AM
    
    # Live weight of offtake
    Offtake_Liveweight_kg_M[i, ] <- Offtake_Liveweight_kg
    
    Offtake_Liveweight_kg_JF_M[i, ] <- Offtake_Liveweight_kg_JF
    Offtake_Liveweight_kg_JM_M[i, ] <- Offtake_Liveweight_kg_JM
    Offtake_Liveweight_kg_AF_M[i, ] <- Offtake_Liveweight_kg_AF
    Offtake_Liveweight_kg_AM_M[i, ] <- Offtake_Liveweight_kg_AM
    
    # Population growth 
    Pop_growth_M[i, ] <- Pop_growth
    
    Pop_growth_NF_M[i, ] <- Pop_growth_NF
    Pop_growth_NM_M[i, ] <- Pop_growth_NM
    Pop_growth_JF_M[i, ] <- Pop_growth_JF
    Pop_growth_JM_M[i, ] <- Pop_growth_JM
    Pop_growth_AF_M[i, ] <- Pop_growth_AF
    Pop_growth_AM_M[i, ] <- Pop_growth_AM
    
    ## Manure
    Quant_Manure_M[i, ] <- Quant_Manure
    
    Quant_Manure_NF_M[i, ] <- Quant_Manure_NF
    Quant_Manure_NM_M[i, ] <- Quant_Manure_NM
    Quant_Manure_JF_M[i, ] <- Quant_Manure_JF
    Quant_Manure_JM_M[i, ] <- Quant_Manure_JM
    Quant_Manure_AF_M[i, ] <- Quant_Manure_AF
    Quant_Manure_AM_M[i, ] <- Quant_Manure_AM
    
    
    ## Hides
    Quant_Hides_M[i, ] <- Quant_Hides
    
    Quant_Hides_JF_M[i, ] <- Quant_Hides_JF
    Quant_Hides_JM_M[i, ] <- Quant_Hides_JM
    Quant_Hides_AF_M[i, ] <- Quant_Hides_AF
    Quant_Hides_AM_M[i, ] <- Quant_Hides_AM
    
    ## Milk
    Quant_Milk_M[i, ] <- Quant_Milk
    
    
    ## Wool 
    ## No calculation for this in the model yet but here as place holder for when the calculation is added
    Quant_Wool_M[i, ] <- Quant_Wool
    
    ## Dry matter
    Cumulative_Dry_Matter_M[i, ] <- Cumulative_Dry_Matter
    
    Cumulative_Dry_Matter_NF_M[i, ] <- Cumulative_Dry_Matter_NF
    Cumulative_Dry_Matter_NM_M[i, ] <- Cumulative_Dry_Matter_NM
    Cumulative_Dry_Matter_JF_M[i, ] <- Cumulative_Dry_Matter_JF
    Cumulative_Dry_Matter_JM_M[i, ] <- Cumulative_Dry_Matter_JM
    Cumulative_Dry_Matter_AF_M[i, ] <- Cumulative_Dry_Matter_AF
    Cumulative_Dry_Matter_AM_M[i, ] <- Cumulative_Dry_Matter_AM
    
    ## Value of offtake
    Value_Offtake_M[i, ] <- Value_Offtake
    
    Value_Offtake_NF_M[i, ] <- Value_Offtake_NF
    Value_Offtake_NM_M[i, ] <- Value_Offtake_NM
    Value_Offtake_JF_M[i, ] <- Value_Offtake_JF
    Value_Offtake_JM_M[i, ] <- Value_Offtake_JM
    Value_Offtake_AF_M[i, ] <- Value_Offtake_AF
    Value_Offtake_AM_M[i, ] <- Value_Offtake_AM
    
    ###################################
    ## Values
    
    ## Value of herd increase
    Value_Herd_Increase_M[i, ] <- Value_Herd_Increase
    
    Value_Herd_Increase_NF_M[i, ] <- Value_Herd_Increase_NF
    Value_Herd_Increase_NM_M[i, ] <- Value_Herd_Increase_NM
    Value_Herd_Increase_JF_M[i, ] <- Value_Herd_Increase_JF
    Value_Herd_Increase_JM_M[i, ] <- Value_Herd_Increase_JM
    Value_Herd_Increase_AF_M[i, ] <- Value_Herd_Increase_AF
    Value_Herd_Increase_AM_M[i, ] <- Value_Herd_Increase_AM
    
    ## Total value increase herd value increase plus offtake value
    Total_Value_increase_M[i, ] <- Total_Value_increase
    
    Total_Value_increase_NF_M[i, ] <- Total_Value_increase_NF
    Total_Value_increase_NM_M[i, ] <- Total_Value_increase_NM
    Total_Value_increase_JF_M[i, ] <- Total_Value_increase_JF
    Total_Value_increase_JM_M[i, ] <- Total_Value_increase_JM
    Total_Value_increase_AF_M[i, ] <- Total_Value_increase_AF
    Total_Value_increase_AM_M[i, ] <- Total_Value_increase_AM
    
    ###################################
    ## Expenditure on system inputs
    
    # Feed
    Feed_cost_M[i, ] <- Feed_cost
    
    Feed_cost_NF_M[i, ] <- Feed_cost_NF
    Feed_cost_NM_M[i, ] <- Feed_cost_NM
    Feed_cost_JF_M[i, ] <- Feed_cost_JF
    Feed_cost_JM_M[i, ] <- Feed_cost_JM
    Feed_cost_AF_M[i, ] <- Feed_cost_AF
    Feed_cost_AM_M[i, ] <- Feed_cost_AM
    
    # Labour
    Labour_cost_M[i, ] <- Labour_cost
    
    Labour_cost_NF_M[i, ] <- Labour_cost_NF
    Labour_cost_NM_M[i, ] <- Labour_cost_NM
    Labour_cost_JF_M[i, ] <- Labour_cost_JF
    Labour_cost_JM_M[i, ] <- Labour_cost_JM
    Labour_cost_AF_M[i, ] <- Labour_cost_AF
    Labour_cost_AM_M[i, ] <- Labour_cost_AM
    
    # Health
    Health_cost_M[i, ] <- Health_cost
    
    Health_cost_NF_M[i, ] <- Health_cost_NF
    Health_cost_NM_M[i, ] <- Health_cost_NM
    Health_cost_JF_M[i, ] <- Health_cost_JF
    Health_cost_JM_M[i, ] <- Health_cost_JM
    Health_cost_AF_M[i, ] <- Health_cost_AF
    Health_cost_AM_M[i, ] <- Health_cost_AM
    
    # Capital
    Capital_cost_M[i, ] <- Capital_cost
    
    Capital_cost_NF_M[i, ] <- Capital_cost_NF
    Capital_cost_NM_M[i, ] <- Capital_cost_NM
    Capital_cost_JF_M[i, ] <- Capital_cost_JF
    Capital_cost_JM_M[i, ] <- Capital_cost_JM
    Capital_cost_AF_M[i, ] <- Capital_cost_AF
    Capital_cost_AM_M[i, ] <- Capital_cost_AM
    
    # Infrastructure
    Infrastructure_cost_M[i, ] <- Infrastructure_cost
    
    Infrastructure_cost_NF_M[i, ] <- Infrastructure_cost_NF
    Infrastructure_cost_NM_M[i, ] <- Infrastructure_cost_NM
    Infrastructure_cost_JF_M[i, ] <- Infrastructure_cost_JF
    Infrastructure_cost_JM_M[i, ] <- Infrastructure_cost_JM
    Infrastructure_cost_AF_M[i, ] <- Infrastructure_cost_AF
    Infrastructure_cost_AM_M[i, ] <- Infrastructure_cost_AM
    
    # total expenditure
    Total_expenditure_M[i, ] <- Total_expenditure
    
    Total_expenditure_NF_M[i, ] <- Total_expenditure_NF
    Total_expenditure_NM_M[i, ] <- Total_expenditure_NM
    Total_expenditure_JF_M[i, ] <- Total_expenditure_JF
    Total_expenditure_JM_M[i, ] <- Total_expenditure_JM
    Total_expenditure_AF_M[i, ] <- Total_expenditure_AF
    Total_expenditure_AM_M[i, ] <- Total_expenditure_AM
    
  }
  
  ## after filling the matrices in the above loop they can be 
  ## worked with - some converted into values and some summed to give total values
  
  ## Change in number of animals (from baseline t0) is the sum of the population growth and the offtake
  ## so the number of animals that have been produced in excess of the starting population at t0
  
  Total_number_change_NF_M <- Num_Offtake_NF_M + Pop_growth_NF_M
  Total_number_change_NM_M <- Num_Offtake_NM_M + Pop_growth_NM_M
  Total_number_change_JF_M <- Num_Offtake_JF_M + Pop_growth_JF_M
  Total_number_change_JM_M <- Num_Offtake_JM_M + Pop_growth_JM_M
  Total_number_change_AF_M <- Num_Offtake_AF_M + Pop_growth_AF_M
  Total_number_change_AM_M <- Num_Offtake_AM_M + Pop_growth_AM_M

  Total_number_change_M <- Total_number_change_NF_M + Total_number_change_NM_M + Total_number_change_JF_M + Total_number_change_JM_M + Total_number_change_AF_M + Total_number_change_AM_M
  
  ## Converting some matrices that are quantities into values and storing as a value version
  
  ## these prices are set in the scenario spreadsheet so are fixed at t0 but a relationship
  ## could be fitted here, or later, informed by WEI models, that allows price to fall as 
  ## number of each product increases in the 'ideal scenario' models
  
  ## Milk
  Value_Milk_M <- Quant_Milk_M * milk_value_ltr
  
  ## Hides
  Value_Hides_M <- Quant_Hides_M * hides_value
  
  Value_Hides_JF_M <- Quant_Hides_JF_M * hides_value
  Value_Hides_JM_M <- Quant_Hides_JM_M * hides_value
  Value_Hides_AF_M <- Quant_Hides_AF_M * hides_value
  Value_Hides_AM_M <- Quant_Hides_AM_M * hides_value
  
  ## Manure
  Value_Manure_M <- Quant_Manure_M * Man_value
  
  Value_Manure_NF_M <- Quant_Manure_NF_M * Man_value
  Value_Manure_NM_M <- Quant_Manure_NM_M * Man_value
  Value_Manure_JF_M <- Quant_Manure_JF_M * Man_value
  Value_Manure_JM_M <- Quant_Manure_JM_M * Man_value
  Value_Manure_AF_M <- Quant_Manure_AF_M * Man_value
  Value_Manure_AM_M <- Quant_Manure_AM_M * Man_value
  
  ## Total production VALUE 
  ## This is the sum of the value of the herd increase and offtake (Total_Value_increase) 
  ## added to the value of the produce from the system
  ## ALWAYS make sure your values in the scenario spreadsheet are in the same currency
  Production_value_herd_offteake_hide_man_M <- Total_Value_increase_M + Value_Manure_M + Value_Hides_M + Value_Milk_M
  
  Production_value_herd_offteake_hide_man_NF_M <- Total_Value_increase_NF_M + Value_Manure_NF_M
  Production_value_herd_offteake_hide_man_NM_M <- Total_Value_increase_NM_M + Value_Manure_NM_M
  Production_value_herd_offteake_hide_man_JF_M <- Total_Value_increase_JF_M + Value_Manure_JF_M + Value_Hides_JF_M
  Production_value_herd_offteake_hide_man_JM_M <- Total_Value_increase_JM_M + Value_Manure_JM_M + Value_Hides_JM_M
  Production_value_herd_offteake_hide_man_AF_M <- Total_Value_increase_AF_M + Value_Manure_AF_M + Value_Hides_AF_M + Value_Milk_M
  Production_value_herd_offteake_hide_man_AM_M <- Total_Value_increase_AM_M + Value_Manure_AM_M + Value_Hides_AM_M
  
  ## Gross margin
  ## this is all income from the system (produce, population value change and offtake) minus 
  ## the total expenditure on the system
  
  ## When doing any calculations with Gross margin always use the total system gross margin, NOT
  ## the individual age-sex groups gross margins. the individual age sex group gross margins tell us how much
  ## money is spent and gained by each age-sex group and might be of interest in future BUT when making changes
  ## to the scenarios such as improving neonatal mortality, or adult female reproductive performance we need to see
  ## and use the impact on the WHOLE system gross margin due to the knock on effect on other age-sex groups 
  ## within the system
  
  Gross_margin_M <- Production_value_herd_offteake_hide_man_M - Total_expenditure_M
  
  Gross_margin_NF_M <- Production_value_herd_offteake_hide_man_NF_M - Total_expenditure_NF_M
  Gross_margin_NM_M <- Production_value_herd_offteake_hide_man_NM_M - Total_expenditure_NM_M
  Gross_margin_JF_M <- Production_value_herd_offteake_hide_man_JF_M - Total_expenditure_JF_M
  Gross_margin_JM_M <- Production_value_herd_offteake_hide_man_JM_M - Total_expenditure_JM_M
  Gross_margin_AF_M <- Production_value_herd_offteake_hide_man_AF_M - Total_expenditure_AF_M
  Gross_margin_AM_M <- Production_value_herd_offteake_hide_man_AM_M - Total_expenditure_AM_M
  
  # -----------------------------------------------------------------
  # Sum sex groups for neonates and juveniles
  # -----------------------------------------------------------------
  
  ## these outputs are needed for the dashboard and showing different scenarios
  Num_Offtake_N_M <- Num_Offtake_NF_M + Num_Offtake_NM_M
  Num_Offtake_J_M <- Num_Offtake_JF_M + Num_Offtake_JM_M
  
  Pop_growth_N_M <- Pop_growth_NF_M + Pop_growth_NM_M
  Pop_growth_J_M <- Pop_growth_JF_M + Pop_growth_JM_M
  
  Total_number_change_N_M <- Num_Offtake_N_M + Pop_growth_N_M
  Total_number_change_J_M <- Num_Offtake_J_M + Pop_growth_J_M
  
  Total_Mortality_N_M <- Total_Mortality_NF_M + Total_Mortality_NM_M
  Total_Mortality_J_M <- Total_Mortality_JF_M + Total_Mortality_JM_M

  # Monetary value of mortality
  Value_of_Total_Mortality_N_M <- Value_of_Total_Mortality_NF_M + Value_of_Total_Mortality_NM_M
  Value_of_Total_Mortality_J_M <- Value_of_Total_Mortality_JF_M + Value_of_Total_Mortality_JM_M

  Quant_Liveweight_kg_J_M <- Quant_Liveweight_kg_JF_M + Quant_Liveweight_kg_JM_M
  
  ## >> Meat not included from neonates and juveniles as no off take from these groups
  ## goes towards meat in this system, but have left in code for potential future use
  ## in different production systems
  
  # Quant_Meat_kg_N_M <- Quant_Meat_kg_NF_M + Quant_Meat_kg_NM_M  
  # Quant_Meat_kg_J_M <- Quant_Meat_kg_JF_M + Quant_Meat_kg_JM_M
 
  Quant_Manure_N_M <- Quant_Manure_NF_M + Quant_Manure_NM_M
  Quant_Manure_J_M <- Quant_Manure_JF_M + Quant_Manure_JM_M
  
  # >> Again no hides produced from neonate group but can add in future for other production systems
  # Quant_Hides_N_M <- Quant_Hides_NF_M + Quant_Hides_NM_M   
  Quant_Hides_J_M <- Quant_Hides_JF_M + Quant_Hides_JM_M
  
  #Quant_Milk_J_M <- Quant_Milk_JF_M + Quant_Milk_JM_M    ## > no milk produced by juveniles in this prod.sys.
  
  ## Wool removed here as not included in code yet but can be in future
  #Quant_Wool_N_M <- Quant_Wool_NF_M + Quant_Wool_NM_M
  #Quant_Wool_J_M <- Quant_Wool_JF_M + Quant_Wool_JM_M
  
  Cumulative_Dry_Matter_N_M <- Cumulative_Dry_Matter_NF_M + Cumulative_Dry_Matter_NM_M
  Cumulative_Dry_Matter_J_M <- Cumulative_Dry_Matter_JF_M + Cumulative_Dry_Matter_JM_M
  
  Value_Offtake_N_M <- Value_Offtake_NF_M + Value_Offtake_NM_M
  Value_Offtake_J_M <- Value_Offtake_JF_M + Value_Offtake_JM_M
  
  Value_Herd_Increase_N_M <- Value_Herd_Increase_NF_M + Value_Herd_Increase_NM_M
  Value_Herd_Increase_J_M <- Value_Herd_Increase_JF_M + Value_Herd_Increase_JM_M
  
  Total_Value_increase_N_M <- Total_Value_increase_NF_M + Total_Value_increase_NM_M
  Total_Value_increase_J_M <- Total_Value_increase_JF_M + Total_Value_increase_JM_M
  
  Value_Manure_N_M <- Value_Manure_NF_M + Value_Manure_NM_M
  Value_Manure_J_M <- Value_Manure_JF_M + Value_Manure_JM_M
  
  #Value_Hides_N_M <- Value_Hides_NF_M + Value_Hides_NM_M
  Value_Hides_J_M <- Value_Hides_JF_M + Value_Hides_JM_M
  
  #Value_Milk_N_M <- Value_Milk_NF_M + Value_Milk_NM_M
  #Value_Milk_J_M <- Value_Milk_JF_M + Value_Milk_JM_M
  
  Production_value_herd_offteake_hide_man_N_M <- Production_value_herd_offteake_hide_man_NF_M + Production_value_herd_offteake_hide_man_NM_M
  Production_value_herd_offteake_hide_man_J_M <- Production_value_herd_offteake_hide_man_JF_M + Production_value_herd_offteake_hide_man_JM_M
  
  Feed_cost_N_M <- Feed_cost_NF_M + Feed_cost_NM_M
  Feed_cost_J_M <- Feed_cost_JF_M + Feed_cost_JM_M
  
  Labour_cost_N_M <- Labour_cost_NF_M + Labour_cost_NM_M
  Labour_cost_J_M <- Labour_cost_JF_M + Labour_cost_JM_M
  
  Health_cost_N_M <- Health_cost_NF_M + Health_cost_NM_M
  Health_cost_J_M <- Health_cost_JF_M + Health_cost_JM_M
  
  Capital_cost_N_M <- Capital_cost_NF_M + Capital_cost_NM_M
  Capital_cost_J_M <- Capital_cost_JF_M + Capital_cost_JM_M
  
  Infrastructure_cost_N_M <- Infrastructure_cost_NF_M + Infrastructure_cost_NM_M
  Infrastructure_cost_J_M <- Infrastructure_cost_JF_M + Infrastructure_cost_JM_M
  
  Total_expenditure_N_M <- Total_expenditure_NF_M + Total_expenditure_NM_M
  Total_expenditure_J_M <- Total_expenditure_JF_M + Total_expenditure_JM_M
  
  Gross_margin_N_M <- Gross_margin_NF_M + Gross_margin_NM_M
  Gross_margin_J_M <- Gross_margin_JF_M + Gross_margin_JM_M
  
  # =================================================================
  # Summarize items and build data frame
  # =================================================================
  ## renamed data frame
  summary_df_updated <- build_summary_df(
    # Labeled list of matrices to summarize. Matrix names should be WITHOUT SUFFIXES (without _M, _NF_M, etc.). Labels will be used in output data.
    items_to_summarize = c(
      'Num Offtake' = 'Num_Offtake'
      ,'Cml Pop Growth' = 'Pop_growth'
      ,'Total Number Increase' = 'Total_number_change'
      ,'Total Mortality' = 'Total_Mortality'
		,'Value of Total Mortality' = 'Value_of_Total_Mortality'
      
      ,'Population Liveweight (kg)' = 'Quant_Liveweight_kg'
      ,'Offtake Liveweight (kg)' = 'Offtake_Liveweight_kg'
      ,'Meat (kg)' = 'Quant_Meat_kg'
      ,'Manure' = 'Quant_Manure'
      ,'Hides' = 'Quant_Hides'
      ,'Milk' = 'Quant_Milk'
      ,'Wool' = 'Quant_Wool'
      ,'Cml Dry Matter' = 'Cumulative_Dry_Matter'
      
      ,'Value of Offtake' = 'Value_Offtake'
      ,'Value of Herd Increase' = 'Value_Herd_Increase'
      ,'Value of Herd Increase plus Offtake' = 'Total_Value_increase'
      ,'Value of Manure' = 'Value_Manure'
      ,'Value of Hides' = 'Value_Hides'
      ,'Value of Milk' = 'Value_Milk'
      ,'Total Production Value' = 'Production_value_herd_offteake_hide_man'
      
      ,'Feed Cost' = 'Feed_cost'
      ,'Labour Cost' = 'Labour_cost'
      ,'Health Cost' = 'Health_cost'
      ,'Capital Cost' = 'Capital_cost'
      ,'Infrastructure Cost' = 'Infrastructure_cost'
      ,'Total Expenditure' = 'Total_expenditure'
      
      ,'Gross Margin' = 'Gross_margin'
    )
  )
  print('Compartmental model finished.')
  return(list(Gross_margin_M[,12] ,summary_df_updated))
}


# =================================================================
# Run scenarios
# =================================================================

# Read in the scenarios control table - pathname was defined at start of script wherever you have the scenario spreadsheet stored
ahle_scenarios <- read_excel(cmd_scenario_file ,'Sheet1')

# Drop rows where parameter name is empty or commented
ahle_scenarios <- ahle_scenarios[!is.na(ahle_scenarios$'AHLE Parameter') ,]
ahle_scenarios <- ahle_scenarios[!grepl('#', ahle_scenarios$'AHLE Parameter') ,] 	# Will drop all rows whose parameter name contains a pound # sign

# Create version of scenario spreadsheet with just scenario columns
remove_cols <- c('AHLE Parameter', 'Notes') # this is the list of columns you want to remove
ahle_scenarios_cln <- subset(ahle_scenarios, select = !(names(ahle_scenarios) %in% remove_cols)) # this creates the clean dataset to use

# Loop through all scenario columns, calling the compartmental model function for each
# and storing all of the outputs in the cmd_output_directory defined at start of script.

for (COLNAME in colnames(ahle_scenarios_cln)){
  print('> Running AHLE scenario:')
  print(COLNAME)
  
  # Construct function arguments as "name = value"
  ahle_scenarios_cln$arglist <- do.call(paste, c(ahle_scenarios[c("AHLE Parameter", COLNAME)], sep="="))
  
  # Get as single string and append cmd arguments
  argstring <- toString(ahle_scenarios_cln$arglist)
  argstring_touse <- paste('nruns=' ,cmd_nruns ,',' ,argstring ,sep='')
  
  # Construct function call
  functioncall_string <- paste('compartmental_model(' ,argstring_touse ,')' ,sep='')
  print('> Function call:')
  print(functioncall_string)
  
  # Call function and save result to file
  result <- eval(parse(text=functioncall_string))
  filename <- paste('ahle_' ,COLNAME ,'.csv' ,sep='')
  write.csv(result[[2]], file.path(cmd_output_directory, filename), row.names=FALSE)
}

### THE END ###

