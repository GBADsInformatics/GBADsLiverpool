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

cmd_nruns <- 1000

# Folder location to save outputs
#cmd_output_directory <- 'F:/First Analytics/Clients/University of Liverpool/GBADs Github/GBADsLiverpool/Ethiopia Workspace/Program outputs'
cmd_output_directory <- '/Users/gemmachaters/Dropbox/Mac/Documents/GitHub/GBADsLiverpool/Ethiopia Workspace/Program outputs/attribution to disease outputs'


# Full path to scenario control file
#cmd_scenario_file <- 'F:/First Analytics/Clients/University of Liverpool/GBADs Github/GBADsLiverpool/Ethiopia Workspace/Code and Control Files/AHLE scenario parameters MAJOR SCENARIOS ONLY.xlsx'
cmd_scenario_file <- '/Users/gemmachaters/Downloads/AHLE scenario parameters_AL.xlsx'

# -----------------------------------------------------------------
# Get from command line arguments
# -----------------------------------------------------------------
# If this was invoked from the command line, look for command arguments
# Will overwrite manual settings above
## ONLY FOR PYTHON ##
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

# =================================================================
# Define functions
# =================================================================
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

compartmental_model <- function(
    nruns 			## Number of iterations (duration of simulation) # representing days 
    ,Num_months 	## NOTE - if you change this you must change rates to be monthly 
    
    # Initial population
    ,N_NF_t0		# Neonatal female
    ,N_NM_t0		# Neonatal male
    ,N_JF_t0		# Juvenile female
    ,N_JM_t0		# Juvenile male
    ,N_AF_t0		# Adult female
    ,N_AM_t0		# Adult male
    
    ,P_NF_t0 # disease prev in pop (normal distribution)
    ,P_NM_t0 # disease prev in pop (normal distribution)
    ,P_JF_t0 # disease prev in pop (normal distribution)
    ,P_JM_t0 # disease prev in pop (normal distribution)
    ,P_AF_t0 # disease prev in pop (normal distribution)
    ,P_AM_t0 # disease prev in pop (normal distribution)
    
    ## Growth rate N -> J and J-> A
    ,Beta
    # Castration rate
    
    # Fertility
    ,part
    ,prolif
    
    # lactation
    ,prop_F_milked
    ,lac_duration #(days)
    ,avg_daily_yield_ltr
    ,milk_value_ltr
    
    # Offtake
    ## Currently fixed, but, should this be dependant on new pop size, to keep pop size as it was at t0
    ## offtake must = offtake + dif between NNFt0 etc and NJF current
    ,GammaF 		# offtake rate female (juv and adult only) 
    ,GammaM 		# offtake rate male
    
    # Mortality ## informed from META analysis
    ,AlphaN		# mortality rate neonate ## parameter derived from mean pooled proportion and variance 
    ,AlphaJ		# mortality rate juvenile ## parameter derived from mean pooled proportion and variance 
    ,AlphaF		# mortality  adult female ##Parameter derived from mean pooled proportion and variance
    ,AlphaM		# motality adult male ##Parameter derived from meat pooled proportion and variancethin the national herd for breeding
    
    # Mortality in diseased population
    ,AlphaN_disease # pert distribution, mortality in diseased population
    ,AlphaJ_disease # pert distribution, mortality in diseased population
    ,AlphaF_disease # pert distribution, mortality in diseased population
    ,AlphaM_disease # pert distribution, mortality in diseased population
    
    # Culls
    ,CullF	 	# cullrate Adult Female ## These will be valueless
    ,CullM		# cullrate Adult Male  ## These will still have a value
    
    ## Production parameters (kg)
    
    # Liveweight conversion (kg) ## Informed from META analysis
    ,lwNF  		# Liveweight Neonate## parameters derived from meta pooled mean and variance 
    ,lwNM  		# Liveweight Neonateparameters derived from meta pooled mean and variance
    ,lwJF 		# Liveweight Juvenille # Same here ##parameters derived from meta pooled mean and variance
    ,lwJM 		# Liveweight Juvenille # Same here##parameters derived from meta pooled mean and variance
    ,lwAF 		# Liveweight Adult # Same here ##parameters derived from meta pooled mean and variance
    ,lwAM 		# Liveweight Adult # Same here ##parameters derived from meta pooled mean and variance
    
    # carcase yeild
    ,ccy 			# As a % of Liveweight for all groups
    
    ## Financial value of live animals
    # Ethiopian Birr
    ,fvNF 		## Financial value of neonatal Female
    ,fvJF 		## Financial value of neonatal Male
    ,fvAF			## Financial value of juv Female
    ,fvNM			## Financial value of juv Male
    ,fvJM			## Financial value of adult Female
    ,fvAM			## Financial value of adult Male  
    
    ## Off take which go for fertility in females (used when calculating hide numbers)
    #,fert_offtake	# for breeding age females only 75% of offtake contribute to skins (25% remain in national breeding herd)
    
    ## skin/hides  
    ## parameters can be updated through expert opinion but adding options for flexibility here
    ,hides_rate			# 1 skin per animal offtake for males
    ,hides_rate_mor	# 50% of dead animals contribute to hides count
    
    # 1 usd per piece = 51 eth birr
    ,hides_value
    
    # manure rate (kg produced/animal/day)
    ,Man_N		# Manure kg/ day from neonates ## means and Sds  are derived from  body wt
    ,Man_J		# Manure kg/ day from juvenile## means and Sds  are derived from  body wt
    ,Man_A		# Manure kg/ day from adults ##means and Sds  are derived from  body wt
    
    # 0.0125 USD / kg = 0.65 eth birr per kg 2021 price
    ,Man_value
    
    ## dry matter requirements as proportion of Liveweight
    ,DM_req_prpn_NF		# Dry matter required by neonates
    ,DM_req_prpn_NM		# Dry matter required by neonates
    ,DM_req_prpn_JF		# Dry matter required by juvenile
    ,DM_req_prpn_JM		# Dry matter required by juvenile
    ,DM_req_prpn_AF		# Dry matter required by adults
    ,DM_req_prpn_AM		# Dry matter required by adults
    
    ## Proportion of livestock keepers that spend any money on feed
    ## NOTE Currently the same for all age*sex groups
    ,prpn_lskeepers_purch_feed
    
    ## For those spending any money on feed, the proportion of feed that is purchased
    ## NOTE Currently the same for all age*sex groups
    ,prpn_feed_paid_for
    
    ## Input parameters ## just example distributions for now
    ,Feed_cost_kg		## Ethiopian birr/kg wheat and barley
    
    ## variable results for the amount of dry matter in wheat and barley and tef in Ethiopia
    ## range 30-90%
    ## taking 70% as an estimate for this trial
    ,DM_in_feed			## change this to choose from data informed distribution
    
    ## Labour cost
    ## birr/head/month
    ## example code to change labour cost to selecting from distribution
    ,Lab_SR
    ,lab_non_health	## 0.86 in ideal this was not used in the current and this may not apply for ideal
    
    ## Helath care costs
    ## birr/head/month
    ## this includes medicines and veterinary care
    ## and changing health care costs to select from distribution
    ,Health_exp			# the two national level estimates(national production and import of vet drugs and vaccines, and LFSDP and RPLRP projects) are used as bound for the price and used for unif distribution 14.3 was from an earlier study covering only two districts 
    
    ## Capital costs
    ## for this we are using bank of Ethiopia inflation rate
    ,Interest_rate
    ,Infrastructure_per_head
)
{
  Mu <- (sample(part, size = 10000, replace = TRUE) * sample(prolif, size = 10000, replace = TRUE)) / 12 # birth rate (parturition rate * prolificacy rate / 12)
  
  ## Off take which go for fertility in females
  #	hides_rate_of = 1 - fert_offtake
  # dont need to adjust for this anymore
  DM_req_prpn_AF <- 0.026
  ## dry matter requirements (measured in kg and calculated as a % of Liveweight)
  kg_DM_req_NF = DM_req_prpn_NF * lwNF  	# Dry matter required by neonates
  kg_DM_req_NM = DM_req_prpn_NM * lwNM  	# Dry matter required by neonates
  kg_DM_req_JF = DM_req_prpn_JF * lwJF  	# Dry matter required by juvenile
  kg_DM_req_JM = DM_req_prpn_JM * lwJM  	# Dry matter required by juvenile
  kg_DM_req_AF = DM_req_prpn_AF * lwAF  	# Dry matter required by adults
  kg_DM_req_AM = DM_req_prpn_AM * lwAM  	# Dry matter required by adults
  
  ## NOTE in the pastoral system this purchased feed will be 0
  
  DM_purch_NF <- (kg_DM_req_NF * prpn_lskeepers_purch_feed * prpn_feed_paid_for) #rpert(10000, 0.1, 1, 0.5))
  DM_purch_NM <- (kg_DM_req_NM * prpn_lskeepers_purch_feed * prpn_feed_paid_for) #rpert(10000, 0.1, 1, 0.5))
  DM_purch_JF <- (kg_DM_req_JF * prpn_lskeepers_purch_feed * prpn_feed_paid_for) #rpert(10000, 0.1, 1, 0.5))
  DM_purch_JM <- (kg_DM_req_JM * prpn_lskeepers_purch_feed * prpn_feed_paid_for) #rpert(10000, 0.1, 1, 0.5))
  DM_purch_AF <- (kg_DM_req_AF * prpn_lskeepers_purch_feed * prpn_feed_paid_for) #rpert(10000, 0.1, 1, 0.5))
  DM_purch_AM <- (kg_DM_req_AM * prpn_lskeepers_purch_feed * prpn_feed_paid_for) #rpert(10000, 0.1, 1, 0.5))
  
  #prpn_lskeepers_purch_feed <- 0.25
  #prpn_feed_paid_for <- rpert(10000, 0, 1, 0.5)
  
  KG_Feed_purchased_NF <- DM_purch_NF / DM_in_feed
  KG_Feed_purchased_NM <- DM_purch_NM / DM_in_feed
  KG_Feed_purchased_JF <- DM_purch_JF / DM_in_feed
  KG_Feed_purchased_JM <- DM_purch_JM / DM_in_feed
  KG_Feed_purchased_AF <- DM_purch_AF / DM_in_feed
  KG_Feed_purchased_AM <- DM_purch_AM / DM_in_feed
  
  ## Expenditure on feed per animal
  Expenditure_on_feed_NF <- KG_Feed_purchased_NF * Feed_cost_kg
  Expenditure_on_feed_NM <- KG_Feed_purchased_NM * Feed_cost_kg
  Expenditure_on_feed_JF <- KG_Feed_purchased_JF * Feed_cost_kg
  Expenditure_on_feed_JM <- KG_Feed_purchased_JM * Feed_cost_kg
  Expenditure_on_feed_AF <- KG_Feed_purchased_AF * Feed_cost_kg
  Expenditure_on_feed_AM <- KG_Feed_purchased_AM * Feed_cost_kg
  
  # --------------------------------------------------------------
  # Create vectors to store the model outputs at each time step
  # --------------------------------------------------------------
  # population
  #	Num_months <- 12
  numNF <- rep(0, Num_months)
  numJF <- rep(0, Num_months)
  numAF <- rep(0, Num_months)
  numNM <- rep(0, Num_months)
  numJM <- rep(0, Num_months)
  numAM <- rep(0, Num_months)
  numN <- rep(0, Num_months)
  
  #births
  births <- rep(0, Num_months)
  
  #growth
  growth_NF <- rep(0, Num_months)
  growth_NM <- rep(0, Num_months)
  growth_JF <- rep(0, Num_months)
  growth_JM <- rep(0, Num_months)
  
  #deaths
  deaths_NF <- rep(0, Num_months)
  deaths_NM <- rep(0, Num_months)
  deaths_JF <- rep(0, Num_months)
  deaths_JM <- rep(0, Num_months)
  deaths_AF <- rep(0, Num_months)
  deaths_AM <- rep(0, Num_months)
  
  #culls
  culls_AF <- rep(0, Num_months)
  culls_AM <- rep(0, Num_months)
  
  Cumulative_culls_AM <- rep(0, Num_months)
  
  # offtake
  offtake_JF <- rep(0, Num_months)
  offtake_JM <- rep(0, Num_months)
  offtake_AF <- rep(0, Num_months)
  offtake_AM <- rep(0, Num_months)
  
  Monthly_mortality <- rep(0, Num_months)
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

  # Production
  
  # Liveweight
  Quant_Liveweight_kg <- rep(0, Num_months)
  
  Quant_Liveweight_kg_NF <- rep(0, Num_months)
  Quant_Liveweight_kg_NM <- rep(0, Num_months)
  Quant_Liveweight_kg_JF <- rep(0, Num_months)
  Quant_Liveweight_kg_JM <- rep(0, Num_months)
  Quant_Liveweight_kg_AF <- rep(0, Num_months)
  Quant_Liveweight_kg_AM <- rep(0, Num_months)
  
  # Meat
  Quant_Meat_kg <- rep(0, Num_months)
  
  # Offtake
  Num_Offtake <- rep(0, Num_months)
  
  ## and for individual age cats
  Num_Offtake_NF <- rep(0, Num_months)
  Num_Offtake_NM <- rep(0, Num_months)
  Num_Offtake_JF <- rep(0, Num_months)
  Num_Offtake_JM <- rep(0, Num_months)
  Num_Offtake_AF <- rep(0, Num_months)
  Num_Offtake_AM <- rep(0, Num_months)
  
  # Offtake Liveweight
  Offtake_Liveweight_kg <- rep(0, Num_months)
  ## and for individual age cats
  Offtake_Liveweight_kg_JF <- rep(0, Num_months)
  Offtake_Liveweight_kg_JM <- rep(0, Num_months)
  Offtake_Liveweight_kg_AF <- rep(0, Num_months)
  Offtake_Liveweight_kg_AM <- rep(0, Num_months)
  
  # Pop growth
  Pop_growth <- rep(0, Num_months)
  
  Pop_growth_NF <- rep(0, Num_months)
  Pop_growth_NM <- rep(0, Num_months)
  Pop_growth_JF <- rep(0, Num_months)
  Pop_growth_JM <- rep(0, Num_months)
  Pop_growth_AF <- rep(0, Num_months)
  Pop_growth_AM <- rep(0, Num_months)
  
  ##
  Monthly_growth_rate <- rep(0, Num_months)
  monthly_pop_growth <- rep(0, Num_months)
  
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
  
  Quant_Milk <- rep(0, Num_months)
  Quant_Wool <- rep(0, Num_months)
  
  ##
  Cumilative_Dry_Matter <- rep(0, Num_months)
  
  Cumilative_Dry_Matter_NF <- rep(0, Num_months)
  Cumilative_Dry_Matter_NM <- rep(0, Num_months)
  Cumilative_Dry_Matter_JF <- rep(0, Num_months)
  Cumilative_Dry_Matter_JM <- rep(0, Num_months)
  Cumilative_Dry_Matter_AF <- rep(0, Num_months)
  Cumilative_Dry_Matter_AM <- rep(0, Num_months)
  
  Monthly_DM <- rep(0, Num_months)
  
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
  
  ## Total value increase herd value increase plus offtake value
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
  
  
  
  # total expenditure
  Total_expenditure <- rep(0, Num_months)
  
  ## Example of making storage output a matrix
  ## Total_expenditure <- rep(0, Num_months)
  
  Total_expenditure_NF <- rep(0, Num_months)
  Total_expenditure_NM <- rep(0, Num_months)
  Total_expenditure_JF <- rep(0, Num_months)
  Total_expenditure_JM <- rep(0, Num_months)
  Total_expenditure_AF <- rep(0, Num_months)
  Total_expenditure_AM <- rep(0, Num_months)
  
  # --------------------------------------------------------------
  # Create matrix to store the model output vectors at each time step
  # --------------------------------------------------------------
  # population
  numNF_M <- matrix(, nrow = nruns, ncol = Num_months)
  numJF_M <- matrix(, nrow = nruns, ncol = Num_months)
  numAF_M <- matrix(, nrow = nruns, ncol = Num_months)
  numNM_M <- matrix(, nrow = nruns, ncol = Num_months)
  numJM_M <- matrix(, nrow = nruns, ncol = Num_months)
  numAM_M <- matrix(, nrow = nruns, ncol = Num_months)
  numN_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Monthly_mortality_M <- matrix(, nrow = nruns, ncol = Num_months)
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

  ###############################################################
  
  # Production
  
  # Liveweight
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
  
  ## and for individual age cats
  Num_Offtake_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Num_Offtake_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Num_Offtake_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Num_Offtake_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Num_Offtake_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Num_Offtake_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## Offtake Liveweight
  # Offtake
  Offtake_Liveweight_kg_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## and for individual age cats
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
  
  ##
  Monthly_growth_rate_M <- matrix(, nrow = nruns, ncol = Num_months)
  monthly_pop_growth_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## Manure
  Quant_Manure_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Manure_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Manure_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Manure_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Manure_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Manure_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Manure_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Value_Manure_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Manure_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Manure_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Manure_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Manure_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Manure_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Manure_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## Havent seperated by age cat as only adults
  Quant_Hides_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Quant_Hides_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Hides_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Hides_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Quant_Hides_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Value_Hides_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Value_Hides_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Hides_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Hides_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Hides_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Quant_Milk_M <- matrix(, nrow = nruns, ncol = Num_months)
  Value_Milk_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Quant_Wool_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ##
  Cumilative_Dry_Matter_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Cumilative_Dry_Matter_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Cumilative_Dry_Matter_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Cumilative_Dry_Matter_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Cumilative_Dry_Matter_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Cumilative_Dry_Matter_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Cumilative_Dry_Matter_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  Monthly_DM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
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
  
  ## all produce values
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
  
    # total expenditure
  Total_expenditure_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  ## Example of making storage output a matrix
  Total_expenditure_NF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_expenditure_NM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_expenditure_JF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_expenditure_JM_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_expenditure_AF_M <- matrix(, nrow = nruns, ncol = Num_months)
  Total_expenditure_AM_M <- matrix(, nrow = nruns, ncol = Num_months)
  
  #nruns <- 10
  ################################################################
  ## FIND START ##
  for (i in c(1:nruns))
  {
    ## Current
    ## Crop-livestock
    ## Sheep
    
    ## Variables
    
    ## Initial population size
    
    # Total population is sum of age*sex segments
    Nt0 <- N_NF_t0 + N_NM_t0 + N_JF_t0 + N_JM_t0 + N_AF_t0 + N_AM_t0
    
    # Define population variables and set initial values from function arguments 
    # define diseased population at start of simulation based on prevalences
    NF_dz <- N_NF_t0 * sample(P_NF_t0, 1)
    NM_dz <- N_NM_t0 * sample(P_NM_t0, 1)
    JF_dz <- N_JF_t0 * sample(P_JF_t0, 1)
    JM_dz <- N_JM_t0 * sample(P_JM_t0, 1)
    AF_dz <- N_AF_t0 * sample(P_AF_t0, 1)
    AM_dz <- N_AM_t0 * sample(P_AM_t0, 1)
    N_dz = NF_dz + NM_dz + JF_dz + JM_dz + AF_dz + AM_dz
    
    # define non-diseased population as pop at t0 - diseased
    N <- Nt0 - N_dz
   
    NF <- N_NF_t0 - NF_dz
    NM <- N_NM_t0 - NM_dz
    JF <- N_JF_t0 - JF_dz
    JM <- N_JM_t0 - JM_dz
    AF <- N_AF_t0 - AF_dz
    AM <- N_AM_t0 - AM_dz
    
    
    ## age sex group prop of pop at t0 - this ratio should probably stay the same
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
    
    ## NOTE these MUST always be reset to 0 before running the model 
    ## otherwise the starting values will be the same as they were at 
    ## the end of the last model run
    
    # Production Values (Value at t0)
    Liveweight_kg <- 0
    
    Liveweight_kg_NF <- 0
    Liveweight_kg_NM <- 0
    Liveweight_kg_JF <- 0
    Liveweight_kg_JM <- 0
    Liveweight_kg_AF <- 0
    Liveweight_kg_AM <- 0
    ##
    
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
    
    prop_F_milked <- 0
    lac_duration <- 0
    avg_daily_yield_ltr <- 0
    milk_value_ltr <- 0
    #
    Cumulitive_DM <- 0
    
    Cumulitive_DM_NF <- 0
    Cumulitive_DM_NM <- 0
    Cumulitive_DM_JF <- 0
    Cumulitive_DM_JM <- 0
    Cumulitive_DM_AF <- 0
    Cumulitive_DM_AM <- 0
    
    
    #
    Monthly_Dry_Matter <- 0
    
    popultation_growth_rate <- 0
    Monthly_growth_rate <- 0
    monthly_pop_growth <- 0
    
    ##
    Value_offt <- 0
    
    Value_offt_NF <- 0 
    Value_offt_NM <- 0 
    Value_offt_JF <- 0 
    Value_offt_JM <- 0 
    Value_offt_AF <- 0 
    Value_offt_AM <- 0 
    
    ##
    Value_herd_inc <- 0
    
    Value_herd_inc_NF <- 0
    Value_herd_inc_NM <- 0
    Value_herd_inc_JF <- 0
    Value_herd_inc_JM <- 0
    Value_herd_inc_AF <- 0
    Value_herd_inc_AM <- 0
    
    
    # Input cost values (Value at t0)
    Feed <- 0
    
    Feed_NF <- 0
    Feed_NM <- 0
    Feed_JF <- 0
    Feed_JM <- 0
    Feed_AF <- 0
    Feed_AM <- 0
    ##
    Labour <- 0
    
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
    
    Capital_NF <- 0
    Capital_NM <- 0
    Capital_JF <- 0
    Capital_JM <- 0
    Capital_AF <- 0
    Capital_AM <- 0
    
    ############################################################
    #############     Simulation model            ##############
    
    ## Gemma editing code here to make individual based sampling rather than a population mean
    
    ####
    
    ## 
    for(month in c(1:Num_months))
    {
      births[month] <- sample(Mu, 1) * AF
      
      deaths_NF[month] <- (sample(AlphaN, 1) * NF) + (sample(AlphaN_disease, 1) * NF_dz)
      deaths_JF[month] <- (sample(AlphaJ, 1) * JF) + (sample(AlphaJ_disease, 1) * JF_dz)
      deaths_AF[month] <- (sample(AlphaF, 1) * AF) + (sample(AlphaF_disease, 1) * AF_dz)
      
      deaths_NM[month] <- (sample(AlphaN, 1) * NM) + (sample(AlphaN_disease, 1) * NM_dz)
      deaths_JM[month] <- (sample(AlphaJ, 1) * JM) + (sample(AlphaJ_disease, 1) * JM_dz)
      deaths_AM[month] <- (sample(AlphaM, 1) * AM) + (sample(AlphaM_disease, 1) * AM_dz)
      
      offtake_JF[month] <- (sample(GammaF, 1) * JF)
      offtake_AF[month] <- (sample(GammaF, 1) * AF)
      offtake_JM[month] <- (sample(GammaM, 1) * JM)
      offtake_AM[month] <- (sample(GammaM, 1) * AM)
      
      growth_NF[month] <- (sample(Beta, 1) * NF)
      growth_JF[month] <- (sample(Beta, 1) * JF)
      growth_NM[month] <- (sample(Beta, 1) * NM)
      growth_JM[month] <- (sample(Beta, 1) * JM)
      
      culls_AF[month] <- (sample(CullF, 1) * AF)
      culls_AM[month] <- (sample(CullM, 1) * AM)
      
      # now the population model uses numbers calculated in stochastic equations above
      numNF[month] = NF + NF_dz + (births[month] * 0.5) - deaths_NF[month] - growth_NF[month]
      numJF[month] = JF + JF_dz + growth_NF[month] - growth_JF[month] - offtake_JF[month] - deaths_JF[month]
      numAF[month] = AF + AF_dz + growth_JF[month] - offtake_AF[month] - deaths_AF[month] - culls_AF[month]
      
      numNM[month] = NM + AM_dz + (births[month] * 0.5) - growth_NM[month] - deaths_NM[month]
      numJM[month] = JM + JM_dz + growth_NM[month] - growth_JM[month] - offtake_JM[month] - deaths_JM[month]
      numAM[month] = AM + AM_dz + growth_JM[month] - offtake_AM[month] - deaths_AM[month] - culls_AM[month]
      
      numN[month] = numNF[month] + numJF[month] + numAF[month] + numNM[month] + numJM[month] + numAM[month]
      
      # population
      NF_dz = numNF[month] * sample(pNF_t0, 1)
      JF_dz = numJF[month] * sample(pJF_t0, 1)
      AF_dz = numAF[month] * sample(pAF_t0, 1)
      NM_dz = numNM[month] * sample(pNM_t0, 1)
      JM_dz = numJM[month] * sample(pJM_t0, 1)
      AM_dz = numAM[month] * sample(pAM_t0, 1)
      
      N_dz = NF_dz + NM_dz + JF_dz + JM_dz + AF_dz + AM_dz
      
      NF = numNF[month] - NF_dz
      JF = numJF[month] - JF_dz
      AF = numAF[month] - AF_dz
      NM = numNM[month] - NM_dz
      JM = numJM[month] - JM_dz
      AM = numAM[month] - AM_dz
      N = numN[month] - N_dz
      
      
      ## total mortality
      
      ## age group deaths (cumilative within age groups then sum for total cumilative)
      Total_Mortality_NF[month] = Num_dead_NF + deaths_NF[month]
      Num_dead_NF = Total_Mortality_NF[month]
      Total_Mortality_NM[month] = Num_dead_NM + deaths_NM[month]
      Num_dead_NM = Total_Mortality_NM[month]
      Total_Mortality_JF[month] = Num_dead_JF + deaths_JF[month]
      Num_dead_JF = Total_Mortality_JF[month]
      Total_Mortality_JM[month] = Num_dead_JM + deaths_JM[month]
      Num_dead_JM = Total_Mortality_JM[month]
      Total_Mortality_AF[month] = Num_dead_AF + deaths_AF[month]
      Num_dead_AF = Total_Mortality_AF[month]
      Total_Mortality_AM[month] = Num_dead_AM + deaths_AM[month]
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

      ## Note, this model is stochastic so the whole N population is different from the individual age groups
      ## to make the total N sum to the same as the other age groups it should be calculated differently
      ## A sum of age groups rather than another simulation
      
      ## This calculates monthly mortality rate so it can be evaluated
      # redundant in model function
      #Monthly_mortality[month] = Total_Mortality[month] / numN[month]
      
      # Population growth (total population in month - original population size)
      Pop_growth[month] =  numN[month] - Nt0
      
      Pop_growth_NF[month] =  numNF[month] - N_NF_t0
      Pop_growth_NM[month] =  numNM[month] - N_NM_t0
      Pop_growth_JF[month] =  numJF[month] - N_JF_t0
      Pop_growth_JM[month] =  numJM[month] - N_JM_t0
      Pop_growth_AF[month] =  numAF[month] - N_AF_t0
      Pop_growth_AM[month] =  numAM[month] - N_AM_t0
      
      # whole population Liveweight (number in each age sex group * Liveweight conversion factor, for each month - NOT cumilative)
      # note there is currently no difference in weights of adult animals
      ## and Liveweight should not be cumulative
      
    ##  >> To here..... 
      
      Quant_Liveweight_kg_NF[month] = (numNF[month] * sample(lwNF, 1))
      Quant_Liveweight_kg_NM[month] = (numNM[month] * sample(lwNM, 1))
      Quant_Liveweight_kg_JF[month] = (numJF[month] * sample(lwJF, 1))
      Quant_Liveweight_kg_JM[month] = (numJM[month] * sample(lwJM, 1))
      Quant_Liveweight_kg_AF[month] = (numAF[month] * sample(lwAF, 1))
      Quant_Liveweight_kg_AM[month] = (numAM[month] * sample(lwAM, 1))
      
      Quant_Liveweight_kg[month] = Quant_Liveweight_kg_NF[month] + Quant_Liveweight_kg_NM[month] +
        Quant_Liveweight_kg_JF[month] + Quant_Liveweight_kg_JM[month] + 
        Quant_Liveweight_kg_AF[month] + Quant_Liveweight_kg_AM[month]
      
      # Offtake (all offtake added + culled adult males)
      
      ## offtake from different age cats (offtake... is from original calculation, Offtake... is cum.sum, Num_Offtake is monthly cumilative)
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
      
      ## Offtake Liveweight
      Offtake_Liveweight_kg_JF[month] = (sample(lwJF, 1) * Offtake_JF)
      Offtake_Liveweight_kg_JM[month] = (sample(lwJM, 1) * Offtake_JM)
      Offtake_Liveweight_kg_AF[month] = (sample(lwAF, 1) * Offtake_AF)
      Offtake_Liveweight_kg_AM[month] = (sample(lwAM, 1) * Offtake_AM)
      
      Offtake_Liveweight_kg[month] <- Offtake_Liveweight_kg_JF[month] + Offtake_Liveweight_kg_JM[month] +
        Offtake_Liveweight_kg_AF[month] + Offtake_Liveweight_kg_AM[month]
      
      # Changed here, made meat from offtake only
      # whole population as meat
      
      Quant_Meat_kg[month] = Meat_kg + (Offtake_Liveweight_kg_JF[month] * ccy) + 
        (Offtake_Liveweight_kg_JM[month] * ccy) +
        (Offtake_Liveweight_kg_AF[month] * ccy) + 
        (Offtake_Liveweight_kg_AM[month] * ccy) 
      
      ## 
      Meat_kg = Quant_Meat_kg[month]
      
      ## to here... tidying code!
      
      # Hides per month (only calculated on offftake as a proportion (1-prop females for fertility), we could add a proportion of dead too? * Expert opinion question)
      
      # Quantity of hides in the dif age sex groups
      Quant_Hides_JF[month] = Hides_JF + (deaths_JF[month] * hides_rate_mor)
      Quant_Hides_JM[month] = Hides_JM + (deaths_JM[month] * hides_rate_mor)
      Quant_Hides_AF[month] = Hides_AF + (deaths_AF[month] * hides_rate_mor)
      Quant_Hides_AM[month] = Hides_AM + (deaths_AM[month] * hides_rate_mor) 
      
      Hides_JF = Quant_Hides_JF[month]
      Hides_JM = Quant_Hides_JM[month]
      Hides_AF = Quant_Hides_AF[month]
      Hides_AM = Quant_Hides_AM[month]
      
      # sum for total population
      Quant_Hides[month] = Quant_Hides_JF[month] + Quant_Hides_JM[month] + 	Quant_Hides_AF[month] + Quant_Hides_AM[month]
      
      Hides = Quant_Hides[month]
      
      ## Milk
      # number of females giving birth in month x, multiplied by number that would be milked
      ## multiplied by lactation duration and daily yield)
      Quant_Milk[month] = Milk + (numAF[month] * (sample(part, 1)) * prop_F_milked * lac_duration * avg_daily_yield_ltr) 
      
      Milk = Quant_Milk[month]
      
      ## Manure 
      ## manure from different age cats
      
      Quant_Manure_NF[month] = Manure_kg_NF + (numNF[month] * (sample(Man_N, 1) * 30))  
      Quant_Manure_NM[month] = Manure_kg_NM + (numNM[month] * (sample(Man_N, 1) * 30))  
      Quant_Manure_JF[month] = Manure_kg_JF + (numJF[month] * (sample(Man_J, 1) * 30))  
      Quant_Manure_JM[month] = Manure_kg_JM + (numJM[month] * (sample(Man_J, 1) * 30))  
      Quant_Manure_AF[month] = Manure_kg_AF + (numAF[month] * (sample(Man_A, 1) * 30))  
      Quant_Manure_AM[month] = Manure_kg_AM + (numAM[month] * (sample(Man_A, 1) * 30))  
      
      Manure_kg_NF = Quant_Manure_NF[month]
      Manure_kg_NM = Quant_Manure_NM[month]
      Manure_kg_JF = Quant_Manure_JF[month]
      Manure_kg_JM = Quant_Manure_JM[month]
      Manure_kg_AF = Quant_Manure_AF[month]
      Manure_kg_AM = Quant_Manure_AM[month]
      
      Quant_Manure[month] = Quant_Manure_NF[month] + Quant_Manure_NM[month] + Quant_Manure_JF[month] + Quant_Manure_JM[month] + Quant_Manure_AF[month] + Quant_Manure_AM[month]
      
      Manure_kg = Quant_Manure[month]
      
      # Cumilative dry matter used by the system
      ## NOTE does this need to be multiplied by 30 to get a monthly dry matter requirement?
      
      Cumilative_Dry_Matter_NF[month] = Cumulitive_DM_NF + (numNF[month] * (sample(kg_DM_req_NF, 1) * 30)) 
      Cumilative_Dry_Matter_NM[month] = Cumulitive_DM_NM + (numNM[month] * (sample(kg_DM_req_NF, 1) * 30)) 
      Cumilative_Dry_Matter_JF[month] = Cumulitive_DM_JF + (numJF[month] * (sample(kg_DM_req_NF, 1) * 30)) 
      Cumilative_Dry_Matter_JM[month] = Cumulitive_DM_JM + (numJM[month] * (sample(kg_DM_req_NF, 1) * 30)) 
      Cumilative_Dry_Matter_AF[month] = Cumulitive_DM_AF + (numAF[month] * (sample(kg_DM_req_NF, 1) * 30)) 
      Cumilative_Dry_Matter_AM[month] = Cumulitive_DM_AM + (numAM[month] * (sample(kg_DM_req_NF, 1) * 30)) 
      
      Cumulitive_DM_NF = Cumilative_Dry_Matter_NF[month]
      Cumulitive_DM_NM = Cumilative_Dry_Matter_NM[month]
      Cumulitive_DM_JF = Cumilative_Dry_Matter_JF[month]
      Cumulitive_DM_JM = Cumilative_Dry_Matter_JM[month]
      Cumulitive_DM_AF = Cumilative_Dry_Matter_AF[month]
      Cumulitive_DM_AM = Cumilative_Dry_Matter_AM[month]
      
      ## Total population
      Cumilative_Dry_Matter[month] = Cumilative_Dry_Matter_NF[month] + Cumilative_Dry_Matter_NM[month] +
        Cumilative_Dry_Matter_JF[month] + Cumilative_Dry_Matter_JM[month] +
        Cumilative_Dry_Matter_AF[month] + Cumilative_Dry_Matter_AM[month]
      
      Cumulitive_DM = Cumilative_Dry_Matter[month]
      
      ## Production values (make sure all the values that update each month are updated here) 
      ## this is so each month the values are new and can be added to
      # deleted what was here as no longer stores output to examine pop growth rate
      
      # financial value of offtake (all offtake and culled males * population sizes * financial value)
      
      ## with the new equation structure below if offtake changes or varies or we add uncertainty then the financial value of offtake will change
      
      ## Juv and adults only
      Value_Offtake_JF[month] = (sample(fvJF, 1) * Offtake_JF) 
      Value_offt_JF = Value_Offtake_JF[month]
      
      Value_Offtake_JM[month] = (sample(fvJM, 1) * Offtake_JM)
      Value_offt_JM = Value_Offtake_JM[month]
      
      Value_Offtake_AF[month] = (sample(fvAF, 1) * Offtake_AF)
      Value_offt_AF = Value_Offtake_AF[month]
      
      Value_Offtake_AM[month] = (sample(fvAM, 1) * Offtake_AM)  
      Value_offt_AM = Value_Offtake_AM[month]
      
      ## sum total population
      Value_Offtake[month] = Value_Offtake_JF[month] + Value_Offtake_JM[month] + Value_Offtake_AF[month] + Value_Offtake_AM[month]
      
      Value_offt = Value_Offtake[month] 
      
      # financial value of heard increase (can only do for months > 1 as doing -1 month calcs)
      
      ## Gemma edits here as this calculation doesnt make sense 
      ## now calculation is change in population since t0 
      ## multiplied by price per head (each month compares to t0)
      ##
      
      ## NOTE ##
      ## This section isn't working now - need to change sampling structure, add if statement or something.
      ## revert to old style of code?
        Value_Herd_Increase_NF[month] = (sample(fvNF, 1) * (numNF[month] - N_NF_t0))
        Value_herd_inc_NF = Value_Herd_Increase_NF[month]
      
        Value_Herd_Increase_NM[month] = (numNM[month] - N_NM_t0) * (sample(fvNM, 1))
        Value_herd_inc_NM = Value_Herd_Increase_NM[month]
      
        Value_Herd_Increase_JF[month] = (numJF[month] - N_JF_t0) * (sample(fvJF, 1))
        Value_herd_inc_JF = Value_Herd_Increase_JF[month]
      
        Value_Herd_Increase_JM[month] = (numJM[month] - N_JM_t0) * (sample(fvJM, 1))
        Value_herd_inc_JM = Value_Herd_Increase_JM[month]
     
        Value_Herd_Increase_AF[month] = (numAF[month] - N_AF_t0) * (sample(fvAF, 1))
        Value_herd_inc_AF = Value_Herd_Increase_AF[month]
      
        Value_Herd_Increase_AM[month] = (numAM[month] - N_AM_t0) * (sample(fvAM, 1))
        Value_herd_inc_AM = Value_Herd_Increase_AM[month]
      
      # total pop value of herd increase
      Value_Herd_Increase[month] = Value_Herd_Increase_NF[month] + Value_Herd_Increase_NM[month] + Value_Herd_Increase_JF[month] + Value_Herd_Increase_JM[month] + Value_Herd_Increase_AF[month] + Value_Herd_Increase_AM[month]
      
      Value_herd_inc = Value_Herd_Increase[month]
      
      
      ## Total value increase
      Total_Value_increase[month] = Value_herd_inc + Value_offt
      
      Total_Value_increase_NF[month] = Value_herd_inc_NF 
      Total_Value_increase_NM[month] = Value_herd_inc_NM 
      Total_Value_increase_JF[month] = Value_herd_inc_JF + Value_offt_JF
      Total_Value_increase_JM[month] = Value_herd_inc_JM + Value_offt_JM
      Total_Value_increase_AF[month] = Value_herd_inc_AF + Value_offt_AF
      Total_Value_increase_AM[month] = Value_herd_inc_AM + Value_offt_AM
      
      ## Expenditure in system
      # Feed cost
      Feed_cost_NF[month] = Feed_NF + (numNF[month] * (sample(Expenditure_on_feed_NF, 1)) * 30) 
      Feed_NF = Feed_cost_NF[month]
      Feed_cost_NM[month] = Feed_NM + (numNM[month] * (sample(Expenditure_on_feed_NM, 1)) * 30) 
      Feed_NM = Feed_cost_NM[month]
      Feed_cost_JF[month] = Feed_JF + (numJF[month] * (sample(Expenditure_on_feed_JF, 1)) * 30)
      Feed_JF = Feed_cost_JF[month]
      Feed_cost_JM[month] = Feed_JM + (numJM[month] * (sample(Expenditure_on_feed_JM, 1)) * 30)
      Feed_JM = Feed_cost_JM[month]
      Feed_cost_AF[month] = Feed_AF + (numAF[month] * (sample(Expenditure_on_feed_AF, 1)) * 30)
      Feed_AF = Feed_cost_AF[month]
      Feed_cost_AM[month] = Feed_AM + (numAM[month] * (sample(Expenditure_on_feed_AM, 1)) * 30) 
      Feed_AM = Feed_cost_AM[month]
      
      # total feed cost
      Feed_cost[month] = Feed_cost_NF[month] + Feed_cost_NM[month] + 
        Feed_cost_JF[month] + Feed_cost_JM[month] +
        Feed_cost_AF[month] + Feed_cost_AM[month]
      
      Feed = Feed_cost[month]
      
      # Labour costs (number of SR's * labour cost per head per month)
      
      Labour_cost_NF[month] = Labour_NF + (numNF[month] * (sample(Lab_SR, 1)) * lab_non_health) 
      Labour_cost_NM[month] = Labour_NM + (numNM[month] * (sample(Lab_SR, 1)) * lab_non_health)  
      Labour_cost_JF[month] = Labour_JF + (numJF[month] * (sample(Lab_SR, 1)) * lab_non_health)  
      Labour_cost_JM[month] = Labour_JM + (numJM[month] * (sample(Lab_SR, 1)) * lab_non_health)  
      Labour_cost_AF[month] = Labour_AF + (numAF[month] * (sample(Lab_SR, 1)) * lab_non_health)  
      Labour_cost_AM[month] = Labour_AM + (numAM[month] * (sample(Lab_SR, 1)) * lab_non_health)  
      
      Labour_NF = Labour_cost_NF[month]
      Labour_NM = Labour_cost_NM[month]
      Labour_JF = Labour_cost_JF[month]
      Labour_JM = Labour_cost_JM[month]
      Labour_AF = Labour_cost_AF[month]
      Labour_AM = Labour_cost_AM[month]
      
      Labour_cost[month] = Labour_cost_NF[month] + Labour_cost_NM[month] + Labour_cost_JF[month] + 
        Labour_cost_JM[month] + Labour_cost_AF[month] + Labour_cost_AM[month]
      Labour =  Labour_cost[month]
      
      # Medicines and veterinary expenditure
      
      Health_cost_NF[month] = Health_NF + (numNF[month] * (sample(Health_exp, 1))) 
      Health_cost_NM[month] = Health_NM + (numNM[month] * (sample(Health_exp, 1))) 
      Health_cost_JF[month] = Health_JF + (numJF[month] * (sample(Health_exp, 1))) 
      Health_cost_JM[month] = Health_JM + (numJM[month] * (sample(Health_exp, 1)))
      Health_cost_AF[month] = Health_AF + (numAF[month] * (sample(Health_exp, 1))) 
      Health_cost_AM[month] = Health_AM + (numAM[month] * (sample(Health_exp, 1))) 
      
      Health_NF = Health_cost_NF[month]
      Health_NM = Health_cost_NM[month]
      Health_JF = Health_cost_JF[month]
      Health_JM = Health_cost_JM[month]
      Health_AF = Health_cost_AF[month]
      Health_AM = Health_cost_AM[month]
      
      Health_cost[month] = Health_cost_NF[month] + Health_cost_NM[month] + Health_cost_JF[month] + Health_cost_JM[month] + Health_cost_AF[month] + Health_cost_AM[month]
      Health = Health_cost[month]
      
      # Capital costs
      
      Capital_cost_NF[month] = numNF[1] * (sample(fvNF, 1)) * Interest_rate 
      Capital_NF = Capital_cost_NF[month]
      
      Capital_cost_NM[month] = numNM[1] * (sample(fvNM, 1)) * Interest_rate  
      Capital_NM = Capital_cost_NM[month]
      
      Capital_cost_JF[month] = numJF[1] * (sample(fvJF, 1)) * Interest_rate  
      Capital_JF = Capital_cost_JF[month]
      
      Capital_cost_JM[month] = numJM[1] * (sample(fvJM, 1)) * Interest_rate  
      Capital_JM = Capital_cost_JM[month]
      
      Capital_cost_AF[month] = numAF[1] * (sample(fvAF, 1)) * Interest_rate  
      Capital_AF = Capital_cost_AF[month]
      
      Capital_cost_AM[month] = numAM[1] * (sample(fvAM, 1)) * Interest_rate  
      Capital_AM = Capital_cost_AM[month]
      
      # total pop capital cost
      Capital_cost[month] = Capital_cost_NF[month] + Capital_cost_NM[month] + Capital_cost_JF[month] + Capital_cost_JM[month] + Capital_cost_AF[month] + Capital_cost_AM[month]
      
      Capital = Capital_cost[month]
      
      ## Infrastructure cost
      ## simple calculation - number of animals at t0 * baseline annual infrastructure cost per head
      Infrastructure_cost_NF[month] <- N_NF_t0 * (sample(Infrastructure_per_head, 1))
      Infrastructure_cost_NM[month] <- N_NM_t0 * (sample(Infrastructure_per_head, 1))
      Infrastructure_cost_JF[month] <- N_JF_t0 * (sample(Infrastructure_per_head, 1))
      Infrastructure_cost_JM[month] <- N_JM_t0 * (sample(Infrastructure_per_head, 1))
      Infrastructure_cost_AF[month] <- N_AF_t0 * (sample(Infrastructure_per_head, 1))
      Infrastructure_cost_AM[month] <- N_AM_t0 * (sample(Infrastructure_per_head, 1))
      
      Infrastructure_cost[month] <- Infrastructure_cost_NF[month] + Infrastructure_cost_NM[month] + Infrastructure_cost_JF[month] + Infrastructure_cost_JM[month] + Infrastructure_cost_AF[month] + Infrastructure_cost_AM[month]
      
      ##
      Total_expenditure[month] =  Feed + Health + Labour + Capital + Infrastructure_cost[month]
      
      Total_expenditure_NF[month] =  Feed_NF + Health_NF + Labour_NF + Capital_NF + Infrastructure_cost_NF[month]
      Total_expenditure_NM[month] =  Feed_NM + Health_NM + Labour_NM + Capital_NM + Infrastructure_cost_NM[month]
      Total_expenditure_JF[month] =  Feed_JF + Health_JF + Labour_JF + Capital_JF + Infrastructure_cost_JF[month]
      Total_expenditure_JM[month] =  Feed_JM + Health_JM + Labour_JM + Capital_JM + Infrastructure_cost_JM[month]
      Total_expenditure_AF[month] =  Feed_AF + Health_AF + Labour_AF + Capital_AF + Infrastructure_cost_AF[month]
      Total_expenditure_AM[month] =  Feed_AM + Health_AM + Labour_AM + Capital_AM + Infrastructure_cost_AM[month]
    }
    
    ### Fill all of the matrices
    
    # population
    
    numNF_M[i, ] <- numNF
    numJF_M[i, ] <- numJF
    numAF_M[i, ] <- numAF
    numNM_M[i, ] <- numNM
    numJM_M[i, ] <- numJM
    numAM_M[i, ] <- numAM
    numN_M[i, ] <- numN
    
    Monthly_mortality_M[i, ] <- Monthly_mortality
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
    
    ## and for individual age cats
    Num_Offtake_NF_M[i, ] <- Num_Offtake_NF
    Num_Offtake_NM_M[i, ] <- Num_Offtake_NM
    Num_Offtake_JF_M[i, ] <- Num_Offtake_JF
    Num_Offtake_JM_M[i, ] <- Num_Offtake_JM
    Num_Offtake_AF_M[i, ] <- Num_Offtake_AF
    Num_Offtake_AM_M[i, ] <- Num_Offtake_AM
    
    # Liveweight of offtake
    # Offtake
    Offtake_Liveweight_kg_M[i, ] <- Offtake_Liveweight_kg
    
    ## and for individual age cats
    Offtake_Liveweight_kg_JF_M[i, ] <- Offtake_Liveweight_kg_JF
    Offtake_Liveweight_kg_JM_M[i, ] <- Offtake_Liveweight_kg_JM
    Offtake_Liveweight_kg_AF_M[i, ] <- Offtake_Liveweight_kg_AF
    Offtake_Liveweight_kg_AM_M[i, ] <- Offtake_Liveweight_kg_AM
    
    # Pop growth
    Pop_growth_M[i, ] <- Pop_growth
    
    Pop_growth_NF_M[i, ] <- Pop_growth_NF
    Pop_growth_NM_M[i, ] <- Pop_growth_NM
    Pop_growth_JF_M[i, ] <- Pop_growth_JF
    Pop_growth_JM_M[i, ] <- Pop_growth_JM
    Pop_growth_AF_M[i, ] <- Pop_growth_AF
    Pop_growth_AM_M[i, ] <- Pop_growth_AM
    
    ##
    Monthly_growth_rate_M[i, ] <- Monthly_growth_rate
    monthly_pop_growth_M[i, ] <- monthly_pop_growth
    
    ## Manure
    Quant_Manure_M[i, ] <- Quant_Manure
    Quant_Manure_NF_M[i, ] <- Quant_Manure_NF
    Quant_Manure_NM_M[i, ] <- Quant_Manure_NM
    Quant_Manure_JF_M[i, ] <- Quant_Manure_JF
    Quant_Manure_JM_M[i, ] <- Quant_Manure_JM
    Quant_Manure_AF_M[i, ] <- Quant_Manure_AF
    Quant_Manure_AM_M[i, ] <- Quant_Manure_AM
    
    
    ## Havent seperated by age cat as only adults
    Quant_Hides_M[i, ] <- Quant_Hides
    
    Quant_Hides_JF_M[i, ] <- Quant_Hides_JF
    Quant_Hides_JM_M[i, ] <- Quant_Hides_JM
    Quant_Hides_AF_M[i, ] <- Quant_Hides_AF
    Quant_Hides_AM_M[i, ] <- Quant_Hides_AM
    
    
    Quant_Milk_M[i, ] <- Quant_Milk
    Quant_Wool_M[i, ] <- Quant_Wool
    
    ##
    Cumilative_Dry_Matter_M[i, ] <- Cumilative_Dry_Matter
    
    Cumilative_Dry_Matter_NF_M[i, ] <- Cumilative_Dry_Matter_NF
    Cumilative_Dry_Matter_NM_M[i, ] <- Cumilative_Dry_Matter_NM
    Cumilative_Dry_Matter_JF_M[i, ] <- Cumilative_Dry_Matter_JF
    Cumilative_Dry_Matter_JM_M[i, ] <- Cumilative_Dry_Matter_JM
    Cumilative_Dry_Matter_AF_M[i, ] <- Cumilative_Dry_Matter_AF
    Cumilative_Dry_Matter_AM_M[i, ] <- Cumilative_Dry_Matter_AM
    
    
    Monthly_DM_M[i, ] <- Monthly_DM
    
    ## Value of offtake
    Value_Offtake_M[i, ] <- Value_Offtake
    
    Value_Offtake_NF_M[i, ] <- Value_Offtake_NF
    Value_Offtake_NM_M[i, ] <- Value_Offtake_NM
    Value_Offtake_JF_M[i, ] <- Value_Offtake_JF
    Value_Offtake_JM_M[i, ] <- Value_Offtake_JM
    Value_Offtake_AF_M[i, ] <- Value_Offtake_AF
    Value_Offtake_AM_M[i, ] <- Value_Offtake_AM
    
    ###################################
    ## Value increase
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
    
    ## Inputs
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
    
    ## Example of making storage output a matrix
    ## Total_expenditure <- matrix(, nrow = nruns, ncol = Num_months)
    Total_expenditure_NF_M[i, ] <- Total_expenditure_NF
    Total_expenditure_NM_M[i, ] <- Total_expenditure_NM
    Total_expenditure_JF_M[i, ] <- Total_expenditure_JF
    Total_expenditure_JM_M[i, ] <- Total_expenditure_JM
    Total_expenditure_AF_M[i, ] <- Total_expenditure_AF
    Total_expenditure_AM_M[i, ] <- Total_expenditure_AM
    
  }
  
  ## now store outputs from current model
  
  ## change some matrices into values
  ## numbers
  
  Total_number_change_NF_M <- Num_Offtake_NF_M + Pop_growth_NF_M
  mean(Total_number_change_NF_M[,12])
  
  Total_number_change_NM_M <- Num_Offtake_NM_M + Pop_growth_NM_M
  mean (Total_number_change_NM_M[,12])
  
  Total_number_change_JF_M <- Num_Offtake_JF_M + Pop_growth_JF_M
  mean(Total_number_change_JF_M)
  Total_number_change_JM_M<- Num_Offtake_JM_M + Pop_growth_JM_M
  mean(Total_number_change_JM_M)
  
  Total_number_change_AF_M <- Num_Offtake_AF_M + Pop_growth_AF_M
  mean(Total_number_change_AF_M)
  Total_number_change_AM_M <- Num_Offtake_AM_M + Pop_growth_AM_M
  mean(Total_number_change_AM_M)
  
  Total_number_change_M <- Total_number_change_NF_M + Total_number_change_NM_M + Total_number_change_JF_M + Total_number_change_JM_M + Total_number_change_AF_M + Total_number_change_AM_M
  
  ## values
  Value_Hides_M <- Quant_Hides_M * hides_value
  Value_Milk_M <- Quant_Milk_M * milk_value_ltr
  
  Value_Hides_JF_M <- Quant_Hides_JF_M * hides_value
  Value_Hides_JM_M <- Quant_Hides_JM_M * hides_value
  Value_Hides_AF_M <- Quant_Hides_AF_M * hides_value
  Value_Hides_AM_M <- Quant_Hides_AM_M * hides_value
  
  Value_Manure_M <- Quant_Manure_M * Man_value
  Value_Manure_NF_M <- Quant_Manure_NF_M * Man_value
  Value_Manure_NM_M <- Quant_Manure_NM_M * Man_value
  Value_Manure_JF_M <- Quant_Manure_JF_M * Man_value
  Value_Manure_JM_M <- Quant_Manure_JM_M * Man_value
  Value_Manure_AF_M <- Quant_Manure_AF_M * Man_value
  Value_Manure_AM_M <- Quant_Manure_AM_M * Man_value
  
  ## VALUE of herd increase and offtake and produce in ETH BIRR
  Production_value_herd_offteake_hide_man_M <- Total_Value_increase_M + Value_Manure_M + Value_Hides_M + Value_Milk_M
  Production_value_herd_offteake_hide_man_NF_M <- Total_Value_increase_NF_M + Value_Manure_NF_M
  Production_value_herd_offteake_hide_man_NM_M <- Total_Value_increase_NM_M + Value_Manure_NM_M
  Production_value_herd_offteake_hide_man_JF_M <- Total_Value_increase_JF_M + Value_Manure_JF_M + Value_Hides_JF_M
  Production_value_herd_offteake_hide_man_JM_M <- Total_Value_increase_JM_M + Value_Manure_JM_M + Value_Hides_JM_M
  Production_value_herd_offteake_hide_man_AF_M <- Total_Value_increase_AF_M + Value_Manure_AF_M + Value_Hides_AF_M + Value_Milk_M
  Production_value_herd_offteake_hide_man_AM_M <- Total_Value_increase_AM_M + Value_Manure_AM_M + Value_Hides_AM_M
  
  ## Gross margin
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
  Num_Offtake_N_M <- Num_Offtake_NF_M + Num_Offtake_NM_M
  Num_Offtake_J_M <- Num_Offtake_JF_M + Num_Offtake_JM_M
  Pop_growth_N_M <- Pop_growth_NF_M + Pop_growth_NM_M
  Pop_growth_J_M <- Pop_growth_JF_M + Pop_growth_JM_M
  ## added Total_number_change
  Total_number_change_N_M <- Num_Offtake_N_M + Pop_growth_N_M
  Total_number_change_J_M <- Num_Offtake_J_M + Pop_growth_J_M
  
  Total_Mortality_N_M <- Total_Mortality_NF_M + Total_Mortality_NM_M
  Total_Mortality_J_M <- Total_Mortality_JF_M + Total_Mortality_JM_M

  # Monetary value of mortality
  Value_of_Total_Mortality_N_M <- Value_of_Total_Mortality_NF_M + Value_of_Total_Mortality_NM_M
  Value_of_Total_Mortality_J_M <- Value_of_Total_Mortality_JF_M + Value_of_Total_Mortality_JM_M

  #Quant_Liveweight_kg_N_M <- Quant_Liveweight_kg_NF_M + Quant_Liveweight_kg_NM_M
  Quant_Liveweight_kg_J_M <- Quant_Liveweight_kg_JF_M + Quant_Liveweight_kg_JM_M
  #Quant_Meat_kg_N_M <- Quant_Meat_kg_NF_M + Quant_Meat_kg_NM_M
  #Quant_Meat_kg_J_M <- Quant_Meat_kg_JF_M + Quant_Meat_kg_JM_M
  Quant_Manure_N_M <- Quant_Manure_NF_M + Quant_Manure_NM_M
  Quant_Manure_J_M <- Quant_Manure_JF_M + Quant_Manure_JM_M
  #Quant_Hides_N_M <- Quant_Hides_NF_M + Quant_Hides_NM_M
  Quant_Hides_J_M <- Quant_Hides_JF_M + Quant_Hides_JM_M
  #Quant_Milk_N_M <- Quant_Milk_NF_M + Quant_Milk_NM_M
  #Quant_Milk_J_M <- Quant_Milk_JF_M + Quant_Milk_JM_M
  #Quant_Wool_N_M <- Quant_Wool_NF_M + Quant_Wool_NM_M
  #Quant_Wool_J_M <- Quant_Wool_JF_M + Quant_Wool_JM_M
  Cumilative_Dry_Matter_N_M <- Cumilative_Dry_Matter_NF_M + Cumilative_Dry_Matter_NM_M
  Cumilative_Dry_Matter_J_M <- Cumilative_Dry_Matter_JF_M + Cumilative_Dry_Matter_JM_M
  
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
  ## renamed dataframe
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
      ,'Cml Dry Matter' = 'Cumilative_Dry_Matter'
      
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
        
        print('base label:')
        print(base_label)
        print('group label:')
        print(group)
        print('matrix to summarize:')
        print(paste(base_matrix, suffix, sep=''))
        
        item_mean <- mean(vector_to_summarize)
        item_sd <- sd(vector_to_summarize)
        item_min <- min(vector_to_summarize)
        item_q1 <- quantile(vector_to_summarize ,0.25)
        item_median <- median(vector_to_summarize)
        item_q3 <- quantile(vector_to_summarize ,0.75)
        item_max <- max(vector_to_summarize)
        
        onerow_df <- data.frame(Item=base_label ,Group=group ,Mean=item_mean ,StDev=item_sd ,Min=item_min ,Q1=item_q1 ,Median=item_median ,Q3=item_q3 ,Max=item_max)
        summary_df_updated <- rbind(summary_df_updated ,onerow_df)
      }
    }
  }
  return(summary_df_updated)
}

# =================================================================
# Run scenarios
# =================================================================
library(readxl)

# Read control table
ahle_scenarios <- read_excel(cmd_scenario_file ,'Sheet1')

# Drop rows where parameter name is empty or commented
ahle_scenarios <- ahle_scenarios[!is.na(ahle_scenarios$'AHLE Parameter') ,]
ahle_scenarios <- ahle_scenarios[!grepl('#', ahle_scenarios$'AHLE Parameter') ,] 	# Will drop all rows whose parameter name contains a pound sign

# Create version with just scenario columns
remove_cols <- c('AHLE Parameter' ,'Notes')
ahle_scenarios_cln <- subset(ahle_scenarios, select = !(names(ahle_scenarios) %in% remove_cols)) 

ahle_scenarios_cln2 <- as.data.frame(cbind(ahle_scenarios_cln$CLM_S_Current,
                            ahle_scenarios_cln$CLM_S_Ideal,
                           ahle_scenarios_cln$CLM_S_Current_PPR))

 colnames(ahle_scenarios_cln2) <- list("CLM_S_Current", "CLM_S_Ideal", "CLM_S_Current_PPR")

# Loop through scenario columns, calling the function for each
for (COLNAME in colnames(ahle_scenarios_cln2)){
  print('> Running AHLE scenario:')
  print(COLNAME)
  
  # Construct function arguments as "name = value"
  ahle_scenarios_cln2$arglist <- do.call(paste, c(ahle_scenarios[c("AHLE Parameter", COLNAME)], sep="="))
  
  # Get as single string and append cmd arguments
  argstring <- toString(ahle_scenarios_cln2$arglist)
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

