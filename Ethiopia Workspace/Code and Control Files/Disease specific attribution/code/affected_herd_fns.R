# Affected herd functions code ====
## These functions are used to create affected herd scenarios for the population model
## Originally written by Andrew Larkins (Murdoch University)

# Load required packages ====
lapply( c("tidyverse", "readxl", "freedom", "truncnorm"), require, character.only = T)

# Functions ====
import_sebi_data <- function(SEBI_file="input/sebi.xlsx", SEBI_species, SEBI_cause, SEBI_test, SEBI_n=0, SEBI_outcome="Individual Prevalence"){
  raw_data <- read_xlsx(SEBI_file, sheet="STUDY_RESULTS", 
                         col_types = c(rep("text",12), rep("numeric",3), "text"))
  filter_data <- subset(raw_data, SPECIES %in% SEBI_species & DISEASE==SEBI_cause & DIAGNOSTIC_TEST %in% SEBI_test & NUMBER_TESTED >= SEBI_n & OUTCOME==SEBI_outcome) %>%
    mutate(NUMBER_POSITIVE = ifelse(is.na(NUMBER_POSITIVE) & NUMBER_TESTED>0 & PERCENTAGE>0,
                                    round(NUMBER_TESTED*PERCENTAGE/100, 0), NUMBER_POSITIVE)) %>%
    distinct(STATE, SPECIES, DISEASE, SAMPLE, DIAGNOSTIC_TEST, OUTCOME, NUMBER_POSITIVE, NUMBER_TESTED, PERCENTAGE, .keep_all = TRUE) %>%
    subset(., !is.na(NUMBER_POSITIVE) & !is.na(NUMBER_TESTED))
  JAGS_data <- list("pos"=as.integer(filter_data$NUMBER_POSITIVE),
                    "n"=as.integer(filter_data$NUMBER_TESTED),
                    "Nstudies"=nrow(filter_data))
    }

run_model <- function(input_file= "input/frequency_inputs.xlsx", freq_col, sheet_number=1, data, inits = NULL, n.iter=6000, n.burnin=1000, n.chains=2, n.thin=1) {
  # Call input parameters from inputs file
  freqency_parameter <- read_excel(input_file, sheet=1)
  parameter_list <- do.call(paste0, c(freqency_parameter["Parameter"],"=",freqency_parameter[freq_col]))
  eval(parse(text=parameter_list))
  
  model <- paste0(
    "model{
  #=== LIKELIHOOD	===#
  for (i in 1:Nstudies) {
  pos[i] ~ dbin(AP[i], n[i])  # Positive from a distribution of apparent prevalence and number tested
  AP[i] = ((1-V)*P[i]/IAff)*Se + (1-(1-V)*P[i]/IAff)*(1-Sp)   # Correct AP for test performance, vaccination, and probability affected

  logit(P[i]) = logit.P[i]                    
  logit.P[i] ~ dnorm(logit.CP, tau.CP) # Study prevalence is a normal distribution with a mean of the true population CP and its precision
  }
  
  #=== PRIORS ===#
  Se ~ dunif(", lowerSe, ", ", upperSe, ")
  Sp ~ dunif(", lowerSp, ", ", upperSp, ")
  IAff ~ dunif(", lowerIAff, ", ", upperIAff, ")
  V ~ dunif(", lowerV, ", ", upperV, ")
  tau.CP = sqrt(1/precision.CP)
  precision.CP ~ dgamma(1,1)
  logit(CP) = logit.CP
  logit.CP ~ dnorm(0,0.001)
  }")
  
  name <- paste0("models/",format(Sys.time(), "%Y%m%d"),"_",freq_col,"_model.txt")
  txt_file <- write.table(model, 
                          name,
                          quote=FALSE, 
                          sep="", 
                          row.names=FALSE,
                          col.names=FALSE)
  
  ## 5 - Run model
  library(R2jags)
  jags.out <- jags.parallel(data=data,                             
                   model.file=name,     
                   parameters.to.save="CP",               
                   n.chains=n.chains,                                 
                   inits=inits,                                
                   n.iter=n.iter,                                
                   n.burnin=n.burnin,
                   n.thin=n.thin)
  
  library(coda)
  jags.mcmc <- as.mcmc(jags.out)
  
  ess <- round(effectiveSize(jags.mcmc[,'CP']),0)
  ess_ratio <- ess/(2*(n.iter-n.burnin))
  ess_print <- paste("Effective sample size from CODA package =", ess,
                     "or a ratio of", ess_ratio)
  gelman <- gelman.diag(jags.mcmc[,"CP"])
  
  mcmc_plots <- list(
    gelman = gelman.plot(jags.mcmc[,"CP"], main = paste0("Gelman-Ruban shrink factor plot: ",freq_col), autoburnin = FALSE),
                     ACF = plot(acfplot(jags.mcmc[,"CP"], main = paste0("Autocorrelation plot: ",freq_col))),
                     TraceDensity = plot(jags.mcmc[,"CP"], main = paste0("Trace & Density plot: ",freq_col))
  )
  
  descr <- c(jags.out, gelman, ess_print)
  
  CP <- c(jags.mcmc[,"CP"][[1]],jags.mcmc[,"CP"][[2]])
  
  list(mcmc_plots=mcmc_plots, descr=descr, CP=CP)
}

age_stratify <- function(model_object, input_file= "input/frequency_inputs.xlsx", age_col, sheet_number=2) {
  age_parameter <- read_excel(input_file, sheet=sheet_number)
  parameter_list <- do.call(paste0, c(age_parameter["Parameter"],"=",age_parameter[age_col]))
  eval(parse(text=parameter_list))
  
  CP <- model_object[["CP"]]
  
  # Recode below section so that na is assessed for each age group rather than just oxen. If na then replace with 0 so that doesn't cause error when combining ideal and diseased parameters
  if(is.na(ageO)==TRUE){
  SA <- CP*pSA/(pSA+pAF+pAM)/ageSA
  AF <- CP*pAF/(pSA+pAF+pAM)/ageAF
  AM <- CP*pAM/(pSA+pAF+pAM)/ageAM
  J <- SA #used to be CP*pJ/ageJ but this gave very high results and prevalence studies usually don't include neonates/juveniles
  Ilist <- list(J=J, SA=SA, AF=AF, AM=AM)
  plots <- list(plot(density(J), main = paste0(age_col, " Incidence density plot: Juvenile")),
                plot(density(SA), main = paste0(age_col, " Incidence density plot: Sub-adult")),
                plot(density(AF), main = paste0(age_col, " Incidence density plot: Adult female")),
                plot(density(AM), main = paste0(age_col, " Incidence density plot: Adult male")))
  }
  
  if(is.na(ageO)==FALSE){
    SA <- CP*pSA/ageSA
    AF <- CP*pAF/ageAF
    AM <- CP*pAM/ageAM
    O <- CP*pO/ageO
    J <- SA #used to be CP*pJ/ageJ but this gave very high results and prevalence studies usually don't include neonates/juveniles
    Ilist <- list(J=J, SA=SA, AF=AF, AM=AM, O=O)
    plots <- list(plot(density(J), main = paste0(age_col, " Incidence density plot: Juvenile")),
                  plot(density(SA), main = paste0(age_col, " Incidence density plot: Sub-adult")),
                  plot(density(AF), main = paste0(age_col, " Incidence density plot: Adult female")),
                  plot(density(AM), main = paste0(age_col, " Incidence density plot: Adult male")),
                  plot(density(O), main = paste0(age_col, " Incidence density plot: Oxen")))
  }
  smry <- as.data.frame(Ilist) %>% pivot_longer(cols=everything()) %>% group_by(name) %>% summarise(med=median(value), minimum=min(value),"5th"=quantile(value,0.05), "95th"=quantile(value,0.95), maximum=max(value), .groups="keep")
  list(plots=plots, I=Ilist, summary=smry)
}

SEBI_to_incidence <- function(..., input_file= "input/frequency_inputs.xlsx", freq_col, age_col, n.iter=6000, n.burnin=1000, n.thin=1) {
  data <- import_sebi_data(...)
  CP <- run_model(input_file=input_file, freq_col=freq_col,data=data, n.iter=n.iter, n.burnin=n.burnin, n.thin=n.thin)
  I <- age_stratify(model_object=CP, input_file=input_file, age_col=age_col)
  return(list(SEBI_data=data, model_CP=CP, stratified_I=I))
  }

import_AHLE_parameters <- function(file, column){
    data <- read_xlsx(file, sheet=1) %>% 
    dplyr::select("AHLE Parameter", eval(column)) %>%
    drop_na("AHLE Parameter")
}

convert_to_sample <- function(object, disease) {
  disease_parameters <- do.call(paste0, c(object[disease]))
  names(disease_parameters) <- as.vector(object[[1]])
  as.list(disease_parameters)
  res <- lapply(disease_parameters, function(x){eval(parse(text=x))})
}

combined_production <- function(production_system_code, species_code, cause_code, ahle_scenario_file, impact_file, affected_value, affected_change, distribution_type="pert") {
  # diseased animal production based on diseased value
  minus_value_CP <- lapply(affected_value, function(x){1-x})
  
  impact_code <- paste0(production_system_code, "_", species_code, "_", cause_code)
  impact_value <- read_xlsx(impact_file, sheet=1, na="NA") %>% dplyr::select(., DiseaseParameter, !! sym(impact_code)) %>% filter(!is.na(!! sym(impact_code)))
  impact_value_samples <- convert_to_sample(impact_value, impact_code)
  affected_value_production <- mapply('*', impact_value_samples, affected_value, SIMPLIFY = FALSE)
  
  AHLE_utopia_code <- paste0(production_system_code, "_", species_code, "_Ideal")
  AHLE_utopia_parameters <- import_AHLE_parameters(ahle_scenario_file, AHLE_utopia_code)
  utopia_value_parmeters_to_change <- subset(AHLE_utopia_parameters, `AHLE Parameter` %in% c(impact_value$DiseaseParameter))
  utopia_value_samples <- convert_to_sample(utopia_value_parmeters_to_change, AHLE_utopia_code)
  utopia_production <- mapply('*', utopia_value_samples, minus_value_CP, SIMPLIFY=FALSE)
  
  results1 <- mapply('+', utopia_production, affected_value_production) %>% as.data.frame() %>% as.list()
  
  # diseased animal production based on change in current values 
  minus_change_CP <- lapply(affected_change, function(x){1-x})
  
  impact_change <- read_xlsx(impact_file, sheet=2, na="NA") %>% dplyr::select(., DiseaseParameter, !! sym(impact_code)) %>% filter(!is.na(!! sym(impact_code)))
  impact_change_samples <- convert_to_sample(impact_change, impact_code)
  AHLE_current_code <- paste0(production_system_code, "_", species_code, "_Current")
  AHLE_current_parameters <- import_AHLE_parameters(ahle_scenario_file, AHLE_current_code)
  AHLE_current_parmeters_to_change <- subset(AHLE_current_parameters, `AHLE Parameter` %in% c(impact_change$DiseaseParameter))
  AHLE_current_samples <- convert_to_sample(AHLE_current_parmeters_to_change, AHLE_current_code)
  affected_change_production <- mapply(function(a,b,c){a*b*c}, AHLE_current_samples, impact_change_samples, affected_change, SIMPLIFY = FALSE)
  
  # utopia animal production for unaffected animals using change production method
  AHLE_utopia_parmeters_impact_change <- subset(AHLE_utopia_parameters, `AHLE Parameter` %in% c(impact_change$DiseaseParameter))
  current_value_samples <- convert_to_sample(AHLE_utopia_parmeters_impact_change, AHLE_utopia_code)
  utopia_change_production <- mapply('*', current_value_samples, minus_change_CP, SIMPLIFY = FALSE)
  
  results2 <- mapply('+', utopia_change_production, affected_change_production) %>% as.data.frame() %>% as.list()
  
  results <- c(results1, results2)
  histo <- mapply(function(x, y)hist(x, main=y, breaks=10), x=results, y=paste0(impact_code,": ", names(results)))
  # Add histogram or curves of ideal compared to diseased
  return(list(results=results
              ,plots=histo
              ))
}

# Plot, create, and format fitted distributions for AHLE
gamma_distributions <- function(system_species_cause, samples, distribution){
  a <- lapply(samples$results, function(x)fitdistrplus::fitdist(data=x, distr=distribution))
  b <- mapply(function(x,y)plot(x, title=title(main=paste0(system_species_cause," ", y), line=-1, outer=TRUE)), x=a, y=names(a))
  c <- lapply(a, function(x)data.frame(dist=x$distname, shape=x$estimate[["shape"]], rate=x$estimate[["rate"]])) %>%
    do.call(rbind.data.frame,.) %>% rownames_to_column(., "Parameters")
  d <- data.frame(`AHLE Parameter` = c$Parameters,
                  tmp = paste0("r",c$dist,"(10000,",c$shape,",",c$rate,")"))
  names(d)<- c("AHLE Parameter", system_species_cause)
  return(list(dist=d, samples=a, plots=b))
}

pert_distributions <- function(system_species_cause, samples) {
  # Use mean then convert to mode based on betapert distribution. Using a mode function give variable results as random sample with many modes
    parameters <- lapply(samples$results, function(x){data.frame(min=min(x), mode=((4+2)*mean(x)-min(x)-max(x))/4, max=max(x))}) %>%
      do.call(rbind.data.frame,.) %>%
      rownames_to_column(., "AHLE Parameter")
  out <- data.frame(`AHLE Parameter` = parameters$"AHLE Parameter",
                    tmp = paste0("rpert(10000,",parameters$min,",",parameters$max,",",parameters$mode,")"))
  names(out)<- c("AHLE Parameter", system_species_cause)
  #mode(out) <- "data.frame"
  return(list(dist=out, plots=""))
}
## Some of the min for diseased herds is greater than for ideal herds. Only by 0.1 or similar but need to add an ifelse condition so that ideal is always better than diseased

# Write new disease parameters into an AHLE spreadsheet
update_AHLE_scenarios <- function(AHLE_scenario_file, ideal_col, affected_parameters_object, col_name, new_file_name) {
  require(writexl)
  read <- read_xlsx(AHLE_scenario_file) %>% select(any_of(ends_with(c("AHLE Parameter","Notes","Ideal","Current","PPR","Bruc")))) %>% select(-any_of((!!col_name)))
  join <- left_join(read, affected_parameters_object$dist, by="AHLE Parameter") 
  mute <- mutate(join, !!col_name := ifelse(is.na(!! sym(col_name)), !! sym(ideal_col), !! sym(col_name)))
  write <- write_xlsx(mute, new_file_name)
  detach("package:writexl", unload=TRUE)
    }

# TO DO ====
# Write up summary documentation and user guide
# CHANGE PER FUNCTION TO HAVE IFELSE SO THAT AFFECTED SCENARIO ALWAYS =< IDEAL for production.
## This is hard to code due to format of AHLE cells (e.g. "rpert(10000, 0.52, 1.8, 0.8)" for parturition. Not always rpert. Some will also be single values or other distributions.)
## Can't just truncate as different parameters have different number of decimals. E.g. disease mortality is often 6 decimals compared to milk being 2
## Currently checking and changing these manually.
# Re-write update_AHLE_scenarios so don't have to run them all at once. Want to be able to update one scenario at a time
# Write end-to-end affected herd function to tidy affected_herd.R code
# Add summary of model inputs, diagnostics, and results to SEBI_to_incidence
# Change incidence conversion code so that any age group can be blank and prevalence is only split by the ages that exist
## This is so that we can account for incubation periods
## e.g. juveniles can't have echinococcus included since it takes 1 year for a cyst to grow 1cm or similar
