# CODE TO RUN POPULATION MODEL FOR CURRENT, IDEAL, AND DISEASED SCENARIOS
# Note: 160,000 runs (10,000 runs x 16 scenarios) takes ~3 hours to run
# Source model and helper functions
source("code/ahle_sr_fns.R")

# Set user, number of runs, scenario file location, and list scenario columns to run
user <- "AL"
n <- 10000
scenario_file <- paste0("scenarios/sr_disease_scenarios.xlsx")
scenario_list <- list(
  "CLM_S_Current", "Past_S_Current", "CLM_G_Current", "Past_G_Current" # Comment (#) out a line if you don't want to run that set of scenarios
  ,"CLM_S_Ideal", "Past_S_Ideal", "CLM_G_Ideal", "Past_G_Ideal"
  , "CLM_S_PPR", "Past_S_PPR", "CLM_G_PPR", "Past_G_PPR"
  , "CLM_S_Bruc", "Past_S_Bruc", "CLM_G_Bruc", "Past_G_Bruc"
  )

# Set seed before running model for reproducible results.
set.seed(123)
# Run model to produce list of results
results_list <- mapply(ahle_sr, file=scenario_file, baseline_scen=scenario_list, nruns=n) %>%
  lapply(., function(x){mutate(x, date = Sys.Date(), time = format(Sys.time(), "%H:%M"), run_by = user, nruns = n, .before = 1)})
# Set names in list of results to scenario column names
names(results_list) <- scenario_list

# Combine list of results into one table
results_long <- map_df(results_list, ~as.data.frame(.x), .id="id") %>%
  relocate(., id, .after=nruns) %>%
  separate_wider_delim(., cols=id, delim="_", names=c("system","species","scenario"))

# Export results and overwrite "ahle_sr.csv" for dashboard
export_results_long <- write_csv(results_long, "output/ahle_sr.csv")

# Add results to tracker csv
cat("\n", file="output/ahle_results_tracker.csv", append=TRUE) # wrtite_csv and write.table were having issues without this line to fix
export_tracker <- write_csv(results_long, "output/ahle_results_tracker.csv", append = TRUE)
