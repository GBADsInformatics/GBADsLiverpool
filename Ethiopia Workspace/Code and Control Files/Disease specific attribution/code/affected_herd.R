# Creating affected herd scenarios ====
source('code/affected_herd_fns.R')
set.seed(123)

# 1 - PPR====
## Sheep CLM ----
### Run Bayesian model to estimate cause frequency
CLM_S_PPR_freq <- SEBI_to_incidence(SEBI_species = c("Sheep","Small Ruminants"),
                                    SEBI_cause = "Peste des petits ruminants",
                                    SEBI_test = c("c-ELISA"),
                                    freq_col = "S_PPR",
                                    age_col = "CLM_S_PPR")

### Combine production of affected animal and ideal animals
CLM_S_PPR_prod <- combined_production(production_system_code = "CLM",
                                      species_code = "S",
                                      cause_code = "PPR",
                                      ahle_scenario_file = "scenarios/AHLE scenario parameters SMALLRUMINANTS.xlsx",
                                      impact_file = "input/impact_inputs.xlsx",
                                      affected_value = list(CP_J = CLM_S_PPR_freq$stratified_I$I$J, CP_SA = CLM_S_PPR_freq$stratified_I$I$SA, CP_AF = CLM_S_PPR_freq$stratified_I$I$AF, CP_AM = CLM_S_PPR_freq$stratified_I$I$AM, CP_Health = Reduce("+",CLM_S_PPR_freq$stratified_I$I)),
                                      affected_change = list(CP_Part=CLM_S_PPR_freq$stratified_I$I$AF))

### Create new scenario using pert distribution for changed objects
CLM_S_PPR <- pert_distributions("CLM_S_PPR", CLM_S_PPR_prod)

### Update affected scenario workbook
update_AHLE_scenarios(AHLE_scenario_file = "scenarios/AHLE scenario parameters SMALLRUMINANTS.xlsx",
                      ideal_col = "CLM_S_Ideal",
                      affected_parameters_object=CLM_S_PPR,
                      col_name = "CLM_S_PPR",
                      new_file_name = "scenarios/sr_disease_scenarios.xlsx")

## Sheep Pastoral  ----
Past_S_PPR_freq <- SEBI_to_incidence(SEBI_species = c("Sheep","Small Ruminants"),
                                     SEBI_cause = "Peste des petits ruminants",
                                     SEBI_test = c("c-ELISA"),
                                     freq_col = "S_PPR",
                                     age_col = "Past_S_PPR")
Past_S_PPR_prod <- combined_production(production_system_code = "Past",
                                       species_code = "S",
                                       cause_code = "PPR",
                                       ahle_scenario_file = "scenarios/AHLE scenario parameters SMALLRUMINANTS.xlsx",
                                       impact_file = "input/impact_inputs.xlsx",
                                       affected_value = list(CP_J = Past_S_PPR_freq$stratified_I$I$J, CP_SA = Past_S_PPR_freq$stratified_I$I$SA, CP_AF = Past_S_PPR_freq$stratified_I$I$AF, CP_AM = Past_S_PPR_freq$stratified_I$I$AM, CP_Health = Reduce("+",Past_S_PPR_freq$stratified_I$I)),
                                       affected_change = list(CP_Part=Past_S_PPR_freq$stratified_I$I$AF))
Past_S_PPR <- pert_distributions("Past_S_PPR", Past_S_PPR_prod)
update_AHLE_scenarios(AHLE_scenario_file= "scenarios/sr_disease_scenarios.xlsx",
                      ideal_col = "Past_S_Ideal",
                      affected_parameters_object= Past_S_PPR,
                      col_name = "Past_S_PPR",
                      new_file_name = "scenarios/sr_disease_scenarios.xlsx")

## Goats CLM ----
CLM_G_PPR_freq <- SEBI_to_incidence(SEBI_species=c("Goats","Small Ruminants"),
                                    SEBI_cause="Peste des petits ruminants",
                                    SEBI_test=c("c-ELISA"),
                                    freq_col = "G_PPR",
                                    age_col = "CLM_G_PPR")
CLM_G_PPR_prod <- combined_production(production_system_code = "CLM",
                                      species_code = "G",
                                      cause_code = "PPR",
                                      ahle_scenario_file = "scenarios/AHLE scenario parameters SMALLRUMINANTS.xlsx",
                                      impact_file = "input/impact_inputs.xlsx",
                                      affected_value = list(CP_J = CLM_G_PPR_freq$stratified_I$I$J, CP_SA = CLM_G_PPR_freq$stratified_I$I$SA, CP_AF = CLM_G_PPR_freq$stratified_I$I$AF, CP_AM = CLM_G_PPR_freq$stratified_I$I$AM, CP_Health = Reduce("+",CLM_G_PPR_freq$stratified_I$I)),
                                      affected_change = list(CP_Part=CLM_G_PPR_freq$stratified_I$I$AF, CP_Milk = CLM_G_PPR_freq$stratified_I$I$AF))
CLM_G_PPR <- pert_distributions("CLM_G_PPR", CLM_G_PPR_prod)
update_AHLE_scenarios(AHLE_scenario_file= "scenarios/sr_disease_scenarios.xlsx",
                      ideal_col = "CLM_G_Ideal",
                      affected_parameters_object=CLM_G_PPR,
                      col_name = "CLM_G_PPR",
                      new_file_name = "scenarios/sr_disease_scenarios.xlsx")

## Goats Pastoral ----
Past_G_PPR_freq <- SEBI_to_incidence(SEBI_species=c("Goats","Small Ruminants"),
                                     SEBI_cause="Peste des petits ruminants",
                                     SEBI_test=c("c-ELISA"),
                                     freq_col = "G_PPR",
                                     age_col = "Past_G_PPR")
Past_G_PPR_prod <- combined_production(production_system_code = "Past",
                                       species_code = "G",
                                       cause_code = "PPR",
                                       ahle_scenario_file = "scenarios/AHLE scenario parameters SMALLRUMINANTS.xlsx",
                                       impact_file = "input/impact_inputs.xlsx",
                                       affected_value = list(CP_J = Past_G_PPR_freq$stratified_I$I$J, CP_SA = Past_G_PPR_freq$stratified_I$I$SA, CP_AF = Past_G_PPR_freq$stratified_I$I$AF, CP_AM = Past_G_PPR_freq$stratified_I$I$AM, CP_Health = Reduce("+",Past_G_PPR_freq$stratified_I$I)),
                                       affected_change = list(CP_Part=Past_G_PPR_freq$stratified_I$I$AF, CP_Milk = Past_G_PPR_freq$stratified_I$I$AF))
Past_G_PPR <- pert_distributions("Past_G_PPR", Past_G_PPR_prod)
update_AHLE_scenarios(AHLE_scenario_file= "scenarios/sr_disease_scenarios.xlsx",
                      ideal_col = "Past_G_Ideal",
                      affected_parameters_object=Past_G_PPR,
                      col_name = "Past_G_PPR",
                      new_file_name = "scenarios/sr_disease_scenarios.xlsx")

# 2 - Brucellosis ====
## Cattle CLM ----
CLM_C_Bruc_freq <- SEBI_to_incidence(SEBI_species="Cattle",
                                     SEBI_cause="Brucellosis",
                                     SEBI_test=c("RBT, CFT"),
                                     freq_col="C_Bruc",
                                     age_col = "CLM_C_Bruc")
CLM_C_Bruc_prod <- combined_production(production_system_code = "CLM",
                                       species_code = "C",
                                       cause_code = "Bruc",
                                       ahle_scenario_file = "scenarios/AHLE scenario parameters CATTLE.xlsx",
                                       impact_file = "input/impact_inputs.xlsx",
                                       affected_value = list(CP_J = CLM_C_Bruc_freq$stratified_I$I$J, CP_AF = CLM_C_Bruc_freq$stratified_I$I$AF,  CP_Health = Reduce("+",CLM_C_Bruc_freq$stratified_I$I)),
                                       affected_change = list(CP_Part=CLM_C_Bruc_freq$stratified_I$I$AF, CP_Milk=CLM_C_Bruc_freq$stratified_I$I$AF, CP_Draught=CLM_C_Bruc_freq$stratified_I$I$O))
CLM_C_Bruc <- pert_distributions("CLM_C_Bruc", CLM_C_Bruc_prod)
update_AHLE_scenarios(AHLE_scenario_file="scenarios/AHLE scenario parameters CATTLE.xlsx",
                      ideal_col = "CLM_C_Ideal",
                      affected_parameters_object=CLM_C_Bruc,
                      col_name = "CLM_C_Bruc",
                      new_file_name = "scenarios/cattle_disease_scenarios.xlsx")

## Cattle Pastoral ----
Past_C_Bruc_freq <- SEBI_to_incidence(SEBI_species="Cattle",
                                      SEBI_cause="Brucellosis",
                                      SEBI_test=c("RBT, CFT"),
                                      freq_col="C_Bruc",
                                      age_col = "Past_C_Bruc")
Past_C_Bruc_prod <- combined_production(production_system_code = "Past",
                                              species_code = "C",
                                              cause_code = "Bruc",
                                              ahle_scenario_file = "scenarios/AHLE scenario parameters CATTLE.xlsx",
                                              impact_file = "input/impact_inputs.xlsx",
                                              affected_value = list(CP_J = Past_C_Bruc_freq$stratified_I$I$J, CP_AF = Past_C_Bruc_freq$stratified_I$I$AF, CP_Health = Reduce("+", Past_C_Bruc_freq$stratified_I$I)),
                                              affected_change = list(CP_Part = Past_C_Bruc_freq$stratified_I$I$AF, CP_Milk = Past_C_Bruc_freq$stratified_I$I$AF))
Past_C_Bruc <- pert_distributions("Past_C_Bruc", Past_C_Bruc_prod)
update_AHLE_scenarios(AHLE_scenario_file="scenarios/cattle_disease_scenarios.xlsx",
                      ideal_col = "Past_C_Ideal",
                      affected_parameters_object=Past_C_Bruc,
                      col_name = "Past_C_Bruc",
                      new_file_name = "scenarios/cattle_disease_scenarios.xlsx")

## Cattle Periurban Dairy ----
PUD_C_Bruc_freq <- SEBI_to_incidence(SEBI_species="Cattle",
                                     SEBI_cause="Brucellosis",
                                     SEBI_test=c("RBT, CFT"),
                                     freq_col="C_Bruc",
                                     age_col = "PUD_C_Bruc")
PUD_C_Bruc_prod <- combined_production(production_system_code = "PUD",
                                             species_code = "C",
                                             cause_code = "Bruc",
                                             ahle_scenario_file = "scenarios/AHLE scenario parameters CATTLE.xlsx",
                                             impact_file = "input/impact_inputs.xlsx",
                                             affected_value = list(CP_J = PUD_C_Bruc_freq$stratified_I$I$J, CP_AF = PUD_C_Bruc_freq$stratified_I$I$AF, CP_Health = Reduce("+", PUD_C_Bruc_freq$stratified_I$I)),
                                             affected_change = list(CP_Part=PUD_C_Bruc_freq$stratified_I$I$AF, CP_Milk=PUD_C_Bruc_freq$stratified_I$I$AF))
PUD_C_Bruc <- pert_distributions("PUD_C_Bruc", PUD_C_Bruc_prod)
update_AHLE_scenarios(AHLE_scenario_file="scenarios/cattle_disease_scenarios.xlsx",
                      ideal_col = "PUD_C_Ideal",
                      affected_parameters_object=PUD_C_Bruc,
                      col_name = "PUD_C_Bruc",
                      new_file_name = "scenarios/cattle_disease_scenarios.xlsx")

## Sheep CLM ----
CLM_S_Bruc_freq <- SEBI_to_incidence(SEBI_species=c("Sheep", "Small Ruminants"),
                                     SEBI_cause="Brucellosis",
                                     SEBI_test=c("RBT, CFT"),
                                     freq_col = "S_Bruc",
                                     age_col = "CLM_S_Bruc")
CLM_S_Bruc_prod <- combined_production(production_system_code = "CLM",
                                             species_code = "S",
                                             cause_code = "Bruc",
                                             ahle_scenario_file = "scenarios/AHLE scenario parameters SMALLRUMINANTS.xlsx",
                                             impact_file = "input/impact_inputs.xlsx",
                                             affected_value = list(CP_J = CLM_S_Bruc_freq$stratified_I$I$J, CP_AF = CLM_S_Bruc_freq$stratified_I$I$AF, CP_Health = Reduce("+",CLM_S_Bruc_freq$stratified_I$I)),
                                             affected_change = list(CP_Part=CLM_S_Bruc_freq$stratified_I$I$AF))
CLM_S_Bruc <- pert_distributions("CLM_S_Bruc", CLM_S_Bruc_prod)
update_AHLE_scenarios(AHLE_scenario_file= "scenarios/sr_disease_scenarios.xlsx",
                      ideal_col = "CLM_S_Ideal",
                      affected_parameters_object=CLM_S_Bruc,
                      col_name = "CLM_S_Bruc",
                      new_file_name = "scenarios/sr_disease_scenarios.xlsx")

## Sheep Pastoral ----
Past_S_Bruc_freq <- SEBI_to_incidence(SEBI_species=c("Sheep", "Small Ruminants"),
                                      SEBI_cause="Brucellosis",
                                      SEBI_test=c("RBT, CFT"),
                                      freq_col = "S_Bruc",
                                      age_col = "Past_S_Bruc")
Past_S_Bruc_prod <- combined_production(production_system_code = "Past",
                                              species_code = "S",
                                              cause_code = "Bruc",
                                              ahle_scenario_file = "scenarios/AHLE scenario parameters SMALLRUMINANTS.xlsx",
                                              impact_file = "input/impact_inputs.xlsx",
                                              affected_value = list(CP_J = Past_S_Bruc_freq$stratified_I$I$J, CP_AF = Past_S_Bruc_freq$stratified_I$I$AF, CP_Health = Reduce("+", Past_S_Bruc_freq$stratified_I$I)),
                                              affected_change = list(CP_Part=Past_S_Bruc_freq$stratified_I$I$AF))
Past_S_Bruc <- pert_distributions("Past_S_Bruc", Past_S_Bruc_prod)
update_AHLE_scenarios(AHLE_scenario_file= "scenarios/sr_disease_scenarios.xlsx",
                      ideal_col = "Past_S_Ideal",
                      affected_parameters_object=Past_S_Bruc,
                      col_name = "Past_S_Bruc",
                      new_file_name = "scenarios/sr_disease_scenarios.xlsx")

## Goats CLM ----
CLM_G_Bruc_freq <- SEBI_to_incidence(SEBI_species=c("Goats", "Small Ruminants"),
                                     SEBI_cause="Brucellosis",
                                     SEBI_test=c("RBT, CFT"),
                                     freq_col = "G_Bruc",
                                     age_col = "CLM_G_Bruc")
CLM_G_Bruc_prod <- combined_production(production_system_code = "CLM",
                                             species_code = "G",
                                             cause_code = "Bruc",
                                             ahle_scenario_file = "scenarios/AHLE scenario parameters SMALLRUMINANTS.xlsx",
                                             impact_file = "input/impact_inputs.xlsx",
                                             affected_value = list(CP_J = CLM_G_Bruc_freq$stratified_I$I$J, CP_AF = CLM_G_Bruc_freq$stratified_I$I$AF, CP_Health = Reduce("+",CLM_G_Bruc_freq$stratified_I$I)),
                                             affected_change = list(CP_Part=CLM_G_Bruc_freq$stratified_I$I$AF, CP_Milk = CLM_G_Bruc_freq$stratified_I$I$AF))
CLM_G_Bruc <- pert_distributions("CLM_G_Bruc", CLM_G_Bruc_prod)
update_AHLE_scenarios(AHLE_scenario_file= "scenarios/sr_disease_scenarios.xlsx",
                      ideal_col = "CLM_G_Ideal",
                      affected_parameters_object=CLM_G_Bruc,
                      col_name = "CLM_G_Bruc",
                      new_file_name = "scenarios/sr_disease_scenarios.xlsx")

## Goats Pastoral ----
Past_G_Bruc_freq <- SEBI_to_incidence(SEBI_species=c("Goats", "Small Ruminants"),
                                      SEBI_cause="Brucellosis",
                                      SEBI_test=c("RBT, CFT"),
                                      freq_col = "G_Bruc",
                                      age_col = "Past_G_Bruc")
Past_G_Bruc_prod <- combined_production(production_system_code = "Past",
                                           species_code = "G",
                                           cause_code = "Bruc",
                                           ahle_scenario_file = "scenarios/AHLE scenario parameters SMALLRUMINANTS.xlsx",
                                           impact_file = "input/impact_inputs.xlsx",
                                           affected_value = list(CP_J = Past_G_Bruc_freq$stratified_I$I$J, CP_AF = Past_G_Bruc_freq$stratified_I$I$AF, CP_Health = Reduce("+",Past_G_Bruc_freq$stratified_I$I)),
                                           affected_change = list(CP_Part=Past_G_Bruc_freq$stratified_I$I$AF, CP_Milk = Past_G_Bruc_freq$stratified_I$I$AF))
Past_G_Bruc <- pert_distributions("Past_G_Bruc", Past_G_Bruc_prod)
update_AHLE_scenarios(AHLE_scenario_file= "scenarios/sr_disease_scenarios.xlsx",
                      ideal_col = "Past_G_Ideal",
                      affected_parameters_object=Past_G_Bruc,
                      col_name = "Past_G_Bruc",
                      new_file_name = "scenarios/sr_disease_scenarios.xlsx")