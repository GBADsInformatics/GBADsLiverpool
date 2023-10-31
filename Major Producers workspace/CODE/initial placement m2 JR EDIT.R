
# something to allow calculating placement rates and time on feed
# if referring to breed standard for 1 or 2 lift flocks

# JR Addition: Read breed standard data
#!!! JR read.csv is giving an error. Online searches turn up problems due to passing multiple files, which does not apply here.
AHstandard <- read.csv('F:\\GBADs_JR\\CODE\\EXPDATA\\poultrybreedstd_cobb500.csv')
AHstandard$day <- AHstandard$dayonfeed
AHstandard$daystartweightAH <- AHstandard$bodyweight_g / 1000

initial_placement_m2 <- function(w_thin, # weight at thinning
                                 # (zero means no thinning, single lift at
                                 # end of flock) (kg live, target weight)

                                 w_clear, # target weight at house clearance

                                 t_thin, #time to thinning
                                 # ( if 0 will calculate using breed standard)

                                 t_clear, # time to clear
                                 # (if 0 will use breed standard)

                                 max_density, #stocking limit, kg/m^2

                                 total_mortality,# (0:1) as a proportion,
                                 # not percentage

                                 d7_proportion){ # (0:1) proportion of
  # total mortality in first 7 days,
  # 0.5 as suggested starting point

  if (t_clear == 0) {
    t_clear <- min(AHstandard$day[AHstandard$daystartweightAH >= w_clear])
    # minimum time to achieve clearance weight lookup
    #AHstandard = As hatched standard, start weight on day x
  }

  # Avara standard works two step mortality rate, gives day7 mortality
  # proportion of total as = -0.315ln(x) + 1.6678,
  # however this could be calculated using placement as denominator
  # in a thinned flock (i.e. weighted toward > number of pre-thinning losses),
  # and different rates with same relative split produce different curves
  # so I think we ignore this data

  daily_mortality_7 <- 1 - (1 - (total_mortality * d7_proportion)) ^ (1 / 7)
  # daily mortality rate to day 7 / nursery period

  daily_mortality_8_clear <- 1 - (1 - (total_mortality  *
                                (1 - d7_proportion))) ^ (1 / (t_clear - 7))
  # daily mortality rate per bird for D8 to clearance

  count_clear <- max_density / w_clear #  max birds per m2 at clearance

  if (w_thin > 0) {
    if (t_thin == 0) { # earliest day to thin for target weight
      t_thin <- min(AHstandard$day[AHstandard$daystartweightAH >= w_thin])
    }

    td_ct <- t_clear - t_thin # time difference clear to thin

    td_t7 <- t_thin - 7 # time difference between thin and nursery period

    count_thin <- max_density / w_thin # max birds per m2 at thinning

    thin_fraction <- 1 -
                        count_clear *
                        (1 + daily_mortality_8_clear) ^ td_ct / count_thin
    # proportion of birds to remove at thin

    count_start <- count_thin *
      (1 + daily_mortality_7) ^ 7 * (1 + daily_mortality_8_clear) ^ td_t7
    # birds per metre

  } else { #  same idea but without thinning
    count_start <- count_clear * (1 + daily_mortality_7) ^ 7 *
                      (1 + daily_mortality_8_clear)^ (t_clear - 7)

    thin_fraction <- 0 # proportion to remove at thinning
  }
  # to do add (nursery mortality, growing mortality (thin/clear)
  # and then average weight, calculate average feed intake etc etc)
  # then total metrage squared as head placed over count start

  return(c(count_start, thin_fraction, t_thin, t_clear))
}

#feed use
#breakpoints:
# day 7/2
# day 7 to thinning/2
# day thinning to end/2

#count_start

# JR addition
cmd_args <- commandArgs(trailingOnly = TRUE)    # Fetch command line arguments
result = initial_placement_m2(cmd_args)
cat(result)     # cat() will add things to the stdout stream
