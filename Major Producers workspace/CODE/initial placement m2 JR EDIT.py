

# something to allow calculating placement rates and time on feed
# if referring to breed standard for 1 or 2 lift flocks

def initial_placement_m2(
      breedstd_df,  # JR: breed standard dataframe. Must contain columns 'dayonfeed' and 'bodyweight_kg'

      w_thin, # weight at thinning
      # (zero means no thinning, single lift at
      # end of flock) (kg live, target weight)

      w_clear, # target weight at house clearance

      t_thin, #time to thinning ( if 0 will calculate using breed standard)

      t_clear, # time to clear (if 0 will use breed standard)

      max_density, #stocking limit, kg/m^2

      total_mortality,# (0:1) as a proportion, not percentage

      d7_proportion  # (0:1) proportion of total mortality in first 7 days, 0.5 as suggested starting point
      ):
   funcname = inspect.currentframe().f_code.co_name

   if t_clear == 0:
      t_clear = min(breedstd_df.loc[breedstd_df['bodyweight_kg'] >= w_clear ,'dayonfeed'])
      # minimum time to achieve clearance weight lookup
      # AHstandard = As hatched standard, start weight on day x

   # Avara standard works two step mortality rate, gives day7 mortality
   # proportion of total as = -0.315ln(x) + 1.6678,
   # however this could be calculated using placement as denominator
   # in a thinned flock (i.e. weighted toward > number of pre-thinning losses),
   # and different rates with same relative split produce different curves
   # so I think we ignore this data

   daily_mortality_7 = 1 - (1 - (total_mortality * d7_proportion))**(1 / 7)
   # daily mortality rate to day 7 / nursery period

   daily_mortality_8_clear = 1 - (1 - (total_mortality * (1 - d7_proportion)))**(1 / (t_clear - 7))
   # daily mortality rate per bird for D8 to clearance

   count_clear = max_density / w_clear #  max birds per m2 at clearance

   if w_thin > 0:
      if t_thin == 0:  # earliest day to thin for target weight
         t_thin = min(breedstd_df.loc[breedstd_df['bodyweight_kg'] >= w_thin ,'dayonfeed'])

      td_ct = t_clear - t_thin # time difference clear to thin
      td_t7 = t_thin - 7 # time difference between thin and nursery period
      count_thin = max_density / w_thin # max birds per m2 at thinning
      thin_fraction = 1 - count_clear * (1 + daily_mortality_8_clear)**td_ct / count_thin
      # proportion of birds to remove at thin

      count_start = count_thin * (1 + daily_mortality_7)**7 * (1 + daily_mortality_8_clear)**td_t7
      # birds per metre

   else: #  same idea but without thinning
      count_start = count_clear * (1 + daily_mortality_7)**7 * (1 + daily_mortality_8_clear)**(t_clear - 7)
      thin_fraction = 0 # proportion to remove at thinning

   # to do add (nursery mortality, growing mortality (thin/clear)
   # and then average weight, calculate average feed intake etc etc)
   # then total metrage squared as head placed over count start

   print(f"\n<{funcname}>")
   print(f"\n   Inputs:\n      {w_thin=}\n      {w_clear=}\n      {max_density=}\n      {total_mortality=}\n      {d7_proportion=}")
   print(f"\n   Outputs:\n      {count_start=}\n      {thin_fraction=}\n      {t_thin=}\n      {t_clear=}")

   return None

#feed use
#breakpoints:
# day 7/2
# day 7 to thinning/2
# day thinning to end/2

#count_start

poultrybreedstd_cobb500 = pd.read_pickle(os.path.join(DASH_DATA_FOLDER ,'poultrybreedstd_cobb500.pkl.gz'))
breedstd_df = poultrybreedstd_cobb500.copy()
breedstd_df['bodyweight_kg'] = breedstd_df['bodyweight_g'] / 1000

initial_placement_m2(
      breedstd_df=breedstd_df,  # JR: breed standard dataframe. Must contain columns 'dayonfeed' and 'bodyweight_kg'

      w_thin=2, # weight at thinning
      # (zero means no thinning, single lift at
      # end of flock) (kg live, target weight)

      w_clear=2.5, # target weight at house clearance

      t_thin=0, # time to thinning (if 0 will calculate using breed standard)

      t_clear=0, # time to clear (if 0 will use breed standard)

      max_density=2, # stocking limit, kg/m^2

      total_mortality=0.05, # (0:1) as a proportion, not percentage

      d7_proportion=0.5  # (0:1) proportion of total mortality in first 7 days, 0.5 as suggested starting point
      )
