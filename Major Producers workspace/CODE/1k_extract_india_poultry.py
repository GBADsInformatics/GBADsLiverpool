#%% About
'''
This program imports India-specific data and estimates.
'''

#%% Chicks placed

# From reference R74 in data organizer
# https://www.infomerics.com/admin/uploads/Poultry_Industry_14.06.2019.pdf
# 65 million birds per week = 3,380M per year
# 7% growth rate in meat production, which for now I will attribute to growth in number of birds placed
# This report is from 2019, so I will say 3,380M birds placed that year and calculate
# other years on a 7% growth rate.
india_poultry = pd.DataFrame(
   {'country':'India'
    ,'year':range(2011, 2021)
   }
)

def extrapolate_chicksplaced_byyear(INPUT_ROW):
   basenumber = 3380000
   baseyear = 2019
   yeardiff = INPUT_ROW['year'] - baseyear
   OUTPUT = basenumber * 1.07**yeardiff  # 7% growth rate
   return OUTPUT
india_poultry['chicksplaced_broilers_thsdhd'] = india_poultry.apply(extrapolate_chicksplaced_byyear ,axis=1)

#%% Export

datainfo(india_poultry)

india_poultry.to_pickle(os.path.join(PRODATA_FOLDER ,'india_poultry.pkl.gz'))
