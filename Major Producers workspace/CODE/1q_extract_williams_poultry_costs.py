#%% About
'''
William shared this data in a spreadsheet which I am recreating here by copy-pasting
the values for each column.
'''
#%% Create table

poultry_costs_fromwill = pd.DataFrame(
   {'country':[
      'India'
      ,'Brazil'
      ,'China'
      ,'USA'
      ,'France'
      ,'Poland'
      ,'Spain'
      ,'Italy'
      ,'Netherlands'
      ,'Germany'
      ,'UK'
      ]
    ,'year':[
       2020
       ,2017
       ,2020
       ,2017
       ,2017
       ,2017
       ,2017
       ,2017
       ,2017
       ,2017
       ,2017
       ]
    ,'avg_slaughterwt_kg':[
       2
       ,2.6
       ,2.88
       ,2.9
       ,1.9
       ,2.3
       ,2.6
       ,2.4
       ,2.4
       ,2.35
       ,2.25
       ]
    ,'thinwt_kg':[
       np.nan
       ,np.nan
       ,np.nan
       ,np.nan
       ,0
       ,2
       ,2.2
       ,np.nan
       ,1.79
       ,1.6
       ,1.8
       ]
    ,'clearwt_kg':[
       np.nan
       ,np.nan
       ,np.nan
       ,np.nan
       ,1.9
       ,2.43
       ,2.77
       ,np.nan
       ,2.6
       ,2.8
       ,2.35
       ]
    ,'daysonfeed':[
       38
       ,43
       ,44.8
       ,47
       ,37
       ,np.nan
       ,np.nan
       ,np.nan
       ,41
       ,42
       ,39
       ]
    ,'fcr_1':[
       1.7
       ,1.83
       ,1.665
       ,1.83
       ,1.67
       ,1.62
       ,1.72
       ,1.68
       ,1.58
       ,1.58
       ,1.62
       ]
    ,'max_stockingdensity_kgperm':[
       21.75
       ,28.5
       ,52.5
       ,41.5
       ,39
       ,39
       ,39
       ,39
       ,42
       ,35
       ,38
       ]
    ,'currency':[
       'INR'
       ,'Euro'
       ,'CNY'
       ,'Euro'
       ,'Euro'
       ,'Euro'
       ,'Euro'
       ,'Euro'
       ,'Euro'
       ,'Euro'
       ,'Euro'
       ]
    ,'chickprice_perhd':[
       45
       ,np.nan
       ,5
       ,0.27
       ,0.303
       ,0.315
       ,0.32
       ,0.33
       ,0.31
       ,0.315
       ,0.405
       ]
    ,'chickcost_perkglive':[
       22.5
       ,0.089
       ,1.715277778
       ,0.105
       ,0.166
       ,0.143
       ,0.129
       ,0.143
       ,0.137
       ,0.139
       ,0.187
       ]
    ,'feedcost_perkglive':[
       25
       ,0.437
       ,6.277777778
       ,0.441
       ,0.487
       ,0.508
       ,0.541
       ,0.556
       ,0.491
       ,0.497
       ,0.511
       ]
    ,'laborcost_perkglive':[
       1.5
       ,0.02
       ,0.157986111
       ,0.019
       ,0.052
       ,0.015
       ,0.028
       ,0.026
       ,0.036
       ,0.037
       ,0.034
       ]
    ,'landhousingcost_perkglive':[
       1
       ,0.049
       ,0.515625
       ,0.029
       ,0.059
       ,0.047
       ,0.062
       ,0.051
       ,0.048
       ,0.058
       ,0.054
       ]
    ,'medicinecost_perkglive':[np.nan] * 11
    ,'othercost_perkglive':[
       17
       ,0.038
       ,0.803819444
       ,0.052
       ,0.095
       ,0.073
       ,0.063
       ,0.08
       ,0.09
       ,0.088
       ,0.079
       ]
    ,'source':[
       'Agricultural Value Chains in India'
       ,'Wageningen U.'
       ,'Chinese Broiler Production Systems'
       ,'Wageningen U.'
       ,'Wageningen U.'
       ,'Wageningen U.'
       ,'Wageningen U.'
       ,'Wageningen U.'
       ,'Wageningen U.'
       ,'Wageningen U.'
       ,'Wageningen U.'
       ]
    }
)

cost_columns = [
   'chickcost_perkglive'
   ,'feedcost_perkglive'
   ,'laborcost_perkglive'
   ,'landhousingcost_perkglive'
   ,'medicinecost_perkglive'
   ,'othercost_perkglive'
]
poultry_costs_fromwill['totalcost_perkglive'] = poultry_costs_fromwill[cost_columns].sum(axis=1)

datainfo(poultry_costs_fromwill)

# Expand to other years by applying inflation rate
# Doing this inside 2a_assemble_poultry because World Bank data is already there

#%% Export

poultry_costs_fromwill.to_pickle(os.path.join(PRODATA_FOLDER ,'poultry_costs_fromwill.pkl.gz'))
