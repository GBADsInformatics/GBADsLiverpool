#%% About
'''
This program imports the various data for China, which is generally not in databases
but rather from published papers.
'''

#%% Mortality

# From reference R39
# https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7142404/

# Paper from 2020
# 3 rearing systems for white-feathered broilers in China:
   # net floor system (NFS), the normal cage system (NCS), and the high standard cage system (HCS)
# 66 broiler chicken flocks on 52 farms in China
# Mortality rates include culls

china_poultry = pd.DataFrame(
   {'country':'China'
    ,'year':2020
    ,'n_flocks_nfs':8
    ,'n_flocks_ncs':20
    ,'n_flocks_hcs':38
    ,'mortality_pct_nfs':6.92
    ,'mortality_pct_ncs':3.79
    ,'mortality_pct_hcs':3.26
   }
   ,index=[0]
)

#!!! For now, taking simple average of rearing systems
china_poultry['mortality_pct_avg'] = china_poultry[['mortality_pct_nfs' ,'mortality_pct_ncs' ,'mortality_pct_hcs']].mean(axis=1)

#%% Production

# From reference R38
# https://apps.fas.usda.gov/newgainapi/api/report/downloadreportbyfilename?filename=Poultry%20and%20Products%20Semi-annual_Beijing_China%20-%20Peoples%20Republic%20of_2-26-2019.pdf

# Report from 2019
# Chicken Meat production breakdown:
   # White Broilers 57% (commercial breeds Cobb/Ross)
   # Yellow Broilers 28% (domestic breeds - slower growing)
   # Ex-layers 8%
   # Hybrid 7%
#!!! Must adjust head slaughtered for these percentages if you only want to show broilers!
