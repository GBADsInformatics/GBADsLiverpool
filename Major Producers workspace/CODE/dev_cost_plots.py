#%% Data

optactcost_1 = pd.DataFrame(
   {'item':['Feed' ,'Chicks' ,'Vet & Med' ,'Labor' ,'Land & Facilities']
      ,'cost_perkgcarc_opt':[0.81 ,0.20 ,0 ,0.3 ,0.1]
      ,'cost_perkgcarc_act':[0.84 ,0.25 ,0.1 ,0.35 ,0.15]
   }
)
optactcost_2 = pd.DataFrame(
   {'item':['Feed' ,'Chicks' ,'Vet & Med' ,'Labor' ,'Land & Facilities']*2
      ,'optoract':['opt']*5 + ['act']*5
      ,'cost_perkgcarc':[0.81 ,0.20 ,0 ,0.3 ,0.1] + [0.84 ,0.25 ,0.1 ,0.35 ,0.15]
   }
)

#%% Plot

fig = px.bar(optactcost_2, x='optoract', y='cost_perkgcarc', color='item')
fig.show()
