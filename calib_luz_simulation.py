import pandas as pd, numpy as np
import models
import urbansim.sim.simulation as sim

np.random.seed(1)

# Simulation run

##Single-year
sim.run(["build_networks", "neighborhood_vars", "rsh_simulate", "nrh_simulate", "nrh_simulate2", 
         "price_vars", "feasibility", "residential_developer", "non_residential_developer"], years = [2013,])
         
##Multi-year
# sim.run(["build_networks"]) #initialize network accessibility engine
# sim.run(["neighborhood_vars", #"scheduled_development_events", #scheduled events and accessibility variables
         # "rsh_simulate", "nrh_simulate", "nrh_simulate2",   #price models
         # "jobs_transition", "elcm_simulate", "households_transition", "hlcm_luz_simulate", #demand/location models
         # "price_vars", "feasibility", "residential_developer", "non_residential_developer", #supply/proforma models
         # ], years=[2013, 2014, 2015,])

# Summarize results at target LUZ level
target_luz = [69, 70, 72]
b = sim.get_table('buildings').to_frame(columns = ['luz_id', 'msa_id', 'mgra_id', 'residential_units', 'non_residential_sqft', 'note'])
b_target = b[b.luz_id.isin(target_luz)]
new_du_region = b[b.note  == 'simulated'].residential_units.sum()
new_du_luz = b_target[b_target.note  == 'simulated'].residential_units.sum()
proportion_in_luz = new_du_luz*1.0/new_du_region

# Write out indicators to calibration directory
pd.DataFrame([proportion_in_luz]).to_csv('.\\data\\calibration\\luz_du_simulated.csv', index = False)