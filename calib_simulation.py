import pandas as pd, numpy as np
import models
import urbansim.sim.simulation as sim

np.random.seed(1)

# Simulation run

##Single-year
# sim.run(["build_networks", "neighborhood_vars", "rsh_simulate", "nrh_simulate", "nrh_simulate2", 
         # "price_vars", "feasibility", "residential_developer", "non_residential_developer"])
         
##Multi-year
sim.run(["build_networks"]) #initialize network accessibility engine
sim.run(["neighborhood_vars", #"scheduled_development_events", #scheduled events and accessibility variables
         "rsh_simulate", "nrh_simulate", "nrh_simulate2",   #price models
         "jobs_transition", "elcm_simulate", "households_transition", "hlcm_luz_simulate", #demand/location models
         "price_vars", "feasibility", "residential_developer", "non_residential_developer", #supply/proforma models
         ], years=[2013, 2014, 2015,])

# Summarize results at MSA level
b = sim.get_table('buildings').to_frame(columns = ['msa_id', 'mgra_id', 'residential_units', 'non_residential_sqft', 'note'])
new_du_by_msa = b[b.note  == 'simulated'].groupby('msa_id').residential_units.sum()
new_nrsf_by_msa = b[b.note  == 'simulated'].groupby('msa_id').non_residential_sqft.sum()
proportion_du_by_msa = new_du_by_msa / new_du_by_msa.sum()
proportion_nrsf_by_msa = new_nrsf_by_msa / new_nrsf_by_msa.sum()

# Write out indicators to calibration directory
proportion_du_by_msa.to_csv('.\\data\\calibration\\msa_du_simulated.csv', header = True)
proportion_nrsf_by_msa.to_csv('.\\data\\calibration\\msa_nrsf_simulated.csv', header = True)