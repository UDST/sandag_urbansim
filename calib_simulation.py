import datetime
import pandas as pd, numpy as np
import models
import urbansim.sim.simulation as sim
from spandex import TableLoader

np.random.seed(1)

loader = TableLoader()

# Simulation run
sim.run(["build_networks", "neighborhood_vars", "rsh_simulate", "nrh_simulate", "nrh_simulate2", 
         "price_vars", "feasibility", "residential_developer"])

# Summarize results at MSA level
b = sim.get_table('buildings').to_frame()
mgra_msa_xref = pd.read_csv(loader.get_path("xref//mgra_msa.csv"))
merged = pd.merge(b, mgra_msa_xref, left_on = 'mgra_id', right_on = 'mgra_id')
new_du_by_msa = merged[merged.note  == 'simulated'].groupby('msa_id').residential_units.sum()
proportion_by_msa = new_du_by_msa / new_du_by_msa.sum()

proportion_by_msa.to_csv('.\\data\\calibration\\msa_simulated.csv', header = True)