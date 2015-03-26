import urbansim.sim.simulation as sim
from urbansim.utils import misc
import os
import sys
import datasources
import variables
from urbansim.models import transition
from urbansim_defaults import models
from urbansim_defaults import utils
import numpy as np
import pandas as pd
import pandana as pdna
from cStringIO import StringIO

@sim.model('build_networks')
def build_networks(parcels):
    st = pd.HDFStore(os.path.join(misc.data_dir(), "osm_sandag.h5"), "r")
    nodes, edges = st.nodes, st.edges
    net = pdna.Network(nodes["x"], nodes["y"], edges["from"], edges["to"],
                       edges[["weight"]])
    net.precompute(3000)
    sim.add_injectable("net", net)
    
    p = parcels.to_frame(parcels.local_columns)
    p['node_id'] = net.get_node_ids(p['x'], p['y'])
    sim.add_table("parcels", p)

@sim.model('households_transition')
def households_transition(households, persons, annual_household_control_totals, year):
    ct = annual_household_control_totals.to_frame()
    tran = transition.TabularTotalsTransition(ct, 'total_number_of_households')
    model = transition.TransitionModel(tran)
    hh = households.to_frame(households.local_columns)
    new, added_hh_idx = \
        model.transition(hh, year,)
    new.loc[added_hh_idx, "building_id"] = -1
    sim.add_table("households", new)
    
@sim.model('nrh_estimate2')
def nrh_estimate2(costar, aggregations):
    return utils.hedonic_estimate("nrh2.yaml", costar, aggregations)


@sim.model('nrh_simulate')
def nrh_simulate2(buildings, aggregations):
    return utils.hedonic_simulate("nrh2.yaml", buildings, aggregations,
                                  "non_residential_price")