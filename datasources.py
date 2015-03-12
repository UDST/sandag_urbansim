import numpy as np
import pandas as pd
import os
from urbansim_defaults import datasources
from urbansim_defaults import utils
from urbansim.utils import misc
import urbansim.sim.simulation as sim

@sim.table('jobs', cache=True)
def jobs(store):
    df = store['jobs']
    return df
    
@sim.table('households', cache=True)
def households(store):
    df = store['households']
    return df
    
@sim.table('buildings', cache=True)
def buildings(store):
    df = store['buildings']
    return df
    
@sim.table('parcels', cache=True)
def parcels(store):
    df = store['parcels']
    return df
    
@sim.injectable('building_sqft_per_job', cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']
    
# non-residential rent data
@sim.table('costar', cache=True)
def costar(store):
    df = store['costar']
    return df

# a table of home sales data
# @sim.table('homesales', cache=True)
# def homesales(store):
    # df = store['homesales']
    # df = df.reset_index(drop=True)
    # return df


# this specifies the relationships between tables
sim.broadcast('nodes', 'costar', cast_index=True, onto_on='node_id')
sim.broadcast('parcels', 'costar', cast_index=True, onto_on='parcel_id')