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
    df = df[df.building_id > 0]
    return df
    
@sim.table('fee_schedule', cache=True)
def fee_schedule(store):
    df = store['fee_schedule']
    return df
    
@sim.table('parcel_fee_schedule', cache=True)
def parcel_fee_schedule(store):
    df = store['parcel_fee_schedule']
    return df
    
@sim.table('zoning', cache=True)
def zoning(store):
    df = store['zoning']
    return df
    
@sim.table('zoning_allowed_uses', cache=True)
def zoning_allowed_uses(store, parcels):
    parcels_allowed = store['zoning_allowed_uses']
    parcels = sim.get_table('parcels').to_frame(columns = ['zoning_id',])
    
    allowed_df = pd.DataFrame(index = parcels.index)
    for devtype in np.unique(parcels_allowed.development_type_id):
        devtype_allowed = parcels_allowed[parcels_allowed.development_type_id == devtype].set_index('zoning_id')
        allowed = misc.reindex(devtype_allowed.development_type_id, parcels.zoning_id)
        df = pd.DataFrame(index=allowed.index)
        df['allowed'] = False
        df[~allowed.isnull()] = True
        allowed_df[devtype] = df.allowed

    return allowed_df
    
@sim.table('households', cache=True)
def households(store):
    df = store['households']
    return df
    
@sim.table('buildings', cache=True)
def buildings(store):
    df = store['buildings']
    df['res_price_per_sqft'] = 0.0
    df['nonres_rent_per_sqft'] = 0.0
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
    
# residential price data
@sim.table('assessor_transactions', cache=True)
def assessor_transactions(store):
    df = store['assessor_transactions']
    df["index"] = df.index
    df.drop_duplicates(cols='index', take_last=True, inplace=True)
    del df["index"]
    return df

# luz price from pecas
@sim.table('pecas_prices', cache=True)
def pecas_prices(store):
    df = store['pecas_prices']
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
sim.broadcast('nodes', 'assessor_transactions', cast_index=True, onto_on='node_id')
sim.broadcast('parcels', 'assessor_transactions', cast_index=True, onto_on='parcel_id')