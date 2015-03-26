import numpy as np
import pandas as pd
from urbansim.utils import misc
import urbansim.sim.simulation as sim
import datasources
from urbansim_defaults import utils
from urbansim_defaults import variables


#####################
# COSTAR VARIABLES
#####################

@sim.column('costar', 'node_id')
def node_id(parcels, costar):
    return misc.reindex(parcels.node_id, costar.parcel_id)

@sim.column('costar', 'mgra_id')
def mgra_id(parcels, costar):
    return misc.reindex(parcels.mgra_id, costar.parcel_id)
    
@sim.column('costar', 'luz_id')
def luz_id(parcels, costar):
    return misc.reindex(parcels.luz_id, costar.parcel_id)
    
@sim.column('costar', 'distance_to_coast')
def distance_to_coast(parcels, costar):
    return misc.reindex(parcels.distance_to_coast, costar.parcel_id)
    
@sim.column('costar', 'distance_to_freeway')
def distance_to_freeway(parcels, costar):
    return misc.reindex(parcels.distance_to_freeway, costar.parcel_id)
    
@sim.column('costar', 'distance_to_onramp')
def distance_to_onramp(parcels, costar):
    return misc.reindex(parcels.distance_to_onramp, costar.parcel_id)
    
@sim.column('costar', 'distance_to_park')
def distance_to_park(parcels, costar):
    return misc.reindex(parcels.distance_to_park, costar.parcel_id)
    
@sim.column('costar', 'distance_to_school')
def distance_to_school(parcels, costar):
    return misc.reindex(parcels.distance_to_school, costar.parcel_id)
    
@sim.column('costar', 'distance_to_transit')
def distance_to_transit(parcels, costar):
    return misc.reindex(parcels.distance_to_transit, costar.parcel_id)
    
# @sim.column('costar', 'nonres_occupancy')
# def nonres_occupancy(nodes, costar):
    # return misc.reindex(nodes.nonres_occupancy, costar.node_id)
    
@sim.column('costar', 'pecas_price')
def pecas_price(costar, pecas_prices):
    costar = costar.to_frame(columns = ['development_type_id', 'luz_id'])
    pecas_prices = pecas_prices.to_frame()
    year = sim.get_injectable('year')
    if year is None:
        year = 2012
    pecas_prices = pecas_prices[pecas_prices.year == year]
    costar.index.name = 'costar_id'
    merged = pd.merge(costar.reset_index(), pecas_prices, left_on = ['luz_id', 'development_type_id'], right_on = ['luz_id', 'development_type_id']).set_index('costar_id')
    return pd.Series(data = merged.price, index = costar.index).fillna(0)
    
    
####NODES
    
@sim.column('nodes', 'nonres_occupancy_3000m')
def nonres_occupancy_3000m(nodes):
    return nodes.jobs_3000m / (nodes.job_spaces_3000m + 1.0)
    
    
#####################
# BUILDINGS VARIABLES
#####################

@sim.column('buildings', 'job_spaces')
def job_spaces(parcels, buildings):
    return np.round(buildings.non_residential_sqft / 200.0)

@sim.column('buildings', 'luz_id')
def luz_id(buildings, parcels):
    return misc.reindex(parcels.luz_id, buildings.parcel_id)
    
@sim.column('buildings', 'pecas_price')
def pecas_price(buildings, pecas_prices):
    buildings = buildings.to_frame(columns = ['development_type_id', 'luz_id'])
    pecas_prices = pecas_prices.to_frame()
    year = sim.get_injectable('year')
    if year is None:
        year = 2012
    pecas_prices = pecas_prices[pecas_prices.year == year]
    merged = pd.merge(buildings.reset_index(), pecas_prices, left_on = ['luz_id', 'development_type_id'], right_on = ['luz_id', 'development_type_id']).set_index('building_id')
    return pd.Series(data = merged.price, index = buildings.index).fillna(0)
    
#####################
# HOUSEHOLD VARIABLES
#####################

@sim.column('households', 'luz_id', cache=True)
def luz_id(households, buildings):
    return misc.reindex(buildings.luz_id, households.building_id)
    
@sim.column('households', 'activity_id')
def activity_id(households):
    idx_38 = (households.income < 25000) & (households.persons < 3)
    idx_39 = (households.income < 25000) & (households.persons >= 3)
    idx_40 = (households.income >= 25000) & (households.income < 150000) & (households.persons < 3)
    idx_41 = (households.income >= 25000) & (households.income < 150000) & (households.persons >= 3)
    idx_42 = (households.income >= 150000) & (households.persons < 3)
    idx_43 = (households.income >= 150000) & (households.persons >= 3)
    return 38*idx_38 + 39*idx_39 + 40*idx_40 + 41*idx_41 + 42*idx_42 + 43*idx_43
    