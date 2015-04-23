import urbansim.sim.simulation as sim
from urbansim.utils import misc
import os
import sys
import datasources
import variables
from urbansim.models import transition
from urbansim.developer import sqftproforma, developer
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


@sim.model('nrh_simulate2')
def nrh_simulate2(buildings, aggregations):
    return utils.hedonic_simulate("nrh2.yaml", buildings, aggregations,
                                  "nonres_rent_per_sqft")
                                  
@sim.model('rsh_estimate')
def rsh_estimate(assessor_transactions, aggregations):
    return utils.hedonic_estimate("rsh.yaml", assessor_transactions, aggregations)
    
@sim.model('rsh_simulate')
def rsh_simulate(buildings, aggregations):
    return utils.hedonic_simulate("rsh.yaml", buildings, aggregations,
                                  "res_price_per_sqft")

@sim.model('nrh_simulate')
def nrh_simulate(buildings, aggregations):
    return utils.hedonic_simulate("nrh.yaml", buildings, aggregations,
                                  "nonres_rent_per_sqft")
                                  
@sim.model('feasibility')
def feasibility(parcels, settings,
                parcel_sales_price_sqft_func,
                parcel_is_allowed_func):
    # Fee table preprocessing
    fee_schedule = sim.get_table('fee_schedule').to_frame()
    parcel_fee_schedule = sim.get_table('parcel_fee_schedule').to_frame()
    parcels = sim.get_table('parcels').to_frame(columns = ['zoning_id','development_type_id'])
    fee_schedule = fee_schedule.groupby(['fee_schedule_id', 'development_type_id']).development_fee_per_unit_space_initial.mean().reset_index()

    parcel_use_allowed_callback = sim.get_injectable('parcel_is_allowed_func')

    def run_proforma_lookup(parcels, fees, pf, use, form, residential_to_yearly, parcel_filter = None):
        if parcel_filter:
            parcels = parcels.query(parcel_filter)
        # add prices for each use (rents).  Apply fees
        parcels[use] = misc.reindex(sim.get_table('nodes')[use], sim.get_table('parcels').node_id) - fees

        # convert from cost to yearly rent
        if residential_to_yearly:
            parcels[use] *= pf.config.cap_rate

        print "Describe of the yearly rent by use"
        print parcels[use].describe()
        allowed = parcel_use_allowed_callback(form).loc[parcels.index]
        feasibility = pf.lookup(form, parcels[allowed], only_built=True,
                                    pass_through=[])

        print len(feasibility)

        return feasibility

    def residential_proforma(form, devtype_id, parking_rate):
        print form
        use = 'residential'
        parcels = sim.get_table('parcels').to_frame()
        
        residential_to_yearly = True
        parcel_filter = None
        pfc = sqftproforma.SqFtProFormaConfig()
        pfc.forms = {form: {use : 1.0}}
        pfc.uses = [use]
        pfc.residential_uses = [True]
        pfc.parking_rates = {use : parking_rate}
        pfc.costs = {use : [170.0, 190.0, 210.0, 240.0]}

        #Fees
        fee_schedule_devtype = fee_schedule[fee_schedule.development_type_id == devtype_id]
        parcel_fee_schedule_devtype = pd.merge(parcel_fee_schedule, fee_schedule_devtype, left_on = 'fee_schedule_id', right_on = 'fee_schedule_id')
        parcel_fee_schedule_devtype['development_fee_per_unit'] = parcel_fee_schedule_devtype.development_fee_per_unit_space_initial*parcel_fee_schedule_devtype.portion
        parcel_fees_processed = parcel_fee_schedule_devtype.groupby('parcel_id').development_fee_per_unit.sum()
        fees = pd.Series(data = parcel_fees_processed, index = parcels.index).fillna(0)

        pf = sqftproforma.SqFtProForma(pfc)
        
        return run_proforma_lookup(parcels, fees, pf, use, form, residential_to_yearly, parcel_filter = parcel_filter)

    def nonresidential_proforma(form, devtype_id, use, parking_rate):
        print form
        parcels = sim.get_table('parcels').to_frame()
        
        residential_to_yearly = False
        parcel_filter = None
        pfc = sqftproforma.SqFtProFormaConfig()
        pfc.forms = {form: {use : 1.0}}
        pfc.uses = [use]
        pfc.residential_uses = [False]
        pfc.parking_rates = {use : parking_rate}
        if use == 'retail':
            pfc.costs = {use : [160.0, 175.0, 200.0, 230.0]}
        elif use == 'industrial':
            pfc.costs = {use : [140.0, 175.0, 200.0, 230.0]}
        else: #office
            pfc.costs = {use : [160.0, 175.0, 200.0, 230.0]}

        #Fees
        fee_schedule_devtype = fee_schedule[fee_schedule.development_type_id == devtype_id]
        parcel_fee_schedule_devtype = pd.merge(parcel_fee_schedule, fee_schedule_devtype, left_on = 'fee_schedule_id', right_on = 'fee_schedule_id')
        parcel_fee_schedule_devtype['development_fee_per_unit'] = parcel_fee_schedule_devtype.development_fee_per_unit_space_initial*parcel_fee_schedule_devtype.portion
        parcel_fees_processed = parcel_fee_schedule_devtype.groupby('parcel_id').development_fee_per_unit.sum()
        fees = pd.Series(data = parcel_fees_processed, index = parcels.index).fillna(0)
        
        pf = sqftproforma.SqFtProForma(pfc)
        fees = fees*pf.config.cap_rate

        return run_proforma_lookup(parcels, fees, pf, use, form, residential_to_yearly, parcel_filter = parcel_filter)

    d = {}

    ##SF DETACHED proforma (devtype 19)
    form = 'sf_detached'
    devtype_id = 19
    d[form] = residential_proforma(form, devtype_id, parking_rate = 1.0)

    ##SF ATTACHED proforma (devtype 20)
    form = 'sf_attached'
    devtype_id = 20
    d[form] = residential_proforma(form, devtype_id, parking_rate = 1.0)

    ##MF_RESIDENTIAL proforma (devtype 21)
    form = 'mf_residential'
    devtype_id = 21
    d[form] = residential_proforma(form, devtype_id, parking_rate = 1.0)

    ##OFFICE (devtype 4)
    form = 'office'
    devtype_id = 4
    d[form] = nonresidential_proforma(form, devtype_id, form, parking_rate = 1.0)

    ##RETAIL (devtype 5)
    form = 'retail'
    devtype_id = 5
    d[form] = nonresidential_proforma(form, devtype_id, form, parking_rate = 2.0)

    ##LIGHT INDUSTRIAL (devtype 2)
    form = 'light_industrial'
    devtype_id = 2
    d[form] = nonresidential_proforma(form, devtype_id, 'industrial', parking_rate = .6)

    ##HEAVY INDUSTRIAL (devtype 3)
    form = 'heavy_industrial'
    devtype_id = 3
    d[form] = nonresidential_proforma(form, devtype_id, 'industrial', parking_rate = .6)

    far_predictions = pd.concat(d.values(), keys=d.keys(), axis=1)
    sim.add_table("feasibility", far_predictions)


@sim.model('residential_developer')
def residential_developer(parcels):
    feas = sim.get_table('feasibility').to_frame()
    dev = developer.Developer(feas)

    print "{:,} feasible buildings before running developer".format(
              len(dev.feasibility))

    defm_resunit_controls = pd.read_csv('data/defm_res_unit_controls.csv')
    buildings = sim.get_table('buildings').to_frame(columns = ['development_type_id', 'residential_units'])

    number_sf_units = buildings[buildings.development_type_id.isin([19, 20])].residential_units.sum()
    number_mf_units = buildings[buildings.development_type_id == 21].residential_units.sum()

    year = sim.get_injectable('year')
    if year is None:
        year = 2012
        
    sf_target = defm_resunit_controls[defm_resunit_controls.year == year].to_dict()['single_family'].itervalues().next()
    mf_target = defm_resunit_controls[defm_resunit_controls.year == year].to_dict()['multi_family'].itervalues().next()
    mf_difference = mf_target - number_mf_units
    sf_difference = sf_target - number_sf_units

    targets = {'mf_residential':mf_difference, 'sf_detached':sf_difference}

    for residential_form in ['sf_detached', 'sf_attached', 'mf_residential']:
        if residential_form in targets.keys():
            old_buildings = sim.get_table('buildings').to_frame(sim.get_table('buildings').local_columns)
            
            print residential_form
            new_buildings = dev.pick(residential_form,
                                     targets[residential_form],
                                     parcels.parcel_size,
                                     parcels.ave_sqft_per_unit,
                                     parcels.total_residential_units,
                                     max_parcel_size=2000000,
                                     min_unit_size=400,
                                     drop_after_build=True,
                                     residential=True,
                                     bldg_sqft_per_job=400.0)

            print new_buildings.residential_units.sum()
            
            new_buildings["year_built"] = year
            new_buildings["stories"] = new_buildings.stories.apply(np.ceil)
            if residential_form == 'sf_detached':
                new_buildings['development_type_id'] = 19
            elif residential_form == 'sf_attached':
                new_buildings['development_type_id'] = 20
            elif residential_form == 'mf_residential':
                new_buildings['development_type_id'] = 21
            new_buildings['improvement_value'] = 0
            new_buildings['note'] = 'simulated'
            new_buildings['res_price_per_sqft'] = 0.0
            new_buildings['nonres_rent_per_sqft'] = 0.0
            new_buildings = new_buildings[old_buildings.columns]
            all_buildings = dev.merge(old_buildings, new_buildings)
            sim.add_table("buildings", all_buildings)