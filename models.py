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
    
@sim.model('elcm_basic_estimate')
def elcm_basic_estimate(jobs, buildings, aggregations):
    return utils.lcm_estimate("elcm_basic.yaml", jobs, "building_id",
                              buildings, aggregations)


@sim.model('elcm_basic_simulate')
def elcm_basic_simulate(jobs, buildings, aggregations):
    return utils.lcm_simulate("elcm_basic.yaml", jobs, buildings, aggregations,
                              "building_id", "job_spaces",
                              "vacant_job_spaces")

@sim.model('households_transition')
def households_transition(households, annual_household_control_totals, year):
    ct = annual_household_control_totals.to_frame()
    tran = transition.TabularTotalsTransition(ct, 'total_number_of_households')
    model = transition.TransitionModel(tran)
    hh = households.to_frame(households.local_columns + ['activity_id', 'luz_id'])
    new, added_hh_idx, empty_dict = \
        model.transition(hh, year,)
    new.loc[added_hh_idx, "building_id"] = -1
    sim.add_table("households", new)
    
@sim.model('hlcm_luz_estimate')
def hlcm_luz_estimate(households, buildings, aggregations):
    return utils.lcm_estimate("hlcm_luz.yaml", households, "building_id",
                              buildings, aggregations)
                              
@sim.model('hlcm_luz_simulate')
def hlcm_luz_simulate(households, buildings, aggregations):
    cfg = "hlcm_luz.yaml"
    choosers = households
    buildings = buildings
    join_tbls = aggregations
    out_fname = "building_id"
    supply_fname = "residential_units"
    vacant_fname = "vacant_residential_units"
    cfg = misc.config(cfg)
    
    #Regional choosers
    choosers_df = utils.to_frame(choosers, [], cfg, additional_columns=[out_fname, 'base_luz'])
    movers = choosers_df[choosers_df[out_fname] == -1]
    print "There are %d total movers for this LCM" % len(movers)
    
    #Regional alternatives
    additional_columns = [supply_fname, vacant_fname, 'luz_id_buildings']
    locations_df = utils.to_frame(buildings, join_tbls, cfg,
                            additional_columns=additional_columns)
    buildings_df = buildings.to_frame(columns = [vacant_fname, 'luz_id_buildings'])
    buildings_df = buildings_df[buildings_df[vacant_fname] > 0]
    vacant_units_regional = buildings_df[vacant_fname]
    luz_id_buildings = buildings_df.luz_id_buildings
    
    
    for luz in np.unique(movers.base_luz):
        print "HLCM for LUZ %s" % luz
        
        movers_luz = movers[movers.base_luz == luz]
        locations_df_luz = locations_df[locations_df.luz_id_buildings == luz]

        available_units = buildings[supply_fname][buildings.luz_id_buildings == luz]
        vacant_units = vacant_units_regional[luz_id_buildings == luz]
        
        print "There are %d total available units" % available_units.sum()
        print "    and %d total choosers" % len(movers_luz)
        print "    but there are %d overfull buildings" % \
              len(vacant_units[vacant_units < 0])

        
        indexes = np.repeat(vacant_units.index.values,
                            vacant_units.values.astype('int'))
        # isin = pd.Series(indexes).isin(locations_df_luz.index)
        # missing = len(isin[isin == False])
        # indexes = indexes[isin.values]
        units = locations_df_luz.loc[indexes].reset_index()
        utils.check_nas(units)

        print "    for a total of %d temporarily empty units" % vacant_units.sum()
        print "    in %d buildings total in the region" % len(vacant_units)

        # if missing > 0:
            # print "WARNING: %d indexes aren't found in the locations df -" % \
                # missing
            # print "    this is usually because of a few records that don't join "
            # print "    correctly between the locations df and the aggregations tables"

        
        if len(movers_luz) > vacant_units.sum():
            print "WARNING: Not enough locations for movers"
            print "    reducing locations to size of movers for performance gain"
            movers_luz = movers_luz.head(vacant_units.sum())

        new_units, _ = utils.yaml_to_class(cfg).predict_from_cfg(movers_luz, units, cfg)
        
        # new_units returns nans when there aren't enough units,
        # get rid of them and they'll stay as -1s
        new_units = new_units.dropna()
        
        # go from units back to buildings
        new_buildings = pd.Series(units.loc[new_units.values][out_fname].values,
                                  index=new_units.index)
        
        choosers.update_col_from_series(out_fname, new_buildings)
        utils._print_number_unplaced(choosers, out_fname)
        
        # vacant_units = buildings.to_frame(columns = [vacant_fname])[vacant_fname][buildings.luz_id_buildings == luz]
        # print "    and there are now %d empty units" % vacant_units.sum()
        # print "    and %d overfull buildings" % len(vacant_units[vacant_units < 0])
    
@sim.model('hlcm_simulate')
def hlcm_simulate(households, buildings, aggregations, settings):
    return utils.lcm_simulate("hlcm.yaml", households, buildings,
                              aggregations,
                              "building_id", "residential_units",
                              "vacant_residential_units",
                              settings.get("enable_supply_correction", None))
    
@sim.model('jobs_transition')
def jobs_transition(jobs):
    return utils.simple_transition(jobs, .05, "building_id")
    
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
        
        #Calibration shifters
        calibration_shifters = pd.read_csv('.\\data\\calibration\\msa_shifters.csv').set_index('msa_id').to_dict()
        
        if use == 'residential':
            shifter_name = 'res_price_shifter'
        else:
            shifter_name = 'nonres_price_shifter'
        parcels[shifter_name] = 1.0
        shifters = calibration_shifters[shifter_name]
        for msa_id in shifters.keys():
            shift = shifters[msa_id]
            parcels[shifter_name][parcels.msa_id == msa_id] = shift
            
        parcels[use] = parcels[use] * parcels[shifter_name]
       
        # convert from cost to yearly rent
        if residential_to_yearly:
            parcels[use] *= pf.config.cap_rate
            
        # Price minimum if hedonic predicts outlier
        parcels[use][parcels[use] <= .5] = .5
        parcels[use][parcels[use].isnull()] = .5

        print "Describe of the yearly rent by use"
        print parcels[use].describe()
        allowed = parcel_use_allowed_callback(form).loc[parcels.index]
        feasibility = pf.lookup(form, parcels[allowed], only_built=True,
                                    pass_through=[])
                                    
        if use == 'residential':
            def iter_feasibility(feasibility, price_scaling_factor):
                if price_scaling_factor > 3.0:
                    return feasibility
                # Get targets
                target_units = residential_space_targets()[form]
                #Calculate number of profitable units
                d = {}
                d[form] = feasibility
                feas = pd.concat(d.values(), keys=d.keys(), axis=1)
                dev = developer.Developer(feas)
                profitable_units = run_developer(dev, form, target_units, get_year(), build = False)

                print 'Feasibility given current prices/zonining indicates %s profitable units and target of %s' % (profitable_units, target_units)
                
                if profitable_units < target_units:
                    price_scaling_factor += .1
                    print 'Scaling prices up by factor of %s' % price_scaling_factor
                    parcels[use] = parcels[use] * price_scaling_factor
                    feasibility = pf.lookup(form, parcels[allowed], only_built=True,
                                        pass_through=[])
                                        
                    return iter_feasibility(feasibility, price_scaling_factor)
                else:
                    price_scaling_factor += .1
                    parcels[use] = parcels[use] * price_scaling_factor
                    feasibility = pf.lookup(form, parcels[allowed], only_built=True,
                                        pass_through=[])
                    return feasibility
            feasibility = iter_feasibility(feasibility, 1.0)
            
        elif use != 'residential':
            def iter_feasibility(feasibility, price_scaling_factor):
                if price_scaling_factor > 3.0:
                    return feasibility
                # Get targets
                targets = non_residential_space_targets()
                target_units = targets[form]/400
                #Calculate number of profitable units
                feasibility['current_units'] = parcels.total_job_spaces
                feasibility["parcel_size"] = parcels.parcel_size
                feasibility = feasibility[feasibility.parcel_size < 200000]
                feasibility['job_spaces'] = np.round(feasibility.non_residential_sqft / 400.0)
                feasibility['net_units'] = feasibility.job_spaces - feasibility.current_units
                feasibility.net_units = feasibility.net_units.fillna(0)
                profitable_units = int(feasibility.net_units.sum())
                print 'Feasibility given current prices/zonining indicates %s profitable units and target of %s' % (profitable_units, target_units)
                
                if profitable_units < target_units:
                    price_scaling_factor += .1
                    print 'Scaling prices up by factor of %s' % price_scaling_factor
                    parcels[use] = parcels[use] * price_scaling_factor
                    feasibility = pf.lookup(form, parcels[allowed], only_built=True,
                                        pass_through=[])
                                        
                    return iter_feasibility(feasibility, price_scaling_factor)
                else:
                    return feasibility
            feasibility = iter_feasibility(feasibility, 1.0)

        print len(feasibility)
        return feasibility

    def residential_proforma(form, devtype_id, parking_rate):
        print form
        use = 'residential'
        parcels = sim.get_table('parcels').to_frame()
        
        residential_to_yearly = True
        # parcel_filter = settings['feasibility']['parcel_filter']
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
        # parcel_filter = settings['feasibility']['parcel_filter']
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
    
def get_year():
    year = sim.get_injectable('year')
    if year is None:
        year = 2012
    return year
    
def residential_space_targets():
    defm_resunit_controls = pd.read_csv('data/defm_res_unit_controls.csv')
    buildings = sim.get_table('buildings').to_frame(columns = ['development_type_id', 'residential_units'])

    number_sf_units = buildings[buildings.development_type_id.isin([19])].residential_units.sum()
    number_sfa_units = buildings[buildings.development_type_id == 20].residential_units.sum()
    number_mf_units = buildings[buildings.development_type_id == 21].residential_units.sum()

    year = get_year()
        
    sf_target = defm_resunit_controls[defm_resunit_controls.year == year].to_dict()['single_family'].itervalues().next()
    sfa_target = defm_resunit_controls[defm_resunit_controls.year == year].to_dict()['sf_attached'].itervalues().next()
    mf_target = defm_resunit_controls[defm_resunit_controls.year == year].to_dict()['multi_family'].itervalues().next()
    mf_difference = mf_target - number_mf_units
    sf_difference = sf_target - number_sf_units
    sfa_difference = sfa_target - number_sfa_units

    targets = {'mf_residential':mf_difference, 'sf_detached':sf_difference, 'sf_attached':sfa_difference}
    return targets
    
def non_residential_space_targets():
    vacancy_multiplier = 1.7
    defm_nonres_controls = pd.read_csv('data/non_res_space_control.csv')
    buildings = sim.get_table('buildings').to_frame(columns = ['development_type_id', 'non_residential_sqft'])
    
    light_industrial_sqft = buildings[buildings.development_type_id == 2].non_residential_sqft.sum()
    heavy_industrial_sqft = buildings[buildings.development_type_id == 3].non_residential_sqft.sum()
    office_sqft = buildings[buildings.development_type_id == 4].non_residential_sqft.sum()
    retail_sqft = buildings[buildings.development_type_id == 5].non_residential_sqft.sum()
    
    year = get_year()
        
    defm_nonres_controls = defm_nonres_controls[defm_nonres_controls.yr == year]
    light_industrial_target = defm_nonres_controls[defm_nonres_controls.development_type_id == 2].total_min_sqft.values[0] * vacancy_multiplier
    heavy_industrial_target = defm_nonres_controls[defm_nonres_controls.development_type_id == 3].total_min_sqft.values[0] * vacancy_multiplier
    office_target = defm_nonres_controls[defm_nonres_controls.development_type_id == 4].total_min_sqft.values[0] * vacancy_multiplier
    retail_target = defm_nonres_controls[defm_nonres_controls.development_type_id == 5].total_min_sqft.values[0] * vacancy_multiplier
    
    light_industrial_difference = light_industrial_target - light_industrial_sqft
    heavy_industrial_difference = heavy_industrial_target - heavy_industrial_sqft
    office_difference = office_target - office_sqft
    retail_difference = retail_target - retail_sqft
    
    targets = {'light_industrial':light_industrial_difference, 'heavy_industrial':heavy_industrial_difference, 'office':office_difference, 'retail':retail_difference}
    return targets

def run_developer(dev, residential_form, target, year, build = False):
    old_buildings = sim.get_table('buildings').to_frame(sim.get_table('buildings').local_columns)
    parcels = sim.get_table('parcels')
    print 'Residential unit target for %s is %s.' % (residential_form, target)
    if target > 0:
        print residential_form
        drop_after_build = True if build else False
        new_buildings = dev.pick(residential_form,
                                 target,
                                 parcels.parcel_size,
                                 parcels.ave_sqft_per_unit,
                                 parcels.total_residential_units,
                                 max_parcel_size=2000000,
                                 min_unit_size=400,
                                 drop_after_build=True,
                                 residential=True,
                                 bldg_sqft_per_job=400.0)
        if build:
            print 'Constructed %s %s buildings, totaling %s new residential_units' % (len(new_buildings), residential_form, new_buildings.residential_units.sum())
            overshoot = new_buildings.residential_units.sum() - target
            print 'Overshot target by %s units' % (overshoot)
            print 'Biggest development has %s units' % new_buildings.residential_units.max()
            if overshoot > 1:
                to_remove = new_buildings[['parcel_id', 'residential_units']].copy()
                to_remove = to_remove[to_remove.residential_units < 4].set_index('parcel_id')
                to_remove = to_remove.sort('residential_units')
                to_remove['du_cumsum'] = to_remove.residential_units.cumsum()
                idx_to_remove = np.searchsorted(to_remove.du_cumsum, overshoot)
                parcel_ids_to_remove = to_remove.index.values[:(idx_to_remove[0] + 1)]
                print 'Removing %s units to match target' % to_remove.residential_units.values[:(idx_to_remove[0] + 1)].sum()
                new_buildings = new_buildings[~new_buildings.parcel_id.isin(parcel_ids_to_remove)]
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
            
            # Remove redeveloped buildings
            redev_buildings = old_buildings.parcel_id.isin(new_buildings.parcel_id)
            l = len(old_buildings)
            drop_buildings = old_buildings[redev_buildings]
            old_buildings = old_buildings[np.logical_not(redev_buildings)]
            l2 = len(old_buildings)
            if l2-l > 0:
                print "Dropped {} buildings because they were redeveloped".\
                    format(l2-l)

            for tbl in ['households', 'jobs']:
                agents = sim.get_table(tbl)
                agents = agents.to_frame(agents.local_columns)
                displaced_agents = agents.building_id.isin(drop_buildings.index)
                print "Unplaced {} before: {}".format(tbl, len(agents.query(
                                                      "building_id == -1")))
                agents.building_id[displaced_agents] = -1
                print "Unplaced {} after: {}".format(tbl, len(agents.query(
                                                     "building_id == -1")))
                sim.add_table(tbl, agents)
            
            # Update buildings table
            all_buildings = dev.merge(old_buildings, new_buildings)
            sim.add_table("buildings", all_buildings)
        
        else:
            return new_buildings.residential_units.sum()
    
@sim.model('residential_developer')
def residential_developer(parcels):
    feas = sim.get_table('feasibility').to_frame()
    
    year = get_year()
              
    targets = residential_space_targets()
    
    print "{:,} feasible buildings before running developer".format(
          len(feas))
          
    # LUZ overrides, if applicable
    p = sim.get_table('parcels').to_frame(columns = ['luz_id', 'total_residential_units', 'total_sfd_du', 'total_sfa_du', 'total_mfr_du'])
    feas['luz_id'] = p.luz_id
    overrides = pd.read_csv('./data/overrides/luz_overrides.csv')
    overrides = overrides[overrides.year == year]
    controlled_luzes = np.unique(overrides.luz_id)
    
    if len(overrides) > 0:
        
        #Record existing LUZ values
        existing_du = p.groupby('luz_id').total_residential_units.sum()
        existing_sfd_du = p.groupby('luz_id').total_sfd_du.sum()
        existing_sfa_du = p.groupby('luz_id').total_sfa_du.sum()
        existing_mfr_du = p.groupby('luz_id').total_mfr_du.sum()
        existing_df = pd.DataFrame({'existing_du':existing_du,
                           19:existing_sfd_du,
                           20:existing_sfa_du,
                           21:existing_mfr_du}).fillna(0)   
        mini_feases = []
        for luz in controlled_luzes:
            overrides_subset = overrides[overrides.luz_id == luz]
            for devtype in np.unique(overrides_subset.development_type_id):
                target = overrides_subset.target[overrides_subset.development_type_id == devtype].values[0]
                print 'LUZ %s has a DU override target for development type %s of %s.' % (luz, devtype, target)
                existing_du = existing_df[devtype][existing_df.index == luz].values[0]
                print '    There are %s existing units of this type in this LUZ' % existing_du
                difference = target - existing_du
                if difference > 0:
                    feas_subset = feas[feas.luz_id == luz]
                    if len(feas_subset) > 0:
                        if devtype == 19:  residential_form = 'sf_detached'
                        if devtype == 20:  residential_form = 'sf_attached'
                        if devtype == 21:  residential_form = 'mf_residential'
                        feasible_units = feas_subset[residential_form].net_units.sum()
                        if feasible_units > 0:
                            if difference > feasible_units:
                                reallocate = difference - feasible_units
                                print '    Moving %s units to the uncontrolled bucket because only part of the target difference was feasible' % reallocate
                                targets[residential_form] = targets[residential_form] + reallocate
                            dev_luz = developer.Developer(feas_subset)
                            run_developer(dev_luz, residential_form, difference, year, build = True)
                            #Store the unbuilt feasible parcels and add back into feasibility later
                            mini_feases.append(dev_luz.feasibility)
                        else:
                            print '    No profitable projects'
    
    #Uncontrolled LUZs
    print 'Running Developer for uncontrolled LUZs'
    feas = feas[~feas.luz_id.isin(controlled_luzes)]
    dev = developer.Developer(feas)
    for residential_form in ['mf_residential', 'sf_attached', 'sf_detached', ]:
        if residential_form in targets.keys():
            target = targets[residential_form]
            run_developer(dev, residential_form, target, year, build = True)

    #Remaining feasible parcels back to feas after running controlled LUZs so that nonres can be built on these parcels if multiple forms allowed and not already built on
    if len(overrides) > 0:
        feas_list = mini_feases.append(dev.feasibility)
        feas = pd.concat(mini_feases)
        sim.add_table("feasibility", feas)
    else:
        sim.add_table("feasibility", dev.feasibility)
    
    b = sim.get_table('buildings')
    b = b.to_frame(b.local_columns)
    b_sim = b[(b.note == 'simulated') * (b.year_built == year)]
    print 'Simulated DU: %s' % b_sim.residential_units.sum()
    print 'Target DU: %s' % (targets['sf_detached'] + targets['mf_residential'] + targets['sf_attached']) #Note:  includes negative when target is lower.
            
@sim.model('non_residential_developer')
def non_residential_developer(parcels):
    feas = sim.get_table('feasibility').to_frame()
    dev = developer.Developer(feas)
    
    print "{:,} feasible buildings before running developer".format(
              len(dev.feasibility))
    
    year = get_year()
    
    targets = non_residential_space_targets()
           
    for non_residential_form in ['heavy_industrial', 'light_industrial', 'retail', 'office']:
        if non_residential_form in targets.keys():
            old_buildings = sim.get_table('buildings').to_frame(sim.get_table('buildings').local_columns)

            target = targets[non_residential_form]
            target = target/400
            print 'Job space target for %s is %s.' % (non_residential_form, target)
            
            if target > 0:
                new_buildings = dev.pick(non_residential_form,
                                         target,
                                         parcels.parcel_size,
                                         parcels.ave_sqft_per_unit,
                                         parcels.total_job_spaces,
                                         max_parcel_size=2000000,
                                         min_unit_size=0,
                                         drop_after_build=True,
                                         residential=False,
                                         bldg_sqft_per_job=400.0)
                
                print 'Constructed %s %s buildings, totaling %s new job spaces.' % (len(new_buildings), non_residential_form, new_buildings.non_residential_sqft.sum()/400)
                
                new_buildings["year_built"] = year
                new_buildings["stories"] = new_buildings.stories.apply(np.ceil)
                if non_residential_form == 'light_industrial':
                    new_buildings['development_type_id'] = 2
                elif non_residential_form == 'heavy_industrial':
                    new_buildings['development_type_id'] = 3
                elif non_residential_form == 'office':
                    new_buildings['development_type_id'] = 4
                elif non_residential_form == 'retail':
                    new_buildings['development_type_id'] = 5
                new_buildings['improvement_value'] = 0
                new_buildings['note'] = 'simulated'
                new_buildings['res_price_per_sqft'] = 0.0
                new_buildings['nonres_rent_per_sqft'] = 0.0
                new_buildings = new_buildings[old_buildings.columns]
                
                # Remove redeveloped buildings
                redev_buildings = old_buildings.parcel_id.isin(new_buildings.parcel_id)
                l = len(old_buildings)
                drop_buildings = old_buildings[redev_buildings]
                old_buildings = old_buildings[np.logical_not(redev_buildings)]
                l2 = len(old_buildings)
                if l2-l > 0:
                    print "Dropped {} buildings because they were redeveloped".\
                        format(l2-l)

                for tbl in ['households', 'jobs']:
                    agents = sim.get_table(tbl)
                    agents = agents.to_frame(agents.local_columns)
                    displaced_agents = agents.building_id.isin(drop_buildings.index)
                    print "Unplaced {} before: {}".format(tbl, len(agents.query(
                                                          "building_id == -1")))
                    agents.building_id[displaced_agents] = -1
                    print "Unplaced {} after: {}".format(tbl, len(agents.query(
                                                         "building_id == -1")))
                    sim.add_table(tbl, agents)
                    
                # Update buildings table
                all_buildings = dev.merge(old_buildings, new_buildings)
                sim.add_table("buildings", all_buildings)
                
    sim.add_table("feasibility", dev.feasibility)

    
@sim.model('scheduled_development_events')
def scheduled_development_events(buildings):
    year = get_year()
    sched_dev = pd.read_csv("./data/scheduled_development_events.csv")
    sched_dev = sched_dev[sched_dev.year_built==year]
    sched_dev['residential_sqft'] = sched_dev.sqft_per_unit*sched_dev.residential_units
    sched_dev['job_spaces'] = sched_dev.non_residential_sqft/400
    if len(sched_dev) > 0:
        max_bid = buildings.index.values.max()
        idx = np.arange(max_bid + 1,max_bid+len(sched_dev)+1)
        sched_dev['building_id'] = idx
        sched_dev = sched_dev.set_index('building_id')
        from urbansim.developer.developer import Developer
        merge = Developer(pd.DataFrame({})).merge
        b = buildings.to_frame(buildings.local_columns)
        all_buildings = merge(b,sched_dev[b.columns])
        sim.add_table("buildings", all_buildings)
        
@sim.model('model_integration_indicators')
def model_integration_indicators():
    year = get_year()
    
    #Households by MGRA to PASEF
    print 'Exporting indicators: households by MGRA to PASEF'
    hh = sim.get_table('households')
    hh = hh.to_frame(hh.local_columns + ['mgra_id', 'activity_id'])
    pasef_indicators = hh.groupby(['mgra_id', 'activity_id']).size().reset_index()
    pasef_indicators.columns = ['mgra_id', 'activity_id', 'number_of_households']
    pasef_indicators.to_csv('./data/pasef/mgra_hh_%s.csv'%year, index = False)
    
    #Space by LUZ to PECAS
    print 'Exporting indicators: space by LUZ to PECAS'
    b = sim.get_table('buildings')
    b = b.to_frame(b.local_columns + ['luz_id'])
    pecas_res_indicators = b[b.residential_units > 0].groupby(['luz_id', 'development_type_id']).residential_units.sum().reset_index()
    pecas_res_indicators.columns = ['luz_id', 'development_type_id', 'residential_units']
    pecas_res_indicators.to_csv('./data/pecas_urbansim_exchange/luz_du_%s.csv'%year, index = False)
    
    pecas_nonres_indicators = b[b.non_residential_sqft > 0].groupby(['luz_id', 'development_type_id']).non_residential_sqft.sum().reset_index()
    pecas_nonres_indicators.columns = ['luz_id', 'development_type_id', 'non_residential_sqft']
    pecas_nonres_indicators.to_csv('./data/pecas_urbansim_exchange/luz_nrsf_%s.csv'%year, index = False)