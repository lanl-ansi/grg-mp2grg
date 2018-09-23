'''functions for reading and writing matpower data files'''

from __future__ import print_function

import warnings
import itertools
import numbers
import copy
import argparse
import math
import json
import functools
import sys

from grg_mpdata.exception import MPDataParsingError
from grg_mpdata.exception import MPDataValidationError
from grg_mpdata.exception import MPDataWarning
from grg_mp2grg.exception import MP2GRGWarning

from grg_mpdata.io import _parse_matrix
from grg_mpdata.io import _extract_assignment_line

from grg_mpdata.cmd import diff

from grg_grgdata.cmd import flatten_network
from grg_grgdata.cmd import components_by_type
from grg_grgdata.cmd import collapse_voltage_points
from grg_grgdata.cmd import active_voltage_points
from grg_grgdata.cmd import isolated_voltage_points
from grg_grgdata.cmd import voltage_level_by_voltage_point

from grg_mp2grg.struct import Bus
from grg_mp2grg.struct import Generator
from grg_mp2grg.struct import GeneratorCost
from grg_mp2grg.struct import Branch
from grg_mp2grg.struct import DCLine
from grg_mp2grg.struct import Bus
from grg_mp2grg.struct import Case

from grg_mpdata.struct import BusName

import grg_mp2grg.common as common
import grg_grgdata.common as grg_common


print_err = functools.partial(print, file=sys.stderr)

def parse_mp_case_file(mpFileName):
    '''opens the given path and parses it as matpower data

    Args:
        mpFileName(str): path to the a matpower data file
    Returns:
        Case: a mpdata case
    '''
    mpFile = open(mpFileName, 'r')
    lines = mpFile.readlines()
    return parse_mp_case_lines(lines)


def parse_grg_case_file(grg_file_name):
    '''opens the given path and parses it as json data

    Args:
        grg_file_name(str): path to the a json data file
    Returns:
        Dict: a dictionary case
    '''

    # TODO validate format via grg_grgdata library!
    with open(grg_file_name, 'r') as grg_data:
        data = json.load(grg_data)
        grg_data.close()

    return data


# TODO see if there is away not to replicate this code
def parse_mp_case_lines(mpLines):
    '''parses a list of strings as matpower data

    Args:
        mpLines(list): the list of matpower data strings
    Returns:
        Case: a grg_mp2grg case
    '''

    version = None
    name = None
    baseMVA = None

    bus = None
    gen = None
    branch = None
    gencost = None
    dcline = None

    parsed_matrixes = []

    last_index = len(mpLines)
    index = 0
    while index < last_index:
        line = mpLines[index].strip()
        if len(line) == 0 or line.startswith('%'):
            index += 1
            continue

        if 'function mpc' in line:
            name = _extract_assignment_line(line).val
        elif 'mpc.version' in line:
            version = _extract_assignment_line(line).val
        elif 'mpc.baseMVA' in line:
            baseMVA = float(_extract_assignment_line(line).val)
        elif '[' in line:
            matrix = _parse_matrix(mpLines, index)
            parsed_matrixes.append(matrix)
            index += matrix['line_count']-1
        index += 1

    for parsed_matrix in parsed_matrixes:
        if parsed_matrix['name'] == 'mpc.bus':
            bus = [Bus(*data) for data in parsed_matrix['data']]

        elif parsed_matrix['name'] == 'mpc.gen':
            gen = [Generator(index, *data)
                   for index, data in enumerate(parsed_matrix['data'])]

        elif parsed_matrix['name'] == 'mpc.branch':
            branch = [Branch(index, *data)
                      for index, data in enumerate(parsed_matrix['data'])]

        elif parsed_matrix['name'] == 'mpc.dcline':
            dcline = [DCLine(index, *data)
                      for index, data in enumerate(parsed_matrix['data'])]

        elif parsed_matrix['name'] == 'mpc.gencost':
            gencost = []
            for index, data in enumerate(parsed_matrix['data']):
                gencost.append(GeneratorCost(index, *data[:4], cost=data[4:]))

        else:
            warnings.warn('unrecognized data matrix named \'%s\': data was '
                'ignored' % parsed_matrix['name'], MPDataWarning)

    case = Case(name, version, baseMVA, bus, gen, branch, gencost, dcline)

    case.validate()

    return case


def build_mp_case(grg_data, mapping_ids=None, add_gen_costs=False, add_bus_names=False):
    # TODO see if this grg_mp2grg case is ok, and should not be grg_mpdata

    #print(json.dumps(flat_components, sort_keys=True, indent=2, separators=(',', ': ')))

    # TODO this functionality should be in grg data structure (components-by-type)
    float_precision = grg_common.default_float_precision

    if mapping_ids == None:
        mapping_ids = grg_data['mappings'].keys()

    master_mapping = {}
    for mid in mapping_ids:
        for (k,v) in grg_data['mappings'][mid].items():
            if k in master_mapping:
                assert(isinstance(master_mapping[k], dict) and isinstance(v, dict))
                master_mapping[k].update(v)
            else:
                master_mapping[k] = v

    operations = None
    if 'operation_constraints' in grg_data:
        operations = grg_data['operation_constraints']

    market = None
    if 'market' in grg_data:
        market = grg_data['market']

    if not grg_data['network']['per_unit']:
        print_err('network data not given in per unit')
        return

    base_mva = 100.0
    if 'base_mva' in grg_data['network']:
        base_mva = grg_data['network']['base_mva']

    cbt = components_by_type(grg_data)
    # print_err('comps: {}'.format(cbt.keys()))

    status_assignment = {}
    for key, value in master_mapping.items():
        if key.count('/') == 1 and key.endswith('/status'):
            status_assignment[key.split('/')[0]] = value

    vp2int = collapse_voltage_points(grg_data, status_assignment)
    # print_err('voltage points to int:')
    # print_err(vp2int)

    avps = active_voltage_points(grg_data, status_assignment)
    # print_err('active voltage points:')
    # print_err(avps)

    ivps = isolated_voltage_points(grg_data, status_assignment)
    # print_err('isolated voltage points:')
    # print_err(ivps)

    vlbvp = voltage_level_by_voltage_point(grg_data)
    #print_err(vlbvp)

    if all('source_id' in bus for bus in cbt['bus']):
        # TODO check for clashes with other voltage point ints
        number_update = {}
        for bus in cbt['bus']:
            number_update[vp2int[bus['link']]] = int(bus['source_id'])
            #vp2int[bus['link']] = int(bus['source_id'])

        for k,v in vp2int.items():
            if v in number_update:
                vp2int[k] = number_update[v]
    else:
        # make 1 based, becouse required by matpower
        for k,v in vp2int.items():
            vp2int[k] = v+1


    buses_by_bid = {}
    for bus in cbt['bus']:
        bid = vp2int[bus['link']]
        if not bid in buses_by_bid:
            buses_by_bid[bid] = []
        buses_by_bid[bid].append(bus)

    loads_by_bid = {}
    for load in cbt['load']:
        bid = vp2int[load['link']]
        if not bid in loads_by_bid:
            loads_by_bid[bid] = []
        loads_by_bid[bid].append(load)

    shunts_by_bid = {}
    for shunt in cbt['shunt']:
        bid = vp2int[shunt['link']]
        if not bid in shunts_by_bid:
            shunts_by_bid[bid] = []
        shunts_by_bid[bid].append(shunt)

    bid_with_active_gen = set()
    for gen in cbt['generator']:
        if gen['link'] in avps:
            bid_with_active_gen.add(vp2int[gen['link']])

    for sc in cbt['synchronous_condenser']:
        if sc['link'] in avps:
            bid_with_active_gen.add(vp2int[sc['link']])

    mp_buses = []
    mp_gens = []
    mp_branches = []
    mp_gencosts = None
    mp_dclines = None
    mp_busnames = None


    areas = {k:grp for k,grp in grg_data['groups'].items() if grp['type'] == 'area'}
    zones = {k:grp for k,grp in grg_data['groups'].items() if grp['type'] == 'zone'}

    area_index_lookup = {}
    if all('source_id' in area for k,area in areas.items()):
        for k,area in areas.items():
            for comp_id in area['component_ids']:
                if not comp_id in area_index_lookup:
                    area_index_lookup[comp_id] = int(area['source_id'])
                else:
                    warnings.warn('component %s is in multiple areas only %s will be used.' % (comp_id, area_index_lookup[comp_id]), MP2GRGWarning)
    else:
        idx = 1
        for k,area in areas.items():
            for comp_id in area['component_ids']:
                if not comp_id in area_index_lookup:
                    area_index_lookup[comp_id] = idx
                else:
                    warnings.warn('component %s is in multiple areas only %s will be used.' % (comp_id, area_index_lookup[comp_id]), MP2GRGWarning)

            idx += 1

    zone_index_lookup = {}
    if all('source_id' in zone for k,zone in zones.items()):
        for k,zone in zones.items():
            for comp_id in zone['component_ids']:
                if not comp_id in zone_index_lookup:
                    zone_index_lookup[comp_id] = int(zone['source_id'])
                else:
                    warnings.warn('component %s is in multiple zones only %s will be used.' % (comp_id, zone_index_lookup[comp_id]), MP2GRGWarning)
    else:
        idx = 1
        for k,zone in zones.items():
            for comp_id in zone['component_ids']:
                if not comp_id in zone_index_lookup:
                    zone_index_lookup[comp_id] = idx
                else:
                    warnings.warn('component %s is in multiple zones only %s will be used.' % (comp_id, zone_index_lookup[comp_id]), MP2GRGWarning)
            idx += 1


    for bid, buses in buses_by_bid.items():
        if len(buses) > 1:
            print_err('warning: merging buses {} into 1'.format(len(buses)))

        bus_type = None

        if bid in bid_with_active_gen:
            bus_type = 2

        if any(bus['link'] in ivps for bus in buses):
            bus_type = 4

        if any('reference' in bus for bus in buses):
            bus_type = 3

        if bus_type == None:
            bus_type = 1

        for bus in buses:
            if 'matpower_bus_type' in bus:
                if bus['matpower_bus_type'] != bus_type:
                    # TODO print warning about inconsistent mp data!
                    bus_type = bus['matpower_bus_type']

        active_load = 0
        reactive_load = 0

        if bid in loads_by_bid:
            loads = loads_by_bid[bid]
            if len(loads) > 1:
                print_err('warning: merging loads {} into 1'.format(len(loads)))
            for load in loads:
                if not grg_common.is_abstract(load['demand']['active']):
                    active_load += load['demand']['active']
                else:
                    key = '{}/demand'.format(load['id'])
                    if key in master_mapping:
                        active_load += master_mapping[key]['active']
                    else:
                        print_err('warning: unable to find active power value for load {}'.format(load['id']))

                if not grg_common.is_abstract(load['demand']['reactive']):
                    reactive_load += sum([load['demand']['reactive'] for load in loads])
                else:
                    key = '{}/demand'.format(load['id'])
                    if key in master_mapping:
                        reactive_load += master_mapping[key]['reactive']
                    else:
                        print_err('warning: unable to find reactive power value for load {}'.format(load['id']))


        g_shunt = 0
        b_shunt = 0

        if bid in shunts_by_bid:
            shunts = shunts_by_bid[bid]
            if len(shunts) > 1:
                print_err('warning: merging shunts {} into 1'.format(len(shunts)))

            shunt_indexes = set()
            for i,shunt in enumerate(shunts):
                if isinstance(shunt['shunt']['conductance'], dict) or \
                    isinstance(shunt['shunt']['susceptance'], dict):
                    print_err('warning: skipping shunt with variable admittance values')
                    continue
                else:
                    shunt_indexes.add(i)

            g_shunt = sum([shunts[i]['shunt']['conductance'] for i in shunt_indexes])
            b_shunt = sum([shunts[i]['shunt']['susceptance'] for i in shunt_indexes])

        area = 0
        for bus in buses:
            if bus['id'] in area_index_lookup:
                bus_area = area_index_lookup[bus['id']]
                if area != 0 and bus_area != area:
                    print_err('warning: inconsistent bus areas found')
                else:
                    area = bus_area

        zone = 0
        for bus in buses:
            if bus['id'] in zone_index_lookup:
                bus_zone = zone_index_lookup[bus['id']]
                if zone != 0 and bus_zone != zone:
                    print_err('warning: inconsistent bus zones found')
                else:
                    zone = bus_zone

        base_kv = 1.0
        for bus in buses:
            vl = vlbvp[bus['link']]
            nv = vl['voltage']['nominal_value']
            if 'mp_base_kv' in vl['voltage']:
                nv = vl['voltage']['mp_base_kv']
            if base_kv != 1.0 and nv != base_kv:
                print_err('warning: inconsistent bus base_kv values found')
            else:
                base_kv = nv

        vmax = float('inf')
        vmin = float('-inf')
        for bus in buses:
            vmin = max(vmin, grg_common.min_value(bus['voltage']['magnitude']))
            vmax = min(vmax, grg_common.max_value(bus['voltage']['magnitude']))

        vm_values = []
        va_values = []
        for bus in buses:
            key = '{}/voltage'.format(bus['id'])
            if key in master_mapping:
                voltage = master_mapping[key]
                if 'magnitude' in voltage:
                    vm_values.append(voltage['magnitude'])
                if 'angle' in voltage:
                    va_values.append(voltage['angle'])

        va = 0.0
        vm = 1.0
        if len(vm_values) > 0:
            vm = sum(vm_values, 0.0) / len(vm_values)
        if len(va_values) > 0:
            va = sum(va_values, 0.0) / len(va_values)


        bus_args = {
            'bus_i': bid,
            'bus_type': bus_type, 
            'pd': round(base_mva*active_load, float_precision), 
            'qd': round(base_mva*reactive_load, float_precision), 
            'gs': round(base_mva*g_shunt, float_precision), 
            'bs': round(base_mva*b_shunt, float_precision), 
            'area': area, 
            'vm': vm, 
            'va': round(math.degrees(va), float_precision), 
            'base_kv': base_kv,
            'zone': zone,
            'vmax': vmax, 
            'vmin': vmin, 
        }
        # needed for full idempodence
        # grg_common.map_to_dict(bus_args, bus, 'lam_p')
        # grg_common.map_to_dict(bus_args, bus, 'lam_q')
        # grg_common.map_to_dict(bus_args, bus, 'mu_vmax')
        # grg_common.map_to_dict(bus_args, bus, 'mu_vmin')

        mp_bus = Bus(**bus_args)
        #print(mp_bus)
        mp_buses.append(mp_bus)

        if add_bus_names:
            mp_busname = BusName(bid, '-'.join([bus['id'] for bus in buses]))
            mp_busnames.append(mp_busname)
        del bus, buses


    mp_buses.sort(key=lambda x: x.bus_i)

    if add_bus_names:
        mp_busnames.sort(key=lambda x: x.index)

    mp_bus_lookup = {bus.bus_i:bus for bus in mp_buses}


    branch_index_lookup = {}
    if all('source_id' in line for line in cbt['ac_line']) and \
        all('source_id' in xfer for xfer in cbt['two_winding_transformer']):
        for line in cbt['ac_line']:
            branch_index_lookup[line['id']] = int(line['source_id'])
        for xfer in cbt['two_winding_transformer']:
            branch_index_lookup[xfer['id']] = int(xfer['source_id'])
    else:
        offset = 0
        for i, k in enumerate(sorted(cbt['ac_line'], key=lambda x: x['id'])):
            branch_index_lookup[k['id']] = i+offset 

        offset = len(cbt['ac_line'])
        for i, k in enumerate(sorted(cbt['two_winding_transformer'], key=lambda x: x['id'])):
            branch_index_lookup[k['id']] = i+offset 


    for i, line in enumerate(cbt['ac_line']):
        from_bus_id = vp2int[line['link_1']]
        to_bus_id = vp2int[line['link_2']]

        br_status = 1
        if line['link_1'] not in avps or line['link_2'] not in avps:
            br_status = 0

        branch_args = {
            'index': branch_index_lookup[line['id']],
            'f_bus': from_bus_id,
            't_bus': to_bus_id,
            'br_r': line['impedance']['resistance'],
            'br_x': line['impedance']['reactance'],
            'br_status': br_status,
            'angmin': -60.0,
            'angmax':  60.0
        }

        shunt_susceptance = 0

        if 'shunt_1' in line:
            sh1 = line['shunt_1']
            shunt_susceptance += sh1['susceptance']
            if sh1['conductance'] != 0.0:
                print_err('warning: ommiting shunt conductance on ac_line')

        if 'shunt_2' in line:
            sh2 = line['shunt_2']
            if sh2['susceptance'] != shunt_susceptance:
                print_err('warning: rebalancing shunt susceptance on ac_line')
            shunt_susceptance += sh2['susceptance']
            if sh2['conductance'] != 0.0:
                print_err('warning: ommiting shunt conductance on ac_line')

        branch_args['br_b'] = shunt_susceptance

        if operations != None:
            key = '{}/angle_difference'.format(line['id'])
            if key in operations:
                ad_var = operations[key]
                branch_args['angmin'] = round(math.degrees(grg_common.min_value(ad_var)), float_precision)
                branch_args['angmax'] = round(math.degrees(grg_common.max_value(ad_var)), float_precision)

        rate_a, rate_b, rate_c = grg_common.get_thermal_rates(line)

        if grg_common.has_current_limits(line):
            c_rates = grg_common.get_current_rates(line)
            c_rate_a, c_rate_b, c_rate_c = currents_to_mvas(c_rates, mp_bus_lookup[from_bus_id], mp_bus_lookup[to_bus_id])
            if grg_common.has_thermal_limits(line):
                rate_a = min(rate_a, c_rate_a)
                rate_b = min(rate_b, c_rate_b)
                rate_c = min(rate_c, c_rate_c)
            else:
                rate_a, rate_b, rate_c = c_rate_a, c_rate_b, c_rate_c

        rate_a = 0.0 if rate_a == float('inf') else rate_a
        rate_b = 0.0 if rate_b == float('inf') else rate_b
        rate_c = 0.0 if rate_c == float('inf') else rate_c

        branch_args['rate_a'] = round(base_mva*rate_a, float_precision)
        branch_args['rate_b'] = round(base_mva*rate_b, float_precision)
        branch_args['rate_c'] = round(base_mva*rate_c, float_precision)

        # needed for full idempodence
        # grg_common.map_to_dict(branch_args, line, 'pf', base_mva, float_precision)
        # grg_common.map_to_dict(branch_args, line, 'qf', base_mva, float_precision)
        # grg_common.map_to_dict(branch_args, line, 'pt', base_mva, float_precision)
        # grg_common.map_to_dict(branch_args, line, 'qt', base_mva, float_precision)
        # grg_common.map_to_dict(branch_args, line, 'mu_sf')
        # grg_common.map_to_dict(branch_args, line, 'mu_st')
        # grg_common.map_to_dict(branch_args, line, 'mu_angmin')
        # grg_common.map_to_dict(branch_args, line, 'mu_angmax')

        mp_branch = Branch(**branch_args)
        #print(mp_branch)
        mp_branches.append(mp_branch)
        del line


    for i, xfer in enumerate(cbt['two_winding_transformer']):
        from_bus_id = vp2int[xfer['link_1']]
        to_bus_id = vp2int[xfer['link_2']]

        br_status = 1
        if xfer['link_1'] not in avps or xfer['link_2'] not in avps:
            br_status = 0

        key = '{}/tap_changer/position'.format(xfer['id'])
        if key in master_mapping:
            tap_position = master_mapping[key]
        else:
            print_err('warning: skipping transformer {} due to missing tap position setting'.format(xfer['id']))
            continue

        tap_value = grg_common.tap_setting(xfer['tap_changer'], tap_position)
        if tap_value == None:
            print_err('warning: skipping transformer {} due to missing tap position values'.format(xfer['id']))

        if tap_value['shunt']['conductance'] != 0.0:
            print_err('warning: ommiting shunt conductance on transformer')

        branch_args = {
            'index': branch_index_lookup[xfer['id']],
            'f_bus': from_bus_id,
            't_bus': to_bus_id,
            'br_r': tap_value['impedance']['resistance'],
            'br_x': tap_value['impedance']['reactance'],
            'br_b': tap_value['shunt']['susceptance'],
            'tap': tap_value['transform']['tap_ratio'],
            'shift': round(math.degrees(tap_value['transform']['angle_shift']), float_precision),
            'br_status': br_status,
            'angmin': -60.0,
            'angmax':  60.0
        }

        if operations != None:
            key = '{}/angle_difference'.format(xfer['id'])
            if key in operations:
                ad_var = operations[key]
                branch_args['angmin'] = round(math.degrees(grg_common.min_value(ad_var)), float_precision)
                branch_args['angmax'] = round(math.degrees(grg_common.max_value(ad_var)), float_precision)

        if tap_value['shunt']['conductance'] != 0.0:
            print_err('warning: omitting conductance on transformer {}'.format(xfer['id']))

        rate_a, rate_b, rate_c = grg_common.get_thermal_rates(xfer)

        if grg_common.has_current_limits(xfer):
            c_rates = grg_common.get_current_rates(xfer)
            c_rate_a, c_rate_b, c_rate_c = currents_to_mvas(c_rates, mp_bus_lookup[from_bus_id], mp_bus_lookup[to_bus_id])
            if grg_common.has_thermal_limits(xfer):
                rate_a = min(rate_a, c_rate_a)
                rate_b = min(rate_b, c_rate_b)
                rate_c = min(rate_c, c_rate_c)
            else:
                rate_a, rate_b, rate_c = c_rate_a, c_rate_b, c_rate_c

        rate_a = 0.0 if rate_a == float('inf') else rate_a
        rate_b = 0.0 if rate_b == float('inf') else rate_b
        rate_c = 0.0 if rate_c == float('inf') else rate_c

        branch_args['rate_a'] = round(base_mva*rate_a, float_precision)
        branch_args['rate_b'] = round(base_mva*rate_b, float_precision)
        branch_args['rate_c'] = round(base_mva*rate_c, float_precision)

        # needed for full idempodence
        # grg_common.map_to_dict(branch_args, trans, 'pf', base_mva, float_precision)
        # grg_common.map_to_dict(branch_args, trans, 'qf', base_mva, float_precision)
        # grg_common.map_to_dict(branch_args, trans, 'pt', base_mva, float_precision)
        # grg_common.map_to_dict(branch_args, trans, 'qt', base_mva, float_precision)
        # grg_common.map_to_dict(branch_args, trans, 'mu_sf')
        # grg_common.map_to_dict(branch_args, trans, 'mu_st')
        # grg_common.map_to_dict(branch_args, trans, 'mu_angmin')
        # grg_common.map_to_dict(branch_args, trans, 'mu_angmax')

        mp_branch = Branch(**branch_args)
        #print(mp_branch)
        mp_branches.append(mp_branch)
        del xfer
    mp_branches.sort(key=lambda x: x.index)


    gen_index_lookup = {}
    if all('source_id' in gen for gen in cbt['generator']) and \
        all('source_id' in syn_cond for syn_cond in cbt['synchronous_condenser']):
        for gen in cbt['generator']:
            gen_index_lookup[gen['id']] = int(gen['source_id'])
        for syn_cond in cbt['synchronous_condenser']:
            gen_index_lookup[syn_cond['id']] = int(syn_cond['source_id'])
    else:
        offset = 0
        for i, k in enumerate(sorted(cbt['generator'], key=lambda x: x['id'])):
            gen_index_lookup[k['id']] = i+offset 

        offset = len(cbt['generator'])
        for i, k in enumerate(sorted(cbt['synchronous_condenser'], key=lambda x: x['id'])):
            gen_index_lookup[k['id']] = i+offset 

    has_cost_functions = market != None and len(market['operational_costs']) > 0

    if has_cost_functions:
        mp_gencosts = []

    for i, gen in enumerate(cbt['generator']):
        bus_id = vp2int[gen['link']]

        pg = 0.0
        qg = 0.0
        key = '{}/output'.format(gen['id'])
        if key in master_mapping:
            output = master_mapping[key]
            if 'active' in output:
                pg = output['active']
            if 'reactive' in output:
                qg = output['reactive']

        vg = 1.0
        if 'vg' in gen:
            vg = gen['vg']

        apf = 0.0
        if 'apf' in gen:
            apf = gen['apf']

        mbase = base_mva
        if 'mbase' in gen:
            mbase = gen['mbase']

        gen_status = 1
        if gen['link'] not in avps:
            gen_status = 0

        #print(gen['id'])
        gen_args = {
            'index': gen_index_lookup[gen['id']],
            'gen_bus': bus_id,
            'pg': round(base_mva*pg, float_precision),
            'qg': round(base_mva*qg, float_precision),
            'qmax': round(base_mva*grg_common.max_value(gen['output']['reactive']), float_precision),
            'qmin': round(base_mva*grg_common.min_value(gen['output']['reactive']), float_precision),
            'vg': vg,
            'mbase' : mbase,
            'gen_status': gen_status,
            'pmax': round(base_mva*grg_common.max_value(gen['output']['active']), float_precision),
            'pmin': round(base_mva*grg_common.min_value(gen['output']['active']), float_precision),
            'apf': apf,
        }

        # need for full idempotence
        # grg_common.map_to_dict(gen_args, gen, 'pc1')
        # grg_common.map_to_dict(gen_args, gen, 'pc2')
        # grg_common.map_to_dict(gen_args, gen, 'qc1min')
        # grg_common.map_to_dict(gen_args, gen, 'qc1max')
        # grg_common.map_to_dict(gen_args, gen, 'qc2min')
        # grg_common.map_to_dict(gen_args, gen, 'qc2max')
        # grg_common.map_to_dict(gen_args, gen, 'ramp_agc')
        # grg_common.map_to_dict(gen_args, gen, 'ramp_10')
        # grg_common.map_to_dict(gen_args, gen, 'ramp_30')
        # grg_common.map_to_dict(gen_args, gen, 'ramp_q')
        # grg_common.map_to_dict(gen_args, gen, 'apf')
        # grg_common.map_to_dict(gen_args, gen, 'mu_pmax')
        # grg_common.map_to_dict(gen_args, gen, 'mu_pmin')
        # grg_common.map_to_dict(gen_args, gen, 'mu_qmax')
        # grg_common.map_to_dict(gen_args, gen, 'mu_qmin')

        mp_gen = Generator(**gen_args)

        if has_cost_functions:
            index = gen_index_lookup[gen['id']]
            key = gen['id']
            if key in market['operational_costs']:
                cost_model = market['operational_costs'][key]
                mp_gencost = build_gen_cost_mp(index, cost_model, base_mva, float_precision)
            else:
                print_err('missing cost information on {}'.format(key))
                mp_gencost = build_gen_cost_mp_default(index, 'polynomial', 3)
            mp_gencosts.append(mp_gencost)

        #print(mp_gen)
        mp_gens.append(mp_gen)
        del gen


    for i, syn_cond in enumerate(cbt['synchronous_condenser']):
        bus_id = vp2int[syn_cond['link']]

        pg = 0.0
        qg = 0.0
        key = '{}/output'.format(syn_cond['id'])
        if key in master_mapping:
            output = master_mapping[key]
            if 'reactive' in output:
                qg = output['reactive']

        vg = 0.0
        if 'vg' in syn_cond:
            vg = syn_cond['vg']

        apf = 0.0
        if 'apf' in syn_cond:
            apf = syn_cond['apf']

        mbase = base_mva
        if 'mbase' in syn_cond:
            mbase = syn_cond['mbase']

        gen_status = 1
        if syn_cond['link'] not in avps:
            gen_status = 0

        gen_args = {
            'index': gen_index_lookup[syn_cond['id']],
            'gen_bus': bus_id,
            'pg': 0,
            'qg': round(base_mva*qg, float_precision),
            'qmax': round(base_mva*grg_common.max_value(syn_cond['output']['reactive']), float_precision),
            'qmin': round(base_mva*grg_common.min_value(syn_cond['output']['reactive']), float_precision),
            'vg': vg,
            'mbase': mbase,
            'gen_status': gen_status,
            'pmax': 0,
            'pmin': 0,
            'apf': apf,
        }

        # need for full idempotence
        # grg_common.map_to_dict(gen_args, syn_cond, 'pc1')
        # grg_common.map_to_dict(gen_args, syn_cond, 'pc2')
        # grg_common.map_to_dict(gen_args, syn_cond, 'qc1min')
        # grg_common.map_to_dict(gen_args, syn_cond, 'qc1max')
        # grg_common.map_to_dict(gen_args, syn_cond, 'qc2min')
        # grg_common.map_to_dict(gen_args, syn_cond, 'qc2max')
        # grg_common.map_to_dict(gen_args, syn_cond, 'ramp_agc')
        # grg_common.map_to_dict(gen_args, syn_cond, 'ramp_10')
        # grg_common.map_to_dict(gen_args, syn_cond, 'ramp_30')
        # grg_common.map_to_dict(gen_args, syn_cond, 'ramp_q')
        # grg_common.map_to_dict(gen_args, syn_cond, 'apf')
        # grg_common.map_to_dict(gen_args, syn_cond, 'mu_pmax')
        # grg_common.map_to_dict(gen_args, syn_cond, 'mu_pmin')
        # grg_common.map_to_dict(gen_args, syn_cond, 'mu_qmax')
        # grg_common.map_to_dict(gen_args, syn_cond, 'mu_qmin')

        mp_gen = Generator(**gen_args)

        if has_cost_functions:
            index = gen_index_lookup[syn_cond['id']]
            key = syn_cond['id']
            if key in market['operational_costs']:
                cost_model = market['operational_costs'][key]
                mp_gencost = build_gen_cost_mp(index, cost_model, base_mva, float_precision)
            else:
                print_err('missing cost information on {}'.format(key))
                mp_gencost = build_gen_cost_mp_default(index, 'polynomial', 3)
            mp_gencosts.append(mp_gencost)

        mp_gens.append(mp_gen)
        del syn_cond

    mp_gens.sort(key=lambda x: x.index)


    if mp_gencosts == None and add_gen_costs:
        #print_err('adding line losses cost model to all generators')
        mp_gencosts = []
        for gen in mp_gens:
            mp_gencosts.append(build_gen_cost_mp_losses(gen.index, 'polynomial', 3))

    if mp_gencosts != None:
        mp_gencosts.sort(key=lambda x: x.index)



    if len(cbt['dc_line']) > 0:
        mp_dclines = []
        
        dcline_index_lookup = {}
        if all('source_id' for dcline in cbt['dc_line']):
            for dcline in cbt['dc_line']:
                dcline_index_lookup[dcline['id']] = int(dcline['source_id'])
        else:
            offset = 0
            for i, k in enumerate(sorted(cbt['dc_line'], key=lambda x: x['id'])):
                dcline_index_lookup[k['id']] = i+offset 


        for dcline in cbt['dc_line']:
            from_bus_id = vp2int[dcline['link_1']]
            to_bus_id = vp2int[dcline['link_2']]

            br_status = 1
            if dcline['link_1'] not in avps or dcline['link_2'] not in avps:
                br_status = 0

            pf = 0.0
            qf = 0.0
            vf = 0.0
            key = '{}/output_1'.format(dcline['id'])
            if key in master_mapping:
                output = master_mapping[key]
                if 'active' in output:
                    pf = output['active']
                if 'reactive' in output:
                    qf = output['reactive']
                if 'vf' in output:
                    vf = output['vf']

            pt = 0.0
            qt = 0.0
            vt = 0.0
            key = '{}/output_2'.format(dcline['id'])
            if key in master_mapping:
                output = master_mapping[key]
                if 'active' in output:
                    pt = output['active']
                if 'reactive' in output:
                    qt = output['reactive']
                if 'vt' in output:
                    vt = output['vt']


            # TODO this needs to be fixed to do the minus sign encoding... 
            pmin = dcline['losses_1']['min']
            pmax = dcline['losses_1']['max']

            dcline_args = {
                'index': dcline_index_lookup[dcline['id']],
                'f_bus': from_bus_id,
                't_bus': to_bus_id,
                'br_status': br_status,

                'pf': round(base_mva*pf, float_precision),
                'pt': round(base_mva*pt, float_precision),
                'qf': round(base_mva*qf, float_precision),
                'qt': round(base_mva*qt, float_precision),
                'vf': round(vf, float_precision),
                'vt': round(vt, float_precision),
                'pmin': round(base_mva*pmin, float_precision),
                'pmax': round(base_mva*pmax, float_precision),
                'qminf': round(base_mva*dcline['output_1']['reactive']['var']['lb'], float_precision),
                'qmaxf': round(base_mva*dcline['output_1']['reactive']['var']['ub'], float_precision),
                'qmint': round(base_mva*dcline['output_2']['reactive']['var']['lb'], float_precision),
                'qmaxt': round(base_mva*dcline['output_2']['reactive']['var']['ub'], float_precision),
                'loss0': dcline['losses_1']['c_0'] + dcline['losses_2']['c_0'],
                'loss1': dcline['losses_1']['c_1'] + dcline['losses_2']['c_1'],
            }

            mp_dcline = DCLine(**dcline_args)

            mp_dclines.append(mp_dcline)
            del dcline
        mp_dclines.sort(key=lambda x: x.index)


    print_err('grg buses: {}'.format(len(cbt['bus'])))
    print_err(' mp buses: {}'.format(len(mp_buses)))

    case = Case(grg_data['network']['id'], '\'2\'', base_mva, mp_buses, mp_gens, mp_branches, mp_gencosts, mp_dclines, mp_busnames)

    return case


def currents_to_mvas(currents, from_bus, to_bus):
    vmax = max(from_bus.vmax, to_bus.vmax)
    return [c*vmax for c in currents]


def build_gen_cost_mp(index, grg_cost_model, base_mva, float_precision):
    if grg_cost_model['type'] == 'polynomial':
        assert('input' in grg_cost_model)
        arg = grg_cost_model['input']
        assert(arg.endswith('/output/active'))
        coefficients = grg_cost_model['coefficients']

        ncost = len(coefficients)
        scaled_coefficients = [ round(v/base_mva**i, float_precision) for i,v in enumerate(coefficients) ]
        scaled_coefficients.reverse()

        startup_cost = 0.0
        if 'startup' in grg_cost_model:
            startup_cost = grg_cost_model['startup']

        shutdown_cost = 0.0
        if 'shutdown' in grg_cost_model:
            shutdown_cost = grg_cost_model['shutdown']

        mp_gen_cost = GeneratorCost(
            index, 
            2, 
            startup_cost, 
            shutdown_cost, 
            ncost, 
            scaled_coefficients
        )
        return mp_gen_cost

    print_err('unsupported cost model type {}'.format(grg_cost_model['type']))
    assert(False) # un-supported cost functoin, should have been filterd out before this function call
    return None


def build_gen_cost_mp_default(index, model_type, degree):
    if model_type == 'polynomial':
        mp_gen_cost = GeneratorCost(
            index, 
            2, 
            0.0, 
            0.0, 
            degree, 
            [ 0.0 for i in range(degree) ]
        )
        return mp_gen_cost

    print_err('unsupported cost model type {}'.format(grg_cost_model['type']))
    assert(False) # un-supported cost functoin, should have been filterd out before this function call
    return None


def build_gen_cost_mp_losses(index, model_type, degree):
    if model_type == 'polynomial':
        mp_gen_cost = GeneratorCost(
            index, 
            2, 
            0.0, 
            0.0, 
            degree, 
            [ 0.0 for i in range(degree) ]
        )
        mp_gen_cost.cost[degree-2] = 1.0
        return mp_gen_cost

    print_err('unsupported cost model type {}'.format(grg_cost_model['type']))
    assert(False) # un-supported cost functoin, should have been filterd out before this function call
    return None






def write_json_case_file(output_file_location, case):
    '''writes a grg data json file

    Args:
        output_file_location (str): the path of the file to write
        case (Case): the data structure to write out
    '''

    output_file = open(output_file_location, 'w')
    output_file.write(json.dumps(case.to_grg(), sort_keys=True, indent=2, \
                         separators=(',', ': ')))
    output_file.close()

def test_idempotent(input_data_file):
    case = parse_mp_case_file(input_data_file)
    grg_data = case.to_grg()
    #print(json.dumps(grg_data))
    case2 = build_mp_case(grg_data, ['starting_points', 'breakers_assignment'])
    #print(case2)
    return case, case2


# Note main(args) used here instead of main(), to enable easy unit testing
def main(args):
    '''reads a matpower or grg case file and processes it based on command 
    line arguments.

    Args:
        args: an argparse data structure
    '''

    #start = time.time()

    if args.file.endswith('.m'):
        if not args.idempotent:
            print_err('translating: {}'.format(args.file))
            case = parse_mp_case_file(args.file)
            #print_err('internal matpower representation:')
            #print(case)
            #print(time.time() - start)
            #start = time.time()
            #print('')

            grg_data = case.to_grg(args.omit_subtypes, args.skip_validation)
            if grg_data != None:
                #print_err('grg data representation:')
                print(json.dumps(grg_data, sort_keys=True, indent=2, \
                                separators=(',', ': ')))
                #print(time.time() - start)
                #print('')
            return
        else:
            case1, case2 = test_idempotent(args.file)
            if case1 != case2:
                diff(case1, case2)
                #print(case1)
                #print(case2)
            print_err('idempotent test: '+str(case1 == case2))
            return


    if args.file.endswith('.json'):
        if args.idempotent:
            print_err('idempotent test only supported on matpower files.')
            return

        grg_data = parse_grg_case_file(args.file)
        #print_err('internal grg data representation:')
        #print_err(grg_data)
        #print_err('')

        print_err('working with mappings: {}'.format(args.mappings))

        case = build_mp_case(grg_data, args.mappings, add_gen_costs=args.add_generator_costs, add_bus_names=args.add_bus_names)

        print_err('matpower representation:')
        print(case.to_matpower())
        print('')
        return

    print_err('file extension not recognized!')


def build_cli_parser():
    parser = argparse.ArgumentParser(
        description='''grg_mp2grg.%(prog)s is a tool for converting power 
            network dataset between the matpower and grg formats.
            The converted file is printed to standard out''',

        epilog='''Please file bugs at...''',
    )
    parser.add_argument('file', help='the data file to operate on (.m|.json)')
    parser.add_argument('-m', '--mappings', help='mappings to be use as a basis for the matpower case', nargs='*', type=str, default=None)
    parser.add_argument('-i', '--idempotent', help='tests the translation of a given matpower file is idempotent', action='store_true')
    parser.add_argument('-os', '--omit-subtypes', help='ommits optional component subtypes when translating from matpower to grg', default=False, action='store_true')
    parser.add_argument('-sv', '--skip-validation', help='skips the grg validation step when translating from matpower to grg', default=False, action='store_true')
    parser.add_argument('-agc', '--add-generator-costs', help='adds generator costs, if they do not exist', default=False, action='store_true')
    parser.add_argument('-abn', '--add-bus-names', help='adds matpower bus names, based on grg bus ids', default=False, action='store_true')

    #parser.add_argument('--foo', help='foo help')
    version = __import__('grg_mp2grg').__version__
    parser.add_argument('-v', '--version', action='version', \
        version='grg_mp2grg.%(prog)s (version '+version+')')

    return parser


if __name__ == '__main__':
    parser = build_cli_parser()
    main(parser.parse_args())
