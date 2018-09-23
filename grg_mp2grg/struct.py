''' extensions to data structures for encoding matpower data files to 
support grg data encoding'''

from grg_mp2grg.exception import MP2GRGWarning

import grg_mpdata.struct
from grg_mpdata.struct import _guard_none

from grg_grgdata.cmd import validate_grg
import grg_grgdata.common as grg_common

import json, math, warnings

# TODO data format strings below should come from grg-grgdata project 
class Case(grg_mpdata.struct.Case):

    def to_grg(self, omit_subtype=False, skip_validation=False):
        '''Returns: an encoding of this data structure as a grg data dictionary'''
        #start = time.time()

        data = {}

        data['grg_version'] = grg_common.grg_version
        data['units'] = grg_common.grg_units

        network = {}
        data['network'] = network

        network['type'] = 'network'
        network['subtype'] = 'bus_breaker'
        network['id'] = self.name
        network['per_unit'] = True
        network['description'] = 'Translated from Matpower data v2 by grg-mp2grg.  No model description is available in this format.'

        base_mva = self.baseMVA
        network['base_mva'] = base_mva

        comp_lookup = self._grg_component_lookup()

        network_components, groups, switch_status = self._grg_components(comp_lookup, base_mva, omit_subtype)
        network['components'] = network_components
        data['groups'] = groups
        data['mappings'] = self._grg_mappings(comp_lookup, switch_status, base_mva)
        data['market'] = self._grg_market(comp_lookup, base_mva)
        data['operation_constraints'] = self._grg_operations(comp_lookup)


        if skip_validation:
            return data

        #print(time.time() - start)
        #start = time.time()
        #print('start validation')
        if validate_grg(data):
            #print('VALID ****')
            #print(time.time() - start)
            return data
        else:
            print('incorrect grg data representation.')
            print(json.dumps(data, sort_keys=True, indent=2, \
                                 separators=(',', ': ')))
            print('This is a bug in grg_mp2grg')
            print('')
        return None

    def _grg_component_lookup(self):
        lookup = {
            'bus':{},
            'voltage':{},
            'load':{},
            'shunt':{},
            'gen':{},
            'branch':{},
            'dcline':{},
            'area':{},
            'zone':{}
        }

        load_count = 1
        shunt_count = 1

        areas = set()
        zones = set()

        zeros = grg_common.calc_zeros(len(self.bus))
        for index, bus in enumerate(self.bus):
            areas.add(bus.area)
            zones.add(bus.zone)

            grg_bus_id = grg_common.bus_name_template % str(index+1).zfill(zeros)
            lookup['bus'][bus.bus_i] = grg_bus_id

            grg_voltage_id = grg_common.bus_voltage_name_template % str(index+1).zfill(zeros)
            lookup['voltage'][bus.bus_i] = grg_voltage_id

            if bus.has_load():
                grg_load_id = grg_common.load_name_template % str(load_count).zfill(zeros)
                lookup['load'][bus.bus_i] = grg_load_id
                load_count += 1

            if bus.has_shunt():
                grg_shunt_id = grg_common.shunt_name_template % str(shunt_count).zfill(zeros)
                lookup['shunt'][bus.bus_i] = grg_shunt_id
                shunt_count += 1

        zeros = grg_common.calc_zeros(len(zones))
        for i, area in enumerate(sorted(areas)):
            lookup['area'][area] = grg_common.area_name_template % str(i+1).zfill(zeros)

        zeros = grg_common.calc_zeros(len(zones))
        for i, zone in enumerate(sorted(zones)):
            lookup['zone'][zone] = grg_common.zone_name_template % str(i+1).zfill(zeros)

        gen_count = 1
        sync_cond_count = 1
        zeros = grg_common.calc_zeros(len(self.gen))
        for gen in self.gen:
            if not gen.is_synchronous_condenser():
                grg_gen_id = grg_common.generator_name_template % str(gen_count).zfill(zeros)
                lookup['gen'][gen.index] = grg_gen_id
                gen_count += 1
            else:
                grg_gen_id = grg_common.sync_cond_name_template % str(sync_cond_count).zfill(zeros)
                lookup['gen'][gen.index] = grg_gen_id
                sync_cond_count += 1

        line_count = 1
        transformer_count = 1
        zeros = grg_common.calc_zeros(len(self.branch))
        for branch in self.branch:
            if not branch.is_transformer():
                grg_branch_id = grg_common.line_name_template % str(line_count).zfill(zeros)
                lookup['branch'][branch.index] = grg_branch_id
                line_count += 1
            else:
                grg_branch_id = grg_common.transformer_name_template % str(transformer_count).zfill(zeros)
                lookup['branch'][branch.index] = grg_branch_id
                transformer_count += 1

        if self.dcline is not None:
            zeros = grg_common.calc_zeros(len(self.dcline))
            for dcline in self.dcline:
                grg_dcline_id = grg_common.dcline_name_template % str(dcline.index+1).zfill(zeros)
                lookup['dcline'][dcline.index] = grg_dcline_id

        return lookup


    def _grg_components(self, lookup, base_mva, omit_subtype=False):
        components = {}
        groups = {}

        for mp_id, grg_id in lookup['area'].items():
            assert(not grg_id in groups)
            groups[grg_id] = {
                'type': 'area',
                'name': str(mp_id),
                'source_id': str(mp_id),
                'component_ids':[]
            }

        for mp_id, grg_id in lookup['zone'].items():
            assert(not grg_id in groups)
            groups[grg_id] = {
                'type': 'zone',
                'name': str(mp_id),
                'source_id': str(mp_id),
                'component_ids':[]
            }

        mp_bus_lookup = {bus.bus_i:bus for bus in self.bus}

        switch_count = 1
        switch_zeros = grg_common.calc_zeros(3*len(self.bus)+len(self.gen)+2*len(self.branch))
        switch_status = {}

        lookup['voltage_level'] = {}
        voltage_levels = {}
        zeros = grg_common.calc_zeros(len(self.bus))
        for index, bus in enumerate(self.bus):
            grg_vl_id = grg_common.voltage_level_name_template % str(index+1).zfill(zeros)

            base_kv = bus.base_kv

            voltage_levels[grg_vl_id] = {
                'id': grg_vl_id,
                'type': 'voltage_level',
                'voltage':{
                    'lower_limit': bus.vmin,
                    'upper_limit': bus.vmax,
                    'nominal_value': base_kv
                },
                'voltage_points':[],
                'voltage_level_components':{},
            }
            lookup['voltage_level'][bus.bus_i] = grg_vl_id

            if voltage_levels[grg_vl_id]['voltage']['nominal_value'] == 0.0:
                warnings.warn('changeing base_kv on bus {} / {} from 0.0 to 1.0'.format(bus.bus_i, grg_vl_id), MP2GRGWarning)
                voltage_levels[grg_vl_id]['voltage']['nominal_value'] = 1.0
                voltage_levels[grg_vl_id]['voltage']['mp_base_kv'] = 0.0


        for bus in self.bus:
            bus_data = bus.to_grg_bus(lookup)
            grg_bus_id = lookup['bus'][bus.bus_i]
            grg_vl_id = lookup['voltage_level'][bus.bus_i]
            voltage_levels[grg_vl_id]['voltage_points'].append(lookup['voltage'][bus.bus_i])

            vl_components = voltage_levels[grg_vl_id]['voltage_level_components']
            vl_components[grg_bus_id] = bus_data

            groups[lookup['area'][bus.area]]['component_ids'].append(grg_bus_id)
            groups[lookup['zone'][bus.zone]]['component_ids'].append(grg_bus_id)

            load_data = bus.to_grg_load(lookup, base_mva, omit_subtype)
            if load_data != None:
                grg_load_id = lookup['load'][bus.bus_i]

                switch, switch_voltage_id = self._insert_switch(load_data, switch_count, switch_zeros)
                switch_status[switch['id']] = bus.get_grg_status()
                switch_count += 1

                voltage_levels[grg_vl_id]['voltage_points'].append(switch_voltage_id)
                vl_components[grg_load_id] = load_data
                vl_components[switch['id']] = switch

            shunt_data = bus.to_grg_shunt(lookup, base_mva, omit_subtype)
            if shunt_data != None:
                grg_shunt_id = lookup['shunt'][bus.bus_i]

                switch, switch_voltage_id = self._insert_switch(shunt_data, switch_count, switch_zeros)
                switch_status[switch['id']] = bus.get_grg_status()
                switch_count += 1

                voltage_levels[grg_vl_id]['voltage_points'].append(switch_voltage_id)
                vl_components[grg_shunt_id] = shunt_data
                vl_components[switch['id']] = switch


        for gen in self.gen:
            gen_data = gen.to_grg_generator(lookup, base_mva, omit_subtype)
            grg_gen_id = lookup['gen'][gen.index]
            grg_vl_id = lookup['voltage_level'][gen.gen_bus]

            switch, switch_voltage_id = self._insert_switch(gen_data, switch_count, switch_zeros)
            switch_status[switch['id']] = self._combine_status(gen, mp_bus_lookup[gen.gen_bus])
            switch_count += 1

            voltage_levels[grg_vl_id]['voltage_points'].append(switch_voltage_id)
            vl_components = voltage_levels[grg_vl_id]['voltage_level_components']
            vl_components[grg_gen_id] = gen_data
            vl_components[switch['id']] = switch


        if self.dcline is not None:
            for dcline in self.dcline:
                dcline_data = dcline.to_grg_dcline(lookup, base_mva, omit_subtype)
                grg_dcline_id = lookup['dcline'][dcline.index]

                grg_vl_id_1 = lookup['voltage_level'][dcline.f_bus]
                grg_vl_id_2 = lookup['voltage_level'][dcline.t_bus]

                switch_1, switch_voltage_id_1, switch_2, switch_voltage_id_2 = self._insert_switches(dcline_data, switch_count, switch_zeros)
                switch_status[switch_1['id']] = self._combine_status(dcline, mp_bus_lookup[dcline.f_bus])
                switch_status[switch_2['id']] = self._combine_status(dcline, mp_bus_lookup[dcline.t_bus])
                switch_count += 2

                components[grg_dcline_id] = dcline_data
                voltage_levels[grg_vl_id_1]['voltage_points'].append(switch_voltage_id_1)
                voltage_levels[grg_vl_id_1]['voltage_level_components'][switch_1['id']] = switch_1

                voltage_levels[grg_vl_id_2]['voltage_points'].append(switch_voltage_id_2)
                voltage_levels[grg_vl_id_2]['voltage_level_components'][switch_2['id']] = switch_2


        transformers = {}
        for branch in self.branch:
            branch_data = branch.to_grg_line(lookup, base_mva, omit_subtype)
            grg_branch_id = lookup['branch'][branch.index]

            if branch_data['type'] == 'ac_line':
                grg_vl_id_1 = lookup['voltage_level'][branch.f_bus]
                grg_vl_id_2 = lookup['voltage_level'][branch.t_bus]

                switch_1, switch_voltage_id_1, switch_2, switch_voltage_id_2 = self._insert_switches(branch_data, switch_count, switch_zeros)
                switch_status[switch_1['id']] = self._combine_status(branch, mp_bus_lookup[branch.f_bus])
                switch_status[switch_2['id']] = self._combine_status(branch, mp_bus_lookup[branch.t_bus])
                switch_count += 2

                components[grg_branch_id] = branch_data
                voltage_levels[grg_vl_id_1]['voltage_points'].append(switch_voltage_id_1)
                voltage_levels[grg_vl_id_1]['voltage_level_components'][switch_1['id']] = switch_1

                voltage_levels[grg_vl_id_2]['voltage_points'].append(switch_voltage_id_2)
                voltage_levels[grg_vl_id_2]['voltage_level_components'][switch_2['id']] = switch_2

            else:
                transformers[grg_branch_id] = (branch, branch_data)

        # cluster buses into substations based on transformers
        bus_sub = {}
        for bus in self.bus:
            bus_id = bus.bus_i
            bus_sub[bus_id] = set([bus_id])

        for grg_id, (mp_data, grg_data) in transformers.items():
            bus_id_set = bus_sub[mp_data.f_bus] | bus_sub[mp_data.t_bus]

            for bus_id in bus_id_set:
                bus_sub[bus_id] = bus_id_set

        sub_buses = {frozenset(buses) for buses in bus_sub.values()}

        lookup['substation'] = {}
        substations = {}
        sub_voltage_levels = {}
        zeros = grg_common.calc_zeros(len(self.bus))
        for index, buses in enumerate(sorted(sub_buses, key=lambda x: min(x))):
            grg_ss_id = grg_common.substation_name_template % str(index+1).zfill(zeros)
            #print(grg_ss_id, buses)
            components[grg_ss_id] = {
                'id': grg_ss_id,
                'type': 'substation',
                'substation_components':{}
            }

            sub_voltage_levels[grg_ss_id] = {}
            for bus_id in buses:
                lookup['substation'][bus_id] = grg_ss_id
                grg_vl_id = lookup['voltage_level'][bus_id]
                sub_voltage_levels[grg_ss_id][grg_vl_id] = voltage_levels[grg_vl_id]

        for grg_ss_id, s_voltage_levels in sub_voltage_levels.items():
            components[grg_ss_id]['substation_components'].update(s_voltage_levels)

            # initial code to merge voltage levels
            # need resolution on voltage bounds beforehand

            # vl_by_base_kv = {}
            # for vl_id, vl_data in voltage_levels.items():
            #     base_kv = vl_data['base_kv']
            #     if not base_kv in vl_by_base_kv:
            #         vl_by_base_kv[base_kv] = []
            #     vl_by_base_kv[base_kv].append( (vl_id,vl_data) )

            # voltage_levels_merged = []
            # for base_kv, voltage_level_list in vl_by_base_kv.items():
            #     if len(voltage_level_list) == 1:
            #         voltage_levels_merged.append(voltage_level_list[0])
            #     else:

            #         voltage_level = {
            #             'id': grg_vl_id,
            #             'type': 'voltage_level',
            #             'voltage':{
            #                 'lower_limit': bus.vmin,
            #                 'upper_limit': bus.vmax,
            #                 'nominal_value': 1.0
            #             },
            #             'voltage_points':[],
            #             'voltage_level_components':{},
            #             'base_kv':bus.base_kv
            #         }

            # print(len(voltage_levels), vl_by_base_kv.keys())

        for grg_id, (mp_data, grg_data) in transformers.items():
            #continue ###
            f_grg_ss_id = lookup['substation'][mp_data.f_bus]
            t_grg_ss_id = lookup['substation'][mp_data.t_bus]
            assert(f_grg_ss_id == t_grg_ss_id) # clustering code failed

            grg_vl_id_1 = lookup['voltage_level'][mp_data.f_bus]
            grg_vl_id_2 = lookup['voltage_level'][mp_data.t_bus]
            assert(grg_vl_id_1 != grg_vl_id_2) # voltage level setting code failed

            switch_1, switch_voltage_id_1, switch_2, switch_voltage_id_2 = self._insert_switches(grg_data, switch_count, switch_zeros)
            switch_status[switch_1['id']] = self._combine_status(mp_data, mp_bus_lookup[mp_data.f_bus])
            switch_status[switch_2['id']] = self._combine_status(mp_data, mp_bus_lookup[mp_data.t_bus])
            switch_count += 2

            components[f_grg_ss_id]['substation_components'][grg_id] = grg_data
            voltage_levels[grg_vl_id_1]['voltage_points'].append(switch_voltage_id_1)
            voltage_levels[grg_vl_id_1]['voltage_level_components'][switch_1['id']] = switch_1

            voltage_levels[grg_vl_id_2]['voltage_points'].append(switch_voltage_id_2)
            voltage_levels[grg_vl_id_2]['voltage_level_components'][switch_2['id']] = switch_2

        return components, groups, switch_status


    def _grg_mappings(self, lookup, switch_status, base_mva):
        mappings = {}

        starting_points = {}
        mappings['starting_points'] = starting_points

        for bus in self.bus:
            key, data = bus.get_grg_bus_setpoint(lookup)
            assert(key not in starting_points)
            starting_points[key] = data

            if bus.has_load():
                key, data = bus.get_grg_load_setpoint(lookup, base_mva)
                assert(key not in starting_points)
                starting_points[key] = data

        for gen in self.gen:
            key, data = gen.get_grg_setpoint(lookup, base_mva)
            assert(key not in starting_points)
            starting_points[key] = data

        for branch in self.branch:
            if branch.is_transformer():
                key, data = branch.get_grg_tap_changer_setpoint(lookup)
                assert(key not in starting_points)
                starting_points[key] = data

        if self.dcline != None:
            for dcline in self.dcline:
                kvs = dcline.get_grg_setpoint(lookup, base_mva)
                for key, data in kvs.items():
                    assert(key not in starting_points)
                    starting_points[key] = data


        breaker_assignment = {}
        mappings['breakers_assignment'] = breaker_assignment
        for switch_id, status_value in switch_status.items():
            switch_pointer = '{}/status'.format(switch_id)
            breaker_assignment[switch_pointer] = status_value

        return mappings


    def _grg_market(self, lookup, base_mva):
        market = {}
        costs = {}
        market['operational_costs'] = costs
        if self.gencost is not None:
            for gencost in self.gencost:
                gen_count = len(self.gen)
                gen_id = gencost.index % gen_count
                gen = self.gen[gen_id]
                active_cost_function = gencost.index < len(self.gen)

                key, value = gencost.get_grg_cost_model(lookup, gen_id, gen_count, base_mva)
                assert(key not in costs)

                if not gen.is_synchronous_condenser() or not active_cost_function:
                    costs[key] = value
                else:
                    pass #TODO check that all costs are 0 
        return market


    def _grg_operations(self, lookup):
        operations = {}

        for branch in self.branch:
            key, data = branch.get_grg_operations(lookup)
            assert(key not in operations)
            operations[key] = data

        return operations


    def _combine_status(self, *mp_comps):
        for mp_comp in mp_comps:
            grg_status = mp_comp.get_grg_status()
            if grg_status == 'off':
                return 'off'
        return 'on'


    def _insert_switch(self, grg_comp, switch_count, switch_zeros):
        assert('link' in grg_comp)

        grg_switch_id = grg_common.switch_name_template % str(switch_count).zfill(switch_zeros)
        grg_switch_voltage_id = grg_common.switch_voltage_name_template % str(switch_count).zfill(switch_zeros)

        comp_voltage_id = grg_comp['link']
        grg_comp['link'] = grg_switch_voltage_id

        switch = {
            'id': grg_switch_id,
            'type': 'switch',
            'subtype': 'breaker',
            'link_1': comp_voltage_id,
            'link_2': grg_switch_voltage_id,
            'status': {'var': ['off','on']}
        }

        return switch, grg_switch_voltage_id


    def _insert_switches(self, grg_comp, switch_count, switch_zeros):
        assert('link_1' in grg_comp)
        assert('link_2' in grg_comp)

        grg_switch_id_1 = grg_common.switch_name_template % str(switch_count).zfill(switch_zeros)
        grg_switch_voltage_id_1 = grg_common.switch_voltage_name_template % str(switch_count).zfill(switch_zeros)

        grg_switch_id_2 = grg_common.switch_name_template % str(switch_count+1).zfill(switch_zeros)
        grg_switch_voltage_id_2 = grg_common.switch_voltage_name_template % str(switch_count+1).zfill(switch_zeros)

        comp_voltage_id_1 = grg_comp['link_1']
        grg_comp['link_1'] = grg_switch_voltage_id_1

        comp_voltage_id_2 = grg_comp['link_2']
        grg_comp['link_2'] = grg_switch_voltage_id_2

        switch_1 = {
            'id': grg_switch_id_1,
            'type': 'switch',
            'subtype': 'breaker',
            'link_1': comp_voltage_id_1,
            'link_2': grg_switch_voltage_id_1,
            'status': {'var': ['off','on']}
        }

        switch_2 = {
            'id': grg_switch_id_2,
            'type': 'switch',
            'subtype': 'breaker',
            'link_1': comp_voltage_id_2,
            'link_2': grg_switch_voltage_id_2,
            'status': {'var': ['off','on']}
        }

        return switch_1, grg_switch_voltage_id_1, switch_2, grg_switch_voltage_id_2


class Bus(grg_mpdata.struct.Bus):

    def has_load(self):
        return not(self.pd == 0 and self.qd == 0)

    def has_shunt(self):
        return not(self.gs == 0 and self.bs == 0)

    def to_grg_bus(self, lookup, omit_subtype=False):
        '''Returns: a grg data bus name and data as a dictionary'''

        if omit_subtype:
            warnings.warn('attempted to omit subtype on bus \'%s\', but this is not allowed.' % str(self.bus_i), MP2GRGWarning)

        data = {
            'source_id': str(self.bus_i),
            'id': lookup['bus'][self.bus_i],
            'type':'bus',
            'link': lookup['voltage'][self.bus_i],
            'voltage': {
                'magnitude': grg_common.build_range_variable(self.vmin, self.vmax),
                'angle' : grg_common.build_range_variable('-Inf', 'Inf'),
            }
        }

        if self.bus_type == 3:
           data['reference'] = True

        return data


    def to_grg_shunt(self, lookup, base_mva, omit_subtype=False):
        '''Returns: a grg data shunt name and data as a dictionary'''
        if not self.has_shunt():
            return None

        data = {
            'id':lookup['shunt'][self.bus_i],
            'type':'shunt',
            'link':lookup['voltage'][self.bus_i],
            'shunt':{
                'conductance': self.gs/base_mva,
                'susceptance': self.bs/base_mva
            }
        }

        if not omit_subtype:
            if self.bs >= 0:
                data['subtype'] = 'inductor'
            else:
                data['subtype'] = 'capacitor'

        return data


    def to_grg_load(self, lookup, base_mva, omit_subtype=False):
        '''Returns: a grg data load name and data as a dictionary'''
        if not self.has_load():
            return None

        data = {
            'id':lookup['load'][self.bus_i],
            'type':'load',
            'link': lookup['voltage'][self.bus_i],
            'demand':{
                'active': self.pd/base_mva,
                'reactive': self.qd/base_mva
            }
        }

        if not omit_subtype:
            data['subtype'] = 'withdrawal'

        return data


    def get_grg_status(self):
        '''Returns: a grg data status assignment as a dictionary'''
        if self.bus_type != 4:
            return 'on'
        else:
            return 'off'


    def get_grg_bus_setpoint(self, lookup):
        '''Returns: a grg data voltage set point as a dictionary'''
        key = lookup['bus'][self.bus_i]+'/voltage'
        value = {
            'magnitude': self.vm,
            'angle': math.radians(self.va)
        }
        return key, value

    def get_grg_load_setpoint(self, lookup, base_mva):
        assert(self.has_load())
        key = lookup['load'][self.bus_i]+'/demand'
        value = {
            'active': self.pd/base_mva,
            'reactive': self.qd/base_mva
        }
        return key, value


class Generator(grg_mpdata.struct.Generator):
    def is_synchronous_condenser(self):
        # NOTE self.pg == 0 is needed for bad data cases, where pg is out of bounds. in time, may be able to remove this.
        return self.pmin == 0 and self.pmax == 0 and self.pg == 0

    def to_grg_generator(self, lookup, base_mva, omit_subtype=False):
        '''Returns: a grg data gen name and data as a dictionary'''

        data = {
            'id': lookup['gen'][self.index],
            'link': lookup['voltage'][self.gen_bus],
            'source_id': str(self.index),
            'mbase':self.mbase,
            'vg':self.vg,
        }

        if self.apf != 0.0:
            data['apf'] = self.apf

        if self.is_synchronous_condenser():
            # TODO throw warning that this gen is becoming a synchronous_condenser
            data.update({
                'type': 'synchronous_condenser',
                'output': {
                    'reactive': grg_common.build_range_variable(self.qmin/base_mva, self.qmax/base_mva),
                }
            })
        else:
            data.update({
                'type': 'generator',
                'output': {
                    'active': grg_common.build_range_variable(self.pmin/base_mva, self.pmax/base_mva),
                    'reactive': grg_common.build_range_variable(self.qmin/base_mva, self.qmax/base_mva),
                }
            })
 
        return data

    def get_grg_status(self):
        '''Returns: a grg data status assignment as a dictionary'''
        if self.gen_status == 1:
            return 'on'
        else:
            return 'off'

    def get_grg_setpoint(self, lookup, base_mva):
        '''Returns: a grg data power output set point as a dictionary'''

        key = lookup['gen'][self.index]+'/output'
        if self.is_synchronous_condenser():
            value = {
                'reactive': self.qg/base_mva
            }
        else:
            value = {
                'active': self.pg/base_mva,
                'reactive': self.qg/base_mva
            }
        return key, value



class GeneratorCost(grg_mpdata.struct.GeneratorCost):
    def get_grg_cost_model(self, lookup, gen_id, gen_count, base_mva):
        '''Returns: a grg data encoding of this data structure as a dictionary'''

        active_cost_function = self.index < gen_count
        grg_id = lookup['gen'][gen_id]

        if active_cost_function:
            key = grg_id
            argument = grg_id+'/output/active'
        else:
            key = grg_id+'_reactive_cost'
            argument = grg_id+'/output/reactive'


        if self.model == 1:
            data = {
                'type':'piecewise_linear',
                'input':argument, 
                'points': [ [self.cost[2*i]/base_mva, self.cost[2*i+1]] for i in range(0, self.ncost) ]
            }
        elif self.model == 2:
            data = {
                'type':'polynomial', 
                'input':argument, 
                'coefficients': [(base_mva ** i) * c for i,c in enumerate(self.cost[::-1])]
            }
        else:
            # TODO throw out of spec error
            # in general this shuold occur if this code is behind grg_mpdata
            assert(False)

        data['startup'] = self.startup
        data['shutdown'] = self.shutdown

        return key, data



class Branch(grg_mpdata.struct.Branch):
    def is_transformer(self):
        return not (self.tap == 0 and self.shift == 0)

    def to_grg_line(self, lookup, base_mva, omit_subtype=False):
        '''Returns: a grg data line name and data as a dictionary'''

        data = {
            'id': lookup['branch'][self.index],
            'source_id': str(self.index),
            'link_1': lookup['voltage'][self.f_bus],
            'link_2': lookup['voltage'][self.t_bus],
            'thermal_limits_1': self._grg_thermal_limit(self.rate_a/base_mva, self.rate_b/base_mva, self.rate_c/base_mva),
            'thermal_limits_2': self._grg_thermal_limit(self.rate_a/base_mva, self.rate_b/base_mva, self.rate_c/base_mva),
            'rates':0,
        }

        if self.rate_a != 0:
            data['rates'] = 1
        if self.rate_b != 0:
            data['rates'] = 2
        if self.rate_c != 0:
            data['rates'] = 3

        if not self.is_transformer():
            data.update({
                'type': 'ac_line',
                'impedance':{
                    'resistance':self.br_r,
                    'reactance':self.br_x,
                },
            })
            if not omit_subtype:
                data['subtype'] = 'overhead'
            data.update({
                'shunt_1':{
                    'conductance': 0.0,
                    'susceptance': self.br_b/2.0
                },
                'shunt_2':{
                    'conductance': 0.0,
                    'susceptance': self.br_b/2.0
                }
            })

        else: # this is a transformer
            data.update({
                'type': 'two_winding_transformer',
                'tap_changer': self._grg_tap_changer()
            })

        return data


    def _grg_thermal_limit(self, rate_a, rate_b, rate_c):
        limits = [
            {'duration': 'Inf', 'min': 0.0, 'max':rate_a, 'report':'off'}
        ]

        if rate_b != 0.0 and rate_b > rate_a:
            limits.append({'duration': 14400, 'min': 0.0, 'max':rate_b, 'report':'off'})
        if rate_c != 0.0 and rate_c > rate_a:
            if len(limits) == 1 or rate_c > rate_b:
                limits.append({'duration': 900, 'min': 0.0, 'max':rate_c, 'report':'off'})


        return limits

    def _grg_tap_changer(self):
        tap_value = self.tap
        if tap_value == 0.0:
            assert(self.shift != 0.0)
            tap_value = 1.0

        return {
            'position': grg_common.build_range_variable(0, 0),
            'impedance': {
                'resistance': grg_common.build_range_variable(self.br_r, self.br_r),
                'reactance': grg_common.build_range_variable(self.br_x, self.br_x)
            },
            'shunt': {
                'conductance': grg_common.build_range_variable(0, 0),
                'susceptance': grg_common.build_range_variable(self.br_b, self.br_b),
            },
            'transform': {
                'tap_ratio': grg_common.build_range_variable(tap_value, tap_value),
                'angle_shift': grg_common.build_range_variable(math.radians(self.shift), math.radians(self.shift)),
            },
            'steps': [
                {
                    'position':0 ,
                    'impedance': {'resistance': self.br_r,'reactance': self.br_x},
                    'shunt': {'conductance': 0.0,'susceptance': self.br_b},
                    'transform': {'tap_ratio': tap_value, 'angle_shift':math.radians(self.shift)}
                }
            ]
        }

    def get_grg_tap_changer_setpoint(self, lookup):
        assert(self.is_transformer())
        key = lookup['branch'][self.index]+'/tap_changer/position'
        value = 0
        return key, value

    def get_grg_status(self):
        '''Returns: a grg data status assignment as a dictionary'''
        if self.br_status == 1:
            return 'on'
        else:
            return 'off'

    def get_grg_setpoint(self, base_mva):
        '''Returns: a grg data power flow set point as a dictionary'''
        assignments = {}
        if self.extended:
            # not part of the offical spec, but left in for idempotentce
            if self.mu_sf == None:
                assignments = {
                    '@/pf':self.pf/base_mva, '@/qf':self.qf/base_mva, '@/pt':self.pt/base_mva, '@/qt':self.qt/base_mva,
                }
            else:
                assignments = {
                    '@/pf':self.pf/base_mva, '@/qf':self.qf/base_mva, '@/pt':self.pt/base_mva, '@/qt':self.qt/base_mva,
                    '@/mu_sf':self.mu_sf, '@/mu_st':self.mu_st,
                    '@/mu_angmin':self.mu_angmin, '@/mu_angmax':self.mu_angmax
                }

        return assignments


    def get_grg_operations(self, lookup):
        key = lookup['branch'][self.index]+'/angle_difference'
        value = grg_common.build_range_variable(math.radians(self.angmin), math.radians(self.angmax))
        return key, value



class DCLine(grg_mpdata.struct.DCLine):
    def to_grg_dcline(self, lookup, base_mva, omit_subtype=False):
        '''Returns: a grg data dc line name and data as a dictionary'''

        if self.pmin >= 0 and self.pmax >= 0:
            active_1_min = self.pmin
            active_1_max = self.pmax
            active_2_min = self.loss0 - active_1_max * (1 - self.loss1)
            active_2_max = self.loss0 - active_1_min * (1 - self.loss1)

        if self.pmin >= 0 and self.pmax < 0:
            active_1_min = self.pmin
            active_2_min = self.pmax
            active_1_max = (-active_2_min + self.loss0) / (1-self.loss1)
            active_2_max = self.loss0 - active_1_min * (1 - self.loss1)

        if self.pmin < 0 and self.pmax >= 0:
            active_2_max = -self.pmin
            active_1_max = self.pmax
            active_1_min = (-active_2_max + self.loss0) / (1-self.loss1)
            active_2_min = self.loss0 - active_1_max * (1 - self.loss1)

        if self.pmin < 0 and self.pmax < 0:
            active_2_max = -self.pmin
            active_2_min = self.pmax
            active_1_max = (-active_2_min + self.loss0) / (1-self.loss1)
            active_1_min = (-active_2_max + self.loss0) / (1-self.loss1)

        data = {
            'type': 'dc_line',
            'id': lookup['dcline'][self.index],
            'source_id': str(self.index),
            'link_1': lookup['voltage'][self.f_bus],
            'link_2': lookup['voltage'][self.t_bus],
            'resistance': 0.0,
            'losses_1': {
                'min': active_1_min/base_mva,
                'max': active_1_max/base_mva,
                'c_0': 0.0,
                'c_1': 0.0,
            },
            'losses_2': {
                'min': active_2_min/base_mva,
                'max': active_2_max/base_mva,
                'c_0': self.loss0,
                'c_1': self.loss1,
            },
            'output_1':{
                'reactive': grg_common.build_range_variable(self.qminf/base_mva, self.qmaxf/base_mva)
            },
            'output_2':{
                'reactive': grg_common.build_range_variable(self.qmint/base_mva, self.qmaxt/base_mva)
            },
        }

        return data

    def get_grg_status(self):
        '''Returns: a grg data status assignment as a dictionary'''
        if self.br_status == 1:
            return 'on'
        else:
            return 'off'

    def get_grg_setpoint(self, lookup, base_mva):
        '''Returns: a grg data power flow set point as a dictionary'''
        data = {}

        key = lookup['dcline'][self.index]+'/output_1'
        value = {
            'vf': self.vf,
            'active': self.pf/base_mva,
            'reactive': self.qf/base_mva
        }
        data[key] = value

        key = lookup['dcline'][self.index]+'/output_2'
        value = {
            'vt': self.vt,
            'active': self.pt/base_mva,
            'reactive': self.qt/base_mva
        }
        data[key] = value

        return data

