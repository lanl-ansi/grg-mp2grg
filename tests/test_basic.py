import os, pytest

import collections
import warnings
warnings.filterwarnings('error')

from grg_grgdata.cmd import components_by_type

import grg_mp2grg

class Test4Bus:
    def setup_method(self, _):
        """Parse a real network file"""
        self.mp_case = grg_mp2grg.io.parse_mp_case_file(os.path.dirname(os.path.realpath(__file__))+'/data/idempotent/pglib-opf/pglib_opf_case14_ieee.m')

    def test_001(self):
        #print(self.mp_case.to_grg())
        grg_case = self.mp_case.to_grg()
        components = components_by_type(grg_case)
        assert len(components['bus']) == 14

    def test_002(self):
        grg_case = self.mp_case.to_grg()
        components = components_by_type(grg_case)

        line_count = len(components['ac_line'])
        transformer_count = len(components['two_winding_transformer'])
        assert line_count + transformer_count == 20

    def test_003(self):
        path = 'tmp.json'
        grg_mp2grg.io.write_json_case_file(path, self.mp_case)
        os.remove(path)


class TestGRGVariants:
    def test_no_operations(self):
        grg_case = grg_mp2grg.io.parse_mp_case_file(os.path.dirname(os.path.realpath(__file__))+'/data/idempotent/pglib-opf/pglib_opf_case5_pjm.m').to_grg()
        del grg_case['operation_constraints']

        mp_case = grg_mp2grg.io.build_mp_case(grg_case)

        assert len(mp_case.bus) == 5
        assert len(mp_case.branch) == 6
        assert len(mp_case.gen) == 5
        assert len(mp_case.gencost) == 5

    def test_no_market(self):
        grg_case = grg_mp2grg.io.parse_mp_case_file(os.path.dirname(os.path.realpath(__file__))+'/data/idempotent/pglib-opf/pglib_opf_case5_pjm.m').to_grg()
        del grg_case['market']

        mp_case = grg_mp2grg.io.build_mp_case(grg_case)

        assert len(mp_case.bus) == 5
        assert len(mp_case.branch) == 6
        assert len(mp_case.gen) == 5
        assert mp_case.gencost == None


