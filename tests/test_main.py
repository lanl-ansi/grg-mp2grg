import os, pytest, grg_mp2grg

class TestCLI:
    def setup_method(self, _):
        """Parse a real network file"""
        self.parser = grg_mp2grg.io.build_cli_parser()

    def test_001(self):
        grg_mp2grg.io.main(self.parser.parse_args([os.path.dirname(os.path.realpath(__file__))+'/data/idempotent/pglib-opf/pglib_opf_case5_pjm.m']))

    def test_002(self):
        with pytest.raises(SystemExit):
            grg_mp2grg.io.main(self.parser.parse_args([os.path.dirname(os.path.realpath(__file__))+'/data/idempotent/pglib-opf/pglib_opf_case5_pjm.m', 'dummy']))

    def test_003(self):
        grg_mp2grg.io.main(self.parser.parse_args(['dummy.bad']))

    def test_004(self):
        with pytest.raises(IOError):
            grg_mp2grg.io.main(self.parser.parse_args(['bloop.m']))

    def test_005(self):
        with pytest.raises(IOError):
            grg_mp2grg.io.main(self.parser.parse_args(['bloop.json']))
