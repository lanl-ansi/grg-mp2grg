import os, pytest, re

import grg_mp2grg
import grg_mpdata

import warnings
#warnings.filterwarnings('error')

from test_common import idempotent_files

#@pytest.mark.filterwarnings('error')
warnings.simplefilter('always')
@pytest.mark.parametrize('input_data', idempotent_files)
def test_001(input_data):
    nums = [ int(num) for num in re.findall(r'\d+', input_data) ]
    max_num = max(nums)

    case, case_2 = grg_mp2grg.io.test_idempotent(input_data)
    #assert case == case_2 # checks full data structure
    #assert not case != case_2
    #assert str(case) == str(case_2) # checks string representation of data structure

    diff_count = grg_mpdata.cmd.diff(case, case_2)
    assert diff_count <= 0

