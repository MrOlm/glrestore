"""
Tests for glrestore
"""

import os
import shutil
import pytest
import importlib
import logging

import glrestore
import glrestore.s3_utils

"""
SET UP TESTING UTILITIES
"""
class TestingClass():
    def teardown(self):
        importlib.reload(logging)
        if os.path.isdir(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.mkdir(self.test_dir)
        importlib.reload(logging)

@pytest.fixture()
def BTO():
    """
    Basic test object
    This object makes no copies of anything; just has references and does setup / cleanup

    aws s3 cp --profile sonn ~/Programs/inStrain/test/test_data/N5_271_010G1_scaffold_min1000.fa s3://sonn-current/users/mattolm/testing_house/glrestore/N5_271_010G1_scaffold_min1000.fa --storage-class GLACIER
    """
    # Set up
    self = TestingClass()

    # Specify some file locations
    self.s3_loc = "s3://sonn-current/users/mattolm/testing_house/glrestore/"
    self.glacerized_file_loc = "s3://sonn-current/users/mattolm/testing_house/glrestore/N5_271_010G1_scaffold_min1000.fa"
    self.non_glacerized_file_loc = "s3://sonn-highavail/testing/ng_N5_271_010G1_scaffold_min1000.fa"

    self.test_dir = load_random_test_dir()

    self.teardown()
    yield self
    self.teardown()

def load_random_test_dir():
    """
    Relies on being run from test_docker.py (not ideal)
    """
    loc = os.path.join(str(os.getcwd()),
                       'temp_testdir/')
    return loc

"""
UNIT TESTS
"""

def test_object_glacerized(BTO):
    """
    test the "s3_utils.object_glacerized" function
    """
    assert glrestore.s3_utils.object_glacerized(BTO.glacerized_file_loc, profile='sonn')
    assert not glrestore.s3_utils.object_glacerized(BTO.non_glacerized_file_loc, profile='sonn')

"""
INTEGRATED TESTS
"""

def test_glrestore_1(BTO):

    assert BTO.s3_loc == "s3://sonn-current/users/mattolm/testing_house/glrestore/"
    print('gotem!')
