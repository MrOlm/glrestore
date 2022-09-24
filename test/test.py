"""
Tests for glrestore
"""

import os
import sys
import time
import shutil
import pytest
import importlib
import logging
import subprocess
import pandas as pd
from time import sleep
from threading import Thread

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
        #os.rmdir(self.s3_testing_loc)
        importlib.reload(logging)

@pytest.fixture()
def BTO():
    """
    Basic test object
    This object makes no copies of anything; just has references and does setup / cleanup

    aws s3 cp --profile sonn ~/Programs/inStrain/test/test_data/N5_271_010G1_scaffold_min1000.fa s3://sonn-current/users/mattolm/testing_house/glrestore/N5_271_010G1_scaffold_min1000.fa --storage-class GLACIER

    aws s3 cp --profile sonn ~/Programs/inStrain/test/test_data/N5_271_010G1_scaffold_min1000.fa s3://sonn-current/users/mattolm/testing_house/glrestore/rest_N5_271_010G1_scaffold_min1000.fa --storage-class GLACIER
    aws s3api restore-object --profile sonn --bucket sonn-current --key users/mattolm/testing_house/glrestore/rest_N5_271_010G1_scaffold_min1000.fa --restore-request '{"Days":300,"GlacierJobParameters":{"Tier":"Expedited"}}'
    """
    # Set up
    self = TestingClass()

    # Specify some s3 locations
    self.s3_testing_loc = "s3://sonn-current/users/mattolm/testing_house/glrestore/temp/"

    # Specify some file locations
    self.glacerized_file_loc = "s3://sonn-current/users/mattolm/testing_house/glrestore/N5_271_010G1_scaffold_min1000.fa"
    self.non_glacerized_file_loc = "s3://sonn-highavail/testing/ng_N5_271_010G1_scaffold_min1000.fa"
    self.restored_file_loc = "s3://sonn-current/users/mattolm/testing_house/glrestore/rest_N5_271_010G1_scaffold_min1000.fa"

    self.glacerized_file_list = os.path.join(str(os.getcwd()), 'testfiles/glacerizedfile.txt')

    self.test_dir = os.path.join(str(os.getcwd()), 'temp_testdir/')

    self.teardown()
    yield self
    self.teardown()

def run_glrestore(cmd):
    """
    Simulate as if you're calling glrestore from the command line
    """
    import glrestore.glrestore
    from unittest.mock import patch
    with patch.object(sys, 'argv', cmd.split(" ")):
        glrestore.glrestore.main()

def upload_glacierized_file():
    cmd = "aws s3 cp ~/Programs/inStrain/test/test_data/N5_271_010G1_scaffold_min1000.fa s3://sonn-current/users/mattolm/testing_house/glrestore/N5_271_010G1_scaffold_min1000.fa --storage-class GLACIER"
    subprocess.call(cmd, shell=True)

"""
UNIT TESTS
"""

def test_object_glacerized_v2(BTO):
    """
    test the "s3_utils.glacier_status" function
    """
    upload_glacierized_file()

    # Test ability to check non-glacierized file
    assert glrestore.s3_utils.glacier_status_v2(BTO.non_glacerized_file_loc) == 'no-glacier'

    # Test ability to check glacierized file
    assert glrestore.s3_utils.glacier_status_v2(BTO.glacerized_file_loc) == 'glacier-no-restore'

    # Test ability to recognize *restored* glacierized file
    assert glrestore.s3_utils.glacier_status_v2(BTO.restored_file_loc) == 'glacier-restored'

    # Test ability to recognize *restoring* glacierized file
    cmd = "aws s3api restore-object --bucket sonn-current --key users/mattolm/testing_house/glrestore/N5_271_010G1_scaffold_min1000.fa --restore-request '{\"Days\":1,\"GlacierJobParameters\":{\"Tier\":\"Expedited\"}}'"
    subprocess.call(cmd, shell=True)
    assert glrestore.s3_utils.glacier_status_v2(BTO.glacerized_file_loc) == 'glacier-restoring'

def test_classify_glacier_objects(BTO):
    """
    test the "s3_utils.classify_glacier_objects" function on individual files
    """
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.max_columns', None)

    upload_glacierized_file()

    db = glrestore.s3_utils.get_object_storage_class_v2(
        [BTO.non_glacerized_file_loc, BTO.glacerized_file_loc, BTO.restored_file_loc])

    assert len(db) == 3
    assert db['storage_class'].value_counts()['GLACIER'] == 2

def test_classify_glacier_objects_2(BTO):
    """
    test the "s3_utils.classify_glacier_objects" function on folders
    """
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.max_columns', None)

    upload_glacierized_file()

    db = glrestore.s3_utils.get_object_storage_class_v2(
        's3://sonn-current/users/mattolm/testing_house/glrestore/')

    assert len(db) == 3
    assert db['storage_class'].value_counts()['GLACIER'] == 3


"""
INTEGRATED TESTS
"""

def test_glrestore_1(BTO):
    """
    A simple test restoring a single object to new location
    """
    upload_glacierized_file()

    # Make sure this file starts off glacierized
    assert glrestore.s3_utils.glacier_status_v2(BTO.glacerized_file_loc) == 'glacier-no-restore'

    cmd = f"glrestore -f {BTO.glacerized_file_loc} --debug"
    run_glrestore(cmd)

    # Make sure this file is now being restored
    assert glrestore.s3_utils.glacier_status_v2(BTO.glacerized_file_loc) == 'glacier-restoring'

def test_glrestore_2(BTO):
    """
    A simple test restoring a single object to new location based on a list of files
    """
    upload_glacierized_file()

    # Make sure this file starts off glacierized
    assert glrestore.s3_utils.glacier_status_v2(BTO.glacerized_file_loc) == 'glacier-no-restore'

    cmd = f"glrestore -f {BTO.glacerized_file_list} --debug"
    run_glrestore(cmd)

    # Make sure this file is now being restored
    assert glrestore.s3_utils.glacier_status_v2(BTO.glacerized_file_loc) == 'glacier-restoring'

def test_glrestore_3(BTO):
    """
    Test the "--wait" functionality
    """
    upload_glacierized_file()

    # Make sure this file starts off glacierized
    assert glrestore.s3_utils.glacier_status_v2(BTO.glacerized_file_loc) == 'glacier-no-restore'

    # Make sure this takes longer than 30 seconds to finish
    t = Thread(target=threaded_restore, args=(BTO.glacerized_file_loc,))  # run the some_task function in another
    t.daemon = True
    t.start()

    sleep(30)

    # Make sure this file is now being restored
    assert glrestore.s3_utils.glacier_status_v2(BTO.glacerized_file_loc) == 'glacier-restoring'

    # Make sure the program is still running
    assert t.is_alive()

def test_glstatus_1(BTO):
    """
    A simple test of glstatus
    """
    outloc = f"{BTO.test_dir}files.csv"
    cmd = f"glrestore -f s3://sonn-current/users/mattolm/testing_house/glrestore/ --report -o {outloc}"
    print(cmd)

    run_glrestore(cmd)

    # Load the report
    cdb = pd.read_csv(outloc)
    assert len(cdb) == 3

def threaded_restore(f):
    """
    Something to thread when testing the wait
    """
    cmd = f"glrestore -f {f} --wait"
    run_glrestore(cmd)