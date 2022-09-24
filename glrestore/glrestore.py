#!/usr/bin/env python
"""
Module Docstring
"""

from .version import __version__
__author__ = "Matt Olm"
__version__ = __version__
__license__ = "MIT"

import os
import sys
import boto3
import time
import copy
import argparse
import logging
import awswrangler
from time import sleep

import pandas as pd
import glrestore.s3_utils

def main():
    """ This is executed when run from the command line """
    args = parse_args()
    RestoreController(args).main()

class RestoreController(object):
    """
    Main controller of the restore command
    """
    def __init__(self, args):
        """
        Initialize and store args
        """
        self.args = args
        self.ori_args = copy.deepcopy(args)
        self.kwargs = vars(self.args)

    def main(self):
        """
        The main controller for restore
        """
        self.parse_arguments()

        logging.debug("Get objects to restore")
        self.file_classifications = self.get_files_to_restore_v2(self.kwargs.get('files'))

        if self.kwargs.get('report', True):
            logging.info("\n!!!!!!!!!!!\nWill NOT RESTORE anything because of --report flag; the following information is FYI only\n!!!!!!!!!!!!")

            logging.debug("Print status")
            self.print_status(sleep=False)

            logging.debug("Create report")
            self.create_report()

        else:
            logging.debug("Print status")
            self.print_status()

            logging.debug("Restoring files")
            self.restore_files()

        if self.kwargs.get('wait'):
            self.wait_for_restore()


    def parse_arguments(self):
        """
        Parse the arguments and add them this object as attributes
        """
        args = self.kwargs

        # Set up the log
        self.setup_log()

        # Set up boto3
        if 'profile' in args:
            boto3.setup_default_session(profile_name=args.get('profile'))
        else:
            session = boto3.session.Session()
            self.kwargs.client = session.client("s3")

    def get_files_to_restore_v2(self, files):
        """
        Return a list of s3 files to restore
        """
        # Get the command line argument
        base_restore = files

        # Load files if need be
        to_restore = []
        for br in base_restore:
            if not br.startswith('s3://'):
                with open(br, 'r') as r:
                    for line in r.readlines():
                        if not line.startswith('s3://'):
                            logging.error(f"CRITICAL ERROR! You passed {br} to -f, which doesn't start with s3://. I assumed this was a file of files to restore, but the line {line} also doesn't start with s3://. Will ignore {br} and {line}")
                        else:
                            to_restore.append(line.strip())
            else:
                to_restore.append(br)

        dbs = []
        for br in to_restore:
            db = glrestore.s3_utils.get_object_storage_class_v2(br)
            dbs.append(db)

        fc = pd.concat(dbs).reset_index(drop=True)
        return fc

    def print_status(self, sleep=True):
        """
        Print status and estimated costs
        """
        debug = self.kwargs.get('debug', False)

        cdb = self.file_classifications
        logging.info(f"Identified {len(cdb)} files")

        tdb = cdb[cdb['restore_status'] != False]
        logging.info(f"Of these, {len(tdb)} are being actively restored or are already restored")

        tdb = cdb[~cdb['storage_class'].isin(['GLACIER', 'DEEP_ARCHIVE'])]
        logging.info(f"Of these, {len(tdb)} are not in glacier")

        fcdb = cdb[(cdb['restore_status'] == False) & (cdb['storage_class'].isin(['GLACIER', 'DEEP_ARCHIVE']))]
        logging.info(f"Restoring the remaining {len(fcdb)} objects will cost the following:")

        self.display_restore_costs(fcdb, sleep=sleep)

        self.files_to_restore_filtered = fcdb['file'].tolist()

        if debug:
            for f in fcdb['file'].tolist():
                logging.debug(f)

    def create_report(self):
        """
        Create a report instead of actually restoring anything
        """
        outloc = self.kwargs.get('output')
        if not outloc.endswith('.csv'):
            outloc += '.csv'

        cdb = self.file_classifications
        logging.info(f"Identified {len(cdb)} files. Will create a report on them at {outloc}")
        cdb.to_csv(outloc, index=False)


    def display_restore_costs(self, fcdb, sleep=True):
        """
        Print how much this is going to cost

        NOTE- YOURE TREATING EVERYTHING AS IF IT'S BEING RESTORED FROM DEEP ARCHIVE; the "standard" is actully a bit cheaper when restoring from flexible
        """
        S3_COST_PER_GB_PER_MONTH = 0.022

        TIER2REQUEST2COST = {
            'Expedited':10,
            'Standard':0.10,
            'Bulk':0.025
        }
        TIER2REQUEST2SIZE_COST = {
            'Expedited': 0.03,
            'Standard': 0.02,
            'Bulk': 0.0025
        }

        # 0) Calculate the size and number of objects to restore
        num_obs = len(fcdb)
        size_obs = sum(fcdb['size_bytes']) / 1e9
        tier = self.kwargs.get('speed')

        # 1) Calculate the cost for the extra storage
        storage_cost = size_obs * (self.kwargs.get('days') / 30) * S3_COST_PER_GB_PER_MONTH

        # 2) Calculate the cost for the retrival costs
        t2cs = {}
        for t in ['Expedited', 'Standard', 'Bulk']:
            t2cs[t] = [(num_obs / 1000) * TIER2REQUEST2COST[t], size_obs * TIER2REQUEST2SIZE_COST[t]]

        # Display this info
        msg = "\n$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n"

        msg += f"It will cost the following to restore {num_obs} objects totalling {size_obs:.2f}GB:\n"
        for t, d in t2cs.items():
            msg += f"\t{t}: ${d[0]:.2f} + ${d[1]:.2f}\n"

        msg += f"It will also cost ${storage_cost:.2f} to restore the {size_obs:.3f}GB of data for {self.kwargs.get('days')} days"
        msg += '\n----------------------------\n'
        msg += f"Your TOTAL COST at {tier} speed will be ${storage_cost + sum(t2cs[tier]):0.2f}\n"

        if sleep:
            msg += f"You chose to restore at {tier} speed. Please quit the program now (ctrl + c) if you'd like to change that! I'll wait 5 seconds"
        msg += "\n$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n"

        logging.info(msg)
        if sleep:
            time.sleep(5)

    def restore_files(self):
        """
        Actually do the file restoring
        """
        files_to_restore_filtered = self.files_to_restore_filtered

        for f in files_to_restore_filtered:

            glrestore.s3_utils.restore_file(f, **self.kwargs)

        logging.info(f"Restore commands finished launching")

    def wait_for_restore(self):
        """
        Enter the loop where you wait for objects to restore before exiting the program
        """
        remaining = self.files_to_restore_filtered
        print(f"I am going to wait for {len(remaining)} files to be restored")

        start = time.time()
        while True:
            sleep(5)

            cdb = self.get_files_to_restore_v2(remaining)
            remaining = cdb[(cdb['restore_status'] == "restoring")]['file'].tolist()

            if len(remaining) == 0:
                break

            elapsed = time.time() - start

            sys.stdout.write('\r')
            # the exact output you're looking for:
            sys.stdout.write(f'Ive been waiting for {time.strftime("%Hh%Mm%Ss", time.gmtime(elapsed))}: {len(remaining)} files remain')
            sys.stdout.flush()

        elapsed = time.time() - start
        print(f'All done! The restore took {time.strftime("%Hh%Mm%Ss", time.gmtime(elapsed))}')

    def setup_log(self):
        args = self.kwargs

        # Set up the log
        root = logging.getLogger()

        if args.get('debug', False):
            root.setLevel(logging.DEBUG)
        else:
            root.setLevel(logging.INFO)

        handler = logging.StreamHandler(sys.stdout)
        if args.get('debug', False):
            root.setLevel(logging.DEBUG)
        else:
            handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        root.addHandler(handler)

        # Play debug message
        logging.debug("!" * 80)
        logging.debug("Command to run was: {0}\n".format(' '.join(sys.argv)))
        logging.debug("glrestore version {0} was run \n".format(__version__))
        logging.debug("!" * 80 + '\n')


def controller(args):
    """ Main entry point of the app """
    logging.info("hello world")
    logging.info(args)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-f', '--files',
        help="File or files to be restored (or a list of files). Can include wildcards. Must start with the bucket in the format (s3://)",
        nargs='*', default=[])

    parser.add_argument(
        '-d', '--days',
        help="Number of days to restore the data to S3 for. During this time we pay for both glacier and S3 storage of the data.",
        default=7, type=int)

    parser.add_argument(
        '-s', '--speed',
        help="Speed at which to restore the data; faster is more expensive. Expedited=(1-5 min), Standard=(3-5 hr), Bulk=(12 hr)",
        default='Expedited', choices=['Expedited', 'Standard', 'Bulk'],)

    parser.add_argument(
        '--profile',
        help="AWS credential profile to use. Will use default by default")

    parser.add_argument(
        '--report',
        help='Rather than actually doing anything, just make a report of which files are matched by the -f argument and what their status is. Will make a file with this info based on the name in the -o argument',
        default=False, action="store_true")

    parser.add_argument(
        '-o', '--output',
        help='Where to store the --report information',
        default='glrestore_report.txt')

    parser.add_argument(
        '--wait',
        help='Wait for restore to finish before exiting the program. Works with --report too',
        default=False, action="store_true")

    parser.add_argument(
        '--debug',
        help='Create debugging log file',
        default=False, action= "store_true")

    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s (version {version})".format(version=__version__))

    args = parser.parse_args()
    return args

if __name__ == "__main__":
    main()

