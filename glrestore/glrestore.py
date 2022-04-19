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
import copy
import argparse
import logging
import awswrangler

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
        debug = self.kwargs.get('debug', False)

        logging.debug("Figuring out what to restore")
        self.get_files_to_restore()
        logging.info(f"Identified {len(self.files_to_restore)} files to restore")
        if debug:
            for f in self.files_to_restore:
                logging.debug(f)

        logging.debug("Restoring files")
        self.restore_files()

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

    def get_files_to_restore(self):
        """
        Return a list of s3 files to restore
        """
        # Get the command line argument
        base_restore = self.kwargs.get('files')

        FILES_TO_RESTORE = []
        for br in base_restore:
            FILES_TO_RESTORE += awswrangler.s3.list_objects(br)

        self.files_to_restore = FILES_TO_RESTORE

    def restore_files(self):
        """
        Actually do the file restoring
        """
        files_to_restore = self.files_to_restore

        for f in files_to_restore:
            status = glrestore.s3_utils.glacier_status(f)
            if status != 'glacier-no-restore':
                logging.info(f"{f} is {status}; skipping")
                continue
            logging.debug(f"Restoring {f}; status is {status}")
            glrestore.s3_utils.restore_file(f, **self.kwargs)

        logging.info(f"Restore commands finished launching")

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
        help="File or files to be restored. Can include wildcards. Must start with the bucket in the format (s3://)",
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

