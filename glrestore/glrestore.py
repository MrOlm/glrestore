#!/usr/bin/env python
"""
Module Docstring
"""

from .version import __version__
__author__ = "Matt Olm"
__version__ = __version__
__license__ = "MIT"

import argparse
import logging

def main():
    """ This is executed when run from the command line """
    args = parse_args()
    controller(args)

def controller(args):
    """ Main entry point of the app """
    logging.info("hello world")
    logging.info(args)

def parse_args():
    parser = argparse.ArgumentParser()

    # Specify output of "--version"
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s (version {version})".format(version=__version__))

    args = parser.parse_args()
    return args

if __name__ == "__main__":
    main()

