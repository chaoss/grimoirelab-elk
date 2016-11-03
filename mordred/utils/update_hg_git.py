#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# This script manage the information about Mordred projects
#
# Copyright (C) 2016 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Quan Zhou <quan@bitergia.com>
#


import argparse
import os
import subprocess
from time import time


def read_arguments():
    desc="Update hg mercurial repositories"
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=desc)


    parser.add_argument("hg_web",
                        action="store",
                        help="hg web")
    parser.add_argument("dir",
                        action="store",
                        help="The directory where you want to clone the hg repositories")

    args = parser.parse_args()

    return args

if __name__ == "__main__":
    time_init = time()
    args = read_arguments()

    path = args.dir
    hg = args.hg_web
    hg_repos = os.popen("python3 repos_from_hgweb.py "+hg).read().split("\n")

    del hg_repos[-1]
    for repo in hg_repos:
        pr = subprocess.Popen( "git clone hg::"+repo , cwd = os.path.dirname( path ), shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
        (out, error) = pr.communicate()
        if "already exists" in str(error):
            pr = subprocess.Popen( "git pull" , cwd = os.path.dirname( path ), shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
    time_final = time()
    print("Time execution: "+str(time_final - time_init))
