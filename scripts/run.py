#!/usr/bin/env python

import os
import subprocess
import sys


python = sys.executable
root_path = os.path.dirname(__file__)

def check_run(filename):
    """Run Python file relative to script, block, assert exit code is zero."""
    path = os.path.join(root_path, filename)
    return subprocess.check_call([python, path])


print("PREPROCESSING: Loading shapefiles and csv's.")

# Load data inputs.
check_run('load.py')

        
print("PROCESSING: Formatting tables and allocating demand-side agents.")
        
# Processing tables for UrbanSim
check_run('process.py')


print("SUMMARIZING: Exporting model tables to HDF5 and UrbanCanvas.")
        
# Outputting for use
check_run('export.py')