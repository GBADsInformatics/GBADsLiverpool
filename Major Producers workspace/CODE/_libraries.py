# =============================================================================
#### Fundamentals
# =============================================================================
import sys                       # System parameters and functions
import os                        # Operating system functions
import subprocess                # For running command prompt or other programs
import io                        # For changing input/output streams
import inspect                   # For inspecting objects
import pkg_resources as pkg      # For getting info about installed packages
import imp                       # Import tools e.g. reload()
import time                      # Time functions
import datetime as dt            # Date and time functions
import itertools                 # Iterator building blocks
import re                        # For handling regular expressions

# =============================================================================
#### Data processing
# =============================================================================
import numpy as np
import scipy as sp
import pandas as pd
pd.set_option('display.max_columns', 300)         # Max columns to show with df.head()
pd.set_option('display.max_info_columns', 300)    # Max columns to show with df.info()

import pickle                             # To save objects to disk
import joblib                             # Tools to provide lightweight pipelining, e.g. saving model objects
import pandasql as psql                   # For using SQL statements with Pandas dataframes
import pandas_profiling as pdp            # Data profiling
import sklearn.preprocessing as skpre     # Data scaling, centering, etc.
import sklearn.model_selection as sksel   # Data splitting (train/test)

import tabula                             # Extracting tables from PDF files

# =============================================================================
#### Visualize and Model
# =============================================================================
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages   # For saving plots to PDF
plt.style.use('seaborn-whitegrid')        # Set graph style
matplotlib.rcParams.update({'figure.autolayout':True})      # To improve auto-fitting of plots in windows

# Matplotlib can use many different backends
# The choice of backend relates to hanging/crashing problems
# Note: specifying a backend will disable Spyder's Plots pane
matplotlib.get_backend()
# matplotlib.use('qt5agg')                 # If you get errors due to "non-GUI backend", specify
# matplotlib.use('webagg')                 # Produces plot in web browser

import plotly.express as px
import seaborn as sns                     # Statistical graphics
import scipy.stats as sps                 # Statistical distributions and tests, used e.g. for seaborn distplot fits
import statsmodels.api as sm
import statsmodels.formula.api as smf
import sklearn.linear_model as sklm
import sklearn.metrics as skmet           # Score functions, model performance metrics
import sklearn.tree as skt                # Decision Trees
