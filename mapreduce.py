''' pybq.mapreduce: for performing map reduce from bqdf operation
'''


import pandas as pd
import numpy as np
import sys
import os
import json
import cfg
sys.path.append(cfg.gsdk_path)
import bq

import util
from bq import apiclient
from apiclient import googleapiclient, oauth2client
import googleapiclient.discovery
import oauth2client.client
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
