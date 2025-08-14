import tensorflow as tf
from keras.layers import Dense, Conv2D, Flatten, MaxPooling2D, Input, Reshape
import json
import numpy as np
from os.path import exists
import pickle
import os
import sys
import configparser
# import copy

print("### The tensorflow version is:")
print(tf.__version__)

config = configparser.RawConfigParser()
config.read('config.cfg')

if not(config.has_option('PLUGIN_CONFIGS', 'modelOutputPath')):
	raise Exception("Error: modelOutputPath is not configured correctly!")
modelOutputPath = config.get('PLUGIN_CONFIGS', 'modelOutputPath')
modelOutputPathAbs = os.path.abspath(modelOutputPath)

with open(modelOutputPathAbs + '/model.pkl', 'rb') as file:  
    model = pickle.load(file)

dcToIndexMap={}
# dcToIndexMap['us-east-1']=0
# dcToIndexMap['us-west-1']=1
# dcToIndexMap['ap-south-1']=2
# dcToIndexMap['ap-southeast-1']=3
# dcToIndexMap['ap-southeast-2']=4
# dcToIndexMap['ap-northeast-1']=5
# dcToIndexMap['eu-west-1']=6
# dcToIndexMap['sa-east-1']=7
if config.has_option('LAUNCH_CONFIGS', 'dcToIndexMap'):
	dcToIndexMap = json.loads(config.get('LAUNCH_CONFIGS', 'dcToIndexMap'))
else:
	raise Exception("Error: dcToIndexMap is not set in LAUNCH_CONFIGS. Please check config.cfg for the configurations!!!")

NUM_DATACENTERS = 8
if config.has_option('PLUGIN_CONFIGS', 'NUM_DATACENTERS'):
	NUM_DATACENTERS = int(config.get('PLUGIN_CONFIGS', 'NUM_DATACENTERS'))

#ACT_DATACENTERS: Actual number of DCs for runtime BW determination
ACT_DATACENTERS = 8
if config.has_option('PLUGIN_CONFIGS', 'ACT_DATACENTERS'):
	ACT_DATACENTERS = int(config.get('PLUGIN_CONFIGS', 'ACT_DATACENTERS'))

fileToRead = sys.argv[1]

if (not exists(fileToRead)):
	raise Exception("Invalid file for prediction: {}! Check if the file exists.".format(fileToRead))

referenceMatrix=np.zeros((NUM_DATACENTERS, NUM_DATACENTERS))

file = open(fileToRead)
data = json.load(file)
for i in data['readings']:
	referenceMatrix[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]] = int(i["sent_mbps"]) + int(i["received_mbps"])

standardDev = np.std(referenceMatrix)

print("The standardDev is: \n{}".format(standardDev))
