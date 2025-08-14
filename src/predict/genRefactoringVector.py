import sys
sys.path.append('/home/ec2-user/realtimeBW/aws/src')
import boto3
import configparser
import json
from measure import *
from distanceBetweenIPs import *
from predict.rfPredict import *
import os
import time
import subprocess
import random

os.chdir('/home/ec2-user/realtimeBW/aws')
config = configparser.RawConfigParser()
config.read('config.cfg')

def compareDynamicWithStatic(staticVals, dynamicVals, NUM_DATACENTERS):
	err50 = 0
	err100 = 0
	err150 = 0
	err200 = 0
	err250 = 0
	err300 = 0
	err300p = 0
	for ind in range(NUM_DATACENTERS * NUM_DATACENTERS):
		if (dynamicVals[0][ind] > 0):
			differenceVal = abs(staticVals[0][ind] - dynamicVals[0][ind])
			if(differenceVal>30 and differenceVal<=50):
				err50 += 1
			elif(differenceVal>50 and differenceVal<=100):
				err100 += 1
			elif(differenceVal>100 and differenceVal<=150):
				err150 += 1
			elif(differenceVal>150 and differenceVal<=200):
				err200 += 1
			elif(differenceVal>200 and differenceVal<=250):
				err250 += 1
			elif(differenceVal>250 and differenceVal<=300):
				err300 += 1
			elif(differenceVal > 300):
				err300p += 1
	return err50, err100, err150, err200, err250, err300, err300p

#mode = 1 for static, 2 for snapshot and 3 for dynamic BWs
def getMetricsFromFile(NUM_DATACENTERS, readIndex, reportPathAbs, mode):
	fileName = ''
	filteredMat = []
	dcToIndexMap={}
	# dcToIndexMap['aws~us-east-1']=0
	# dcToIndexMap['aws~us-west-1']=1
	# dcToIndexMap['gcp~ap-south-1']=2
	# dcToIndexMap['gcp~ap-southeast-1']=3
	# dcToIndexMap['gcp~ap-southeast-2']=4
	# dcToIndexMap['gcp~ap-northeast-1']=5
	# dcToIndexMap['aws~eu-west-1']=6
	# dcToIndexMap['aws~sa-east-1']=7
	if config.has_option('PREDICT_CONFIGS', 'providerDCToIndexMap'):
		dcToIndexMap = json.loads(config.get('PREDICT_CONFIGS', 'providerDCToIndexMap'))
	else:
		raise Exception("Error: providerDCToIndexMap is not set in PREDICT_CONFIGS. Please check config.cfg for the configurations!!!")
	if mode == 1:
		fileName = reportPathAbs + '/static'+str(readIndex)+'_20.json'
		filteredMat = np.zeros((1, NUM_DATACENTERS * NUM_DATACENTERS))
		filter_x_i=np.zeros((NUM_DATACENTERS, NUM_DATACENTERS))
		file=open(fileName)
		data = json.load(file)
		for i in data['readings']:
			if (filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]] == 0):
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]] = float(i["sent_mbps"]) + float(i["received_mbps"])
			else:
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]] = filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]] + float(i["sent_mbps"]) + float(i["received_mbps"])
		filteredMat[0]=filter_x_i.reshape([1,NUM_DATACENTERS * NUM_DATACENTERS])
		file.close()
	elif mode == 2:
		fileName = reportPathAbs + '/snapshot'+str(readIndex)+'_1.json'
		if not(os.path.isfile(fileName)):
			raise Exception("Error: Snapshot file for provided index must exist to generate the factoring matrix!")
		filteredMat = np.zeros((1, NUM_DATACENTERS * NUM_DATACENTERS * 7))
		file = open(fileName)
		data = json.load(file)
		filter_x_i=np.zeros((NUM_DATACENTERS, NUM_DATACENTERS, 7))
		numDCs = int(data["numDCs"])
		for i in data['readings']:
			filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][0] = numDCs
			if (filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][1] == 0):
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][1] = int(i["num_retransmits"])
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][2] = round(float(i["memory_util"])*100)
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][3] = round(float(i["local_cpu_total"]))
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][4] = round(float(i["remote_cpu_total"]))
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][5] = round(float(i["ipDistance"]))
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][6] = float(i["sent_Kbytes_per_second"]) + float(i["received_Kbytes_per_second"])
			else:
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][1] = filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][1] + int(i["num_retransmits"])
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][2] = filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][2] + round(float(i["memory_util"])*100)
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][3] = filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][3] + round(float(i["local_cpu_total"]))
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][4] = filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][4] + round(float(i["remote_cpu_total"]))
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][5] = filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][5] + round(float(i["ipDistance"]))
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][6] = filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][6] + float(i["sent_Kbytes_per_second"]) + float(i["received_Kbytes_per_second"])
		filteredMat[0]=filter_x_i.reshape([1,NUM_DATACENTERS * NUM_DATACENTERS * 7])
		file.close()
	else:
		fileName = reportPathAbs + '/dynamic'+str(readIndex)+'_20.json'
		if not(os.path.isfile(fileName)):
			raise Exception("Error: Dynamic file for provided index must exist to generate the factoring matrix!")
		filteredMat = np.zeros((1, NUM_DATACENTERS * NUM_DATACENTERS))
		filter_x_i=np.zeros((NUM_DATACENTERS, NUM_DATACENTERS))
		file=open(fileName)
		data = json.load(file)
		for i in data['readings']:
			if (filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]] == 0):
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]] = float(i["sent_mbps"]) + float(i["received_mbps"])
			else:
				filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]] = filter_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]] + float(i["sent_mbps"]) + float(i["received_mbps"])
		filteredMat[0]=filter_x_i.reshape([1,NUM_DATACENTERS * NUM_DATACENTERS])
		file.close()
	return filteredMat

if not config.has_option('PREDICT_CONFIGS', 'reportPath'):
	raise Exception("Error: reportPath is not configured!!!")
reportPath = config.get('PREDICT_CONFIGS', 'reportPath')
reportPathAbs = os.path.abspath(reportPath)

if not(config.has_option('PLUGIN_CONFIGS', 'modelOutputPath')):
	raise Exception("Error: Model output path is not configured correctly!")
modelOutputPath = config.get('PLUGIN_CONFIGS', 'modelOutputPath')

if len(sys.argv) < 2:
	raise Exception("Error: Provide the index of snapshot/dynamic file with which refactoring matrix should be created! Note that both snapshot and dynamic file must exist for such an index in the configured <reportPath>. If not create these files first using livePredictor!")
index=int(sys.argv[1])

NUM_DATACENTERS = 8
if config.has_option('PLUGIN_CONFIGS', 'NUM_DATACENTERS'):
	NUM_DATACENTERS = int(config.get('PLUGIN_CONFIGS', 'NUM_DATACENTERS'))

snapshotVals=[]
dynamicVals=[]

dynamicVals = getMetricsFromFile(NUM_DATACENTERS, index, reportPathAbs, 3)

snapshotVals = getMetricsFromFile(NUM_DATACENTERS, index, reportPathAbs, 2)

refactoringVector = np.ones((1, NUM_DATACENTERS * NUM_DATACENTERS))
mseVal, maeVal, sqrtVal, err50, err100, err150, err200, err250, err300, err300p, predict_y = wPredict(NUM_DATACENTERS, modelOutputPath, snapshotVals, dynamicVals, False, refactoringVector)

isRevert = False
for i in range(NUM_DATACENTERS*NUM_DATACENTERS):
	if float(predict_y[0][i]) > 0:
		refactoringVector[0][i] = float(dynamicVals[0][i])/float(predict_y[0][i])
	if dynamicVals[0][i] > 0 and predict_y[0][i] == 0:
		isRevert = True
		break
if isRevert:
	# if ML model fails to predict runtime BWs based on training data collected, fall back to stable dynamic values
	refactoringVector = dynamicVals

with open('./refactoringVector.pkl', 'wb') as file:  
	pickle.dump(refactoringVector, file)

print("Refactoring vector generated successfully!")

