import sys
sys.path.append('/home/ec2-user/realtimeBW/aws/src')
import boto3
import configparser
import json
from measure import *
from distanceBetweenIPs import *
from predict.rfPredict import *
from optimization.greedyOptimization import *
import os
import time
import subprocess
import random
import datetime

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

captureStatic = False
if config.has_option('PREDICT_CONFIGS', 'captureStatic'):
	if config.get('PREDICT_CONFIGS', 'captureStatic').strip().upper() == "TRUE":
		captureStatic = True

if not config.has_option('PREDICT_CONFIGS', 'reportPath'):
	raise Exception("Error: reportPath is not configured!!!")
reportPath = config.get('PREDICT_CONFIGS', 'reportPath')
reportPathAbs = os.path.abspath(reportPath)

isExistDir = os.path.exists(reportPathAbs)
if not isExistDir:
	os.makedirs(reportPathAbs)

if not(config.has_option('PLUGIN_CONFIGS', 'modelOutputPath')):
	raise Exception("Error: Model output path is not configured correctly!")
modelOutputPath = config.get('PLUGIN_CONFIGS', 'modelOutputPath')

index = 1
ctrSnapshotFiles = 0
for file_name in os.listdir(reportPathAbs):
	if(file_name.startswith('snapshot')):
		ctrSnapshotFiles = ctrSnapshotFiles + 1
if ctrSnapshotFiles != 0:
	index = ctrSnapshotFiles + 1

inUseIpsToProviderRegions = {}
if config.has_option('PREDICT_CONFIGS', 'inUseIpsToProviderRegions'):
	inUseIpsToProviderRegions = json.loads(config.get('PREDICT_CONFIGS', 'inUseIpsToProviderRegions'))
else:
	raise Exception("Error: inUseIps is not set in PREDICT_CONFIGS. Please check config.cfg for the configurations!!!")

ipToInstanceType = {}
if config.has_option('PREDICT_CONFIGS', 'ipToInstanceTypes'):
	ipToInstanceType = json.loads(config.get('PREDICT_CONFIGS', 'ipToInstanceTypes'))
else:
	raise Exception("Error: ipToInstanceTypes is not set in PREDICT_CONFIGS. Please check config.cfg for the configurations!!!")

pubToPrivateIps = {}
if config.has_option('PREDICT_CONFIGS', 'pubToPrivateIps'):
	pubToPrivateIps = json.loads(config.get('PREDICT_CONFIGS', 'pubToPrivateIps'))

MULTI_CLOUD_ENABLED=False

providerToKeyPath = {}
if config.has_option('PREDICT_CONFIGS', 'providerToKeyPath'):
	MULTI_CLOUD_ENABLED=True
	providerToKeyPath = json.loads(config.get('PREDICT_CONFIGS', 'providerToKeyPath'))
else:
	MULTI_CLOUD_ENABLED=False

providerToUsername = {}
if config.has_option('PREDICT_CONFIGS', 'providerToUsername'):
	MULTI_CLOUD_ENABLED=True
	providerToUsername = json.loads(config.get('PREDICT_CONFIGS', 'providerToUsername'))
else:
	MULTI_CLOUD_ENABLED=False

if not config.has_option('PLUGIN_CONFIGS', 'username'):
	raise Exception("Username is not set!!!")
username = config.get('PLUGIN_CONFIGS', 'username')

if not config.has_option('PLUGIN_CONFIGS', 'basePort'):
	raise Exception("Error: Base port is not configured!!!")
basePort=config.get('PLUGIN_CONFIGS', 'basePort')

if not config.has_option('PLUGIN_CONFIGS', 'privateKeyPath'):
	raise Exception("Error: Private key path is not specified!!!")
privateKeyPath = config.get('PLUGIN_CONFIGS', 'privateKeyPath')
privateKeyPathAbs = os.path.abspath(privateKeyPath)

if MULTI_CLOUD_ENABLED:
	#override the contents in username and private key variables
	privateKeyPathAbs = providerToKeyPath
	username = providerToUsername

privateIPEnabled = False
if config.has_option('PLUGIN_CONFIGS', 'isPrivateIPUsageEnabled'):
	privateIPEnabledStr = config.get('PLUGIN_CONFIGS', 'isPrivateIPUsageEnabled')
	if privateIPEnabledStr.upper() == "TRUE":
		privateIPEnabled = True

debugEnabled = False
if config.has_option('PLUGIN_CONFIGS', 'isDebugEnabled'):
	debugEnabledStr = config.get('PLUGIN_CONFIGS', 'isDebugEnabled')
	if debugEnabledStr.upper() == "TRUE":
		debugEnabled = True

statusProgressMsgEnabled = False
if config.has_option('PLUGIN_CONFIGS', 'statusProgressMsgEnabled'):
	statusProgressKeyStr = config.get('PLUGIN_CONFIGS', 'statusProgressMsgEnabled')
	if statusProgressKeyStr.upper() == "TRUE":
		statusProgressMsgEnabled = True

NUM_DATACENTERS = 8
if config.has_option('PLUGIN_CONFIGS', 'NUM_DATACENTERS'):
	NUM_DATACENTERS = int(config.get('PLUGIN_CONFIGS', 'NUM_DATACENTERS'))

compareFrequency = 3
if config.has_option('PREDICT_CONFIGS', 'compareFrequency'):
	compareFrequency = int(config.get('PREDICT_CONFIGS', 'compareFrequency'))

refactoringVector = np.ones((1, NUM_DATACENTERS * NUM_DATACENTERS))
if os.path.isfile('./refactoringVector.pkl'):
	with open('./refactoringVector.pkl', 'rb') as file:  
		refactoringVector = pickle.load(file)

uniqueDCSet = {inUseIpsToProviderRegions[k] for k in inUseIpsToProviderRegions}
numDCs = len(uniqueDCSet)

allIPs = list(inUseIpsToProviderRegions.keys())
ipToRegions = inUseIpsToProviderRegions
if len(pubToPrivateIps) > 0:
	print("Private IPs mode")
	allIPs = [pubToPrivateIps[k] for k in pubToPrivateIps]
	print(allIPs)
	ipToRegions = {}
	for k in pubToPrivateIps:
		ipToRegions[pubToPrivateIps[k]] = inUseIpsToProviderRegions[k]

#removeThis
# TEST_DCs=8
# IS_RANDOM_SAMPLE=False
# if IS_RANDOM_SAMPLE and False:
# 	fromEachProvider = TEST_DCs//2
# 	fromAWS=fromEachProvider
# 	fromGCP=fromEachProvider
# 	print("fromEachProvider is: {}".format(fromEachProvider))
# 	if (fromEachProvider*2 == TEST_DCs):
# 		fromAWS=fromEachProvider
# 		fromGCP=fromEachProvider
# 	else:
# 		fromAWS=fromEachProvider
# 		fromGCP=fromEachProvider+1
# 	print("fromAWS: {}".format(fromAWS))
# 	print("fromGCP: {}".format(fromGCP))
# 	numDCs = TEST_DCs
# 	awsIps=[]
# 	gcpIps=[]
# 	for k,v in inUseIpsToProviderRegions.items():
# 		if "gcp" in v:
# 			gcpIps.append(k)
# 		else:
# 			awsIps.append(k)
# 	print("All AWS IPs: {}".format(awsIps))
# 	print("All GCP IPs: {}".format(gcpIps))
# 	filteredAWSIps = random.sample(awsIps, fromAWS)
# 	print("Filtered AWS IPs: {}".format(filteredAWSIps))
# 	filteredGCPIps = random.sample(gcpIps, fromGCP)
# 	print("Filtered GCP IPs: {}".format(filteredGCPIps))
# 	allIPs = filteredAWSIps + filteredGCPIps
# 	with open('./filteredIps.pkl', 'wb') as file:  
# 		pickle.dump(allIPs, file)
# elif False:
# 	numDCs = TEST_DCs
# 	with open('./filteredIps.pkl', 'rb') as file:  
# 		allIPs = pickle.load(file)
# print("FilteredIPs are: {}".format(allIPs))
#removeThis

# allIPs = random.sample(allIPs, TEST_DCs)

ip1_ip2_distance_map = {}
isCachedObjectExist = os.path.isfile('./ip-distance.pkl')
if isCachedObjectExist:
	with open('./ip-distance.pkl', 'rb') as file:  
		ip1_ip2_distance_map = pickle.load(file)
else:
	for ip1 in list(inUseIpsToProviderRegions.keys()):
		for ip2 in list(inUseIpsToProviderRegions.keys()):
			if ip1 != ip2:
				keyToStore = ip1+"-"+ip2
				if len(pubToPrivateIps) > 0:
					#private ips in use, hence update key
					keyToStore = pubToPrivateIps[ip1]+"-"+pubToPrivateIps[ip2]
				ip1_ip2_distance_map[keyToStore] = round(computeIPDistance(ip1, ip2),2)
	with open('./ip-distance.pkl', 'wb') as file:  
		pickle.dump(ip1_ip2_distance_map, file)
	print("Distance between IPs stored in pickle object for faster reference. Delete src/predict/ip-distance.pkl if error is generated!")

staticFlowOn = False
dyanmicFlowOn = False
staticStartTimer=time.time()
if(captureStatic):
	#calling predict monitor to capture static BWs (since this metric is enabled)
	statusCode = runMonitor(username, allIPs, ipToRegions, basePort, privateKeyPathAbs, privateIPEnabled, reportPathAbs, index, 't2.medium', False, '20', debugEnabled, statusProgressMsgEnabled, ip1_ip2_distance_map, numDCs, 30, 1)
	staticFlowOn = True
	if statusCode is not None:
		raise Exception("Error in static BWs collection in the predict flow!")
staticEndTimer=time.time()

# cleanupMode should be passed as 2 in command line only for the first time after model training. thereafter it should be passed as 3 so that monitoring is done using existing files. This is designed so as to reduce the overhead for subsequent livePredictor calls
cleanupMode = int(sys.argv[1])
#calling predict monitor to capture dynamic BWs (if enabled)
dynamicStartTimer=time.time()
if index % compareFrequency == 0:
	statusCode = runMonitor(username, allIPs, ipToRegions, basePort, privateKeyPathAbs, privateIPEnabled, reportPathAbs, index, 't2.medium', True, '20', debugEnabled, statusProgressMsgEnabled, ip1_ip2_distance_map, numDCs, 30, cleanupMode)
	dyanmicFlowOn = True
	if statusCode is not None:
		raise Exception("Error in dynamic BWs collection in the predict flow!")
dynamicEndTimer=time.time()

#calling predict monitor to capture snapshot
snapshotStartTimer=time.time()
proc_update = subprocess.Popen(["sed", "-i", "s/"+"test_duration="+str(20)+"/"+"test_duration="+str(1)+"/g", "src/bwtesting-client.py"])
proc_update.wait()
proc_update = subprocess.Popen(["sed", "-i", "s/"+"test_duration="+str(20)+"/"+"test_duration="+str(1)+"/g", "src/bwtesting-client-copy.py"])
proc_update.wait()

statusCode = runMonitor(username, allIPs, ipToRegions, basePort, privateKeyPathAbs, privateIPEnabled, reportPathAbs, index, 't2.medium', True, '1', debugEnabled, statusProgressMsgEnabled, ip1_ip2_distance_map, numDCs, 10, cleanupMode)
proc_update = subprocess.Popen(["sed", "-i", "s/"+"test_duration="+str(1)+"/"+"test_duration="+str(20)+"/g", "src/bwtesting-client.py"])
proc_update.wait()
proc_update = subprocess.Popen(["sed", "-i", "s/"+"test_duration="+str(1)+"/"+"test_duration="+str(20)+"/g", "src/bwtesting-client-copy.py"])
proc_update.wait()
if statusCode is not None:
	raise Exception("Error in snapshot collection in the predict flow!")
snapshotEndTimer=time.time()

# all metrics collected, now going for prediction
staticVals=[]
snapshotVals=[]
dynamicVals=[]
if staticFlowOn:
	staticVals = getMetricsFromFile(NUM_DATACENTERS, index, reportPathAbs, 1)

if dyanmicFlowOn:
	dynamicVals = getMetricsFromFile(NUM_DATACENTERS, index, reportPathAbs, 3)

snapshotVals = getMetricsFromFile(NUM_DATACENTERS, index, reportPathAbs, 2)

mseVal, maeVal, sqrtVal, err50, err100, err150, err200, err250, err300, err300p, predict_y = wPredict(NUM_DATACENTERS, modelOutputPath, snapshotVals, dynamicVals, dyanmicFlowOn, refactoringVector)
if dyanmicFlowOn:
	print("Comparing with dynamic BWs with predicted values based on the configured interval!")
	print("The MSE, RMSE and MAE are {}, {} and {}, respectively.".format(mseVal, sqrtVal, maeVal))
	print("The err50 is {}".format(err50))
	print("The err100 is {}".format(err100))
	print("The err150 is {}".format(err150))
	print("The err200 is {}".format(err200))
	print("The err250 is {}".format(err250))
	print("The err300 is {}".format(err300))
	print("The err300p is {}".format(err300p))

if captureStatic:
	err50, err100, err150, err200, err250, err300, err300p = compareDynamicWithStatic(staticVals, dynamicVals, NUM_DATACENTERS)
	print("\n\nComparison values with static monitoring : ")
	print("The err50 is {}".format(err50))
	print("The err100 is {}".format(err100))
	print("The err150 is {}".format(err150))
	print("The err200 is {}".format(err200))
	print("The err250 is {}".format(err250))
	print("The err300 is {}".format(err300))
	print("The err300p is {}".format(err300p))

print("Static monitoring took: {} seconds!".format(staticEndTimer - staticStartTimer))
print("Dynamic monitoring took: {} seconds!".format(dynamicEndTimer - dynamicStartTimer))
print("Snapshot monitoring took: {} seconds!".format(snapshotEndTimer - snapshotStartTimer))

greedyOptimization = False
if config.has_option('PREDICT_CONFIGS', 'greedyOptimization'):
	greedyOptimizationStr = config.get('PREDICT_CONFIGS', 'greedyOptimization')
	if greedyOptimizationStr.upper() == "TRUE":
		greedyOptimization = True

if greedyOptimization:
	predictedBWs = np.reshape(predict_y[0], (-1, NUM_DATACENTERS))
	for i in range(NUM_DATACENTERS):
		predictedBWs[i][i] = -1000
	greedyApp(NUM_DATACENTERS, allIPs, ipToRegions, privateKeyPathAbs, username, predictedBWs)

errorThreshold = 10
if config.has_option('PREDICT_CONFIGS', 'errorNumThreshold'):
	errorThreshold = float(config.get('PREDICT_CONFIGS', 'errorNumThreshold'))

if (err150 + err200 + err250 + err300 + err300p) > errorThreshold:
	reportStat = open(reportPathAbs + "/compareStatus.txt", "a")
	reportStat.write(str(datetime.datetime.now()) + "###Retrain needed!!!\n")
	reportStat.close()
else:
	reportStat = open(reportPathAbs + "/compareStatus.txt", "a")
	reportStat.write(str(datetime.datetime.now()) + "Compare OK.\n")
	reportStat.close()

