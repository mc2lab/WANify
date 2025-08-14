import boto3
import configparser
import json
from measure import *
from distanceBetweenIPs import *
from predict.rfTrain import *
import os
import time
import subprocess
import random

config = configparser.RawConfigParser()
config.read('config.cfg')
regionAMIMap = {}
subnetsMap = {}
instanceType = ""
all_IPs=[]
priv_To_Pub_IPs={}
allRegions_Dict={}
ip1_ip2_distance_map={}
ip_To_Region={}
spawnFromScrath = False

def launchInstances(regions, subnets, instType, privateIPEnabled, debugEnabled):
	NUM_REGIONS = len(regions)
	NUM_INSTANCES_EACH_REGION = 1
	NUM_CLIENTS_EACH_INSTANCE = 1
	NUM_SERVERS=NUM_REGIONS*NUM_INSTANCES_EACH_REGION*NUM_CLIENTS_EACH_INSTANCE
	INST_TYPE=instType

	all_IPs=[]
	priv_To_Pub_IPs={}
	allRegions_Dict={}
	ip_To_Region={}

	for region in regions:
		region_client = boto3.client('ec2', region_name=region)
		resp = region_client.run_instances(ImageId=regions[region], InstanceType=INST_TYPE, MinCount=NUM_INSTANCES_EACH_REGION, MaxCount=NUM_INSTANCES_EACH_REGION, SubnetId=subnets[region])
		instanceIdList=[]
		for instance in resp['Instances']:
			if debugEnabled:
				print(instance['InstanceId'])
			instanceIdList.append(instance['InstanceId'])
			instance_runner_waiter = region_client.get_waiter('instance_running')
			instance_runner_waiter.wait(InstanceIds=instance['InstanceId'].split())
			newResp = region_client.describe_instances(Filters=[{'Name': 'instance-id', 'Values': [instance['InstanceId']]}])
			if debugEnabled:
				print(newResp['Reservations'][0]['Instances'][0]['PublicIpAddress'])
				print(newResp['Reservations'][0]['Instances'][0]['PrivateIpAddress'])
			if privateIPEnabled:
				all_IPs.append(newResp['Reservations'][0]['Instances'][0]['PrivateIpAddress'])
				priv_To_Pub_IPs[newResp['Reservations'][0]['Instances'][0]['PrivateIpAddress']]=newResp['Reservations'][0]['Instances'][0]['PublicIpAddress']
				ip_To_Region[newResp['Reservations'][0]['Instances'][0]['PrivateIpAddress']]=region
			else:
				all_IPs.append(newResp['Reservations'][0]['Instances'][0]['PublicIpAddress'])
				ip_To_Region[newResp['Reservations'][0]['Instances'][0]['PublicIpAddress']]=region
		allRegions_Dict[region]=instanceIdList

	return all_IPs, priv_To_Pub_IPs, ip_To_Region, allRegions_Dict

def terminateInstances(allRegions_Dict):
	#Terminate all the newly spawned instances
	for key in allRegions_Dict:
		# if(key != 'ap-south-1'):
			if debugEnabled:
				print("Deleting instance in region: " + key)
				#print("The value in dictionary is: ")
				print(allRegions_Dict[key])
			region_client = boto3.client('ec2', region_name=key)
			region_client.terminate_instances(InstanceIds=allRegions_Dict[key])

# checking model training configs for prediction
isModelTrainingEnabled = False
trainModeOnly = False
if config.has_option('PLUGIN_CONFIGS', 'buildModel'):
	isModelTrainEnabledStr = config.get('PLUGIN_CONFIGS', 'buildModel')
	if isModelTrainEnabledStr.upper() == "TRUE":
		print("Model Training is enabled!")
		isModelTrainingEnabled = True
	if config.has_option('PLUGIN_CONFIGS', 'trainModeOnly'):
		trainModeOnlyStr = config.get('PLUGIN_CONFIGS', 'trainModeOnly')
		if trainModeOnlyStr.upper() == "TRUE":
			print("Skipping monitoring and only training the prediction model for datasets present in the configured directory!")
			trainModeOnly = True

NUM_DATACENTERS = 8
if config.has_option('PLUGIN_CONFIGS', 'NUM_DATACENTERS'):
	NUM_DATACENTERS = int(config.get('PLUGIN_CONFIGS', 'NUM_DATACENTERS'))

if not config.has_option('PLUGIN_CONFIGS', 'datasetPath'):
	raise Exception("Error: datasetPath is not configured!!!")
datasetPath = config.get('PLUGIN_CONFIGS', 'datasetPath')
datasetPathAbs = os.path.abspath(datasetPath)

if (isModelTrainingEnabled or trainModeOnly) and not(config.has_option('PLUGIN_CONFIGS', 'modelOutputPath')):
	raise Exception("Error: Model output path is not configured correctly!")
modelOutputPath = config.get('PLUGIN_CONFIGS', 'modelOutputPath')
modelOutputPathAbs = os.path.abspath(modelOutputPath)

isExistDir = os.path.exists(datasetPathAbs)
if not isExistDir:
	os.makedirs(datasetPathAbs)

if(isModelTrainingEnabled and trainModeOnly):
	if not isExistDir:
		raise Exception("Error: dataset directory might be empty.")
	startRfTrain(NUM_DATACENTERS, datasetPathAbs, modelOutputPathAbs)
else:
	print("Checking configs and launching instances ...")
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

	runForDurations = []
	runForDurationsContains1 = False
	if config.has_option('PLUGIN_CONFIGS', 'runForDurations'):
		runForDurations = config.get('PLUGIN_CONFIGS', 'runForDurations')
		runForDurations = runForDurations.split(",")
	for runDurationEle in runForDurations:
		if (int(runDurationEle.strip()) == 1):
			runForDurationsContains1 = True
			break
	if not(runForDurationsContains1):
		runForDurations.insert(0, "1")

	if config.has_option('LAUNCH_CONFIGS', 'AMIs') and config.has_option('LAUNCH_CONFIGS', 'Subnets') and config.has_option('LAUNCH_CONFIGS', 'instanceType'):
		regionAMIMap = json.loads(config.get('LAUNCH_CONFIGS', 'AMIs'))
		subnetsMap = json.loads(config.get('LAUNCH_CONFIGS', 'Subnets'))
		instanceType = config.get('LAUNCH_CONFIGS', 'instanceType')
		spawnFromScrath = True
		all_IPs, priv_To_Pub_IPs, ip_To_Region, allRegions_Dict = launchInstances(regionAMIMap, subnetsMap, instanceType, privateIPEnabled, debugEnabled)
		if len(priv_To_Pub_IPs) > 0:
			#private IPs are used for monitoring, but use public IPs for tracking distance between DCs
			for ip1 in all_IPs:
				for ip2 in all_IPs:
					if ip1 != ip2:
						ip1_ip2_distance_map[ip1+"-"+ip2] = round(computeIPDistance(priv_To_Pub_IPs[ip1], priv_To_Pub_IPs[ip2]),2)
			print(ip1_ip2_distance_map)
		else:
			for ip1 in all_IPs:
				for ip2 in all_IPs:
					if ip1 != ip2:
						ip1_ip2_distance_map[ip1+"-"+ip2] = round(computeIPDistance(ip1, ip2),2)
			print(ip1_ip2_distance_map)

	datasetSize = 10
	if not config.has_option('PLUGIN_CONFIGS', 'datasetSize'):
		print("Dataset size is not set. Defaulting to 10!")
	else:
		datasetSize = int(config.get('PLUGIN_CONFIGS', 'datasetSize'))

	index = 1
	if not config.has_option('PLUGIN_CONFIGS', 'resumeDatasetFromIndex'):
		print("No resumeDatasetFromIndex flag is set! Hence starting from beginning from index 1!")
	else:
		index = int(config.get('PLUGIN_CONFIGS', 'resumeDatasetFromIndex'))

	maxRetires = 0
	if config.has_option('PLUGIN_CONFIGS', 'maxRetries'):
		maxRetries = int(config.get('PLUGIN_CONFIGS', 'maxRetries'))
	initialMaxRetries = maxRetries

	runMode = 'BOTH'
	if config.has_option('PLUGIN_CONFIGS', 'RUN_MODE'):
		runMode = config.get('PLUGIN_CONFIGS', 'RUN_MODE')
		runMode = runMode.upper()

	runInterval = 'once'
	sleepTimeInSeconds = -1
	if config.has_option('PLUGIN_CONFIGS', 'runInterval'):
		runInterval = config.get('PLUGIN_CONFIGS', 'runInterval')
		runInterval = runInterval.lower()
	if not(runInterval == "once"):
		if "s" in runInterval:
			sleepTimeInSeconds = round(float(runInterval.split("s")[0]))
		elif "d" in runInterval:
			sleepTimeInSeconds = round(float(runInterval.split("d")[0]) * 24 * 3600)
		elif "w" in runInterval:
			sleepTimeInSeconds = round(float(runInterval.split("w")[0]) * 24 * 3600 * 7)
		elif "m" in runInterval:
			sleepTimeInSeconds = round(float(runInterval.split("m")[0]) * 24 * 3600 * 7 * 30)

	print("Launching instances completed! BW monitoring in progress ... This can take several minutes or days depending on the dataset size requested!")
	startIndex = index
	startOfMonitorLoop = time.time()
	prevRunDurationRef = 20
	originalPrevRunDurationRef = prevRunDurationRef
	#cleanupMode 1 denotes full cleanup after each monitoring, 2 denotes sending monitoring files the first time but skipping cleanup, 3 denotes no send and no cleanup (assuming monitoring files are already transferred)
	cleanupMode = 1
	while index <= datasetSize:
		numDCs = random.randint(2, NUM_DATACENTERS)
		print("The numDCs is: {}".format(numDCs))
		dcIndexChoices=[]
		selectedIPsForProbe=[]
		startDCSelect=0
		if(numDCs != NUM_DATACENTERS):
			while (startDCSelect<numDCs):
				randValRead = random.randint(2, NUM_DATACENTERS)
				if not(randValRead in dcIndexChoices):
					dcIndexChoices.append(randValRead)
					startDCSelect = startDCSelect + 1
			for dcIndexEle in dcIndexChoices:
				selectedIPsForProbe.append(all_IPs[dcIndexEle-1])
			print("The selectedIPsForProbe are: {}".format(selectedIPsForProbe))
		else:
			selectedIPsForProbe = all_IPs
			print("The selectedIPsForProbe are: {}".format(selectedIPsForProbe))

		for runDurationEle in runForDurations:
			runDurationEleNoSpace = runDurationEle.strip()
			proc_update = subprocess.Popen(["sed", "-i", "s/"+"test_duration="+str(prevRunDurationRef)+"/"+"test_duration="+runDurationEleNoSpace+"/g", "src/bwtesting-client.py"])
			proc_update.wait()
			proc_update = subprocess.Popen(["sed", "-i", "s/"+"test_duration="+str(prevRunDurationRef)+"/"+"test_duration="+runDurationEleNoSpace+"/g", "src/bwtesting-client-copy.py"])
			proc_update.wait()
			prevRunDurationRef = int(runDurationEleNoSpace)
			sleepTimeForMonitor = 8
			if int(runDurationEleNoSpace) != 1:
				sleepTimeForMonitor = int(runDurationEleNoSpace) + 10
			if (runMode == "BOTH" or runMode == "STATIC") and (int(runDurationEleNoSpace) != 1):
				if statusProgressMsgEnabled:
					print("Starting static monitoring measurement for sample# {}".format(index), flush=True)
				# Calling runMonitor in measure.py with isDynamic = False
				statusCode = runMonitor(username, selectedIPsForProbe, ip_To_Region, basePort, privateKeyPathAbs, privateIPEnabled, datasetPathAbs, index, instanceType, False, runDurationEleNoSpace, debugEnabled, statusProgressMsgEnabled, ip1_ip2_distance_map, numDCs, sleepTimeForMonitor, 1)
				if statusCode is not None:
					print("Some error has occurred in call to runMonitor for static measurements!", flush=True)
					if maxRetries > 0:
						print("Retrying #{} for static monitoring!".format(initialMaxRetries - maxRetries + 1), flush=True)
						maxRetries = maxRetries - 1
						outputFilePrefix = "static"
						outputFileName = datasetPathAbs+"/"+outputFilePrefix+str(index)+".json"
						proc = subprocess.Popen(["rm", "-f", outputFileName])
						proc.wait()
						continue
					if maxRetries == 0:
						print("Reached maximum retry limit with error for static monitor. Hence exiting!!!", flush=True)
						break
				if statusProgressMsgEnabled:
					print("Completed static monitoring measurement for sample# {}".format(index), flush=True)
					maxRetries = initialMaxRetries

			if runMode == "BOTH" or runMode == "DYNAMIC":
				if len(runForDurations) == 2 and cleanupMode == 1:
					cleanupMode = 2
				if statusProgressMsgEnabled:
					print("Starting dynamic/real-time monitoring measurement for sample# {}".format(index), flush=True)
				# Calling runMonitor in measure.py with isDynamic = True
				statusCode = runMonitor(username, selectedIPsForProbe, ip_To_Region, basePort, privateKeyPathAbs, privateIPEnabled, datasetPathAbs, index, instanceType, True, runDurationEleNoSpace, debugEnabled, statusProgressMsgEnabled, ip1_ip2_distance_map, numDCs, sleepTimeForMonitor, cleanupMode)
				if statusCode is not None:
					print("Some error has occurred in call to runMonitor for dynamic measurements!", flush=True)
					if maxRetries > 0:
						print("Retrying #{} for dynamic monitoring!".format(initialMaxRetries - maxRetries + 1), flush=True)
						cleanupMode = 1
						maxRetries = maxRetries - 1
						outputFilePrefix = "static"
						outputFileName = datasetPathAbs+"/"+outputFilePrefix+str(index)+".json"
						proc = subprocess.Popen(["rm", "-f", outputFileName])
						proc.wait()
						outputFilePrefix = "dynamic"
						outputFileName = datasetPathAbs+"/"+outputFilePrefix+str(index)+".json"
						proc = subprocess.Popen(["rm", "-f", outputFileName])
						proc.wait()
						continue
					if maxRetries == 0:
						print("Reached maximum retry limit with error for dynamic monitor. Hence exiting!!!", flush=True)
						break
				if statusProgressMsgEnabled:
					print("Completed measurement for sample# {}".format(index), flush=True)
					maxRetries = initialMaxRetries
		if runMode == "BOTH" or runMode == "DYNAMIC":
			if len(runForDurations) == 2 and cleanupMode == 2:
				cleanupMode = 3
		index = index + 1
		if (sleepTimeInSeconds > 0 and index <= datasetSize):
			print("Going into sleep for {} seconds.".format(sleepTimeInSeconds), flush=True)
			time.sleep(sleepTimeInSeconds)
	endOfMonitorLoop = time.time()
	proc_update = subprocess.Popen(["sed", "-i", "s/"+"test_duration="+str(prevRunDurationRef)+"/"+"test_duration="+str(originalPrevRunDurationRef)+"/g", "src/bwtesting-client.py"])
	proc_update.wait()
	proc_update = subprocess.Popen(["sed", "-i", "s/"+"test_duration="+str(prevRunDurationRef)+"/"+"test_duration="+str(originalPrevRunDurationRef)+"/g", "src/bwtesting-client-copy.py"])
	proc_update.wait()

	print("Monitoring Compledted for {} samples in {} seconds !!!".format((datasetSize - startIndex + 1), (endOfMonitorLoop - startOfMonitorLoop)), flush=True)

	if spawnFromScrath:
		try:
			#terminateInstances(allRegions_Dict)
			print("Instances deleted!!!")
		except:
			print("Some error occurred during deletion of instances!!!")

	if (isModelTrainingEnabled):
		startRfTrain(NUM_DATACENTERS, datasetPathAbs, modelOutputPathAbs)
