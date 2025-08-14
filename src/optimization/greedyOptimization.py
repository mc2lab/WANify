import numpy as np
import sys
sys.path.append('/home/ec2-user/realtimeBW/aws/src')
import configparser
from statistics import mean
from scipy.optimize import minimize
import math
import copy
import pickle
import subprocess
import os
import json
from collections import Counter

os.chdir('/home/ec2-user/realtimeBW/aws')
config = configparser.RawConfigParser()
config.read('config.cfg')

MIN_DIFF_FOR_INFER_RELATIONSHIP = 30
# all_IP_List=['172.31.94.153', '172.32.88.142', '172.33.36.175', '172.34.93.84', '172.35.94.124', '172.36.95.236', '172.37.83.1', '172.38.40.78']

#br = [[-1000, 200, 10],[190, -1000, 100], [10, 20, -1000]]
#br = [[-1000, 1982, 118],[1736, -1000, 113], [185, 185, -1000]]
#br = [[-1000, 431, 138, 114, 129, 154, 383, 228], [426, -1000, 111, 144, 186, 246, 204, 148], [140, 108, -1000, 497, 166, 202, 213,  84], [ 85, 144, 492, -1000, 282, 347, 150,  69], [129, 189, 186, 279, -1000, 232,  98,  78], [157, 244, 204, 350, 255, -1000, 115,  90], [383, 202, 206, 141,  97, 118, -1000, 140], [227, 138,  85,  73,  80,  88, 143, -1000]]
#br = [[  -1000, 337.30283, 137., 111.275856, 120.33837, 156., 336.60477, 224.], [436.1069,  -1000, 114.90687, 150.,   186., 235.22163, 203., 152.26817], [144.74866, 114., -1000, 494.71136, 184.5925, 199.28458, 217.57664, 85.], [108.262115, 152.49364, 409.43506, -1000, 277.508, 317.3957, 143.99734, 68.29658], [118.70307, 179.10771, 160.32344, 279.91138, -1000, 211.58582, 93.53233, 82.], [149.41809, 247.25578, 178.93463, 352.54105, 223.80852, -1000, 120.,  82.51628], [374.10724, 194.72083, 218., 128.15489, 102.60738, 122., -1000, 144.75813], [237.09206, 148.94429, 80.20878, 81.362076, 78.05918, 94., 147., -1000]]

#Based on emperical results, each DC connection can use 8 channels for maximizing BW
CHANNELS_FACTOR = 8

def binarySearch(searchEle, br_unique_list):
	start = 1
	end = len(br_unique_list) - 1
	mid = ( start + end )//2
	while(start <= end):
		if br_unique_list[mid] == searchEle:
			return [-1, mid, -1]
		elif br_unique_list[mid] > searchEle:
			if ((mid - 1) == start) and br_unique_list[mid-1] != searchEle:
				return [start, -1, mid]
			end = mid - 1
			mid = (start + end)//2
		elif br_unique_list[mid] < searchEle:
			if ((mid + 1) == end) and br_unique_list[mid+1] != searchEle:
				return [mid, -1, end]
			start = mid + 1
			mid = (start + end)//2
	return [-1, -1, -1]

def inferDCPairRelationship(numDCs, br):
	br_set = set.union(*map(set,br))
	br_unique_list = list(br_set)
	br_unique_list.sort()
	# print("###The sorted list is: ")
	# print(br_unique_list)
	prevVal = -1
	i = 0
	while (i < len(br_unique_list)):
		if br_unique_list[i]>0 and prevVal>0:
			# print("i is {}".format(i))
			# print("bruniquelist_i is {} and prevVal is {}".format(br_unique_list[i], prevVal))
			if (br_unique_list[i] - prevVal) < MIN_DIFF_FOR_INFER_RELATIONSHIP:
				br_unique_list.remove(br_unique_list[i])
				i = i - 1
				# print("i updated is {}".format(i))
		prevVal=br_unique_list[i]
		i = i + 1

	# print("###The unique list is: ")
	# print(br_unique_list)
	startAssignParam = 1

	rel = np.ones((numDCs,numDCs))
	#one denotes closest and higher values denote farther away

	for i in range(len(br)):
		for j in range(len(br)):
			if i != j:
				retVal = binarySearch(br[i][j], br_unique_list)
				# print("The searchEle is: {}, br_unique_list is: {} and result is: {}".format(br[i][j], br_unique_list, retVal))
				if retVal[1] > 0:
					#match found
					rel[i][j] = len(br_unique_list) - retVal[1] + startAssignParam
				else:
					#infer closeness
					checkInd1 = retVal[0]
					checkInd2 = retVal[2]
					val1_check = br_unique_list[checkInd1]
					val2_check = br_unique_list[checkInd2]
					if(abs(val1_check - br[i][j]) > abs(val2_check - br[i][j])):
						#val2 is closest
						rel[i][j] = len(br_unique_list) - retVal[2] + startAssignParam
					else:
						rel[i][j] = len(br_unique_list) - retVal[0] + startAssignParam
	# print("Given BW relationship is: ")
	# print(br)
	# print("Final inferred closeness relationship is: ")
	# print(rel)
	return rel

def greedyApp(numDCs, all_IP_List, ipToRegions, privateKeyPathAbs, username, br):
	dcToIndexMap={}
	# dcToIndexMap['aws~us-east-1']=0
	# dcToIndexMap['aws~us-west-1']=1
	# dcToIndexMap['aws~ap-south-1']=2
	# dcToIndexMap['aws~ap-southeast-1']=3
	# dcToIndexMap['aws~ap-southeast-2']=4
	# dcToIndexMap['aws~ap-northeast-1']=5
	# dcToIndexMap['aws~eu-west-1']=6
	# dcToIndexMap['aws~sa-east-1']=7
	if config.has_option('PREDICT_CONFIGS', 'providerDCToIndexMap'):
		dcToIndexMap = json.loads(config.get('PREDICT_CONFIGS', 'providerDCToIndexMap'))
	else:
		raise Exception("Error: providerDCToIndexMap is not set in PREDICT_CONFIGS. Please check config.cfg for the configurations!!!")
	regionCounts = Counter(ipToRegions.values())

	rel = inferDCPairRelationship(numDCs, br)
	
	#Assume min 1 connection between each pair
	chAllMin = copy.deepcopy(rel)
	chAllMax = copy.deepcopy(rel)
	bWTgtMin = copy.deepcopy(br)
	bWTgtMax = copy.deepcopy(br)

	resultRowWise = list(map(max, rel))
	#resultRowWise = [x - 1 for x in resultRowWise]
	resultAll = sum(map(sum, rel)) - len(rel)

	#Here we identify how these channels can be distributed based on the inferred closeness relationships in a greedy way
	for i in range(len(rel)):
		for j in range(len(rel)):
			if i != j:
				chAllMin[i][j]=max(math.floor((rel[i][j]/resultAll) * (CHANNELS_FACTOR - 1)), 1)
				#chAllMax[i][j]=math.ceil((rel[i][j]/resultRowWise[i]) * (CHANNELS_FACTOR - 1))
				chAllMax[i][j]=math.ceil((CHANNELS_FACTOR/resultRowWise[i]) * rel[i][j])
				bWTgtMin[i][j]=br[i][j]*chAllMin[i][j]
				bWTgtMax[i][j]=br[i][j]*chAllMax[i][j]

	print("Printing final results: ")
	print("\n###Initial BW Matrix:")
	print(br)
	print("\n###Inferred closeness is:")
	print(rel)
	print("\n###Min Channel config is:")
	print(chAllMin)
	print("\n###Max Channel config is:")
	print(chAllMax)
	print("\n###Min Target BW is:")
	print(bWTgtMin)
	print("\n###Max Target BW is:")
	print(bWTgtMax)
	br_flat = np.array(br).flatten()
	brMinInit = np.min(br_flat[br_flat>0])
	print("\n###The initial BW_min is: {}".format(brMinInit))
	brMaxInit = max(map(max,br))
	print("\n###The initial BW_max is: {}".format(brMaxInit))
	#brAvgMin = (sum(map(sum, bWTgtMin)) + 1000*len(bWTgtMin))/(len(bWTgtMin)*len(bWTgtMin)-len(bWTgtMin))
	#brMinNew = min(map(min, bWTgtMin))
	bw_TgtMax_flat = np.array(bWTgtMax).flatten()
	brMinNew = np.min(bw_TgtMax_flat[bw_TgtMax_flat>0])
	print("\n###The max BW_min is: {}".format(brMinNew))
	brMaxNew = max(map(max, bWTgtMax))
	print("\n###The max BW_max is: {}".format(brMaxNew))
	for ip in all_IP_List:
		tmpDirPath = os.path.abspath(ip)
		isExistDir = os.path.exists(tmpDirPath)
		if not isExistDir:
			os.makedirs(tmpDirPath)
		with open(ip+'/minChannel.pkl','wb') as f:
			pickle.dump(np.ceil(chAllMin[dcToIndexMap[ipToRegions[ip]]]/regionCounts[ipToRegions[ip]]), f)
		with open(ip+'/maxChannel.pkl', 'wb') as f:
			pickle.dump(np.ceil(chAllMax[dcToIndexMap[ipToRegions[ip]]]/regionCounts[ipToRegions[ip]]), f)
		with open(ip+'/minBW.pkl', 'wb') as f:
			pickle.dump(np.ceil(bWTgtMin[dcToIndexMap[ipToRegions[ip]]]/regionCounts[ipToRegions[ip]]), f)
		with open(ip+'/maxBW.pkl', 'wb') as f:
			pickle.dump(np.ceil(bWTgtMax[dcToIndexMap[ipToRegions[ip]]]/regionCounts[ipToRegions[ip]]), f)
		usernameStr="ubuntu"
		if isinstance(username, dict):
			usernameStr=username[ipToRegions[ip].split("~")[0]]
		usernameStr="ubuntu"
		destHostAddress=usernameStr+"@"+ip+":/usr/local/spark/WB_Optimizer/"
		# print("###The destHostAddress is: {}".format(destHostAddress))
		if isinstance(privateKeyPathAbs, dict):
			proc = subprocess.Popen(["scp", "-i", privateKeyPathAbs[ipToRegions[ip].split("~")[0]], "-o", "StrictHostKeyChecking=no", "-q", ip+"/minChannel.pkl", destHostAddress])
			# out, err = proc.communicate()
			proc = subprocess.Popen(["scp", "-i", privateKeyPathAbs[ipToRegions[ip].split("~")[0]], "-o", "StrictHostKeyChecking=no", "-q", ip+"/maxChannel.pkl", destHostAddress])
			# out, err = proc.communicate()
			proc = subprocess.Popen(["scp", "-i", privateKeyPathAbs[ipToRegions[ip].split("~")[0]], "-o", "StrictHostKeyChecking=no", "-q", ip+"/minBW.pkl", destHostAddress])
			# out, err = proc.communicate()
			proc = subprocess.Popen(["scp", "-i", privateKeyPathAbs[ipToRegions[ip].split("~")[0]], "-o", "StrictHostKeyChecking=no", "-q", ip+"/maxBW.pkl", destHostAddress])
			# out, err = proc.communicate()
		else:
			proc = subprocess.Popen(["scp", "-i", privateKeyPathAbs, "-o", "StrictHostKeyChecking=no", "-q", ip+"/minChannel.pkl", destHostAddress])
			# out, err = proc.communicate()
			proc = subprocess.Popen(["scp", "-i", privateKeyPathAbs, "-o", "StrictHostKeyChecking=no", "-q", ip+"/maxChannel.pkl", destHostAddress])
			# out, err = proc.communicate()
			proc = subprocess.Popen(["scp", "-i", privateKeyPathAbs, "-o", "StrictHostKeyChecking=no", "-q", ip+"/minBW.pkl", destHostAddress])
			# out, err = proc.communicate()
			proc = subprocess.Popen(["scp", "-i", privateKeyPathAbs, "-o", "StrictHostKeyChecking=no", "-q", ip+"/maxBW.pkl", destHostAddress])
			# out, err = proc.communicate()
	#subprocess.Popen(["sh", "/home/ec2-user/realtimeBW/aws/scripts/stopBWServiceOnWorkers.sh"])

