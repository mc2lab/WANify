import sys
sys.path.append('/home/ec2-user/realtimeBW/aws/src')
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error as mae
from sklearn.metrics import mean_squared_error as mse
from sklearn.ensemble import RandomForestRegressor
import json
import numpy as np
from os.path import exists
import os
import pickle
import time

os.chdir('/home/ec2-user/realtimeBW/aws')
config = configparser.RawConfigParser()
config.read('config.cfg')

def createRFModel():
	model = RandomForestRegressor(n_estimators=100, random_state=30)
	return model

def startRfTrain(NUM_DATACENTERS, datasetPath, rfModelPath):
	print("Inside startRfTrain method!")

	# NUM_SAMPLES is the total number of static/dynamic samples in the generated dataset. 
	NUM_SAMPLES = 0
	for file_name in os.listdir(datasetPath):
		if(file_name.endswith('json')):
			NUM_SAMPLES = NUM_SAMPLES + 1
	NUM_SAMPLES = NUM_SAMPLES // 2
	print("NUM_SAMPLES determined is {} !".format(NUM_SAMPLES))
	
	model = createRFModel()

	#Read train_samples and train_targets
	print("Processing the training dataset!")
	readIndex = 1
	train_x = np.zeros((NUM_SAMPLES, NUM_DATACENTERS * NUM_DATACENTERS * 7))
	train_y = np.zeros((NUM_SAMPLES, NUM_DATACENTERS * NUM_DATACENTERS))

	#Enumerate the IP addresses here
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

	while (readIndex <= NUM_SAMPLES):

		train_x_i=np.zeros((NUM_DATACENTERS, NUM_DATACENTERS, 7))
		train_y_i=np.zeros((NUM_DATACENTERS, NUM_DATACENTERS))

		file = open(datasetPath + '/snapshot'+str(readIndex)+'_1.json')
		data = json.load(file)
		numDCs = int(data["numDCs"])
		for i in data['readings']:
			train_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][0] = numDCs
			train_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][1] = int(i["num_retransmits"])
			train_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][2] = round(float(i["memory_util"])*100)
			train_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][3] = round(float(i["local_cpu_total"]))
			train_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][4] = round(float(i["remote_cpu_total"]))
			train_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][5] = round(float(i["ipDistance"]))
			train_x_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]][6] = float(i["sent_Kbytes_per_second"]) + float(i["received_Kbytes_per_second"])
		train_x[readIndex-1]=train_x_i.reshape([1,NUM_DATACENTERS * NUM_DATACENTERS * 7])
		file.close()

		file = open(datasetPath + '/dynamic'+str(readIndex)+'_20.json')
		data = json.load(file)
		for i in data['readings']:
			train_y_i[dcToIndexMap[i["srcRegion"]]][dcToIndexMap[i["destRegion"]]] = float(i["sent_mbps"]) + float(i["received_mbps"])
		train_y[readIndex-1]=train_y_i.reshape([1,NUM_DATACENTERS * NUM_DATACENTERS])
		file.close()

		readIndex = readIndex + 1
	print("Dataset processing completed!")

	start_time = time.time()
	history = model.fit(train_x, train_y)
	end_time = time.time()
	print("The train accuracy is : {}".format(model.score(train_x, train_y)))
	print("Model Training time: {}".format(end_time-start_time))

	isExistDir = exists(rfModelPath)
	if not isExistDir:
		os.makedirs(rfModelPath)

	with open(rfModelPath + '/model.pkl', 'wb') as file:  
	    pickle.dump(model, file)
	with open(rfModelPath + '/history.pkl', 'wb') as file:  
	    pickle.dump(history, file)

