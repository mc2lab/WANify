from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error as mae
from sklearn.metrics import mean_squared_error as mse
from sklearn.ensemble import RandomForestRegressor
import json
import numpy as np
from os.path import exists
import pickle
import os
import sys
import configparser
import math

def wPredict(NUM_DATACENTERS, rfModelPath, train_x, train_y, isCompare, refactoringVector):
	err50 = 0
	err100 = 0
	err150 = 0
	err200 = 0
	err250 = 0
	err300 = 0
	err300p = 0
	mseVal = 0
	maeVal = 0
	sqrtVal = 0

	with open(rfModelPath + '/model.pkl', 'rb') as file:  
		model = pickle.load(file)

	predict_y = model.predict(train_x)
	isRevert = False
	for i in range(NUM_DATACENTERS*NUM_DATACENTERS):
		predict_y[0][i] = predict_y[0][i] * refactoringVector[0][i]
		# hardcoded 6 below since using 7 features (0~6) and the snapshot equivalent of BW is stored at index 6.
		if train_x[0][(i+1)*6] > 0 and predict_y[0][i] == 0 and (i % (NUM_DATACENTERS+1) != 0):
			isRevert = True
			break
	if isRevert:
		# this is an edge case scenario when the ML model fails to predict for a scenario based on the training dataset used. We revert to refactoring vector (which stores dynamic runtime values) in this case.
		predict_y = refactoringVector
	print("Model Prediction Completed!!!")
	if (not isCompare):
		return mseVal, maeVal, sqrtVal, err50, err100, err150, err200, err250, err300, err300p, predict_y
	else:
		mseVal = mse(train_y, predict_y)
		maeVal = mae(train_y, predict_y)
		sqrtVal = math.sqrt(mseVal)
		print("The mse is : {}".format(mseVal))
		print("The rmse is : {}".format(sqrtVal))
		print("The mae is : {}".format(maeVal))
		# print("The test accuracy is : {}".format(model.score(train_x, train_y)))

		for j in range(NUM_DATACENTERS*NUM_DATACENTERS):
			if train_y[0][j] > 0:
				differenceVal = abs(predict_y[0][j] - train_y[0][j])
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

		print("Comparisons completed!\n")
		return mseVal, maeVal, sqrtVal, err50, err100, err150, err200, err250, err300, err300p, predict_y
		# print("###Diff between (10-30] is {}".format(err30))
		# print("###Diff between (30-50] is {}".format(err50))
		# print("###Diff between (50-100] is {}".format(err100))
		# print("###Diff between (100-150] is {}".format(err150))
		# print("###Diff between (150-200] is {}".format(err200))
		# print("###Diff between (200-250] is {}".format(err250))
		# print("###Diff between (250-300] is {}".format(err300))
		# print("###Diff > 300 is {}".format(err300p))
