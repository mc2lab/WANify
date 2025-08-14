import boto3
import subprocess
import time
import os
from datetime import datetime
import traceback
import json
import random
from collections import Counter
import math

def runMonitor(username, all_IPs, ip_To_Region, basePort, privateKeyPath, privateIPEnabled, datasetPath, datasetIndex, instanceType, isDynamic, runDurationEle, debugEnabled, statusProgressMsgEnabled, ip1_ip2_distance_map, numDCs, sleepTime, cleanupMode):
	BASE_PORT=int(basePort)
	countPortIncrs=0
	outputFilePrefix=""
	forceStartEnabled=False
	outputFileSuffix=runDurationEle
	sumOfDuplicates = len(all_IPs)
	regionCounts = Counter(ip_To_Region.values())
	for key, value in regionCounts.items():
		if value > 1:
			sumOfDuplicates += math.factorial(value)
	if not isDynamic:
		outputFilePrefix="static"
	elif int(runDurationEle) == 1:
		outputFilePrefix="snapshot"
	else:
		outputFilePrefix="dynamic"
	# cleanupArg 0 denotes clearing run-scripts_sp, 1 denotes clearing run-scripts_d and any other value denotes clearing run-scripts
	cleanupArg = 2
	globalCountPortIncrs=countPortIncrs
	all_unique_ip_pairs={}

	ip_reg1_used={}
	ip_reg2_used={}
	all_send_ips=[]
	all_send_ips_dict={}
	ip_filtered_stats=[]
	ip_filtered_stats_dict={}

	proc_restore1 = subprocess.Popen(["rm", "src/bwtesting-client.py"])
	proc_restore1.wait()
	proc_restore2 = subprocess.Popen(["cp", "src/bwtesting-client-copy.py", "src/bwtesting-client.py"])
	proc_restore2.wait()
	prevIP="172.31.13.247"
	for ip in all_IPs:
		countPortIncrs=0
		if not isDynamic:
			all_send_ips_dict = {}
		if not(ip in all_send_ips_dict):
			all_send_ips.append(ip)
			all_send_ips_dict[ip]=1
		if debugEnabled and cleanupMode != 3:
			print("####### IP-source is: {}".format(ip))
		for ip2 in all_IPs:
			if ip != ip2 and (not((ip+'#'+ip2) in all_unique_ip_pairs or (ip2+'#'+ip) in all_unique_ip_pairs)) and not(ip in ip_reg1_used) and not(ip2 in ip_reg2_used) and (ip_To_Region[ip] != ip_To_Region[ip2]):
				if cleanupMode == 1 or cleanupMode == 2:
					#print("#########ip1: {} and ip2: {}".format(ip,ip2))
					proc_update = subprocess.Popen(["sed", "-i", "s/"+prevIP+"/"+str(ip)+"/g", "src/bwtesting-client.py"])
					proc_update.wait()
					portToBeUpdatedInFile=BASE_PORT+countPortIncrs
					if countPortIncrs != 0:
						#prevPortVal = portToBeUpdatedInFile - 1
						proc_sb_update=subprocess.Popen(["sed", "-i", "s/"+str(BASE_PORT)+"/"+str(portToBeUpdatedInFile)+"/g", "src/bwtesting-client.py"])
						proc_sb_update.wait()
					if debugEnabled:
						print("############ SENDING CONN REQ: {}:{}".format(ip2,portToBeUpdatedInFile))
					if not isDynamic:
						all_send_ips=[]
						ip_filtered_stats=[]
					updatedFileName="src/bwtesting-client-"+str(globalCountPortIncrs+1)+".py"
					procCpy = subprocess.Popen(["cp","src/bwtesting-client.py", updatedFileName])
					procCpy.wait()

					privateKeyPathStr = ''
					userNameStr = ''
					if isinstance(privateKeyPath, dict):
						print("The provider derived is: {}".format(ip_To_Region[ip2].split("~")[0]))
						userNameStr = username[ip_To_Region[ip2].split("~")[0]]
						privateKeyPathStr = privateKeyPath[ip_To_Region[ip2].split("~")[0]]
					else:
						privateKeyPathStr = privateKeyPath
						userNameStr = username

					destHostAddress=userNameStr+"@"+ip2+":~/run-scripts"
					if outputFilePrefix == "snapshot":
						destHostAddress=destHostAddress+"_sp"
					elif outputFilePrefix == "dynamic":
						destHostAddress=destHostAddress+"_d"
					destHostAddress=destHostAddress+"/"

					print("#######The updatedFileName is : {}".format(updatedFileName))
					while(True):
						proc = subprocess.Popen(["scp", "-i", privateKeyPathStr, "-o", "StrictHostKeyChecking=no", "-q", updatedFileName, destHostAddress], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
						out, err = proc.communicate()
						print("####Tried to send!!!")
						if not err:
							if debugEnabled:
								print('No error.\n', out.decode())
							break
						else:
							if debugEnabled:
								print('Will retry! Error!!!\n', err.decode())
					countPortIncrs+=1
					subprocess.Popen(["rm", updatedFileName])
					globalCountPortIncrs+=1
					if not isDynamic:
						all_send_ips.append(ip)
						all_send_ips.append(ip2)
					else:
						if not(ip2 in all_send_ips_dict):
							all_send_ips.append(ip2)
							all_send_ips_dict[ip2]=1

					# all_unique_ip_pairs[ip+'#'+ip2]=1
					# ip_reg1_used[ip]=1
					# ip_reg2_used[ip2]=1
					if not isDynamic:
						ip_filtered_stats.append(ip2)
					else:
						if not(ip2 in ip_filtered_stats_dict):
							ip_filtered_stats.append(ip2)
							ip_filtered_stats_dict[ip2]=1
					proc_restore1 = subprocess.Popen(["rm", "src/bwtesting-client.py"])
					proc_restore1.wait()
					proc_restore2 = subprocess.Popen(["cp", "src/bwtesting-client-copy.py", "src/bwtesting-client.py"])
					proc_restore2.wait()
					if debugEnabled:
						print("isDynamic is {} and globalCountPortIncrs is {} with termination condition for dynamic monitoring at {}".format(isDynamic, globalCountPortIncrs, (len(all_IPs)*len(all_IPs) - sumOfDuplicates)))
				else:
					all_send_ips = all_IPs
					ip_filtered_stats = all_IPs
					forceStartEnabled = True
				if not isDynamic or (isDynamic and globalCountPortIncrs == (len(all_IPs)*len(all_IPs) - sumOfDuplicates)) or forceStartEnabled:
					randomValForMemTest = random.randrange(100)
					if (randomValForMemTest % 4 == 0):
						for ip_st in all_send_ips:

							privateKeyPathStr = ''
							userNameStr = ''
							if isinstance(privateKeyPath, dict):
								print("The provider derived is: {}".format(ip_To_Region[ip_st].split("~")[0]))
								userNameStr = username[ip_To_Region[ip_st].split("~")[0]]
								privateKeyPathStr = privateKeyPath[ip_To_Region[ip_st].split("~")[0]]
							else:
								privateKeyPathStr = privateKeyPath
								userNameStr = username

							destHostAddress=userNameStr+"@"+ip_st
							print("The destHostAddress is : {}".format(destHostAddress))
							subprocess.Popen(["ssh", "-i", privateKeyPathStr, "-o", "StrictHostKeyChecking=no", destHostAddress, "sh consumeMemory.sh"])
					# Sending start.txt to initiate the bandwidth measurement process
					with open("src/template/start.txt", "w") as file:
						if outputFilePrefix == "snapshot":
							file.write("sp")
						elif outputFilePrefix == "dynamic":
							file.write("d")
						else:
							pass
					for ip_st in all_send_ips:

						privateKeyPathStr = ''
						userNameStr = ''
						if isinstance(privateKeyPath, dict):
							print("The provider derived is: {}".format(ip_To_Region[ip_st].split("~")[0]))
							userNameStr = username[ip_To_Region[ip_st].split("~")[0]]
							privateKeyPathStr = privateKeyPath[ip_To_Region[ip_st].split("~")[0]]
						else:
							privateKeyPathStr = privateKeyPath
							userNameStr = username

						destHostAddress=userNameStr+"@"+ip_st+":~/"
						print("Sending start.txt to {}".format(destHostAddress))
						subprocess.Popen(["scp", "-i", privateKeyPathStr, "-o", "StrictHostKeyChecking=no", "-q", "src/template/start.txt", destHostAddress])

					time.sleep(sleepTime)
					if statusProgressMsgEnabled and not isDynamic:
						print("Static monitoring in progress ...", flush=True)
					elif statusProgressMsgEnabled and isDynamic:
						print("Dynamic monitoring in progress ...", flush=True)
					#Gather stats
					statsDict={}
					resultList = []
					try:
						completeStatusStartTime = time.time()
						collectingStatusFileTime = 0
						processingStatusFileTime = 0
						statusProcessed = 0
						totalLenOfStatusFiles = len(ip_filtered_stats)
						print("The ip filtered for stats is: {}".format(ip_filtered_stats))
						for ip_f in ip_filtered_stats:
							# if debugEnabled:
							# 	print("The source IP for status collection is: {}".format(ip_f))

							privateKeyPathStr = ''
							userNameStr = ''
							if isinstance(privateKeyPath, dict):
								print("The provider derived is: {}".format(ip_To_Region[ip_f].split("~")[0]))
								userNameStr = username[ip_To_Region[ip_f].split("~")[0]]
								privateKeyPathStr = privateKeyPath[ip_To_Region[ip_f].split("~")[0]]
							else:
								privateKeyPathStr = privateKeyPath
								userNameStr = username

							destHostAddress=userNameStr+"@"+ip_f+":~/status.txt"
							statusCollectStartTime=time.time()
							try:
								# print("{} is assigned to index {}".format(ip_f, statusIndex))
								proc = subprocess.Popen(["scp", "-i", privateKeyPathStr, destHostAddress, datasetPath + "/status"+ip_f+".txt"])
								# proc.wait()
							except Exception as e:
								print("SCP Exception details: ", e)
							# proc.wait()
							statusCollectEndTime=time.time()
							collectingStatusFileTime += (statusCollectEndTime - statusCollectStartTime)

							# if(proc.returncode != 0):
							# 	now = datetime.now()
							# 	print("now =", now)
							# 	print("The return code is: {}".format(proc.returncode))
							# 	raise Exception("Some ERROR occurred while fetching bandwidth information from instance: {}!".format(ip_f))

						while (statusProcessed < totalLenOfStatusFiles):
							statusFileSuffix = ip_filtered_stats[statusProcessed]
							FILE_TO_READ=datasetPath+"/status"+statusFileSuffix+".txt"

							if (os.path.exists(FILE_TO_READ) and os.path.getsize(FILE_TO_READ) > 0):
		
								statusFileProcessStart = time.time()
								with open(FILE_TO_READ, 'r') as f:
									for line in f:
										if(len(line)>1):
											statsDict={}
											
											# if debugEnabled:
											# 	print("Obtained line as: \n {}".format(line))
											splitStr = line.split("{")
											statsDict["src"]=splitStr[0]
											statsDict["dest"]=statusFileSuffix
											statsDict["srcRegion"]=ip_To_Region[statsDict["src"]]
											statsDict["destRegion"]=ip_To_Region[statsDict["dest"]]
											# statsDict["machineType"]=instanceType
											bwValuesSplit = splitStr[1].split(",")
											if len(bwValuesSplit) == 2:
												bwValueSent = int(bwValuesSplit[0].split(":")[1].strip())
												statsDict["sent_mbps"]=str(bwValueSent)
												bwValueReceived = int(bwValuesSplit[1].split(":")[1].split("}")[0].strip())
												statsDict["received_mbps"]=str(bwValueReceived)
											else:
												sent_Kbytes_per_second = float(bwValuesSplit[0].split(":")[1].strip())
												statsDict["sent_Kbytes_per_second"]=str(sent_Kbytes_per_second)
												received_Kbytes_per_second = float(bwValuesSplit[1].split(":")[1].strip())
												statsDict["received_Kbytes_per_second"]=str(received_Kbytes_per_second)
												num_retransmits = int(bwValuesSplit[2].split(":")[1].strip())
												statsDict["num_retransmits"]=str(num_retransmits)
												memory_util = float(bwValuesSplit[3].split(":")[1].strip())
												statsDict["memory_util"]=str(memory_util)
												local_cpu_total = int(bwValuesSplit[4].split(":")[1].strip())
												statsDict["local_cpu_total"]=str(local_cpu_total)
												remote_cpu_total = int(bwValuesSplit[5].split(":")[1].split("}")[0].strip())
												statsDict["remote_cpu_total"]=str(remote_cpu_total)
												ipDistance = ip1_ip2_distance_map[statsDict["src"]+"-"+statsDict["dest"]]
												statsDict["ipDistance"]=str(ipDistance)
												# statsDict["numDCs"]=str(numDCs)
											resultList.append(statsDict)
											# if debugEnabled:
											# 	print("Information collected in dictionary is: \n {}".format(statsDict))
											# 	print("Information collected in result list is: \n {}".format(resultList))

								proc = subprocess.Popen(["rm", "-f", datasetPath+"/status"+statusFileSuffix+".txt"])
								# proc.wait()
								statusProcessed += 1
								statusFileProcessEnd = time.time()
								processingStatusFileTime += (statusFileProcessEnd - statusFileProcessStart)
							else:
								continue

						outputFileName = datasetPath+"/"+outputFilePrefix+str(datasetIndex)+"_"+outputFileSuffix+".json"
						isExistFile = os.path.exists(outputFileName)
						if not isExistFile:
							if debugEnabled:
								print("Output file does not exist. Hence, creating new json file!!!")
							with open(outputFileName, "w") as sampleFile:
								statsDictNew = {}
								statsDictNew["numDCs"]=str(numDCs)
								statsDictNew["machineType"]=instanceType
								statsDictNew["readings"] = resultList
								json.dump(statsDictNew, sampleFile, indent = 4)
						else:
							# if debugEnabled:
							# 	print("Output file exists, therefore SKIPPING file creation!")
							# 	print("Result list to be dumped is: \n {}".format(resultList))
							with open(outputFileName,'r+') as sampleFile:
								fileContents = json.load(sampleFile)
								fileContents["readings"].extend(resultList)
								sampleFile.seek(0)
								json.dump(fileContents, sampleFile, indent = 4)

						# Calling cleanup on associated instances
						if outputFilePrefix == "snapshot" and cleanupMode == 1:
							cleanupArg = 0
						elif outputFilePrefix == "dynamic" and cleanupMode == 1:
							cleanupArg = 1
						# print("cleanupMode is: {} and cleanupArg is {}".format(cleanupMode, cleanupArg))
						for ip_c in all_send_ips:

							privateKeyPathStr = ''
							userNameStr = ''
							if isinstance(privateKeyPath, dict):
								print("The provider derived is: {}".format(ip_To_Region[ip_c].split("~")[0]))
								userNameStr = username[ip_To_Region[ip_c].split("~")[0]]
								privateKeyPathStr = privateKeyPath[ip_To_Region[ip_c].split("~")[0]]
							else:
								privateKeyPathStr = privateKeyPath
								userNameStr = username

							destHostAddress=userNameStr+"@"+ip_c
							print("Sending cleaning event for: {}".format(destHostAddress))
							subprocess.Popen(["ssh", "-i", privateKeyPathStr, "-o", "StrictHostKeyChecking=no", destHostAddress, "sh cleanup.sh " + str(cleanupArg)])

						completeStatusEndTime = time.time()
						print("The total time for collecting and processing monitor info is : {}".format(completeStatusEndTime - completeStatusStartTime))
						print("The total file fetch time is : {}".format(collectingStatusFileTime))
						print("The total file processing time is : {}".format(processingStatusFileTime))

					except Exception as e:
						# Calling cleanup on associated instances
						for ip_c in all_send_ips:
							privateKeyPathStr = ''
							userNameStr = ''
							if isinstance(privateKeyPath, dict):
								print("The provider derived is: {}".format(ip_To_Region[ip_c].split("~")[0]))
								userNameStr = username[ip_To_Region[ip_c].split("~")[0]]
								privateKeyPathStr = privateKeyPath[ip_To_Region[ip_c].split("~")[0]]
							else:
								privateKeyPathStr = privateKeyPath
								userNameStr = username
							destHostAddress=userNameStr+"@"+ip_c
							subprocess.Popen(["ssh", "-i", privateKeyPathStr, "-o", "StrictHostKeyChecking=no", destHostAddress, "sh cleanup.sh " + str(0)])

						for ip_c in all_send_ips:
							privateKeyPathStr = ''
							userNameStr = ''
							if isinstance(privateKeyPath, dict):
								print("The provider derived is: {}".format(ip_To_Region[ip_c].split("~")[0]))
								userNameStr = username[ip_To_Region[ip_c].split("~")[0]]
								privateKeyPathStr = privateKeyPath[ip_To_Region[ip_c].split("~")[0]]
							else:
								privateKeyPathStr = privateKeyPath
								userNameStr = username
							destHostAddress=userNameStr+"@"+ip_c
							subprocess.Popen(["ssh", "-i", privateKeyPathStr, "-o", "StrictHostKeyChecking=no", destHostAddress, "sh cleanup.sh " + str(1)])

						for ip_c in all_send_ips:
							privateKeyPathStr = ''
							userNameStr = ''
							if isinstance(privateKeyPath, dict):
								print("The provider derived is: {}".format(ip_To_Region[ip_c].split("~")[0]))
								userNameStr = username[ip_To_Region[ip_c].split("~")[0]]
								privateKeyPathStr = privateKeyPath[ip_To_Region[ip_c].split("~")[0]]
							else:
								privateKeyPathStr = privateKeyPath
								userNameStr = username
							destHostAddress=userNameStr+"@"+ip_c
							subprocess.Popen(["ssh", "-i", privateKeyPathStr, "-o", "StrictHostKeyChecking=no", destHostAddress, "sh cleanup.sh " + str(2)])

						print("An ERROR occurred while collecting bandwidth information from instances and saving into file!!!")
						print("Exception details: ", e)
						traceback.print_exc()

						subprocess.Popen(["rm", "src/bwtesting-client.py"])
						subprocess.Popen(["cp", "src/bwtesting-client-copy.py", "src/bwtesting-client.py"])
						return -1
			if forceStartEnabled:
				break
		if forceStartEnabled:
			break

	subprocess.Popen(["rm", "src/bwtesting-client.py"])
	subprocess.Popen(["cp", "src/bwtesting-client-copy.py", "src/bwtesting-client.py"])
