import iperf3
import urllib.request
from subprocess import run
from subprocess import PIPE
import datetime
import json
#from ec2_metadata import ec2_metadata
from multiprocessing import Lock
from os.path import expanduser
home = expanduser("~")
# Set vars
# Remote iperf server IP
remote_site = '172.31.13.247'
# How long to run iperf3 test in seconds
test_duration=20

# Set Iperf Client Options
# Run 10 parallel streams on port 5201 for duration w/ reverse
client = iperf3.Client()
client.server_hostname = remote_site
client.zerocopy = True
client.verbose = False
client.reverse=True
client.port = 5000
client.num_streams = 1
client.duration = int(test_duration)
client.bandwidth = 1000000000

runCmdObj1 = run("free -m | awk 'NR==2{print $7}'", stdout=PIPE, stderr=PIPE, shell=True)
availMem = int(runCmdObj1.stdout)
runCmdObj2 = run("free -m | awk 'NR==2{print $3}'", stdout=PIPE, stderr=PIPE, shell=True)
usedMem = int(runCmdObj2.stdout)
memory_util = round((float(usedMem)/float(availMem)), 2)

# Run iperf3 test
result = client.run()
with open(home+"/statusDel.txt", "a") as myfile:
	myfile.write("\n###New exec:")
	myfile.write(str(result))

# extract relevant data
sent_mbps = int(result.sent_Mbps)
received_mbps = int(result.received_Mbps)
sent_Kbytes = round(float(result.sent_bytes)/float(1024), 2)
received_Kbytes = round(float(result.received_bytes)/float(1024), 2)
num_retransmits = int(result.retransmits)
local_cpu_total = int(result.local_cpu_total)
remote_cpu_total = int(result.remote_cpu_total)

lock = Lock()
lock.acquire()
with open(home+"/status.txt", "a") as myfile:
	if (test_duration != 1):
		myfile.write(remote_site+"{sent_mbps: "+str(sent_mbps)+",")
		myfile.write("received_mbps: "+str(received_mbps)+"}\n")
	else:
		myfile.write(remote_site+"{sent_Kbytes_per_second: "+str(sent_Kbytes)+",")
		myfile.write("received_Kbytes_per_second: "+str(received_Kbytes)+",")
		myfile.write("num_retransmits: "+str(num_retransmits)+",")
		myfile.write("memory_util: "+str(memory_util)+",")
		myfile.write("local_cpu_total: "+str(local_cpu_total)+",")
		myfile.write("remote_cpu_total: "+str(remote_cpu_total)+"}\n")
lock.release()
