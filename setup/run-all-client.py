import os
import subprocess
import sys
base_dir="/home/ec2-user/run-scripts"
if len(sys.argv) > 1:
	dirIndex = sys.argv[1]
	if len(dirIndex)>0:
		base_dir = base_dir + "_" + dirIndex
for file in os.listdir(base_dir):
	if file.endswith(".py"):
		print(os.path.join(base_dir, file))
		subprocess.Popen(["python3", os.path.join(base_dir, file)])
