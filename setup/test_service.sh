#!/usr/bin/env bash
FILE=/home/ec2-user/start.txt
now=$(date)
ec2InstanceId=$(ec2-metadata --instance-id | cut -d " " -f 2);
LOG_FILE="/home/ec2-user/log_${ec2InstanceId}.txt"
echo "======NEW EXECUTION======$now" >> ${LOG_FILE}
if [ -f "$FILE" ]; then
    echo "$FILE exists." >> ${LOG_FILE}
else 
    echo "$FILE does not exist." >> ${LOG_FILE}
fi
while [ ! -f "$FILE" ]
do
	sleep 0.2
done
sudo su - ec2-user -c "iperf3 -s -p 5000 -1 &"
sudo su - ec2-user -c "iperf3 -s -p 5001 -1 &"
sudo su - ec2-user -c "iperf3 -s -p 5002 -1 &"
sudo su - ec2-user -c "iperf3 -s -p 5003 -1 &"
sudo su - ec2-user -c "iperf3 -s -p 5004 -1 &"
sudo su - ec2-user -c "iperf3 -s -p 5005 -1 &"
sudo su - ec2-user -c "iperf3 -s -p 5006 -1 &"
sleep 3
echo "$FILE found! Will begin execution" >> ${LOG_FILE}
charSent=$(cat /home/ec2-user/start.txt)
rm /home/ec2-user/start.txt
sudo chmod 777 ${LOG_FILE}
sudo su - ec2-user -c "python3 /home/ec2-user/bw_scripts/run-all-client.py $(echo $charSent)"
