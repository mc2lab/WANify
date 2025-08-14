Few setups are required on the AMI images before they can be used.

1. Place run-all-client.py in appropriate directory and refer it in the test_service.sh accordingly.
2. Use test_service.sh to set a linux service, preferably boot-enabled as follows:

	[Unit]
	Description=This is a bootable script for bandwidth testing

	[Service]
	ExecStart=/bin/bash /usr/bin/test_service.sh

	[Install]
	WantedBy=multi-user.target

	This can be created in the /etc/systemd/system/ directory and refer linux service creation for detailed steps.

3. Create directories called "run-scripts", "run-scripts_sp" and "run-scripts_d" in the home directory.
4. Place the cleanup.sh in the home directory after updating the service name and run-scripts directory.
5. Above changes should be part of the AMI image, which is to be specified in config.cfg file. An easy way to do this is by setting up the AMI in 1 region (say us-east-1) and then copying the same AMI to other required regions.
