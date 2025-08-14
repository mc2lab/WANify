if [ $1  -eq  0 ]; then
	rm -rf ~/run-scripts_sp/*
elif [ $1  -eq  1 ]; then
	rm -rf ~/run-scripts_d/*
else
	rm -rf ~/run-scripts/*
fi
rm -f ~/status.txt
sudo systemctl restart bwtesting.service
