from geopy.geocoders import Nominatim
import json
from urllib.request import urlopen
import sys
from geopy.distance import geodesic

def computeIPDistance(ip1, ip2):
	urlRef1 = 'http://ipinfo.io/'+ ip1 +'/json'
	respVal1 = urlopen(urlRef1)
	ipData1 = json.load(respVal1)

	reg1 = ipData1['loc']
	# print(reg1)

	urlRef2 = 'http://ipinfo.io/'+ ip2 +'/json'
	respVal2 = urlopen(urlRef2)
	ipData2 = json.load(respVal2)

	reg2 = ipData2['loc']
	# print(reg2)

	distComputed = geodesic(reg1, reg2).miles
	# print("The distance computed is: {}".format(distComputed))
	return distComputed

# print("The distance between IPs is: {}".format(computeIPDistance('35.173.255.198', '172.59.76.170')))
