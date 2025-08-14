import random
import time
from subprocess import run

runCmdObj = run("free -m | awk 'NR==2{print $7}'", capture_output=True, shell=True, text=True)
runCmdOut = int(runCmdObj.stdout)
print(runCmdOut)

#occupy ~1% of memory, 10% of memory, ...
multiplierList = []
multiplierList.append(round(0.01*runCmdOut))
multiplierList.append(round(0.1*runCmdOut))
multiplierList.append(round(0.2*runCmdOut))
multiplierList.append(round(0.3*runCmdOut))
multiplierList.append(round(0.4*runCmdOut))
multiplierList.append(round(0.5*runCmdOut))
multiplierList.append(round(0.6*runCmdOut))
print(multiplierList)

randChoice = random.choice(multiplierList)
print(randChoice)
x = bytearray(randChoice*1024*1024)
time.sleep(40)
