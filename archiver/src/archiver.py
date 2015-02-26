import time
import subprocess
import select
from pprint import pprint
import sys
import getopt
import logging

config = {}
class observation:
  def __init__(self):
    self.timestamp = 0
    self.payload = ""


def initLog():
	logger = logging.getLogger(config["logname"])
	hdlr = logging.FileHandler(config["logFile"])
	formatter = logging.Formatter(config["logFormat"],config["logTimeFormat"])
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr) 
	logger.setLevel(logging.INFO)
	return logger

def tailLog(fileName):
	logger = logging.getLogger(config["logname"])
	f = subprocess.Popen(['tail','-f', '-n', '0',fileName], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
	p = select.poll()
	p.register(f.stdout)

	zAdd=[]
	done = False
	while not done:
	    if p.poll(1):
		if f.stdout.readline()[:4].upper() == 'ZADD':
			logger.info("Got a ZADD")
			observations=[]
		        while p.poll(1):				
				f.stdout.readline()  #junk
				key = f.stdout.readline() #key
				logger.info("Key: "+key)
				done2=False
				while not done2:
					line = f.stdout.readline() 
					if not line:
						done2 = True
						logger.info("Exiting none")
					elif line[:1]=="*":
						done2 = True 
						logger.info("Exiting star")
					elif line[:1]=="$":
						logger.info("Got Junk")
					else:
						newObservation=observation()
						newObservation.timestamp = line #timestamp
						logger.info("Timestamp: "+str(newObservation.timestamp))
						f.stdout.readline()  #junk
						newObservation.payload = f.stdout.readline() #data
						observations.append(newObservation)
					if not p.poll(1):
						done2=True
				logger.info("Out done2")
			#dumpToCS(key,observations)
		        observations=[]
		else:
			f.stdout.readline()  #junk

def archiveOnce(fileName):
	logger = logging.getLogger(config["logname"])
	counter = 0
	with open(fileName,'r') as logFile:
	    while True:
		line=logFile.readline()
		counter = counter+1
		if not line: break
		if any(x in line for x in config["validKeys"]):
			line=logFile.readline() #junk
			key = logFile.readline().rstrip()
			line=logFile.readline() #junk
    			logger.info(str(counter)+": ZADD ("+key+")")
			if key[-3:]=="DAT":
				done = False
				with open(config["tempDrive"]+"/"+key.replace(":DAT",".DAT"), "a") as myfile:
					while not done:
						line = logFile.readline().rstrip()
						counter = counter +1
						if any(x in line for x in ['ZADD', 'zadd']):
							done=True
						else:
							try:
								score=int(line)
								myfile.write(score+"\n")
								line = logFile.readline().rstrip()
								logger.info(str(counter)+" score: "+str(score))
							except:
								myfile.write(line+"\n")
				myfile.close()

def main(argv):
 	try:
      		opts, args = getopt.getopt(argv,"hc:m:",["configfile=","mode="])
	except getopt.GetoptError:
		print ('archiver.py -c <configfile> -m <mode>')
      		sys.exit(2)
	for opt, arg in opts:
      		if opt == '-h':
         		print ('archiver.py -c <configfile> -m <mode>')
         		sys.exit()
		elif opt in ("-c", "--configfile"):
			configFile=arg
			try:
   				with open(configFile): pass
			except IOError:
   				print ('Configuration file: '+configFile+' not found')
				sys.exit(2)
		elif opt in ("-m","--mode"):
			if arg == "Tail":
				normalMode = True
			else:
				normalMode = False
						
	execfile(configFile, config)
	logger=initLog()
	logger.info('Starting Archive: ========================================')
	if normalMode:
		tailLog(config["transactionLog"])
	else:
		archiveOnce(config["transactionLog"])

if __name__ == "__main__":
	main(sys.argv[1:])
