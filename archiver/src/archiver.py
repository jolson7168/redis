import time
import subprocess
import select
from pprint import pprint
import sys
import getopt
import logging
import S3

config = {}
observations=[]
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

def dumpToCS(key,observations):
#	theLogger = logging.getLogger(config["logname"])
#	key=addArray[1]
#	score=addArray[3]
#	payload=addArray[5]
#	S3.uploadStringToS3(config["AWS_ACCESS_KEY"],config["AWS_ACCESS_SECRET_KEY"],score+","+payload,config["bucket"],key+'/'+score+'.txt',content_type="application/text",logger=theLogger)
	print("key: "+key)
	pprint(observations)

def doArchive(fileName):

	f = subprocess.Popen(['tail','-f', '-n', '0',fileName], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
	p = select.poll()
	p.register(f.stdout)

	zAdd=[]
	done = False
	while not done:
	    if p.poll(1):
		if f.stdout.readline()[:4].upper() == 'ZADD':	
		        while p.poll(1):
				f.stdout.readline()  #junk
				key = f.stdout.readline() #key
				f.stdout.readline()  #junk
				done2=False
				while not done2:
					if f.stdout.readline()[:1]="*":
						done2 = True
					else:
						newObservation=observation()
						newObservation.timestamp = f.stdout.readline() #timestamp
						f.stdout.readline()  #junk
						newObservation.payload = f.stdout.readline() #data
						observations.append(newObservation)
			dumpToCS(key,observations)
		        observations=[]
		else:
			f.stdout.readline()  #junk


def main(argv):
 	try:
      		opts, args = getopt.getopt(argv,"hc:",["configfile="])
	except getopt.GetoptError:
		print ('archiver.py -c <configfile>')
      		sys.exit(2)
	for opt, arg in opts:
      		if opt == '-h':
         		print ('archiver.py -c <configfile>')
         		sys.exit()
		elif opt in ("-c", "--configfile"):
			configFile=arg
			try:
   				with open(configFile): pass
			except IOError:
   				print ('Configuration file: '+configFile+' not found')
				sys.exit(2)
	execfile(configFile, config)
	logger=initLog()
	logger.info('Starting Archive: ========================================')
	doArchive(config["transactionLog"])

if __name__ == "__main__":
	main(sys.argv[1:])
