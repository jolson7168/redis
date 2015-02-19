import time
import subprocess
import select
from pprint import pprint


config = {}

def initLog():
	logger = logging.getLogger(config["logname"])
	hdlr = logging.FileHandler(config["logFile"])
	formatter = logging.Formatter(config["logFormat"],config["logTimeFormat"])
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr) 
	logger.setLevel(logging.INFO)
	return logger

def doArchive(fileName):
	f = subprocess.Popen(['tail','-F',fileName], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
	p = select.poll()
	p.register(f.stdout)

	zAdd=[]
	done = False
	while not done:
	    if p.poll(1):
		if f.stdout.readline()[:4].upper() == 'ZADD':
		        while p.poll(1):
		                zAdd.append(f.stdout.readline())
		        pprint(zAdd)
		        zAdd=[]



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
	logger.info('Starting Archive: '+currentDayStr()+'  ========================================')
	doArchive(config["transactionLog"])

if __name__ == "__main__":
	main(sys.argv[1:])
