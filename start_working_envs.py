import sys
import os
from termcolor import cprint
try:
    os.chdir("/home/centos/")
    print("INFO: Starting Up The Jupyter Container")
    os.system("docker run --rm -d -p 8888:8888  -v /home/centos/notebooks:/notebooks -v /home/centos/h2o/artifacts:/h2o-artifacts -v /home/centos/working/sasdemo:/cas-artifacts jupyter4sas:v01")
    cprint("SUCCESS --> JUPYTER CONTAINER LAUNCHED",'green')
except:
    cprint("ERROR:FAILED STARTING JUPYTER CONTAINER",'red')

# Now try to launch the CAS Container  

try:
    print("INFO: Starting Up The CAS Container")
    os.chdir("/home/centos/working/")
    print("INFO: CD Into the Proper Folder")
    os.system("./launchsas01.sh")
    print("INFO: Launch CAS Container Script Executed!")
    cprint("SUCCESS --> *CAS* CONTAINER LAUNCHED",'green')
except:
    cprint("ERROR:CAS CONTAINER FAILED TO LAUNCH",'red')

# Now try to launch the h2o Container  

try:
    print("INFO: Starting Up The h2o Environment")
    os.chdir("/home/centos/")
    os.system("docker run --rm -d -p 54321:54321 -v /home/centos/h2o/artifacts:/artifacts h2o:v01 java -Xmx1g -jar /opt/h2o.jar")
    cprint("SUCCESS --> 'h2o' CONTAINER LAUNCHED",'green')
except:
    cprint("ERROR: h2o CONTAINER FAILED TO LAUNCH",'red')


