import sys
import os
from termcolor import cprint

# Set Properties for mapping volumes
jupyter_home = '/home/centos/'
cas_home = '/home/centos/working/'
h2o_home = '/home/centos/'
notebooks_folder = '/home/centos/notebooks'
h2o_stuff = '/home/centos/h2o/artifacts'
cas_stuff = '/home/centos/working/sasdemo'

try:
    os.chdir(jupyter_home)
    print("INFO: Starting Up The Jupyter Container")
    os.system(f"docker run --rm -d -p 8888:8888  -v {notebooks_folder}:/notebooks -v {h2o_stuff}:/h2o-artifacts -v {cas_stuff}:/cas-artifacts jupyter4sas:v01")
    cprint("SUCCESS --> JUPYTER CONTAINER LAUNCHED",'green')
except:
    cprint("ERROR:FAILED STARTING JUPYTER CONTAINER",'red')

# Now try to launch the CAS Container  

try:
    print("INFO: Starting Up The CAS Container")
    os.chdir(cas_home)
    print("INFO: CD Into the Proper Folder")
    os.system("./launchsas01.sh")
    print("INFO: Launch CAS Container Script Executed!")
    cprint("SUCCESS --> *CAS* CONTAINER LAUNCHED",'green')
except:
    cprint("ERROR:CAS CONTAINER FAILED TO LAUNCH",'red')

# Now try to launch the h2o Container  

try:
    print("INFO: Starting Up The h2o Environment")
    os.chdir(h2o_home)
    os.system(f"docker run --rm -d -p 54321:54321 -v {h2o_home}:/artifacts h2o:v01 java -Xmx1g -jar /opt/h2o.jar")
    cprint("SUCCESS --> 'h2o' CONTAINER LAUNCHED",'green')
except:
    cprint("ERROR: h2o CONTAINER FAILED TO LAUNCH",'red')


