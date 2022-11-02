# James Gibson 
# 06/09/2022
# Version 3
#
# Step 3 b: Dissolve roads

 
import arcpy, sys, os
import os.path
from collections import defaultdict
from arcpy import env
from arcpy.sa import *
import time
import datetime
import pandas as pd
import numpy as np
import multiprocessing
arcpy.env.overwriteOutput = True


print('Start Script')


#Global Variables
GADMGlobal = r'G:\HumanPlanet\GADM\GADM.gdb\GADMCopy3'


def process(iso):
    message = None
    if message is None:
        try:
            #Create geodatabase
            gdb = r'G:\HumanPlanet\SDG91\Version3\Countries\%s.gdb' % iso
            workspace = gdb
            arcpy.env.workspace = workspace
            arcpy.env.overwriteOutput = True
            #Buffered Roads
            Buffered_output = "%s_osm_roads_buffered" % iso
            Dissolved_output = "%s_osm_roads_dissolved" % iso
            arcpy.PairwiseDissolve_analysis(Buffered_output,Dissolved_output)
            message = 'Completed: ' + iso
        except Exception as e:
            message = 'Failed: ' + iso + ' ' + str(e)
    return message


def main():
    print('Starting Script...')
    #Start Time
    Start_Time = time.time()
    print('Start Time: %s' % str(Start_Time))
    mylist=[]
    with arcpy.da.SearchCursor(GADMGlobal,['GID_0']) as cursor:
        for row in cursor:
            if row[0] in mylist:
                pass
            else:
                mylist.append(row[0])
    print('Start Processing')
    length = len(mylist)
    pool = multiprocessing.Pool(processes=20, maxtasksperchild=1)
    results = pool.imap_unordered(process,mylist)
    counter = 0
    for result in results:
        print(result)
        counter = counter + 1
        print("{} countries processed out of {}".format(counter,length))
        print('---------------------------------------------------------')
    pool.close()
    pool.join()
    End_Time = time.time()
    Total_Time = End_Time - Start_Time
    print('Total Time: %s' % str(Total_Time))
    print('Script Complete')
        
if __name__ == '__main__':
    main()
        
