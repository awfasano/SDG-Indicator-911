# James Gibson 
# 06/09/2022
# Version 3
#
# Step 2: Extract Roads Data

 
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
            #Paths to Data
            OSM_Roads = r'G:\HumanPlanet\SDG91\Version2\OSM Highway Data\highway_EPSG4326\JG_OSM_Roads_Extract.gdb\highway_EPSG4326_line'
            #Boundary
            boundary = '%s_admin0' % iso
            #Get Roads
            select_roads = arcpy.SelectLayerByLocation_management(OSM_Roads,"INTERSECT",boundary)
            osm_roads = '%s_osm_roads' % iso
            arcpy.CopyFeatures_management(select_roads,osm_roads)

            #Filter Roads by Highway tag
            keeplist = ['primary',
                        'primary_link',
                        'secondary',
                        'secondary_link',
                        'tertiary',
                        'tertiary_link',
                        'trunk',
                        'trunk_link',
                        'motorway',
                        'motorway_link',
                        'road',
                        'unclassified',
                        'track']
            
            with arcpy.da.UpdateCursor(osm_roads, ["highway"]) as cursor:
                for row in cursor:
                    if row[0] in keeplist:
                        pass
                    else:
                        cursor.deleteRow()
            #Remove ungraded tracks
            with arcpy.da.UpdateCursor(osm_roads, ["highway","tracktype"]) as cursor:
                for row in cursor:
                    if row[0] == 'track' and row[1] == None:
                        cursor.deleteRow()

            #Remove tracks graded 2-5
            grades = ['grade2','grade3','grade4','grade5']
            with arcpy.da.UpdateCursor(osm_roads, ["highway","tracktype"]) as cursor:
                for row in cursor:
                    if (row[0] == 'track') and (row[1] in grades):
                        cursor.deleteRow()
                    else:
                        pass

            #Remove roads with access no or private
            no_access = ['no','private']
            with arcpy.da.UpdateCursor(osm_roads,["access"]) as cursor:
                for row in cursor:
                    if row[0] in no_access:
                        cursor.deleteRow()
                    else:
                        pass

            #apply surface tags
            unpaved = ['unpaved',
                       'compacted',
                       'fine_gravel',
                       'gravel',
                       'rock',
                       'pebblestone',
                       'ground',
                       'dirt',
                       'earth',
                       'grass',
                       'grass_paver',
                       'mud',
                       'sand',
                       'woodchips',
                       'snow',
                       'ice',
                       'salt']
            #remove unpaved roads
            with arcpy.da.UpdateCursor(osm_roads,['surface']) as cursor:
                for row in cursor:
                    if row[0] in unpaved:
                        cursor.deleteRow()
                    else:
                        pass
            
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
        
