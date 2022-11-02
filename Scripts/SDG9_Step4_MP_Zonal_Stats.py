# James Gibson 
# 06/09/2022
# Version 3
#
# Step 4: Zonal Stats

 
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

arcpy.Delete_management("memory")


def process(iso):
    message = None
    if message is None:
        try:
            gdb = r'G:\HumanPlanet\SDG91\Version3\Countries\%s.gdb' % iso
            workspace = gdb
            arcpy.env.workspace = workspace
            arcpy.env.overwriteOutput = True
            #Path to WP Data
            WP = r'G:\HumanPlanet\WorldPopData\unconstrained\2020\%s\%s_ppp_2020_UNadj.tif' % (iso,iso.lower())
            #Get Data
            adm2 = '%s_admin2' % iso
            roads_dissolved = "%s_osm_roads_dissolved" % iso
            SMOD_Clip = '%s_SMOD' % iso
            boundary = '%s_admin0' % iso
            ##Prepare zonal layers
            #First intersect SMOD and Admin 2 units to get (urban and) rural zones per adm 2 unit
            inFeatures = [adm2, SMOD_Clip] 
            SMOD_adm2_intersect = "%s_ur_adm2_zones" % iso
            arcpy.Intersect_analysis(inFeatures, SMOD_adm2_intersect)
            #Select and Extract by gridcode
            adm2_rural_select = arcpy.SelectLayerByAttribute_management(SMOD_adm2_intersect,"NEW_SELECTION","gridcode = 1")
            adm2_rural = "%s_adm2_rural" % iso
            arcpy.CopyFeatures_management(adm2_rural_select,adm2_rural)
            ## Intersect renamed Split outputs with roads buffer
            inFeatures = [adm2_rural,roads_dissolved]
            adm2_r_buf = '%s_adm2_r_buf' % iso
            arcpy.Intersect_analysis(inFeatures,adm2_r_buf)
            ##zonal statistics
            #Copy of admin 2 to join all results to
            adm2_complete = '%s_adm2_complete' % iso
            arcpy.CopyFeatures_management(adm2,adm2_complete)
            #Create ZoneList
            zone_r = '%s_adm2_rural' % iso
            zone_r_buff = '%s_adm2_r_buf' % iso
            ZoneList = [zone_r,zone_r_buff]
            #WP for Loop
            pop = WP
            for zone in ZoneList:
                arcpy.env.snapRaster = WP
                arcpy.env.cellSize = WP
                outtablename = r'memory\tbl_%s' % zone
                ZonalStatisticsAsTable(zone,"UniqueID",pop,outtablename,"DATA","SUM")
                arcpy.JoinField_management(adm2_complete,"UniqueID",outtablename,"UniqueID",["SUM"])
                rename = zone.replace('%s_' % iso,'')
                arcpy.AlterField_management(adm2_complete,"SUM",rename,rename)
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
        
