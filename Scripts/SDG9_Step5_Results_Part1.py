# James Gibson 
# 06/15/2022
#
# Step 5: Get Results
# Part 1

 
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

#Globals
GADMGlobal = r'G:\HumanPlanet\GADM\GADM.gdb\GADMCopy3'
WPP = r'G:\HumanPlanet\WPP\WPP2019.csv'

print('Start Script')
arcpy.Delete_management("memory")

mylist=[]
with arcpy.da.SearchCursor(GADMGlobal,['GID_0']) as cursor:
    for row in cursor:
        if row[0] in mylist:
            pass
        else:
            mylist.append(row[0])

#Part 1
#Use Zonal Stats to get total pop of zones
print('--------------------------------------------------------')
print('Part 1')
print('Zonal Stats to Calculate Total Pop of Subnational Units')
print('--------------------------------------------------------')
for iso in mylist:
    try:
        #Set Workspace
        gdb = r'G:\HumanPlanet\SDG91\Version3\Countries\%s.gdb' % iso
        workspace = gdb
        arcpy.env.workspace = workspace
        arcpy.env.overwriteOutput = True
        #Path to WP data
        WP = r'G:\HumanPlanet\WorldPopData\unconstrained\2020\%s\%s_ppp_2020_UNadj.tif' % (iso,iso.lower())
        #Get layers
        adm2 = '%s_admin2' % iso
        adm2_complete = '%s_adm2_complete' % iso
        #Create another copy of adm2 for zones
        adm2_zones = '%s_adm2_zones' % iso
        arcpy.CopyFeatures_management(adm2,adm2_zones)
        #Zonal Stats
        zone = 'adm2_zones'
        arcpy.DeleteField_management(adm2_complete,zone)
        pop = WP
        arcpy.env.snapRaster = WP
        arcpy.env.cellSize = WP
        outtablename = r'memory\tbl_%s_%s' % (iso,zone)
        ZonalStatisticsAsTable(adm2_zones,"UniqueID",pop,outtablename,"DATA","SUM")
        arcpy.JoinField_management(adm2_complete,"UniqueID",outtablename,"UniqueID",["SUM"])
        arcpy.AlterField_management(adm2_complete,"SUM",zone,zone)
        print('Done: %s' % iso)
    except Exception as e:
        print('##########')
        print('Error: %s' % iso)
        print(e)
        print('##########')
    
