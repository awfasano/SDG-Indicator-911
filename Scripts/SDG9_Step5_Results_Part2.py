# James Gibson 
# 06/15/2022
#
# Step 5: Get Results
# Part 2

 
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



#Part 2
#Calculating the Subnational RAI
#QA and QC
print('--------------------------------------------------------')
print('Part 2')
print('Calculating the Subnational RAI')
print('--------------------------------------------------------')

for iso in mylist:
    try:
        #Set Workspace
        gdb = r'G:\HumanPlanet\SDG91\Version3\Countries\%s.gdb' % iso
        workspace = gdb
        arcpy.env.workspace = workspace
        arcpy.env.overwriteOutput = True
        #Get layers
        adm2 = '%s_admin2' % iso
        adm2_complete = '%s_adm2_complete' % iso
        #Create Zone Fields List
        zone_r = 'adm2_rural' 
        zone_r_buff = 'adm2_r_buf'
        adm2_zones = 'adm2_zones' 
        ZoneList = [zone_r,zone_r_buff,adm2_zones]
        #Nan to zero
        for zone in ZoneList:
            with arcpy.da.UpdateCursor(adm2_complete, [zone]) as cursor:
                for row in cursor:
                    if row[0] == None:
                        row[0] = 0
                        cursor.updateRow(row)
                continue
        #calculations
        rai_r = "adm2_pct_r"
        arcpy.AddField_management(adm2_complete,rai_r,"DOUBLE")
        expression1 = "(!%s! / !%s!) * 100" %(zone_r_buff,zone_r)
        arcpy.CalculateField_management(adm2_complete,rai_r,expression1,"PYTHON")
        #'0/0 = -1'
        with arcpy.da.UpdateCursor(adm2_complete,rai_r) as cursor:
            for row in cursor:
                if row[0] == None:
                    row[0] = -1
                    cursor.updateRow(row)
        print('Done: %s' % iso)
    except Exception as e:
        print('##########')
        print('Error: %s' % iso)
        print(e)
        print('##########')
