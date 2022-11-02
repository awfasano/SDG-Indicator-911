# James Gibson 
# 06/15/2022
#
# Step 5: Get Results
# Part 3

 
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

#Part 3
#Calculating National RAI
#Calculating National Pop Percent Error
print('--------------------------------------------------------')
print('Calculating the National RAI and Pop Percent Error')
print('--------------------------------------------------------')

globalfc = r'G:\HumanPlanet\SDG91\Version3\Results\Results.gdb\SDG9_RAI_National_062422_Data_Set'
arcpy.AddField_management(globalfc,'RAI','DOUBLE')
arcpy.AddField_management(globalfc,'PCT_ERROR','DOUBLE')
print('Fields added to global fc')


for iso in mylist:
    try:
        #Set Workspace
        gdb = r'G:\HumanPlanet\SDG91\Version3\Countries\%s.gdb' % iso
        workspace = gdb
        arcpy.env.workspace = workspace
        arcpy.env.overwriteOutput = True
        #Paths to Data
        WPP = r'G:\HumanPlanet\WPP\WPP2019.csv'
        WP = r'G:\HumanPlanet\WorldPopData\unconstrained\2020\%s\%s_ppp_2020_UNadj.tif' % (iso,iso.lower())
        #Get layers
        adm2 = '%s_admin2' % iso
        adm2_complete = '%s_adm2_complete' % iso
        #Zone Fields
        zone_r = 'adm2_rural' 
        zone_r_buff = 'adm2_r_buf' 
        adm2_zones = 'adm2_zones'
        #Get National RAI
        rural_pop = 0
        rural_pop_w_access = 0
        with arcpy.da.SearchCursor(adm2_complete,[zone_r,zone_r_buff]) as cursor:
            for row in cursor:
                rural_pop = rural_pop + row[0]
                rural_pop_w_access = rural_pop_w_access + row[1]
        rai = (rural_pop_w_access/rural_pop) * 100
        #Append to global layer
        globalfc = r'G:\HumanPlanet\SDG91\Version3\Results\Results.gdb\SDG9_RAI_National_062422_Data_Set'
        with arcpy.da.UpdateCursor(globalfc,['GID_0','RAI']) as cursor:
            for row in cursor:
                if row[0] == iso:
                    row[1] = rai
                    cursor.updateRow(row)
                else:
                    pass
        ## Checks ##
        ##WPP Check
        #Get Total POP
        adm2_zones = 'adm2_zones'
        total_pop = 0
        with arcpy.da.SearchCursor(adm2_complete,[adm2_zones]) as cursor:
            for row in cursor:
                total_pop = total_pop + row[0]
        #Percent Error Calc
        df = pd.read_csv(WPP)
        query = df.loc[df['ISO'] == iso]
        WPP = query['WPP2020'].values[0]
        pct_error = abs((float(total_pop) - float(WPP))/float(WPP)) * 100
        with arcpy.da.UpdateCursor(globalfc,['GID_0','PCT_ERROR']) as cursor:
            for row in cursor:
                if row[0] == iso:
                    row[1] = pct_error
                    cursor.updateRow(row)
                else:
                    pass
        print('Done: %s' % iso)
    except Exception as e:
        print('##########')
        print('Error: %s' % iso)
        print(e)
        print('##########')


