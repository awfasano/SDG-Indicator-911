# James Gibson 
# 06/15/2022
#
# Step 5: Get Results
# Part 4

 
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


#Part 4
#Create Global Subnational Data Set
print('--------------------------------------------------------')
print('Part 4')
print('Create Global Subnational Data Set')
print('--------------------------------------------------------')

arcpy.env.workspace = r'G:\HumanPlanet\SDG91\Version3\Countries'

workspaces = arcpy.ListWorkspaces("*","FileGDB")

#First we copy over the target feature classes
for workspace in workspaces:
    try:
        iso = workspace[-7:-4]
        arcpy.env.workspace = workspace
        complete = '%s_adm2_complete' % iso
        out_copy = r'G:\HumanPlanet\SDG91\Version3\Results\Mapping.gdb\%s_complete_copy' % iso
        arcpy.CopyFeatures_management(complete,out_copy)
        print('Copied: %s' % iso)
    except:
        print('##########')
        print('Error: %s' % iso)
        print('##########')

print('---------------------')

outGDB = r'G:\HumanPlanet\SDG91\Version3\Results\Mapping.gdb'
arcpy.env.workspace = outGDB

merge_list = []

fcs = arcpy.ListFeatureClasses()


for fc in fcs:
    iso = fc[:3]
    try:
        if 'complete_copy' in fc:
            print(fc)
            merge_list.append(fc)
        else:
            pass
    except:
        print('##########')
        print('Error: %s'  % fc)
        print('##########')
        
#merge
length = len(merge_list)
print('The Number of Fcs Being Merged is: %s' % length)
out_merge = 'SDG9_V3_062422_Results_Adm2'
arcpy.Merge_management(merge_list,out_merge)
print('Merge Complete')

print('Adding Percent Errors, inc, etc...')
#More editing!
reference = r'G:\HumanPlanet\SDG91\Version3\Results\Results.gdb\SDG9_RAI_National_062422_Data_Set'

ISOS = []
Pct_Errors = []

with arcpy.da.SearchCursor(reference,['GID_0','PCT_ERROR']) as cursor:
    for row in cursor:
        ISOS.append(row[0])
        Pct_Errors.append(row[1])


df = pd.DataFrame({'ISO':ISOS,
                   'Percent_Error':Pct_Errors})

arcpy.AddField_management(out_merge,'PCT_ERROR','DOUBLE')

with arcpy.da.UpdateCursor(out_merge,['GID_0','PCT_ERROR']) as cursor:
    for row in cursor:
        query = df.loc[df['ISO'] == row[0]]
        pct_error = query['Percent_Error'].values[0]
        row[1] = pct_error
        cursor.updateRow(row)


#copy
out_copy = r'G:\HumanPlanet\SDG91\Version3\Results\Results.gdb\SDG9_RAI_Subnational_062422_Data_Set'
arcpy.CopyFeatures_management(out_merge,out_copy)
print('copied')
print('DONE')

      




        

