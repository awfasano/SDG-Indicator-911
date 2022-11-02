# James Gibson 
# 06/09/2022
#
# This is an improved script for calculating RAI
# MP version
# Version 3
# No need to calculate urban
# Step 1: Data Prep

 
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


#Global Variables
GADMGlobal = r'G:\HumanPlanet\GADM\GADM.gdb\GADMCopy3'


def process(iso):
    #Paths to important data
    WP = r'G:\HumanPlanet\WorldPopData\unconstrained\2020\%s\%s_ppp_2020_UNadj.tif' % (iso,iso.lower())
    message = None
    if message is None:
        try:
            #Create geodatabase
            gdb = r'G:\HumanPlanet\SDG91\Version3\Countries\%s.gdb' % iso
            outFolder = r'G:\HumanPlanet\SDG91\Version3\Countries'
            if not os.path.exists(gdb):
                arcpy.CreateFileGDB_management(outFolder,'%s.gdb' % iso)
                
            #Set Workspace
            workspace = gdb
            arcpy.env.workspace = workspace
            arcpy.env.overwriteOutput = True


            #Get GADM Layer
            where_clause = '"GID_0" = \'%s\'' % iso
            out_gadm = '%s_gadm' % iso
            arcpy.Select_analysis(GADMGlobal,out_gadm,where_clause)

            gadm_copy = '%s_gadm_copy' % iso
            arcpy.CopyFeatures_management(out_gadm,gadm_copy)

            #Dissolve to admin0 level
            boundary = '%s_admin0' % iso
            dissolveFields = ['GID_0','NAME_0']
            arcpy.Dissolve_management(gadm_copy, boundary, dissolveFields, "", 
                                    "MULTI_PART", "DISSOLVE_LINES")
            
            #COD IDs need to be adjusted
            if iso == 'COD':
                with arcpy.da.UpdateCursor(gadm_copy,'ID_0') as cursor:
                    for row in cursor:
                        row[0] = 1
                        cursor.updateRow(row)
                
                name_1 = []
                counter1 = 0
                with arcpy.da.UpdateCursor(gadm_copy,['ID_1','NAME_1']) as cursor:
                    for row in cursor:
                        if row[1] in name_1:
                            pass
                        else:
                            row[0] = counter1
                            cursor.updateRow(row)
                            counter1 = counter1 + 1
                name_2 = []
                counter2 = 0
                with arcpy.da.UpdateCursor(gadm_copy,['ID_2','NAME_2']) as cursor:
                    for row in cursor:
                        if row[1] in name_2:
                            pass
                        else:
                            row[0] = counter2
                            cursor.updateRow(row)
                            counter2 = counter2 + 1
            else:
                pass

                    
            #Dissolve to admin2 level
            #Add UniqueIDs to adm2
            FieldName = 'UniqueID'
            fieldprecision = 9
            fieldAlias = 'UniqueID'
            arcpy.AddField_management(gadm_copy,FieldName,"TEXT",fieldprecision,field_alias=fieldAlias,field_is_nullable="NULLABLE")

            #Populate UniqueID field
            expression = '"{}_{}_{}".format(!ID_0!, !ID_1!, !ID_2!)'
            arcpy.CalculateField_management(gadm_copy,FieldName,expression,"PYTHON")

            #Dissolve to admin2 level
            adm2 = '%s_admin2' % iso
            dissolveFields2 = ['GID_0','NAME_0','ID_1','NAME_1','VARNAME_1','TYPE_1','ENGTYPE_1','ID_2','VARNAME_2','HASC_2','TYPE_2','ENGTYPE_2','NAME_2','UniqueID']

            #Execute Dissolve
            arcpy.Dissolve_management(gadm_copy, adm2, dissolveFields2, "","MULTI_PART", "DISSOLVE_LINES")

            #Clipping SMOD data to country
            #SMOD has already been converted to a polygon layer
            SMOD = r'G:\HumanPlanet\GHS_SMOD\2015\SMOD_2015.gdb\SMOD_urban_rural_polygons_not_projected'
            SMOD_Clip = '%s_SMOD' % iso
            
            arcpy.Clip_analysis(SMOD,boundary,SMOD_Clip)

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
        
