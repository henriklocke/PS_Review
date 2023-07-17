

import pypyodbc #used to run Access queries

import arcpy
from arcpy import env #used to create mdb
import shutil #For copy file
import ctypes
import os # for file operations
import pandas as pd
import sqlite3
import re



def readQuery(SQL, fullPath):
    queryOutput = []
    if ".mdb" in fullPath:
        conn = pypyodbc.win_connect_mdb(fullPath)
    elif ".sqlite" in fullPath:
        conn = sqlite3.connect(fullPath)
##        print "connection established with ",fullPath
    else:
        MessageBox(None, "Tool ends." + fullPath + " not recognized as mdb or sqlite", 'Info', 0)
        exit()
    ###################
    cur = conn.cursor()
    print SQL
    cur.execute(SQL)
    while True:
        row = cur.fetchone()
        if not row:
            break
        queryOutput.append(row)
    cur.close()
    conn.commit()
    conn.close()
    return queryOutput
##    columns = [column[0].lower() for column in cur.description]
##    return [columns,queryOutput]

working_folder = os.getcwd()

##mu_path = r'J:\SEWER_AREA_MODELS\FSA\03_SIMULATION_WORK\MU_Classic_To_MIKE+_Sim_Compare\MIKEPLUS_2023_March_2023\Model\FSA_Base_2020pop_V050.sqlite'
mu_path = r'C:\Users\hloecke\PS_Review\Lisa_Base.sqlite'


def main(working_folder,mu_path):
##if 1 == 1: #For debugging in idle/pyscripter uncomment this line and comment the above.

    extension = os.path.splitext(mu_path)[1][1:].lower()

    if extension == 'mdb':
        sql = "SELECT msm_Pump.AssetName, msm_Node.MUID, InvertLevel, GroundLevel, msm_Pump.MUID AS Pump, StartLevel, StopLevel, "
        sql += "IIF(SpeedNo=1,'Constant','Variable') AS Speed, IIF(SpeedNo=2,msm_Pump.WetWellSetPoint,'') AS Setpoint, "
        sql += "IIF(CapTypeNo=3,'Constant',IIF(CapTypeNo=2,'Q-DeltaH','QH')) AS Capacity_Type, IIF(CapTypeNo=3,msm_Pump.DutyPoint,'') AS Discharge, "
        sql += "IIF(CapTypeNo<>3,msm_Pump.QMaxSetID,'') AS Pump_Curve_Max, IIF(SpeedNo=2,msm_Pump.QMinSetID,'') AS Pump_Curve_Min "
        sql += "FROM msm_Pump INNER JOIN msm_Node ON msm_Node.MUID = msm_Pump.FROMNODE WHERE msm_Pump.AssetName <> '' ORDER BY msm_Pump.AssetName, StartLevel, msm_Pump.MUID"
    else:
        sql = "SELECT msm_Pump.AssetName, msm_Node.MUID, InvertLevel, GroundLevel, msm_Pump.MUID AS Pump, StartLevel, StopLevel, "
        sql += "CASE SpeedNo WHEN 1 THEN 'Constant' ELSE 'Variable' END AS Speed, CASE SpeedNo WHEN 2 THEN msm_Pump.WetWellSetPoint ELSE '' END AS Setpoint, "
        sql += "CASE CapTypeNo WHEN 3 THEN 'Constant' WHEN 2 THEN 'Q-DeltaH' ELSE 'QH' END AS Capacity_Type, CASE CapTypeNo WHEN 3 THEN msm_Pump.DutyPoint ELSE '' END AS Discharge, "
        sql += "CASE CapTypeNo WHEN 3 THEN '' ELSE msm_Pump.QMaxSetID END AS Pump_Curve_Max, CASE SpeedNo WHEN 2 THEN msm_Pump.QMinSetID ELSE '' END AS Pump_Curve_Min "
        sql += "FROM msm_Pump INNER JOIN msm_Node ON msm_Node.MUID = msm_Pump.FROMNODEID "
        sql += "WHERE msm_Pump.Active = 1 AND msm_Node.Active = 1 AND msm_Pump.AssetName <> '' "
        sql += "ORDER BY msm_Pump.AssetName, StartLevel, msm_Pump.MUID"
    pumps = readQuery(sql,mu_path)

    #Works for both
    if extension == 'mdb':
        sql = "SELECT TabID, Sqn, Value1, Value2 FROM ms_TabD ORDER BY TabID, Sqn "
    else:
        sql = "SELECT TabID, Sqn, Value1, Value2 FROM ms_TabD WHERE Active = 1 ORDER BY TabID, Sqn"
    curves = readQuery(sql,mu_path)

    if extension == 'mdb':
        sql = "SELECT msm_RTCDevice.PumpID, msm_RTCDeviceD.Sqn, msm_RTCDeviceD.ConditionID, msm_RTCDeviceD.FunctionID "
        sql += "FROM msm_RTCDevice INNER JOIN msm_RTCDeviceD ON msm_RTCDevice.MUID = msm_RTCDeviceD.DeviceID WHERE msm_RTCDevice.DeviceNo=1 ORDER BY msm_RTCDevice.PumpID, msm_RTCDeviceD.Sqn"
    else:
        sql = "SELECT msm_RTC.PumpID, msm_RTCRule.Sqn, msm_RTCRule.Condition, msm_RTCRule.ActionID "
        sql += "FROM msm_RTC INNER JOIN msm_RTCRule ON msm_RTC.MUID = msm_RTCRule.RtcMUID "
        sql += "WHERE msm_RTC.StructureTypeNo = 1 AND msm_RTC.Active = 1 AND msm_RTCRule.Active = 1 "
        sql += "ORDER BY msm_RTC.PumpID, msm_RTCRule.Sqn "
    rtc = readQuery(sql,mu_path)

    ##            master_list.append([backup_no,'Network','Number of Nodes','Number',nodes])

    df = pd.DataFrame(pumps,columns=['PS','Sump','Invert_Level','Ground_Level','Pump','Start_Level','Stop_Level','Speed','Setpoint','Capacity_Type','Discharge','Pump_Curve_Max','Pump_Curve_Min'])
    df.to_csv(working_folder + '\\PS_Info.csv',index=False)

    df = pd.DataFrame(curves,columns=['Curve','SQN','HGL','Discharge'])
    df.to_csv(working_folder + '\\PS_Curves.csv',index=False)

    df = pd.DataFrame(rtc,columns=['Pump','SQN','Condition','Action'])
    df.to_csv(working_folder + '\\PS_RTC.csv',index=False)

    arcpy.env.overwriteOutput = True
    if extension == "mdb":
        msm_Node = mu_path + "\\mu_Geometry\\msm_Node"
    else:

        arcpy.FeatureClassToFeatureClass_conversion (mu_path + "\\msm_Node", working_folder, 'msm_Node.shp')
        msm_Node = working_folder + '\\msm_Node.shp'
        arcpy.DefineProjection_management(msm_Node, "PROJCS['NAD_1983_2011_UTM_Zone_10N',GEOGCS['GCS_NAD_1983_2011',DATUM['D_NAD_1983_2011',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Transverse_Mercator'],PARAMETER['False_Easting',500000.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-123.0],PARAMETER['Scale_Factor',0.9996],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]]")

    Google_Coordinates_shp = working_folder + "\\Google_Coordinates.shp"
    arcpy.Project_management(msm_Node, Google_Coordinates_shp, "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]", "WGS_1984_(ITRF08)_To_NAD_1983_2011", "PROJCS['NAD_1983_2011_UTM_Zone_10N',GEOGCS['GCS_NAD_1983_2011',DATUM['D_NAD_1983_2011',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Transverse_Mercator'],PARAMETER['False_Easting',500000.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-123.0],PARAMETER['Scale_Factor',0.9996],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]]", "NO_PRESERVE_SHAPE", "", "NO_VERTICAL")
    centroids = arcpy.da.FeatureClassToNumPyArray(Google_Coordinates_shp, ("MUID","SHAPE@X","SHAPE@Y"))
    centroids = centroids.tolist()
    centroids_df = pd.DataFrame(centroids, columns =['MUID','X','Y'])
    centroids_df.to_csv(working_folder + '\\Google_Coordinates.csv', index = False)

if __name__ == "__main__":
    main(sys.argv[1],sys.argv[2])


