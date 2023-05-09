

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

mu_path = r'C:\Users\hloecke\PS_Review\NSSA_Base.mdb'


##def main(working_folder,mu_path,use_accumulation):
if 1 == 1: #For debugging in idle/pyscripter uncomment this line and comment the above.


    extension = os.path.splitext(mu_path)[1][1:].lower()


    if extension == 'mdb':
        sql = "SELECT msm_Pump.AssetName, msm_Node.MUID, InvertLevel, GroundLevel, msm_Pump.MUID AS Pump, StartLevel, StopLevel, "
        sql += "IIF(SpeedNo=1,'Constant','Variable') AS Speed, IIF(SpeedNo=2,msm_Pump.WetWellSetPoint,'') AS Setpoint, "
        sql += "IIF(CapTypeNo=3,'Constant',IIF(CapTypeNo=2,'Q-DeltaH','QH')) AS Capacity_Type, IIF(CapTypeNo=3,msm_Pump.DutyPoint,'') AS Discharge, "
        sql += "IIF(CapTypeNo<>3,msm_Pump.QMaxSetID,'') AS Pump_Curve_Max, IIF(SpeedNo=2,msm_Pump.QMinSetID,'') AS Pump_Curve_Min "
        sql += "FROM msm_Pump INNER JOIN msm_Node ON msm_Node.MUID = msm_Pump.FROMNODE WHERE msm_Pump.AssetName <> '' ORDER BY msm_Pump.AssetName, StartLevel, msm_Pump.MUID"
    else:
        pass
    pumps = readQuery(sql,mu_path)

    if extension == 'mdb':
        sql = "SELECT TabID, Sqn, Value1, Value2 FROM ms_TabD ORDER BY TabID, Sqn"
    else:
        pass

    curves = readQuery(sql,mu_path)

    pass
##            master_list.append([backup_no,'Network','Number of Nodes','Number',nodes])

df = pd.DataFrame(pumps,columns=['PS','Sump','Invert_Level','Ground_Level','Pump','Start_Level','Stop_Level','Speed','Setpoint','Capacity_Type','Discharge','Pump_Curve_Max','Pump_Curve_Min'])
df.to_csv(working_folder + '\\PS_Info.csv',index=False)

df = pd.DataFrame(curves,columns=['Curve','SQN','HGL','Discharge'])
df.to_csv(working_folder + '\\PS_Curves.csv',index=False)


##if __name__ == "__main__":
##    print sys.argv[3]
##    main(sys.argv[1],sys.argv[2],sys.argv[3])


