import sys
import py_path
from utils.neucmd.hive_query_neusql import NeuSQL
from utils.configs import Config
import json
import os
from os import path
import pandas as pd 
import re
def main(csv_file_loc:str, platform_config, sh_file_location:str,wrapper_location):
    Config.get_platform_config(platform_config)
    file_to_open = csv_file_loc
    sh_file_location = sh_file_location
    deleteQueryFile(sh_file_location)
    sourceNeucmd(platform_config, wrapper_location, sh_file_location)
    df = pd.read_csv(file_to_open)
    if (df.empty):
        raise Exception("uploaded csv file is empty")
    else :
        for i in range(len(df)):
            source_table_name=(df["source_db_name"][i]).strip()+"."+(df["source_table_name"][i]).strip()
            target_table_name=(df["target_db_name"][i]).strip()+"."+(df["source_table_name"][i]).strip()
            source_s3_location = parseResult((df["source_table_name"][i]).strip(),(df["source_db_name"][i]).strip())
            destination_s3_loaction = parseResult((df["source_table_name"][i]).strip(),(df["target_db_name"][i]).strip())
            hiveQuery="NeuSQL -S -e \" DROP TABLE IF EXISTS "+ target_table_name +";\"\n\nif [[ \"$?\" != 0 ]]; then \n\t echo \"Command execution is not successful\"\n\t exit 1 \nfi\n"
            updateQueryFile(hiveQuery, sh_file_location)
            copy_query="NeuHadoop -awss3cli -cp \""+source_s3_location+"\" \""+destination_s3_loaction+"\" --recursive \nif [[ \"$?\" != 0 ]]; then \n\t echo \"Command execution is not successful\"\n\t exit 1 \nfi\n"
            updateQueryFile(copy_query, sh_file_location)
            partition_value = getPartitionResult(source_table_name)
            if (partition_value != ''):
                hiveQuery="NeuSQL -S -e \" CREATE TABLE "+target_table_name+" LIKE "+source_table_name +"; \n ALTER TABLE "+target_table_name+" RECOVER PARTITIONs;\"\n\nif [[ \"$?\" != 0 ]]; then \n\t echo \"Command execution is not successful\"\n\t exit 1 \nfi\n"
                updateQueryFile(hiveQuery, sh_file_location)
                validation(partition_value,source_table_name,target_table_name,sh_file_location)
            else:
                hiveQuery="NeuSQL -S -e \" CREATE TABLE "+target_table_name+" LIKE "+source_table_name +";\"\n\nif [[ \"$?\" != 0 ]]; then \n\t echo \"Command execution is not successful\"\n\t exit 1 \nfi\n"
                updateQueryFile(hiveQuery, sh_file_location)
                validation(partition_value,source_table_name,target_table_name,sh_file_location)
def updateQueryFile(hiveQuery:str, sh_file_location:str):
    query_file = open(sh_file_location, "a")
    query_file.write(hiveQuery)
    query_file.close()

def deleteQueryFile(sh_file_location:str):
    if (str(path.exists(sh_file_location)) == 'True'):
        os.remove(sh_file_location)
    query_file = open(sh_file_location, "x")
    query_file.close()

def parseResult(table_name:str,database_name:str):
    #file_to_open = os.path.join("Downloads", "full_result_345759981.csv")
    #data = pd.read_csv(file_to_open)
    data = NeuSQL.hive_query(query='DESCRIBE DATABASE EXTENDED ' + database_name + ';', return_data = True, debug=True)
    a = data['location'][0]
    s3_location=a+'/'+table_name+'/'
    return s3_location

def getPartitionResult(table_name:str):
    #file_to_open = os.path.join("Downloads", "full_result_345759981.csv")
    #data = pd.read_csv(file_to_open)
    data = NeuSQL.hive_query(query='DESCRIBE EXTENDED ' + table_name + ';', return_data = True, debug=True)
    a = data['data_type'][len(data)-1]
    regex = '^[Tt]able\(.*partitionKeys:\[(.*)\], .*\)$'
    matched = re.findall(regex, a)
    b = matched[0].split(',')
    partitions = ''
    if (len(b) > 1):
        for i in range(len(b)):
            c = b[i].split(':')
            if (partitions == '' and c[0] != ' comment' and c[0] != ' type'):
                partitions = c[1]
            elif (partitions !='' and c[0] != ' comment' and c[0] != ' type'):
                    partitions = partitions +","+ c[1]
        return partitions
    else:
        return partitions

def sourceNeucmd(platform_config, wrapper_location, sh_file_location):
    source_cmd="source /home/backenduser/MTA/releases/10.1/msaction_backend/common/setup/scripts/Neu_cmd.sh \nif [[ \"$?\" != 0 ]]; then \n\t echo \"Command execution is not successful\"\n\t exit 1 \nfi\n export PLATFORM_CONFIG_FILE="+platform_config+"\nif [[ \"$?\" != 0 ]]; then \n\t echo \"Command execution is not successful\"\n\t exit 1 \nfi\n"
    updateQueryFile(source_cmd, sh_file_location)

def validation(partition_value, source_table, destination_table, sh_file_location):
    if (partition_value !=''):
        partition_list = partition_value.split(',')
        if (len(partition_list) > 1):
            case_statement=''
            on_statement=''
            for i in range(len(partition_list)):
                if (case_statement == ''):
                    case_statement = " AND (t1."+partition_list[i]+" is not null) AND (t2."+partition_list[i]+" is not null)"
                elif (case_statement !=''):
                    case_statement = case_statement+" AND (t1."+partition_list[i]+" is not null) AND (t2."+partition_list[i]+" is not null)"
                if (on_statement == ''):
                    on_statement = " ON (t1."+partition_list[i]+" = t2."+partition_list[i]
                elif (on_statement != ''):
                    on_statement = on_statement+" AND t1."+partition_list[i]+" = t2."+partition_list[i]+")"
            validation_query =  "res_var=`NeuSQL -S -e \"select sum(case when (t1.numrows = t2.numrows) "+case_statement+" then 0 else 1 end) as mismatch \nfrom(select count(*) as numrows,"+partition_value+" from "+source_table+" group by "+partition_value+") t1 \nfull outer join\n(select count(*) as numrows, "+partition_value+" from "+destination_table+" group by "+partition_value+") t2 \n "+on_statement+";\n\"`"
            validation_query = validation_query+"\nif [[ $res_var != 0 ]]; then \n\t echo \"Validation execution Failed for "+destination_table+"\"\n\t exit 1 \nfi\n"
            updateQueryFile(validation_query, sh_file_location)
        else:
            validation_query =  "res_var=`NeuSQL -S -e \"select sum(case when (t1.numrows = t2.numrows) AND (t1."+partition_value+" is not null) AND (t2."+partition_value+" is not null)   then 0 else 1 end) as mismatch \nfrom(select count(*) as numrows,"+partition_value+" from "+source_table+" group by "+partition_value+") t1 \nfull outer join\n(select count(*) as numrows, "+partition_value+" from "+destination_table+" group by "+partition_value+") t2 \nON (t1."+partition_value+" = t2."+partition_value+");\n\"`"
            validation_query = validation_query+"\nif [[ $res_var != 0 ]]; then \n\t echo \"Validation execution Failed for "+destination_table+"\"\n\t exit 1 \nfi\n"
            updateQueryFile(validation_query, sh_file_location)
    else:
        validation_query = "res_var=`NeuSQL -S -e \"select sum(case when (t1.numrows = t2.numrows) then 0 else 1 end) as mismatch \nfrom(select count(*) as numrows from "+source_table+") t1 \njoin(select count(*) as numrows from "+destination_table+") t2;\n\"`"
        validation_query = validation_query+"\nif [[ $res_var != 0 ]]; then \n\t echo \"Validation execution Failed for "+destination_table+"\"\n\t exit 1 \nfi\n"
        updateQueryFile(validation_query, sh_file_location)

if __name__ == '__main__':
        args=sys.argv
        main(args[1],args[2],args[3],args[4])
