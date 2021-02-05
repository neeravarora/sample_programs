import sys
import py_path
from utils.neucmd.hive_query_neusql import NeuSQL
from utils.configs import Config
import json
import os
from os import path
import pandas as pd 
import re
def main(ccpa_json:str, platform_config, create_sh_file_location:str,select_sh_file_location:str,insert_sh_file_location:str,jars_location):
    create_sh_file_location = create_sh_file_location
    deleteQueryFile(create_sh_file_location)
    hiveSettings(create_sh_file_location, jars_location)
    Config.get_platform_config(platform_config)
    hive_setting="set hive.mapjoin.smalltable.filesize=300000000;\nset hive.exec.dynamic.partition.mode=nonstrict;\nset hive.exec.max.dynamic.partitions.pernode=1500;\nset hive.exec.max.dynamic.partitions=10000;\nset hive.stats.autogather=true;\nset hive.compute.query.using.stats=true;\nset mapreduce.input.fileinputformat.input.dir.recursive=true;\nset hive.mapred.supports.subdirectories=true;\nset hive.enforce.bucketing=true;\nset hive.auto.convert.join=true;\nset hive.optimize.metadataonly=true;\nset hive.mapred.mode=nonstrict;\nset hive.strict.checks.cartesian.product=false;\nset mapreduce.task.io.sort.mb=1024;\nset mapreduce.task.index.cache.limit.bytes=100000000;\nset mapreduce.input.fileinputformat.split.minsize=33554432;\nset mapreduce.input.fileinputformat.split.maxsize=500000000;\nset mapreduce.task.timeout=1200000;\nset hive.qubole.filestatus.recurse=true;\n"
    murmur_jars_location='s3://neustar-dsdk/Common/Jars/murmur-hash3-128-udf-1.0-with-dependencies.jar'
    hiveudf_hash_jar_location='s3://neustar-dsdk/Common/Jars/hiveudf_hash.jar'
    hive_jars="add jar "+murmur_jars_location+";\n"+"add jar "+hiveudf_hash_jar_location+";\n"
    hive_function="create temporary function murmur_hash_3_128_salt0 as 'MurmurHash3_128_Salt0';\ncreate temporary function hash_string as 'HashString';\n"
    full_hive_setting=hive_setting+"\n"+hive_function+"\n"
    with open(ccpa_json) as json_file:
        item_dict = json.load(json_file)
    client_name = item_dict['dnClientName']
    database_name = item_dict['dataBaseName']
    target_table = item_dict['tableName']
    target_table_name = database_name+"."+target_table
    json_len = len(item_dict['domainNameColumnName'])
    mydict= {}
    s3_loc1 = ''
    domainName=''
    count = 0
    for i in range(json_len):
        if item_dict['domainNameColumnName'][i]['schemaType'] == 'Online_Portal':
            column_1 = item_dict['domainNameColumnName'][i]['columnName']
            s3_loc1 = item_dict['domainNameColumnName'][i]['dataAssetLocation']
            domainName=item_dict['domainNameColumnName'][i]['domainName']
            if column_1 in mydict.keys(): 
                mydict[column_1].append({'s3_location':s3_loc1,'table_name':"CCPA_DELETE_LIST_778_ext"})
            else :
                mydict[column_1] = [{'s3_location':s3_loc1,'table_name':"CCPA_DELETE_LIST_778_ext"}]
        elif item_dict['domainNameColumnName'][i]['schemaType'] == 'Online_API':
            column_2 = item_dict['domainNameColumnName'][i]['columnName']
            s3_loc2 = item_dict['domainNameColumnName'][i]['dataAssetLocation']
            count += 1
            table_name = 'CCPA_DELETE_LIST_ext_'+ str(count)
            if column_2 in mydict.keys(): 
                mydict[column_2].append({'s3_location':s3_loc2,'table_name':table_name})
            else :
                mydict[column_2] = [{'s3_location':s3_loc2,'table_name':table_name}]
        else :
            raise Exception("Inavlid schemaType, SchemaType must be either Online_Portal or Online_API")
    unionquery= ''
    result_unionquery = ''

    join_query = ''
    final_join_query = ''
    column_count=1

    final_join_sub_query=''
    join_sub_query=''
    select_join_sub_query = ''
    select_final_join_sub_query = ''
    view_number = 0
    list_view=[]
    view_count = 0
    final_query = ''
    domainName=domainName.upper()
    select_final_query = "NeuSQL -S -engine=spark -e \""+full_hive_setting+"\n select count(1) from "+target_table_name+" t1 "

    if (s3_loc1 is not None and s3_loc1 != '' and domainName !='AK_USER_ID'):
        hive_query = onlinePortalQuery(s3_loc1,list_view,domainName)
        updateQueryFile(hive_query, create_sh_file_location)
    if (s3_loc1 is not None and s3_loc1 != '' and domainName =='AK_USER_ID'):
        for data in mydict.keys():
            for u in mydict[data]:
                if(u['s3_location']!= s3_loc1 ):
                    view_number+=1
                    view_name = 'CCPA_DELETE_LIST_'+ str(view_number)
                    hive_query = onlinePortalApiQuery(u['s3_location'], u['table_name'], view_name)
                    updateQueryFile(hive_query, create_sh_file_location)
                    list_view.append(view_name)
    if(len(list_view) >= 0 and domainName =='AK_USER_ID'):
        hive_query = onlinePortalQuery(s3_loc1,list_view,domainName)
        updateQueryFile(hive_query, create_sh_file_location)
    
    for data in mydict.keys():
        list_view_api = []
        for u in mydict[data]:
            if(u['s3_location']!= s3_loc1 ):
                view_count+=1
                view_name = 'CCPA_DELETE_LIST_'+ str(view_count)
                api_table_name = 'CCPA_DELETE_LIST_API_'+ str(view_count)
                list_view_api.append(api_table_name)
                hive_query =onlineApiQuery(u['s3_location'], u['table_name'], api_table_name,list_view)
                updateQueryFile(hive_query, create_sh_file_location)
            elif(u['s3_location'] == s3_loc1 ):
                list_view_api.append('CCPA_DELETE_LIST_COOKIES')
        column_count+=len(list_view_api)
        if (len(list_view_api) > 1):
            unionquery = unionQuery(target_table, data , list_view_api, unionquery, column_count)    
            result_unionquery = result_unionquery + unionquery
            join_sub_query=checkNull(column_count)
            select_join_sub_query=checkNotNull(column_count, data)
        else:
            join_query = joinQuery(list_view_api, data, column_count)
            final_join_query=final_join_query + join_query
            join_sub_query=checkNull(column_count)
            select_join_sub_query=checkNotNull(column_count, data)
        if (join_sub_query != '' and final_join_sub_query != '' and select_join_sub_query != ''):
            final_join_sub_query=final_join_sub_query+' AND '+join_sub_query
            select_final_join_sub_query=select_final_join_sub_query+' OR '+select_join_sub_query
        else:
            final_join_sub_query=final_join_sub_query+join_sub_query
            select_final_join_sub_query=select_final_join_sub_query+select_join_sub_query
    final_query= final_query+final_join_query+result_unionquery+' WHERE '+final_join_sub_query
    deleteQueryFile(select_sh_file_location)
    sourceNeucmd(platform_config,select_sh_file_location)
    select_final_query="res_var=`"+select_final_query+final_join_query+result_unionquery+' WHERE '+select_final_join_sub_query+';'+"\"`\n echo $res_var\n"
    updateQueryFile(select_final_query, select_sh_file_location)
    deleteQueryFile(insert_sh_file_location)
    hiveSettings(insert_sh_file_location, jars_location)
    insertOverwriteQuery(target_table_name, final_query, insert_sh_file_location)


def updateQueryFile(hiveQuery:str, sh_file_location:str):
    query_file = open(sh_file_location, "a")
    query_file.write(hiveQuery)
    query_file.close()

def deleteQueryFile(sh_file_location:str):
    if (str(path.exists(sh_file_location)) == 'True'):
        os.remove(sh_file_location)
    query_file = open(sh_file_location, "x")
    query_file.close()

def sourceNeucmd(platform_config,sh_file_location):
    source_cmd="source /home/backenduser/MTA/releases/10.2/msaction_backend/common/setup/scripts/Neu_cmd.sh \nif [[ \"$?\" != 0 ]]; then \n\t echo \"Command execution is not successful\"\n\t exit 1 \nfi\n export PLATFORM_CONFIG_FILE="+platform_config+"\nif [[ \"$?\" != 0 ]]; then \n\t echo \"Command execution is not successful\"\n\t exit 1 \nfi\n"
    updateQueryFile(source_cmd, sh_file_location)

def parseResult(table_name:str):
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

def insertOverwriteQuery(table_name:str, final_query, create_sh_file_location:str):
    partition_value = parseResult(table_name)
    if (partition_value == ''):
        hive_query = "\n--If someone tries to run this query manually, then before deleting <table>_BKP  check whether <table> exists or not. If  <table> does not exist then donot delete <table>_BKP \nDESC FORMATTED "+table_name+";\n\nDROP TABLE IF EXISTS "+table_name+"_BKP ;\nCREATE TABLE "+table_name+"_BKP LIKE "+table_name+";\nINSERT OVERWRITE TABLE "+table_name+"_BKP \n\tSELECT t1.* FROM "+table_name+" t1\n"+final_query+";\nDROP TABLE "+ table_name+";\nCREATE TABLE "+table_name+" LIKE "+table_name+"_BKP;\nINSERT OVERWRITE TABLE "+ table_name+" select * from "+table_name+"_BKP;"
        updateQueryFile(hive_query, create_sh_file_location) 
    else:
        hive_query = "\n--If someone tries to run this query manually, then before deleting <table>_BKP  check whether <table> exists or not. If  <table> does not exist then donot delete <table>_BKP \nDESC FORMATTED "+table_name+";\n\nDROP TABLE IF EXISTS "+table_name+"_BKP ;\nCREATE TABLE "+table_name+"_BKP LIKE "+table_name+";\nINSERT OVERWRITE TABLE "+table_name+"_BKP PARTITION("+partition_value+")\n\t\tSELECT t1.* FROM "+table_name+" t1\n"+final_query+";\nDROP TABLE "+ table_name+";\nCREATE TABLE "+table_name+" LIKE "+table_name+"_BKP;\nINSERT OVERWRITE TABLE "+ table_name+" PARTITION("+partition_value+") select * from "+table_name+"_BKP;"
        updateQueryFile(hive_query, create_sh_file_location)

def unionQuery(table_name:str, column_name:str , list_view_name, unionquery, column_count):
    test_query = ''
    count=column_count
    table_variable_name='t'
    for i in range(len(list_view_name)):
        table_variable=table_variable_name+str(count)
        if (test_query == ''):
            test_query =  "select delete_col from "+list_view_name[i]
        else:
            test_query =  test_query+" UNION select delete_col from "+list_view_name[i]
    result_query = "LEFT OUTER JOIN ("+test_query+")"+" "+ table_variable +  " ON (t1."+column_name+" = "+table_variable+".delete_col)\n"
    if(unionquery == ''):
        unionquery = result_query+"\n"
        return unionquery
    else:
        unionquery = "LEFT OUTER JOIN ("+test_query+")"+" "+ table_variable +  " ON (t1."+column_name+" = "+table_variable+".delete_col)\n"
        return unionquery
    count-=1

def hiveSettings(sh_file_location:str,jars_location):
    hive_setting="set hive.mapjoin.smalltable.filesize=300000000;\nset hive.exec.dynamic.partition.mode=nonstrict;\nset hive.exec.max.dynamic.partitions.pernode=1500;\nset hive.exec.max.dynamic.partitions=10000;\nset hive.optimize.sort.dynamic.partition=true;\nset hive.stats.autogather=true;\nset hive.compute.query.using.stats=true;\nset mapreduce.input.fileinputformat.input.dir.recursive=true;\nset hive.mapred.supports.subdirectories=true;\nset hive.enforce.bucketing=true;\nset hive.auto.convert.join=true;\nset hive.optimize.metadataonly=true;\nset hive.mapred.mode=nonstrict;\nset hive.strict.checks.cartesian.product=false;\nset mapreduce.task.io.sort.mb=1024;\nset mapreduce.task.index.cache.limit.bytes=100000000;\nset mapreduce.input.fileinputformat.split.minsize=500000000;\nset mapreduce.input.fileinputformat.split.maxsize=500000000;\nset mapreduce.task.timeout=1200000;\nset hive.qubole.filestatus.recurse=true;\n"
    murmur_jars_location='s3://neustar-dsdk/Common/Jars/murmur-hash3-128-udf-1.0-with-dependencies.jar'
    hiveudf_hash_jar_location='s3://neustar-dsdk/Common/Jars/hiveudf_hash.jar'
    hive_jars="add jar "+murmur_jars_location+";\n"+"add jar "+hiveudf_hash_jar_location+";\n"
    hive_function="create temporary function murmur_hash_3_128_salt0 as 'MurmurHash3_128_Salt0';\ncreate temporary function hash_string as 'HashString';\n"
    full_hive_setting=hive_setting+"\n"+hive_function+"\n"
    updateQueryFile(full_hive_setting, sh_file_location)

def onlinePortalQuery(s3_loc1:str,list_view,domainName):
    if(len(list_view) == 0 and domainName !='AK_USER_ID'):
        hive_query = "DROP TABLE IF EXISTS CCPA_DELETE_LIST_778_ext;\n CREATE EXTERNAL TABLE CCPA_DELETE_LIST_778_ext (requestId string, logDateTime string, Cookie string, mobileAdId string, isCookie string,  ipAddress string, emailId string, cluster array<struct<id:string, metaData:string, hhPid:string, emails:string, userAgent:string, geo:string, geoAll:string>>, requestType string, isCaliforniaResident string)\n ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n with serdeproperties ( 'paths'='requestId, logDateTime, Cookie, mobileAdId, isCookie, ipAddress, emailId, cluster,isCaliforniaResident' )\n LOCATION" + "'"+s3_loc1+"' ;\n\n DROP TABLE IF EXISTS CCPA_DELETE_LIST_COOKIES;\n CREATE TABLE CCPA_DELETE_LIST_COOKIES AS \n SELECT \n explode( \n case when \n\t TRIM(cookie) IS NOT NULL and TRIM(cookie) != '""' AND TRIM(mobileAdId) IS NOT NULL and TRIM(mobileAdId) != '""' \n\t then \n\t\t array(murmur_hash_3_128_salt0(cookie),murmur_hash_3_128_salt0(mobileAdId)) \n\t WHEN TRIM(cookie) IS NOT NULL and TRIM(cookie) != '""' \n\t then \n\t\t array(murmur_hash_3_128_salt0(cookie)) \n\t else \n array(murmur_hash_3_128_salt0(mobileAdId))\n end ) \n as delete_col \n from CCPA_DELETE_LIST_778_ext \n where (TRIM(cookie) IS NOT NULL and TRIM(cookie) != '') or (TRIM(mobileAdId) IS NOT NULL and TRIM(mobileAdId) != '');\n\n"
        return hive_query
    elif(len(list_view) >= 1 and domainName =='AK_USER_ID'):
        sub_hive_query="DROP TABLE IF EXISTS CCPA_DELETE_LIST_COOKIES;\n CREATE TABLE CCPA_DELETE_LIST_COOKIES AS SELECT delete_col from (SELECT delete_col from CCPA_DELETE_LIST_778"
        hive_query = "DROP TABLE IF EXISTS CCPA_DELETE_LIST_778_ext;\n CREATE EXTERNAL TABLE CCPA_DELETE_LIST_778_ext (requestId string, logDateTime string, Cookie string, mobileAdId string, isCookie string,  ipAddress string, emailId string, cluster array<struct<id:string, metaData:string, hhPid:string, emails:string, userAgent:string, geo:string, geoAll:string>>, requestType string, isCaliforniaResident string)\n ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n with serdeproperties ( 'paths'='requestId, logDateTime, Cookie, mobileAdId, isCookie, ipAddress, emailId, cluster,isCaliforniaResident' )\n LOCATION" + "'"+s3_loc1+"' ;\n\n DROP TABLE IF EXISTS CCPA_DELETE_LIST_778;\n CREATE TABLE CCPA_DELETE_LIST_778 AS \n SELECT \n explode( \n case when \n\t TRIM(cookie) IS NOT NULL and TRIM(cookie) != '""' AND TRIM(mobileAdId) IS NOT NULL and TRIM(mobileAdId) != '""' \n\t then \n\t\t array(murmur_hash_3_128_salt0(cookie),murmur_hash_3_128_salt0(mobileAdId)) \n\t WHEN TRIM(cookie) IS NOT NULL and TRIM(cookie) != '""' \n\t then \n\t\t array(murmur_hash_3_128_salt0(cookie)) \n\t else \n array(murmur_hash_3_128_salt0(mobileAdId))\n end ) \n as delete_col \n from CCPA_DELETE_LIST_778_ext \n where (TRIM(cookie) IS NOT NULL and TRIM(cookie) != '') or (TRIM(mobileAdId) IS NOT NULL and TRIM(mobileAdId) != '');\n\n"
        for i in range(len(list_view)):
            sub_hive_query=sub_hive_query+" UNION ALL\n"+"SELECT delete_col from "+list_view[i]+"\n"
        final_hive_query= hive_query+sub_hive_query+")ccpa_delete_list;\n"
        return final_hive_query
    elif(len(list_view) == 0 and domainName =='AK_USER_ID'):
        hive_query = "DROP TABLE IF EXISTS CCPA_DELETE_LIST_778_ext;\n CREATE EXTERNAL TABLE CCPA_DELETE_LIST_778_ext (requestId string, logDateTime string, Cookie string, mobileAdId string, isCookie string,  ipAddress string, emailId string, cluster array<struct<id:string, metaData:string, hhPid:string, emails:string, userAgent:string, geo:string, geoAll:string>>, requestType string, isCaliforniaResident string)\n ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n with serdeproperties ( 'paths'='requestId, logDateTime, Cookie, mobileAdId, isCookie, ipAddress, emailId, cluster,isCaliforniaResident' )\n LOCATION" + "'"+s3_loc1+"' ;\n\n DROP TABLE IF EXISTS CCPA_DELETE_LIST_COOKIES;\n CREATE TABLE CCPA_DELETE_LIST_COOKIES AS \n SELECT \n explode( \n case when \n\t TRIM(cookie) IS NOT NULL and TRIM(cookie) != '""' AND TRIM(mobileAdId) IS NOT NULL and TRIM(mobileAdId) != '""' \n\t then \n\t\t array(murmur_hash_3_128_salt0(cookie),murmur_hash_3_128_salt0(mobileAdId)) \n\t WHEN TRIM(cookie) IS NOT NULL and TRIM(cookie) != '""' \n\t then \n\t\t array(murmur_hash_3_128_salt0(cookie)) \n\t else \n array(murmur_hash_3_128_salt0(mobileAdId))\n end ) \n as delete_col \n from CCPA_DELETE_LIST_778_ext \n where (TRIM(cookie) IS NOT NULL and TRIM(cookie) != '') or (TRIM(mobileAdId) IS NOT NULL and TRIM(mobileAdId) != '');\n\n"
        return hive_query

def onlineApiQuery(s3_loc2:str, table_name:str,api_table_name:str,list_view):
    if(len(list_view) >= 1):
        hive_query="\nDROP TABLE IF EXISTS " +api_table_name+";\n CREATE TABLE "+api_table_name+" AS SELECT consumerId AS delete_col \n FROM " + table_name +"\n WHERE (consumerId IS NOT NULL) AND (consumerId != "'""'");\n\n"
        return hive_query
    else:
        hive_query="\nDROP TABLE IF EXISTS " +table_name+";\nCREATE EXTERNAL TABLE " + table_name+" (requestId string, requestStatus string, customerId string,neustarId string,consumerId string, thirdPartyIds array<String>, thirdPartyIdType string, cookies array<String>, emailIds array<String>, emailHashType string, maids array<String>, maidHashType string, ipAddress string, consumerRequestDate string)\n ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n with serdeproperties ( 'paths'='requestId , requestStatus , customerId ,neustarId ,consumerId , thirdPartyIds , thirdPartyIdType , cookies , emailIds , emailHashType , maids , maidHashType , ipAddress , consumerRequestDate ' )\n LOCATION" + "'"+s3_loc2+"'"+";\n\nDROP VIEW IF EXISTS " +api_table_name+";\nDROP TABLE IF EXISTS " +api_table_name+";\n CREATE TABLE "+api_table_name+" AS SELECT consumerId AS delete_col \n FROM " + table_name +"\n WHERE (consumerId IS NOT NULL) AND (consumerId != "'""'");\n\n"
        return hive_query

def onlinePortalApiQuery(s3_loc2:str, table_name:str, view_name:str):
    hive_query="DROP TABLE IF EXISTS " +table_name+";\nCREATE EXTERNAL TABLE " + table_name+" (requestId string, requestStatus string, customerId string,neustarId string,consumerId string, thirdPartyIds array<String>, thirdPartyIdType string, cookies array<String>, emailIds array<String>, emailHashType string, maids array<String>, maidHashType string, ipAddress string, consumerRequestDate string)\n ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n with serdeproperties ( 'paths'='requestId , requestStatus , customerId ,neustarId ,consumerId , thirdPartyIds , thirdPartyIdType , cookies , emailIds , emailHashType , maids , maidHashType , ipAddress , consumerRequestDate ' )\n LOCATION" + "'"+s3_loc2+"'"+";\n\nDROP VIEW IF EXISTS " +view_name+";\n DROP TABLE IF EXISTS "+ view_name+";\n CREATE TABLE "+ view_name+" AS \n SELECT DISTINCT murmur_hash_3_128_salt0(delete_col) as delete_col \n FROM ( \n SELECT EXPLODE(cookies) AS delete_col \n FROM "+table_name+"  \n WHERE (cookies IS NOT NULL) AND (SIZE(cookies) != 0)\n UNION ALL\n SELECT EXPLODE(ARRAY(hash_string(maid_copy,"'"SHA-1"'"),hash_string(maid_copy,"'"MD5"'")))\n  FROM (\n\t SELECT EXPLODE(ARRAY(LOWER(maid), UPPER(maid))) as maid_copy\n\tFROM (\n\t\t SELECT EXPLODE(maids) AS maid\n\t\t FROM " + table_name+"\n\t\t WHERE (maids IS NOT NULL) AND (SIZE(maids) != 0) AND maidHashType="'"NONE"'"\n\t\t ) maid_table_without_hash_1\n\t) maid_table_without_hash_2 \n\t UNION ALL \n\t SELECT EXPLODE(maids) AS delete_col \n\t FROM " + table_name+" \n\t WHERE (maids IS NOT NULL) AND (SIZE(maids) != 0) AND (TRIM(maidHashType) IN ("'"SHA1"'","'"SHA256"'","'"MD5"'","'"NONE"'"))\n) t1 ;\n\n"
    return hive_query

def checkNull(column_count):
    table_variable_name = 't'
    hive_query=''
    table_variable=table_variable_name+str(column_count)
    if (hive_query == ''):
        hive_query = table_variable+".delete_col IS NULL "
    else:
        hive_query = hive_query+" AND "+table_variable+".delete_col IS NULL "
    return hive_query

def checkNotNull(column_count,column_name):
    table_variable_name = 't'
    hive_query=''
    table_variable=table_variable_name+str(column_count)
    if (hive_query == ''):
        hive_query = '('+table_variable+".delete_col = t1."+column_name+')'
    else:
        hive_query = hive_query+" or "+'('+table_variable+".delete_col = t1."+column_name+')'
    return hive_query

def joinQuery(list_view_name, column_name:str, column_count):
    test_join_query = ''
    table_variable_name = 't'
    count=column_count
    for i in range(len(list_view_name)):
        table_variable=table_variable_name+str(count)
        if (test_join_query == ''):
            test_join_query =  "LEFT OUTER JOIN "+list_view_name[i]+" "+ table_variable + " ON (t1."+column_name+" = "+table_variable+".delete_col)\n"
        else:
            test_join_query =  test_join_query+"\nLEFT OUTER JOIN "+list_view_name[i]+" "+ table_variable + " ON (t1."+column_name+" = "+table_variable+".delete_col)\n"
        count-=1
        
    return test_join_query

if __name__ == '__main__':
        args=sys.argv
        main(args[1],args[2],args[3],args[4],args[5],args[6])
