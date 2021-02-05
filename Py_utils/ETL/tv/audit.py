import json
import pandas as pd
from datetime import datetime
from tv.states import StateService

class Audit:

    @classmethod
    def stat(cls, type='LOCAL', max_stat=10, status=''):
        if type == 'SAVED':
            item_dict = StateService.read_original()
        else:
            item_dict = StateService.read()
        
        #item_dict = json.loads(json_data)
        json_len=len(item_dict['tv_etl_history'])
        val2=[]
        for i in range(json_len):
            List = []
            if item_dict['tv_etl_history'][i]['status'] == status or not status:
                List.append(item_dict['tv_etl_history'][i]['status'])
                if item_dict['tv_etl_history'][i]['started_on'] is None or item_dict['tv_etl_history'][i]['started_on'] == -1:
                    List.append("NA")
                else:
                    List.append(datetime.fromtimestamp(item_dict['tv_etl_history'][i]['started_on']))
                if item_dict['tv_etl_history'][i]['finished_on'] is None or item_dict['tv_etl_history'][i]['finished_on'] == -1:
                    List.append("NA")
                else:
                    List.append(datetime.fromtimestamp(item_dict['tv_etl_history'][i]['finished_on']))
                List.append(item_dict['tv_etl_history'][i]['tv-start-date'])
                List.append(item_dict['tv_etl_history'][i]['tv-end-date'])
                List.append(item_dict['tv_etl_history'][i]['trigger_no_of_days'])
                # List.append(item_dict['tv_etl_history'][i]['description'])
                val2.append(List)
                if len(val2) == max_stat :
                    break
        #df=pd.DataFrame(val2, columns=["status", "started_on", "finished_on","tv-start-date","tv-end-date","trigger_no_of_days","description"])
        df=pd.DataFrame(val2, columns=["Status", "Started_on", "Finished_on","TV-START-DATE","TV-END-DATE","Trigger_no_of_days"])
        print(df)