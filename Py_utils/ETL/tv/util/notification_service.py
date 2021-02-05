import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import random
import threading
import time
import logging
import re
import traceback
import sys, os
from jinja2 import Environment, FileSystemLoader


from tv.configs import Config
from tv import path_resolver


'''
Thread safe LRU Cache
'''
class LRUCache(object):

    def __init__(self, cacheSize=10):
        self.lock = threading.Lock()
        # this variables maps keys to values
        self.Cache = {}
        self.logger = logging.getLogger(__name__)

        # this variables maps keys to timestamp of last request
        # filed for them. Is volatile - requires local clock to never change,
        # or that there is a separate clock service that guarantees global
        # consistency. 
        self.RequestTimestamps = {}

        self.CacheSize = cacheSize

    def insert(self, key, value):

        '''
        Insert a new key into our cache. If inserting would exceed our cache size,
        then we shall make room for it by removing the least recently used. 
        '''

        with self.lock:

            if len(self.Cache) == self.CacheSize:
                self.removeLeastRecentlyUsed()

        self.Cache[key] = value
        self.RequestTimestamps[key] = time.time()

    def get(self, key):

        '''
        Retrieve an key from our cache, keeping note of the time we last 
        retrieved it. 
        '''

        with self.lock:
            if key in self.Cache:
                self.RequestTimestamps[key] = time.time()
                return self.Cache[key]
            else:
                #raise KeyError("Not found!")
                return None

    def removeLeastRecentlyUsed(self):

        '''
        Should only be called in a threadsafe context.
        '''

        if self.RequestTimestamps:

            # scan through and find the least recently used key by finding
            # the lowest timestamp among all the keys.

            leastRecentlyUsedKey = min(self.RequestTimestamps, key=lambda key: self.RequestTimestamps[key])

            # now that the least recently used key has been found, 
            # remove it from the cache and from our list of request timestamps.
            
            return self.remove(leastRecentlyUsedKey)
            # self.Cache.pop(leastRecentlyUsedKey, None)
            # self.RequestTimestamps.pop(leastRecentlyUsedKey, None)
            #return 

        # only called in the event requestTimestamps does not exist - randomly 
        # pick a key to erase.
        # randomChoice = random.choice(self.Cache.keys())
        # return self.Cache.pop(randomChoice, None)
    
    def remove(self, item_key):
        with self.lock:
            item = self.Cache.pop(item_key, None)
            self.RequestTimestamps.pop(item_key, None)
            return item
    
    def pop(self):
        with self.lock:
            return self.removeLeastRecentlyUsed()

    def values(self):
        with self.lock:
            values = list(self.Cache.values())
        return values

    def pop_all(self):
        with self.lock:
            values = list(self.Cache.values())
            self.Cache = {}
            self.RequestTimestamps = {}
        return values 

    def size(self):
        #with self.lock:
        return len(self.Cache)

    def __str__(self):
        return self.Cache.__str__()

class NotificationService:
    
    logger = logging.getLogger(__name__)
    cache = LRUCache(10)
    #email = Email()

    @classmethod
    def print_data(cls, data_dict):
        cls.logger.info("#########")
        cls.logger.info("NOTIFICATION")
        cls.logger.info(data_dict)

    @classmethod
    def send_mail(cls, data_dict = {}, dry_run = False, mock_run=False):
        mail_type = data_dict.get("status")
        if mail_type is not None:
            if (re.match("^SUCCESS|[Ss]uccess|SKIP|[Ss]kip.*$", mail_type.upper()) is not None):
                Email.sendSuccessMail(data_dict, dry_run, mock_run) 
            else:
                Email.sendFailureMail(data_dict, dry_run, mock_run) 
        else:
            raise ValueError("Data status is not defined properly for notification")

    @classmethod
    def notify_from_queue(cls, item_key:str=None, dry_run = False, mock_run =True):
        if item_key == None:
            size = cls.cache.size()
            cls.logger.debug("++++++++++++++++++++++")
            cls.logger.debug(size)
            while size > 0:
                cls.notify(cls.cache.pop(), dry_run, mock_run)
                size = size - 1
                cls.logger.info(size)
        else:
            cls.notify(cls.get(item_key), dry_run, mock_run)
            cls.cache.remove(item_key)
        
    
    @classmethod
    def notify(cls, data_dict, dry_run = False, mock_run =True):
        cls.logger.info("Notifying.....\n")
        #thr = threading.Thread(target=cls.print_data, args=(data_dict, ))
        thr = threading.Thread(target=cls.send_mail, args=(data_dict, dry_run, mock_run ))
        thr.start()

    @classmethod
    def put_in_queue(cls, item_key:str, item_obj):
        cls.cache.insert(item_key, item_obj)

    @classmethod
    def get(cls, item_key:str):
        return cls.cache.get(item_key)


class Email:
    
    mailConfig = None
    logger = logging.getLogger(__name__)
    
    @classmethod
    def sendmail(cls, msg:MIMEMultipart, mailFor='FAIL', dry_run = False, mock_run = False, mail_test_flag = True):
        try:
            if mock_run:
                return
            cls.logger.info("Sending Mail.....")
            mailConfig = cls.getMailConfig(mailFor)
            if (re.match("^SUCCESS|[Ss]uccess|SKIP|[Ss]kip.*$", mailFor.upper()) is not None):
                recipients = cls.is_valid_string(mailConfig.get('recipients_group1'), 'recipients')
            else :
                recipients = cls.is_valid_string(mailConfig.get('recipients_group2'), 'recipients')
            recipient_list = re.split('\\s*,\\s*', recipients)
            valid=1
            for i in recipient_list: 
                if ("@team.neustar" in i):
                    valid=0
                    break
            if (valid == 1):
                raise Exception("No Valid email id , please pass valid email id's in recipients group")
            sender = cls.is_valid_string(mailConfig.get('sender'), 'sender')
            smtp_server = cls.is_valid_string(mailConfig.get('smtp_server'), 'smtp_server')
            smtp_port= cls.is_valid_int(mailConfig.get('smtp_port'), 'smtp_port')

            s = smtplib.SMTP(smtp_server, smtp_port)
            s.ehlo()
            # start TLS for security
            s.starttls()
            #msg['to'] = ", ".join(recipient_list)
            msg['bcc'] = ", ".join(recipient_list)
            # sending the mail
            if not dry_run or mail_test_flag:
                s.sendmail(sender, recipient_list, msg.as_string())
            else:
                cls.logger.info('sender: -' + str(sender))
                cls.logger.info('recipient: -' + str(recipient_list))
                cls.logger.info('content: -' + msg.as_string())
            #s.sendmail(SENDER, RECIPIENT, msg.as_string())
            # terminating the session
            s.close()
            s = None
        except Exception as e:
            cls.logger.error("Error: ", e)
            traceback.print_exc(file=sys.stdout)
            logging.error(e, exc_info=True)
            traceback.print_stack()
        finally :
            if s is not None:
                s.close()
                s = None

    @classmethod
    def sendFailureMail(cls, dataDict:dict, dry_run = False, mock_run = False):
        try:
            cls.logger.info("Sending ETL Failure Mail.....")
            cls.logger.debug("Data Model Dict: "+ str(dataDict))
            status='Failed'
            #cls.validateDataDict(dataDict)
            
            start_timestamp = cls.is_valid_int(dataDict.get('trigger_on'), 'start_timestamp')
            trigger_start_date = str(datetime.fromtimestamp(start_timestamp))
            #trigger_end_date = datetime.fromtimestamp(dataDict.get('finish_on'))
            
            start_date=dataDict.get('start_date')
            end_date = dataDict.get('end_date')
            
#             recursive_trigger = dataDict.get('recursive_trigger')
#             if (recursive_trigger is not None):
#                 recursive_trigger_num = recursive_trigger.get('recursive_trigger_num')
#                 recursive_timestamp = recursive_trigger.get('recur_started_on')
#                 recursive_start_date = str(datetime.fromtimestamp(recursive_timestamp))
#             else:
#                 recursive_trigger_num = None
#                 recursive_timestamp = None
#                 recursive_start_date = None
                
            recursive_trigger_num = dataDict.get('recursive_trigger_num')
            recursive_timestamp = dataDict.get('recur_started_on')
            recursive_start_date = str(datetime.fromtimestamp(recursive_timestamp))
            
            final_start_date = dataDict.get('tv_start_date')
            final_end_date = dataDict.get('tv_end_date')
            logs_loc = dataDict.get('staging_path')
            description = dataDict.get('description')
            stack_trace = dataDict.get('stack_trace')
            start_week = dataDict.get('start_week')
            start_year = dataDict.get('start_year')
            end_week = dataDict.get('end_week')
            end_year = dataDict.get('end_year')
            error_type = cls.is_valid_string(dataDict.get('error_type'), 'error_type')

            if(error_type == 'MTA_IMP_GENERATION_FAILED'):
                subject = 'TV ETL Run has {STATUS}'
                html = cls.get_html_template('failure_mail.html', START_DATE=start_date, END_DATE=end_date, RECURSIVE_TRIGGER_NUM=recursive_trigger_num, FINAL_START_DATE=final_start_date, FINAL_END_DATE=final_end_date, TRIGGER_START_DATE=trigger_start_date, logs_loc=logs_loc, RECURSIVE_START_DATE=recursive_start_date, START_TIMESTAMP= start_timestamp, RECURSIVE_TIMESTAMP = recursive_timestamp, DESCRIPTION=description, STACK_TRACE=stack_trace)
            elif(error_type == 'CREATIVE_UPDATE_FAILED'):
                subject = 'Creative LMT update trigger has {STATUS}'
                html = cls.get_html_template('creative_failure_mail.html', TRIGGER_START_DATE=trigger_start_date, logs_loc=logs_loc, START_TIMESTAMP= start_timestamp, DESCRIPTION=description, STACK_TRACE=stack_trace, START_WEEK=start_week, START_YEAR=start_year, END_WEEK=end_week, END_YEAR=end_year)
            # Create message container - the correct MIME type is multipart/alternative.
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject.format(STATUS=status)

            # Record the MIME types of both parts - text/plain and text/html.
            part1 = MIMEText(html, "html")
            msg.attach(part1)
            if not mock_run:
                cls.sendmail(msg, mailFor='Failure', dry_run=dry_run, mock_run=mock_run)
            else:
                cls.logger.info("EMAIL CONTENT :\n {}\n".format(msg.as_string()))
               
        except Exception as e:
            cls.logger.error("Error: ", e)
            traceback.print_exc(file=sys.stdout)
            logging.error(e, exc_info=True)
            traceback.print_stack()

    @classmethod
    def sendSuccessMail(cls, dataDict:dict, dry_run = False, mock_run = False):
        try:
            cls.logger.info("Sending ETL Successful Mail.....")
            cls.logger.debug("Data Model Dict: "+ str(dataDict))
            tivo = dataDict.get('tivo')
            if(tivo is None):
                raise Exception('tivo dict must be non empty.{}'.format(tivo))
            tivo_start_date = cls.is_valid_string(tivo.get('start_date'), 'tivo_start_date')
            tivo_end_date = cls.is_valid_string(tivo.get('end_date'), 'tivo_end_date') 
            tivo_desc = cls.is_valid_string(tivo.get('description'), 'tivo_desc')
            
            experian = dataDict.get('experian')
            if(experian is None):
                raise Exception('experian dict must be non empty.{}'.format(experian))
            experian_start_date = cls.is_valid_string(experian.get('start_date'), 'experian_start_date')
            experian_end_date = cls.is_valid_string(experian.get('end_date'), 'experian_end_date')
            experian_desc = cls.is_valid_string(experian.get('description'), 'experian_desc')
            
            kantar = dataDict.get('kantar')
            if(kantar is None):
                raise Exception('kantar dict must be non empty.{}'.format(kantar))
            kantar_start_date = cls.is_valid_string(kantar.get('start_date'), 'kantar_start_date')
            kantar_end_date = cls.is_valid_string(kantar.get('end_date'), 'kantar_end_date')
            kantar_desc = cls.is_valid_string(kantar.get('description'), 'kantar_desc')
            if kantar.get('start_week') == 'NA':
                kantar_start_week = None
            else:
                kantar_start_week = cls.is_valid_int(kantar.get('start_week'), 'kantar_start_week')
            if kantar.get('end_week') == 'NA':
                kantar_end_week = None
            else:
                kantar_end_week = cls.is_valid_int(kantar.get('end_week'), 'kantar_end_week')
            
            start_timestamp = cls.is_valid_int(dataDict.get('trigger_on'), 'start_timestamp')
            trigger_start_date = datetime.fromtimestamp(start_timestamp)
                
            #end_timestamp = dataDict.get('finish_on')
            recursive_trigger = dataDict.get('recursive_trigger')
            if(recursive_trigger is None):
                raise Exception('recursive_trigger dict must be non empty.{}'.format(recursive_trigger))
            recursive_trigger_num = cls.is_valid_int(recursive_trigger.get('recursive_trigger_num'), 'recursive_trigger_num')
            recursive_timestamp = cls.is_valid_int(recursive_trigger.get('recur_started_on'), 'recursive_timestamp')
            #recursive_start_date = (datetime.fromtimestamp(recursive_timestamp)).strftime(format='%Y-%m-%d')
            recursive_start_date = str(datetime.fromtimestamp(recursive_timestamp))
            
            final_start_date = cls.is_valid_string(dataDict.get('tv_start_date'), 'final_start_date')
            final_end_date = cls.is_valid_string(dataDict.get('tv_end_date'), 'final_end_date')
            
            creative_lmt = dataDict.get('creative_lmt')
            creative_lmt_msg = cls.is_valid_string(creative_lmt.get('msg'), 'creative_lmt_msg')
            
            status = cls.is_valid_string(dataDict.get('status'), 'status')
            start_date=cls.is_valid_string(dataDict.get('start_date'), 'start_date')
            end_date = cls.is_valid_string(dataDict.get('end_date'), 'end_date')
            
            staging_path = dataDict.get('staging_path')
            # message to be sent

            subject = 'TV ETL Run is {STATUS} from {START_DATE} to {END_DATE}'
            # Create message container - the correct MIME type is multipart/alternative.
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject.format(STATUS=status, START_DATE=start_date, END_DATE=end_date)

            html = cls.get_html_template('successful_mail.html', START_DATE=start_date, END_DATE=end_date, RECURSIVE_TRIGGER_NUM=recursive_trigger_num, FINAL_START_DATE=final_start_date, FINAL_END_DATE=final_end_date,TIVO_START_DATE=tivo_start_date, TIVO_END_DATE=tivo_end_date, TIVO_DESC=tivo_desc, EXPERIAN_START_DATE=experian_start_date,EXPERIAN_END_DATE=experian_end_date, EXPERIAN_DESC=experian_desc, KANTAR_START_DATE=kantar_start_date, KANTAR_END_DATE=kantar_end_date, KANTAR_DESC=kantar_desc, START_TIMESTAMP=start_timestamp, RECURSIVE_TIMESTAMP=recursive_timestamp, RECURSIVE_START_DATE=recursive_start_date, KANTAR_START_WEEK=kantar_start_week, KANTAR_END_WEEK=kantar_end_week, CREATIVE_LMT=creative_lmt_msg, TRIGGER_START_DATE=trigger_start_date, STAGING_PATH=staging_path, STATUS=status)
            
            
            # Record the MIME types of both parts - text/plain and text/html.
            part1 = MIMEText(html, "html")
            msg.attach(part1)
            if not mock_run:
                cls.sendmail(msg, mailFor='Success', dry_run=dry_run, mock_run=mock_run)
            else:
                cls.logger.info("EMAIL CONTENT :\n {}\n".format(msg.as_string()))
                
        except Exception as e:
            cls.logger.error("Error: ", e)
            traceback.print_exc(file=sys.stdout)
            logging.error(e, exc_info=True)
            traceback.print_stack()


    @classmethod	  
    def getMailConfig(cls, mailFor=''):
        if cls.mailConfig is None:
            cls.mailConfig = Config.get_configs()
#         cls.logger.debug("CONFIG:-->>>")
#         cls.logger.debug(cls.mailConfig)
        return cls.mailConfig
    
    @classmethod
    def get_html_template(cls, template_file_name, **kwargs):

        path = os.path.join("templates", "html") 
        templates_dir = path_resolver.resolve(path)
        env = Environment( loader = FileSystemLoader(templates_dir) )
        template = env.get_template(template_file_name)
        return template.render(**kwargs)
    
    @classmethod
    def is_valid_string(cls, to_validate:str, level:str):
        if (to_validate is not None):
            res = to_validate.strip()
            if (len(res) > 0):
                return res
            else : 
                raise Exception(level + ' length must be greater then zero:{}'.format(to_validate))
        else : 
            raise Exception(level + ' must be non empty String:{}'.format(to_validate))
    
    @classmethod
    def is_valid_int(cls, to_validate, level:str):
        if (to_validate is not None):
            if(type(to_validate) == str):
                res = to_validate.strip()
                if (len(res) > 0):
                    try:
                        res = int(res)
                        return res
                    except Exception as e:
                        raise TypeError(level + ' must be of valid numeric type',e)
                else : 
                    raise Exception(level + ' length must be greater then zero:{}'.format(to_validate))
            elif (type(to_validate) == int):
                return to_validate
        else : 
            raise Exception(level + ' must be non empty String:{}'.format(to_validate))


