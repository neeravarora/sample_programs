
import logging


star_row = "**************************************************************************************************"
box_star_row = star_row+'**'

class LOG_FORMATTER:
    DEFAULT_FORMAT='%(asctime)s:%(process)2s %(threadName)10s %(name)s: %(message)s\n'
    SIMPLE_FORMAT='%(message)10s'



'''
    @log_format(formatter='===> [[ test2 ]]  %(name)2s %(message)10s')
    def test2(abc):
        log = logging.getLogger()
        log.info(abc)
        log.info(abc)
    test2('hi') 

    output:

                ===> [[ test2 ]]  root         hi
                ===> [[ test2 ]]  root         hi

'''
def log_format(formatter=LOG_FORMATTER.SIMPLE_FORMAT):                            
    def decorator(fn):                                            
        def decorated(*args,**kwargs):
            ROOT_LOGGER = logging.getLogger()
            exiting_formatter = ROOT_LOGGER.handlers[0].formatter
            ROOT_LOGGER.handlers[0].setFormatter(logging.Formatter(formatter))
            ret = fn(*args,**kwargs) 
            ROOT_LOGGER.handlers[0].setFormatter(exiting_formatter)
            return ret                 
        return decorated                                          
    return decorator

'''
    @box_logged
    def test2(abc):
        log = logging.getLogger()
        log.info(abc)
        log.info(abc)
    test2('hi') 

    output:

                ********************************************************
                **          
                **        hi
                **        hi
                **          
                ********************************************************


'''
def box_logged(fn):
    @log_format(formatter=' **%(message)10s')
    def decorated(*args,**kwargs):
        log = logging.getLogger("") 
        
        log.info(box_star_row)
        #log.info('')         
        ret = fn(*args,**kwargs)
        #log.info('')    
        log.info(box_star_row+"\n\n\n")
        return ret
    return decorated

def box_titled(fn):
    @log_format(formatter=' ****%(message)10s')
    def decorated(*args,**kwargs):
        log = logging.getLogger("") 
        
        log.info(star_row)
        #log.info('')         
        ret = fn(*args,**kwargs)
        #log.info('')    
        log.info(star_row)
        return ret
    return decorated

'''
    @elapsed_time(func_name="test2")
    def test2(abc):
        log = logging.getLogger()
        log.info(abc)
        log.info(abc)
    test2('hi')  

    output:

                test2 **Start Time = 2020-10-23 12:00:55.169943

                root **        hi
                root **        hi
                test2 **End Time = 2020-10-23 12:00:55.171749
                test2 **Elapsed Time = 0:00:00.001806


    @elapsed_time(func_name="test2")
    @log_format(formatter=PAT)
    def test2(abc):
        log = logging.getLogger()
        log.info(abc)
        log.info(abc)
    test2('hi')  

    output:

                test2 **Start Time = 2020-10-23 12:02:36.618194

                2020-10-23 12:02:36,619:24869 MainThread root: hi
                2020-10-23 12:02:36,619:24869 MainThread root: hi
                test2 **End Time = 2020-10-23 12:02:36.620541
                test2 **Elapsed Time = 0:00:00.002347


    @elapsed_time(func_name="test2")
    @box_logged
    def test2(abc):
        log = logging.getLogger()
        log.info(abc)
        log.info(abc)
    test2('hi') 

    output:

                test2 **Start Time = 2020-10-23 12:04:23.962149

                ********************************************************
                **          
                **        hi
                **        hi
                **          
                ********************************************************


                test2 **End Time = 2020-10-23 12:04:23.965766
                test2 **Elapsed Time = 0:00:00.003617 
'''
def elapsed_time(func_name=''):
    def decorator(fn):
        @log_format(formatter='%(name)2s **%(message)10s')
        def decorated(*args,**kwargs):
            import datetime
            log = logging.getLogger(func_name)
            before = datetime.datetime.now() 
            log.info("Start Time = {0}\n".format(before))
            x = fn(*args,**kwargs)                
            after = datetime.datetime.now()
            log.info("End Time = {0}".format(after))
            log.info("Elapsed Time = {0}\n".format(after-before))    
            return x                                             
        return decorated
    return decorator



def process_lock(lock=None):
    import multiprocessing
    def decorator(fn):
        def decorated(*args, **kwargs):
            if lock is None:
                return fn(*args, **kwargs)
            if isinstance(lock, multiprocessing.Lock) or isinstance(lock, multiprocessing.RLock):
                with lock:
                    return fn(*args, **kwargs)
            else:
                raise Exception("Lock")
            
        return decorated
    return decorator


def thread_lock(lock=None):
    import threading
    def decorator(fn):
        def decorated(*args, **kwargs):
            if lock is None:
                return fn(*args, **kwargs)
            if isinstance(lock, threading.Lock) or isinstance(lock, threading.RLock):
                with lock:
                    return fn(*args, **kwargs)
            else:
                raise Exception("Lock")
            
        return decorated
    return decorator