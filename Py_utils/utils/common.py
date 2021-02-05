import os, sys, time, logging, datetime

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def is_numeric(val):
    if(val is not None
        and ((type(val) == str and val.isdigit())
             or type(val) == int 
            )
        ):
        return True
    return False


def retry(fun, cnt, retry_on_Exce = [], retry_always = False, *args, **kwargs):
    try:
        logging.info("Retry..... Count Down: "+str(cnt-1))
        if len(args) ==0:
            return fun()
        else:
            return fun(*args, **kwargs)
    except Exception as e: 
        if cnt > 1:
            if retry_always:
                retry(fun, cnt-1, retry_on_Exce, retry_always, *args,  **kwargs)
            else:
                for i in retry_on_Exce:
                    if type(e).__name__ == i.__name__:
                        retry(fun, cnt-1, retry_on_Exce, retry_always, *args,  **kwargs)
        else:
            logging.info("Operation has been failed! ")
                    
        return e


def remove(path):
    """
    Remove the file or directory
    """
    if os.path.isdir(path):
        try:
            os.rmdir(path)
        except OSError:
            logging.info("Unable to remove folder: {}".format(path))
    else:
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            logging.info("Unable to remove file: {}".format(path))



def cleanup(number_of_days, path):
    """
    Removes files from the passed in path that are older than or equal 
    to the number_of_days
    """
    time_in_secs = time.time() - (number_of_days * 24 * 60 * 60)
    for root, dirs, files in os.walk(path, topdown=False):
        for file_ in files:
            full_path = os.path.join(root, file_)
            stat = os.stat(full_path)
 
            if stat.st_mtime <= time_in_secs:
                remove(full_path)
 
        if not os.listdir(root):
            remove(root)



def configure_logging(log_path = '/tmp', loglevel = logging.INFO, log_file=None, started_on=int(time.time()), 
                        is_console_handler = True, is_file_handler = True):
    
    if is_console_handler or is_file_handler:
        log_handlers = []
        if is_file_handler and log_file is None:
            log_path = os.path.join(log_path, 'logs')
            os.makedirs(log_path, exist_ok=True)
            log_file="{0}/{1}_run.log".format(log_path, str(started_on))
            file_handler = logging.FileHandler(log_file)
            log_handlers.append(file_handler)
        
        if is_console_handler:
            stdout_handler = logging.StreamHandler(sys.stdout)
            log_handlers.append(stdout_handler)

        # Setting up logging
        numeric_level = getattr(logging, loglevel.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % loglevel)

        logging.basicConfig(level=numeric_level,
                            format='%(asctime)s %(levelname)s: %(message)s',
                            handlers=[stdout_handler, file_handler])
        logging.info('Logger configured!!')



def create_week_boundaries(start, end):
    """Generates a list of tuples containing the start and end of weeks (Sun-Sat)"""
    start_dt = datetime.datetime.strptime(start, "%Y-%m-%d")
    start_iso = start_dt.isocalendar()
    end_dt = datetime.datetime.strptime(end, "%Y-%m-%d")
    end_iso = end_dt.isocalendar()
    if start_dt >= end_dt:
        raise ValueError("Start date must be earlier than end date")
    max_dt = start_dt
    weeks = []
    counter = 0
    day = datetime.timedelta(days=1)
    while max_dt < end_dt:
        first = start_dt + counter * 7 * day
        last = start_dt + ((counter + 1) * 7 - 1) * day
        weeks.append((first, last if last <= end_dt else end_dt))
        counter += 1
        max_dt = last
    return weeks

