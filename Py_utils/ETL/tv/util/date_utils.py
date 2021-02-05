
'''
    
    ISO week start from Monday  
    ISO starts oridinal 1 instead of 0
    
    These equations are based on ISO 8601 
    date and time standard issued by the International Organization 
    for Standardization (ISO) since 1988 (last revised in 2004)

'''

from datetime import datetime
from datetime import timedelta
from datetime import date

'''
        convert_date_fmt('2019-06-09')
        Output will be =  20190609
'''

def convert_date_fmt(date: str, current_fmt = '%Y-%m-%d', output_fmt = '%Y%m%d'):
    dt = datetime.strptime(date, current_fmt)
    formatted_date = datetime.strftime(dt, output_fmt)
    return  formatted_date

def parse_date_str(date, fmt='%Y-%m-%d'):
    dt = None
    try:
        dt = datetime.strptime(date, fmt)
    except ValueError as e:
        assert dt is not None, "Date format should be {}".format(fmt)
    return dt

'''
        Return date str if date is given format otherwise throw assert error
        valid_date_fmt('2019-06-09')
        Output will be =  2019-06-09

        valid_date_fmt('2019.06.09', fmt='%Y-%m-%d')
        Output will be =  AssertionError: Date format should be %Y-%m-%d
'''

def valid_date_fmt(date, fmt='%Y-%m-%d'):
    parse_date_str(date, fmt)
    return date

'''
        date_diff('2019-06-09', '2019-06-11')
        output = 2
        date_diff('09-06-2019', '11-06-2019', fmt = '%d-%m-%Y')
        output = 2

'''
def date_diff(date1: str, date2: str, fmt = '%Y-%m-%d'):
    dt1 = parse_date_str(date1, fmt) 
    dt2 = parse_date_str(date2, fmt)
    delta = dt1 - dt2
    
    return delta.days

'''
        date_operations('2019-06-09', days = 6)
        output = '2019-06-03'
        date_operations('2019-06-09', operator='+', days = 6)
        output = '2019-06-15'

'''
def date_operations(date: str, fmt = '%Y-%m-%d', operator= '-', **kwargs):
    dt = datetime.strptime(date, fmt)
    if len(kwargs) == 1:
        if operator == '+':
            dt = dt + timedelta(**kwargs)
        if operator == '-':
            dt = dt - timedelta(**kwargs)
    return datetime.strftime(dt, fmt)


def get_time_tuple(date: str, fmt = '%Y.%m.%d'):
    dt = datetime.strptime(date, fmt)
    tt = dt.timetuple()
    return tt

def get_yday_ordinal(date: str, fmt = '%Y.%m.%d'):
    tt = get_time_tuple(date, fmt)
    return tt.tm_yday

def get_week_day(date: str, fmt = '%Y.%m.%d'):
    tt = get_time_tuple(date, fmt)
    return tt.tm_wday

'''
    num_of_weeks function implemented based on given mathmatical equation
    weeks(year) = 52 + {1 if p(year) == 4 or p(year-1) ==3,
                       0 otherwise}

    p(year) = (year + year/4 - year/100 + year/400) % 7

'''
def num_of_weeks(year: int):
    if int((1.2425 * year)%7) ==4 or int((1.2425 * (year-1))%7) ==3:
        return 53
    else:
        return 52


'''
    get_current_week_with_year function implemented based on given mathmatical equation
    week(date) = (ordinal(date) - weekday(date) + 10)/7

    week = {lastweek(year - 1), if week < 1
            1, if week > lastweek(year)
            week, otherwise}

'''
def get_current_week_with_year(date: str, fmt = '%Y.%m.%d'):
    tt = get_time_tuple(date, fmt)
    week = int((tt.tm_yday - (tt.tm_wday+1) + 10)/7)
    if week < 1:
        return {'week' : num_of_weeks(tt.tm_year - 1), 'year' : tt.tm_year - 1}
    elif week > num_of_weeks(tt.tm_year) :
        return {'week' : 1, 'year' : tt.tm_year + 1}
    else :
        return {'week' : week, 'year' : tt.tm_year}



def get_current_week(date: str, fmt = '%Y.%m.%d'):
    return get_current_week_with_year(date, fmt)['week']


def get_week_by_date(date_obj: date):
    date_str = date_obj.strftime('%Y.%m.%d')
    return get_current_week(date_str)


def get_date_range_for_week(week: int, year: int, fmt = '%Y-%m-%d'):
    if week < 1 or num_of_weeks(year) < week:
        return {"Error" : "Iso Week Num: {} doesn't exist in year: {}!".format(week, year)}
    oridinal = week*7
    dt = datetime.fromordinal(oridinal)
    
    tt = dt.timetuple()
    
    mm = tt.tm_mon
    dd = tt.tm_mday
    resultant_date = None
    date_obj = date(year, mm, dd)
    cnt = 1
    
    if not get_week_by_date(date_obj) == week:
        while cnt <= 3:
            date1 = date_obj - timedelta(days=3*cnt)
            if get_week_by_date(date1) == week:
                resultant_date = date1
                break
            date2 = date_obj + timedelta(days=3*cnt)
            if get_week_by_date(date2) == week:
                resultant_date = date2
                break
    else : resultant_date = date_obj
    
    if resultant_date is None:
        return None
    
    week_day_idx = resultant_date.weekday()
    week_start_date = resultant_date - timedelta(days=week_day_idx)
    week_end_date = resultant_date + timedelta(days=6 - week_day_idx)
    
    week_start_date_str = week_start_date.strftime(fmt)
    week_end_date_str = week_end_date.strftime(fmt)
    
    return {"week_start_date" : week_start_date,
            "week_end_date" : week_end_date,
           "week_start_date_str" : week_start_date_str,
           "week_end_date_str" : week_end_date_str}
    

    
    
def get_date_range_for_kantar_week(kantar_week: int, year: int, fmt = '%Y-%m-%d'):
    return get_date_range_for_week(kantar_week + 1, year, fmt)

def get_kantar_week_with_year(date: str, fmt = '%Y.%m.%d'):
    res_dict = get_current_week_with_year(date, fmt)
    return {'week' : res_dict['week'] - 1, 'year' : res_dict['year']}

def get_kantar_week(date: str, fmt = '%Y.%m.%d'):
    return get_kantar_week_with_year(date, fmt)['week']