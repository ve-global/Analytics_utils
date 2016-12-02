import pyspark.sql.functions as F

from .feeds import AppNexus, Events


def get_date(by='D', data_type=AppNexus.standard.value):
    """
    Return the date to be filtered / grouped on
    :param by:
    :param data_type:
    :return:
    """
    not_i_year_data = [x.value for x in AppNexus] + [Events.transaction.value]

    if data_type not in set(not_i_year_data):
        year, month, day = "i_year", "i_month", "i_day"
    else:
        year, month, day = "year", "month", "day"

    if by == 'D':
        f = F.to_date(F.concat_ws('-', year, month, day))
    elif by == 'M':
        f = F.to_date(F.concat_ws('-', year, month))
    elif by == 'Y':
        f = F.to_date(year)
    elif by == 'W':
        f = F.weekofyear(get_date('D', data_type=data_type))
    else:
        raise NotImplementedError("By '%s' not implemented" % by)
    return f


def filter_date(from_date=None, to_date=None, data_type=AppNexus.standard.value):
    """
    Return a function to filter on date for a given DataFrame.
    Date format can either be a datetime or a string of format '%Y-%m-%d'
    :param from_date:
    :param to_date:
    :param data_type:
    :return:
    """
    if from_date and to_date:
        f = get_date(data_type=data_type).between(from_date, to_date)
    elif from_date:
        f = get_date(data_type=data_type) > from_date
    elif to_date:
        f = get_date(data_type=data_type) < to_date
    else:
        raise ValueError('from_date or to_date must be specified')

    return f

