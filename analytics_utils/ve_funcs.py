import pyspark.sql.functions as F

from analytics_utils.feeds import VeCapture, AppNexus


revenue = (F.sum('post_click_revenue') + F.sum('post_view_revenue'))
cpm = (F.sum('media_cost_dollars_cpm'))
cpm_with_fees = F.sum('cpm_including_fees')
cpm_data = (F.sum('data_costs_cpm'))
begin_auction, end_auction = F.unix_timestamp(F.min('datetime')), F.unix_timestamp(F.max('datetime'))
duration = (end_auction - begin_auction)


def get_date(by='D', data_type=AppNexus.standard.value):
    """
    Return the date to be filtered / grouped on
    :param by:
    :param data_type:
    :return:
    """
    if data_type in (x.value for x in VeCapture):
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