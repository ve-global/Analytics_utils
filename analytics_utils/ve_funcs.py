import pyspark.sql.functions as F

revenue = (F.sum('post_click_revenue') + F.sum('post_view_revenue'))
cpm = (F.sum('media_cost_dollars_cpm'))
cpm_with_fees = F.sum('cpm_including_fees')
cpm_data = (F.sum('data_costs_cpm'))
begin_auction, end_auction = F.unix_timestamp(F.min('datetime')), F.unix_timestamp(F.max('datetime'))
duration = (end_auction - begin_auction)


def get_date(by='D'):
    """
    Return the date to be filtered / grouped on
    :param by:
    :return:
    """
    if by == 'D':
        f = F.to_date(F.concat_ws('-', 'year', 'month', 'day'))
    elif by == 'M':
        f = F.to_date(F.concat_ws('-', 'year', 'month'))
    elif by == 'Y':
        f = F.to_date('year')
    elif by == 'W':
        f = F.weekofyear(get_date('D'))
    else:
        raise NotImplementedError("By '%s' not implemented" % by)
    return f


def filter_date(from_date=None, to_date=None):
    """
    Return a function to filter on date for a given DataFrame.
    Date format can either be a datetime or a string of format '%Y-%m-%d'
    :param from_date:
    :param to_date:
    :return:
    """
    if from_date and to_date:
        f = get_date().between(from_date, to_date)
    elif from_date:
        f = get_date() > from_date
    elif to_date:
        f = get_date() < to_date
    else:
        raise ValueError('from_date or to_date must be specified')

    return f