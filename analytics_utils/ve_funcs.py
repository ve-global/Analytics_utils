import pyspark.sql.functions as F

from analytics_utils.feeds import VeCapture, AppNexus


revenue = (F.sum('post_click_revenue') + F.sum('post_view_revenue'))
cpm = (F.sum('media_cost_dollars_cpm'))
cpm_with_fees = F.sum('cpm_including_fees')
cpm_data = (F.sum('data_costs_cpm'))
begin, end = F.unix_timestamp(F.min('datetime')), F.unix_timestamp(F.max('datetime'))
duration = (end - begin)


def is_converted(df):
    """
    :param df:
    :return:
    """
    return F.when(df.event_type.isin(['pc_conv', 'pv_conv']), 1).otherwise(0)


def is_impression(df):
    """
    :param df:
    :return:
    """
    return F.when(df.event_type == "imp", 1).otherwise(0)


def is_viewed(df):
    """
    :param df:
    :return:
    """
    return F.when(df.view_result_type == 1, 1).otherwise(0)


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


def join(left_value, right_value, conditions, to_drop=None, drop_from=None, **kwargs):

    joined = left_value.join(right_value, conditions, **kwargs)

    if not to_drop:
        return joined

    to_drop_from = right_value
    if drop_from == 'right':
        pass
    elif to_drop_from == 'left':
        to_drop_from = left_value
    else:
        raise ValueError('drop_from must be either "right" or "left"')

    for d in to_drop:
        joined = joined.drop(to_drop_from[d])

    return joined


