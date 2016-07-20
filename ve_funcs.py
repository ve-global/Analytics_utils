import pyspark.sql.functions as F

revenue = (F.sum('post_click_revenue') + F.sum('post_view_revenue'))
cost = (F.sum('media_cost_dollars_cpm'))
cost_data = (F.sum('data_costs_cpm'))
begin_auction, end_auction = F.unix_timestamp(F.min('datetime')), F.unix_timestamp(F.max('datetime'))
duration = (end_auction - begin_auction)


def get_date(by='D'):
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


def filter_date(from_date, to_date):
    if from_date and to_date:
        f = get_date().between(from_date, to_date)
    elif from_date:
        f = get_date() > from_date
    elif to_date:
        f = get_date() < to_date
    else:
        f = True
    return f