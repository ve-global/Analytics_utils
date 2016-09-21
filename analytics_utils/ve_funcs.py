import pyspark.sql.functions as F
from enum import Enum
import pandas as pd

from analytics_utils.feeds import VeCapture, AppNexus
from .ve_utils import clock, to_pd
from . import logs


class AuctionType(Enum):
    retargeting = 1
    prospecting = 2
    prospecting_retargeting = 3


class PixelType(Enum):
    conversion = 1
    landing = 0


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
    """
    Join two dataframe given a certain conditions on drop the key specified in `to_drop`

    :param left_value:
    :param right_value:
    :param conditions:
    :param to_drop:
    :param drop_from:
    :param kwargs:
    :return:
    """
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


@clock()
@to_pd()
def get_pixel_converted_users(standard_feed, converted_pixel_ids):
    users_that_converted = (standard_feed
                            .filter(standard_feed.pixel_id.isin(converted_pixel_ids))
                            .select('othuser_id_64').distinct())
    return users_that_converted


def filter_pixel_converted_users(standard_feed, pixels_mapping_df, is_conv_func):
    """
    Filter the standard feed only on users where the pixel name is associated to a conversion.
    :param standard_feed
    :param pixels_mapping_df:
    :param is_conv_func:
    :return:
    """
    pixels_mapping_df['is_conv_pixel'] = pixels_mapping_df['pixel_name'].apply(is_conv_func)
    converted_pixels = pixels_mapping_df[pixels_mapping_df['is_conv_pixel'] == 1]['pixel_id'].tolist()

    users_that_converted = get_pixel_converted_users(standard_feed,
                                                     converted_pixels)['othuser_id_64'].tolist()

    converted_users = standard_feed.filter(standard_feed.othuser_id_64.isin(users_that_converted))
    converted_users = converted_users.join(pixels_mapping_df,
                                           pixels_mapping_df['pixel_id'] == converted_users.pixel_id,
                                           how='left_outer').drop(pixels_mapping_df['pixel_id'])

    return converted_users, users_that_converted


# Rules

def pixel_type_FR(pixel_name):
    return PixelType.conversion.value if 'converted' in pixel_name.lower() else PixelType.landing.value


def campaign_type_FR(x):
    if not x:
        return None
    name = x.lower()
    if 'retargeting' in name and 'prospecting' in name:
        return AuctionType.prospecting_retargeting.name
    elif 'prospecting' in name:
        return AuctionType.prospecting.name
    elif 'retargeting' in name or 'AFF' in x or 'affiliate' in name or 'CPA' in x:
        return AuctionType.retargeting.name
    else:
        return AuctionType.prospecting.name


mapping_rules = {
    'FR': {
        'pixel_type': pixel_type_FR,
        'campaign_type': campaign_type_FR
    }
}


def get_converted_pixels(pixel_names, rule):
    converted_pixels = [k for k, x in pixel_names.items()
                        if rule(x)]
    return converted_pixels


def filter_pixel_converted_users(standard_feed, mappings, pixel_rules, logger=None):
    """
    Find conversion pixels and returns all the events for users that have at least
    one event that converted
    """
    logger = logger or logs.logger

    converted_pixels = get_converted_pixels(mappings['pixel_name'], pixel_rules)
    logger.info("Converted pixels: %d" % len(converted_pixels))

    users_that_converted = (standard_feed
                            .filter(standard_feed.pixel_id.isin(converted_pixels))
                            .select('othuser_id_64').distinct()).toPandas()['othuser_id_64'].tolist()

    logger.info("Users that converted: %d" % len(users_that_converted))

    converted_users = standard_feed.filter(standard_feed.othuser_id_64.isin(
        [int(x) for x in users_that_converted]))

    return converted_users


def add_mapping_data(feed, sql_context, mappings, rules):
    """
    Join a feed with the mapping:
     - advertiser_id -> name
     - pixel_id -> name
     - line_item_id -> name
     - insertion_order_name

    :param feed:
    :param sql_context:
    :param mappings:
    :param rule: rule to deter
    :return:
    """
    ids, names = zip(*mappings['pixel_name'].items())
    pixels_df = pd.DataFrame({'pixel_id': ids, 'pixel_name': names})
    pixels_df['is_conv_pixel'] = pixels_df['pixel_name'].apply(rules['pixel_type'])
    pixels_df['is_conv_pixel'] = pixels_df['pixel_name'].apply(rules['pixel_type'])

    pixels_df = sql_context.createDataFrame(pixels_df)

    feed = (feed.join(pixels_df, feed.pixel_id == pixels_df.pixel_id)
                .drop(pixels_df.pixel_id))

    return feed