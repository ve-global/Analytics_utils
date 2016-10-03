import pyspark.sql.functions as F
from enum import Enum
import pandas as pd

from .feeds import VeCapture, AppNexus
from .ve_utils import clock, to_pd#, counter_udf, most_common_udf
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


@clock()
@to_pd()
def get_pixel_converted_users(standard_feed, converted_pixel_ids):
    users_that_converted = (standard_feed
                            .filter(standard_feed.pixel_id.isin(converted_pixel_ids))
                            .select('othuser_id_64').distinct())
    return users_that_converted


def get_converted_pixels(pixels_mapping, pixel_naming_rule):
    """
    Returns all the pixels that are conversion one given the
    `pixel_naming_rule`
    :param pixels_mapping: mapping between a pixel_id and pixel_name
    :param pixel_naming_rule: function return whether or not a pixel is
    a conversion pixel
    :return:
    """
    converted_pixels = [k for k, x in pixels_mapping.items()
                        if pixel_naming_rule(x)]
    return converted_pixels


def filter_pixel_converted_users(standard_feed, pixels_mapping, pixel_naming_rule, logger=None):
    """
    Find conversion pixels and returns all the events for users that have at least
    one event that converted.
    :param standard_feed:
    :param pixels_mapping:
    :param pixel_naming_rule:
    :param logger:
    :return:
    """
    logger = logger or logs.logger

    converted_pixels = get_converted_pixels(pixels_mapping, pixel_naming_rule)
    logger.info("Converted pixels: %d" % len(converted_pixels))

    users_that_converted = get_pixel_converted_users(standard_feed,
                                                     converted_pixels)['othuser_id_64'].tolist()

    logger.info("Users that converted: %d" % len(users_that_converted))

    converted_users = standard_feed.filter(standard_feed.othuser_id_64.isin(
        [int(x) for x in users_that_converted]))

    return converted_users


def map_pixels(feed, sql_context, pixels_mapping, pixel_naming_rule):
    """
    Add the `pixel_name` to the dataframe mapping on the `pixel_id`
    :param feed:
    :param sql_context:
    :param pixels_mapping:
    :param pixel_naming_rule:
    :return:
    """
    ids, names = zip(*pixels_mapping.items())
    df = pd.DataFrame({'pixel_id': ids, 'pixel_name': names})
    df['is_conv_pixel'] = df['pixel_name'].apply(pixel_naming_rule)
    df = sql_context.createDataFrame(df)

    feed = (feed.join(df, feed.pixel_id == df.pixel_id, how="left_outer")
                .drop(df.pixel_id))

    return feed


def map_insertion_orders(feed, sql_context, insertion_orders_mapping, insertion_orders_mapping_rule):
    """
    Add the `insertion_order_name` to the dataframe mapping on the `insertion_order_id`
    :param feed:
    :param sql_context:
    :param insertion_order_mapping:
    :param insertion_order_mapping_rule:
    :return:
    """
    ids, names = zip(*insertion_orders_mapping.items())
    df = pd.DataFrame({'insertion_order_id': ids, 'insertion_order_name': names})
    df['insertion_order_type'] = df['insertion_order_name'].apply(insertion_orders_mapping_rule)
    df = sql_context.createDataFrame(df)

    feed = (feed.join(df, feed.insertion_order_id == df.insertion_order_id, how="left_outer")
                .drop(df.insertion_order_id))

    return feed


def map_line_items(feed, sql_context, line_items_mapping):
    """
    Add the `line_item_name` to the dataframe mapping on the `campaign_group_id`
    :param feed:
    :param sql_context:
    :param line_items_mapping:
    :return:
    """
    ids, names = zip(*line_items_mapping.items())
    df = pd.DataFrame({'campaign_group_id': ids, 'line_item_name': names})
    df = sql_context.createDataFrame(df)

    feed = (feed.join(df, feed.campaign_group_id == df.campaign_group_id, how="left_outer")
                .drop(df.campaign_group_id))

    return feed


def map_advertisers(feed, sql_context, advertisers_mapping):
    """
    Add the `line_item_name` to the dataframe mapping on the `line_item_id`
    :param feed:
    :param sql_context:
    :param feed:
    :param advertisers_mapping:
    :return:
    """
    ids, names = zip(*advertisers_mapping.items())
    df = pd.DataFrame({'advertiser_id': ids, 'advertiser_name': names})
    df = sql_context.createDataFrame(df)

    feed = (feed.join(df, feed.advertiser_id == df.advertiser_id, how="left_outer")
            .drop(df.advertiser_id))

    return feed


def get_user_infos(feed):
    """
    Returns an aggregated version of users information with:
     - the age
     - the operating system
     - the gender
    :param feed:
    :return:
    """
    feed = feed.withColumn('age_clean',
                            F.when((feed.age < 10) | (feed.age > 100), None).otherwise(feed.age)
                            )

    users = feed.groupby('othuser_id_64').agg(
             F.collect_list('operating_system').alias('operating_systems'),
             F.collect_list('gender').alias("genders"),
             F.collect_list('age_clean').alias("ages")
        )
    # users = (users.withColumn('operating_systems', counter_udf('operating_systems'))
    #               .withColumn('genders', counter_udf('genders'))
    #               .withColumn('ages', counter_udf('ages'))
    #          )
    return users


def map_whitelist(sql_context, feed, whitelist_df):
    whitelist_df = whitelist_df[['Advertiser Id', 'Customer ID', 'Sector']]
    whitelist_df = whitelist_df.rename(columns={'Advertiser Id': 'advertiser_id',
                                                'Customer ID': 'customer_id',
                                                'Sector': 'advertiser_categ_whitelist'})
    whitelist_df = whitelist_df[whitelist_df.advertiser_id.notnull() & whitelist_df.customer_id.notnull()]
    whitelist = sql_context.createDataFrame(whitelist_df)

    feed = (feed.join(whitelist, feed.advertiser_id == whitelist.advertiser_id, how='left_outer')
                .drop(whitelist.advertiser_id)
            )
    return feed
