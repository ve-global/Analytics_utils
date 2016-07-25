from enum import Enum

import pyspark.sql.functions as F
from ve_utils import clock, to_pd

from reporting import ve_funcs as VeFuncs


class Feeds(Enum):
    standard = "Standard"
    segment = "Segment"
    pixel = "ConversionPixel"


class VeData(Enum):
    category = "CategoryView"
    page = "PageView"


class DataFeeds(object):
    """
    Reminder: !hdfs dfs -ls "wasb://derived@du2storvehdp1dn.blob.core.windows.net/PageView/"
    """
    url_ve = "wasb://derived@du2storvehdp1dn.blob.core.windows.net"
    url_lld = "wasb://appnexus@du2storvehdp1dn.blob.core.windows.net"

    standard_df_raw = '%s/Standard/raw_parquet' % url_lld
    segment_df_raw = '%s/Segment/raw_parquet' % url_lld
    pixel_df_raw = '%s/ConversionPixel/raw_parquet' % url_lld

    ve_categ_raw = "%s/CategoryView/data/v1/all/ve/" % url_ve
    ve_page_raw = "%s/PageView/data/v1/" % url_ve

    @staticmethod
    def add_columns(df):
        """
        Add useful columns to the dataframe (date, is_conv, is_imp, is_click, is_viewed)
        :param df:
        :return:
        """
        df = (df.withColumn('date', VeFuncs.get_date('D'))
              .withColumn('is_conv', F.when(df.event_type.isin(['pc_conv', 'pv_conv']), 1).otherwise(0))
              .withColumn('is_imp', F.when(df.event_type == 'imp', 1).otherwise(0))
              .withColumn('is_click', F.when(df.event_type == 'click', 1).otherwise(0))
              .withColumn('is_viewed', F.when(df.view_result_type == 1, 1).otherwise(0))
              )
        return df

    @staticmethod
    def get_feed_parquet(sql_context, data_type, from_date=None, to_date=None):
        """ Date format can either be a datetime or a string of format '%Y-%m-%d'
        Warning: for VeData, the time filter may need to be also based on datetime.
        """
        if data_type == Feeds.standard:
            data = sql_context.read.parquet(DataFeeds.standard_df_raw)
        elif data_type == Feeds.segment:
            data = sql_context.read.parquet(DataFeeds.segment_df_raw)
        elif data_type == Feeds.pixel:
            data = sql_context.read.parquet(DataFeeds.pixel_df_raw)
        elif data_type == VeData.category:
            data = sql_context.read.parquet(DataFeeds.ve_categ_raw)
        elif data_type == VeData.page:
            data = sql_context.read.parquet(DataFeeds.ve_page_raw)
        else:
            raise ValueError('Data type "%s" not implemented' % data_type)

        if from_date or to_date:
            data = data.filter(VeFuncs.filter_date(from_date, to_date))

        return data

    @staticmethod
    @clock()
    @to_pd()
    def count_lines(df, by='D'):
        if by == 'D':
            data = df.groupBy(VeFuncs.get_date('D').alias('day')).count()
        elif by == 'M':
            data = df.groupBy(VeFuncs.get_date('M').alias('month')).count()
        elif by == 'Y':
            data = df.groupBy(VeFuncs.get_date('Y').alias('year')).count()
        elif by == 'W':
            data = df.groupBy(VeFuncs.get_date('W').alias('weekofyear')).count()
        else:
            raise NotImplementedError("By '%s' not implemented" % str(by))
        return data

    @staticmethod
    @clock()
    @to_pd()
    def get_converted_user_ids(auctions):
        return auctions.filter(auctions.nb_convs > 0).select('othuser_id_64').distinct()

    @staticmethod
    @clock()
    def get_converted_users(df):
        if 'is_conv' not in df.columns:
            df = df.withColumn('is_conv',
                                F.when(df.event_type.isin(['pc_conv', 'pv_conv']), 1).otherwise(0))

        auctions = (df.groupby('othuser_id_64', 'auction_id_64')
                    .agg(F.sum('is_conv').alias('nb_convs')))

        converted_users = DataFeeds.get_converted_user_ids(auctions)['othuser_id_64'].tolist()
        users_df = df.filter(df.othuser_id_64.isin(converted_users))
        return users_df
