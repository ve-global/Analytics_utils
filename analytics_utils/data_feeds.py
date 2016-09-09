import pyspark.sql.functions as F

from analytics_utils import ve_funcs as VeFuncs
from analytics_utils.ve_utils import clock, to_pd
from analytics_utils.feeds import AppNexus, VeCapture


class DataFeeds(object):
    """
    Reminder: !hdfs dfs -ls "wasb://derived@du2storvehdp1dn.blob.core.windows.net/PageView/"
    """
    url_ve = "wasb://derived@du2storvehdp1dn.blob.core.windows.net"
    url_lld = "wasb://appnexus@du2storvehdp1dn.blob.core.windows.net"

    standard_df_raw = '%s/Standard/raw_parquet' % url_lld
    segment_df_raw = '%s/Segment/raw_parquet' % url_lld
    pixel_df_raw = '%s/ConversionPixel/raw_parquet' % url_lld

    ve_categ_1d = "%s/CategoryView/data/v1/1d/ve/" % url_ve
    ve_categ_7d = "%s/CategoryView/data/v1/7d/ve/" % url_ve
    ve_categ_30d = "%s/CategoryView/data/v1/30d/ve/" % url_ve
    ve_page_raw = "%s/PageView/data/v1/" % url_ve
    ve_categorizer = "%s/categorizer/raw_parquet/" % url_ve

    @staticmethod
    def add_columns(df, data_type=AppNexus.standard.value):
        """
        Add useful columns to the dataframe (date, is_conv, is_viewed)
        :param df:
        :param data_type:
        :return:
        """
        df = (df.withColumn('date', VeFuncs.get_date('D', data_type))
              .withColumn('is_impression', VeFuncs.is_impression(df))
              .withColumn('is_conv', VeFuncs.is_converted(df))
              .withColumn('is_viewed', VeFuncs.is_viewed(df))
              )
        return df

    @staticmethod
    def get_feed_parquet(sql_context, data_type, from_date=None, to_date=None,
                         countries=None):
        """ Date format can either be a datetime or a string of format '%Y-%m-%d'
        Warning: for VeData, the time filter may need to be also based on datetime.
        """
        if data_type == AppNexus.standard:
            data = sql_context.read.parquet(DataFeeds.standard_df_raw)
        elif data_type == AppNexus.segment:
            data = sql_context.read.parquet(DataFeeds.segment_df_raw)
        elif data_type == AppNexus.pixel:
            data = sql_context.read.parquet(DataFeeds.pixel_df_raw)
        elif data_type == VeCapture.category_1d:
            data = sql_context.read.parquet(DataFeeds.ve_categ_1d)
        elif data_type == VeCapture.category_7d:
            data = sql_context.read.parquet(DataFeeds.ve_categ_7d)
        elif data_type == VeCapture.category_30d:
            data = sql_context.read.parquet(DataFeeds.ve_categ_30d)
        elif data_type == VeCapture.page:
            data = sql_context.read.parquet(DataFeeds.ve_page_raw)
        elif data_type == VeCapture.categorizer:
            data = sql_context.read.parquet(DataFeeds.ve_categorizer)
        else:
            raise ValueError('Data type "%s" not implemented' % data_type)

        if from_date or to_date:
            data = data.filter(VeFuncs.filter_date(from_date, to_date, data_type.value))

        if countries:
            data = data.filter(data.geo_country.isin([x.upper() for x in countries]))

        return data

    @staticmethod
    def get_appnexus_feeds(sql_context, standard_feed=None, segment_feed=None, pixel_feed=None,
                           from_date=None, to_date=None, countries=None, only_converted=False):
        """
        Returns an enriched version containing all the data from the AppNexus feed

        :param sql_context:
        :param standard_feed:
        :param segment_feed:
        :param pixel_feed:
        :param from_date:
        :param to_date:
        :param countries:
        :param only_converted:
        :return:
        """
        standard_feed = standard_feed or DataFeeds.get_feed_parquet(sql_context, AppNexus.standard,
                                                                    from_date=from_date, to_date=to_date,
                                                                    countries=countries)
        segment_feed = segment_feed or DataFeeds.get_feed_parquet(sql_context, AppNexus.segment,
                                                                  from_date=from_date, to_date=to_date)
        pixel_feed = pixel_feed or DataFeeds.get_feed_parquet(sql_context, AppNexus.pixel,
                                                              from_date=from_date, to_date=to_date)

        if only_converted:
            standard_feed, converted_users_ids = DataFeeds.get_converted_users(standard_feed)
            segment_feed = segment_feed.filter(segment_feed.user_id_64.isin(converted_users_ids))
            pixel_feed = pixel_feed.filter(pixel_feed.user_id_64.isin(converted_users_ids))

        pixel_feed = pixel_feed.select('datetime', 'user_id_64', pixel_feed['pixel_id'].alias('pixel_id_2'))

        # Mapping Users to Segment Feed
        conditions_1 = ((standard_feed.datetime == segment_feed.datetime) &
                        (standard_feed.othuser_id_64 == segment_feed.user_id_64))
        to_drop_1 = ['datetime', 'user_id_64', 'year', 'month', 'day']
        # Change order once updated
        users_1 = VeFuncs.join(standard_feed, segment_feed, conditions_1,
                               how='left_outer', to_drop=to_drop_1, drop_from='right')

        # Mapping Users to the Conversion Pixel feed
        conditions_2 = ((users_1.othuser_id_64 == pixel_feed.user_id_64) &
                        (users_1.datetime == pixel_feed.datetime))

        to_drop_2 = ['datetime', 'user_id_64']
        # Change order once updated
        users_2 = VeFuncs.join(users_1, pixel_feed, conditions_2,
                               how='left_outer', to_drop=to_drop_2, drop_from='right')

        return users_2

    @staticmethod
    @clock()
    @to_pd()
    def count_lines(df, by='D', data_type=AppNexus.standard.value):
        """
        Count the number of lines by: Day, Week, Month, Year
        :param df:
        :param by: data to group on (D, M, Y, W)
        :param data_type:
        :return: safe pandas dataframe
        """
        if by == 'D':
            data = df.groupBy(VeFuncs.get_date('D', data_type=data_type).alias('day')).count()
        elif by == 'M':
            data = df.groupBy(VeFuncs.get_date('M', data_type=data_type).alias('month')).count()
        elif by == 'Y':
            data = df.groupBy(VeFuncs.get_date('Y', data_type=data_type).alias('year')).count()
        elif by == 'W':
            data = df.groupBy(VeFuncs.get_date('W', data_type=data_type).alias('weekofyear')).count()
        else:
            raise NotImplementedError("By '%s' not implemented" % str(by))
        return data

    @staticmethod
    @clock()
    @to_pd(limit=10000000)
    def get_converted_user_ids(df):
        """
        Get all the converted users ids.
        :param auctions: the auctions to filter on
        :return: safe pandas dataframe
        """
        return df.filter(df.nb_convs > 0).select('othuser_id_64').distinct()

    @staticmethod
    @clock()
    def get_converted_users(df):
        """
        Returns a Dataframe containing all the users that converted
        :param df: dataframe to filter
        :return:  dataframe of converted users
        """
        if 'is_conv' not in df.columns:
            df = df.withColumn('is_conv', VeFuncs.is_converted(df))

        all_users = (df.groupby('othuser_id_64')
                       .agg(F.sum('is_conv').alias('nb_convs')))

        converted_users_ids = DataFeeds.get_converted_user_ids(all_users)['othuser_id_64'].tolist()
        users_df = df.filter(df.othuser_id_64.isin(converted_users_ids))
        return users_df, converted_users_ids

    @staticmethod
    @clock()
    def get_converted_auctions(df):
        if 'is_conv' not in df.columns:
            df = df.withColumn('is_conv', VeFuncs.is_converted(df))
        auctions = df.groupby('auction_id_64').agg(F.sum('is_conv').alias('nb_convs'))
        return auctions.filter(auctions.nb_convs > 0)