import pyspark.sql.functions as F

from analytics_utils import logs
from analytics_utils import ve_funcs, ve_utils
from analytics_utils.ve_utils import clock, take, to_pd
from analytics_utils.feeds import AppNexus, VeCapture, Events


class DataFeeds(object):
    """
    Reminder: !hdfs dfs -ls "wasb://derived@du2storvehdp1dn.blob.core.windows.net/PageView/"
    """
    url_blob = "wasb://{container}@du2storvehdp1dn.blob.core.windows.net"

    _parquet_paths = {
        # Appnexus
        AppNexus.standard: "{}/{}".format(url_blob.format(container='appnexus'),
                                          'Standard/raw_parquet'),
        AppNexus.segment: "{}/{}".format(url_blob.format(container='appnexus'),
                                         'Segment/raw_parquet'),
        AppNexus.pixel: "{}/{}".format(url_blob.format(container='appnexus'),
                                       'ConversionPixel/raw_parquet'),
        # VeCapture
        VeCapture.category_1d: "{}/{}".format(url_blob.format(container='derived'),
                                              'CategoryView/data/v1/1d/ve'),
        VeCapture.category_7d: "{}/{}".format(url_blob.format(container='derived'),
                                              'CategoryView/data/v1/7d/ve'),
        VeCapture.category_30d: "{}/{}".format(url_blob.format(container='derived'),
                                               'CategoryView/data/v1/30d/ve'),
        VeCapture.page_view: "{}/{}".format(url_blob.format(container='derived'),
                                            'PageView/data/v1'),
        VeCapture.categorizer: "{}/{}".format(url_blob.format(container='derived'),
                                              'categorizer/raw_parquet/'),
        VeCapture.cookie_sync: "{}/{}".format(url_blob.format(container='vecapture'),
                                              'raw_parquet/CookieSyncMessage/v1'),
        VeCapture.new_data: "{}/{}".format(url_blob.format(container='vecapture'),
                                           'raw_parquet/NewDataMessage/v1'),
        VeCapture.update_abandon_state: "{}/{}".format(url_blob.format(container='vecapture'),
                                                       'raw_parquet/UpdateAbandonStateMessage/v1'),
        VeCapture.update_data: "{}/{}".format(url_blob.format(container='vecapture'),
                                              'raw_parquet/UpdateDataMessage/v1'),
    }

    _json_paths = {
        Events.browser: "{}/{}".format(url_blob.format(container=Events.browser.value),
                                       'raw/v1'), #i_year
        Events.email: "{}/{}".format(url_blob.format(container=Events.browser.value),
                                     'raw/v1'), #i_year
        Events.apps: "{}/{}".format(url_blob.format(container=Events.browser.value),
                                    'raw/v1'), #i_year
        AppNexus.advertiser_meta: "{}/{}".format(url_blob.format(container='appnexus'),
                                                 'Meta/raw/advertiser'), #year
        AppNexus.campaign_meta: "{}/{}".format(url_blob.format(container='appnexus'),
                                               'Meta/raw/campaign'),#year
        AppNexus.device_meta: "{}/{}".format(url_blob.format(container='appnexus'),
                                             'Meta/raw/device'),#year
        AppNexus.insertion_order_meta: "{}/{}".format(url_blob.format(container='appnexus'),
                                                      'Meta/raw/insertion_order'),#year
        AppNexus.line_item_meta: "{}/{}".format(url_blob.format(container='appnexus'),
                                                'Meta/raw/line_item'),#year
        AppNexus.pixel_meta: "{}/{}".format(url_blob.format(container='appnexus'),
                                            'Meta/raw/pixel'),#year
        AppNexus.publisher_meta: "{}/{}".format(url_blob.format(container='appnexus'),
                                                'Meta/raw/publisher')#year
    }

    @staticmethod
    def enrich_appnexus_columns(df, data_type=AppNexus.standard.value):
        """
        Add useful columns to the dataframe:
         - date
         - is_impression
         - is_conv, is_pc_conv, is_pv_conv
         - is_viewed
        :param df:
        :param data_type:
        :return:
        """
        df = (df.withColumn('date', ve_funcs.get_date('D', data_type))
              .withColumn('is_impression', ve_funcs.is_impression(df))
              .withColumn('is_conv', ve_funcs.is_converted(df))
              .withColumn('is_viewed', ve_funcs.is_viewed(df))
              .withColumn('is_pc_conv',
                          F.when(df.event_type == 'pc_conv', 1).otherwise(0)
                          )
              .withColumn('is_pv_conv',
                          F.when(df.event_type == 'pv_conv', 1).otherwise(0)
                          )
              .withColumn('age_clean',
                          F.when((df.age < 10) | (df.age > 100), None).otherwise(df.age)
                          ))
        return df

    @staticmethod
    def get_feed_parquet(sql_context, data_type, from_date=None, to_date=None,
                         countries=None):
        """ Date format can either be a datetime or a string of format '%Y-%m-%d'
        Warning: for VeData, the time filter may need to be also based on datetime.
        """
        try:
            data = sql_context.read.parquet(DataFeeds._parquet_paths[data_type])
        except KeyError:
            raise KeyError('Data type "%s" not implemented for parquet data' % data_type)

        if from_date or to_date:
            data = data.filter(ve_funcs.filter_date(from_date, to_date, data_type.value))

        if countries:
            countries = [x.upper() for x in countries]
            if isinstance(data_type, AppNexus):
                data = data.filter(data.geo_country.isin(countries))
            elif isinstance(data_type, VeCapture) and not isinstance(data_type, VeCapture.categorizer):
                data = data.filter(data.geo_info.getField('country_code').isin(countries))
            else:
                raise NotImplementedError('Countries filter not implemented for this type of data')

        return data

    @staticmethod
    def get_feed_json(sql_context, data_type, from_date=None,
                      to_date=None):
        try:
            data = sql_context.read.json(DataFeeds._json_paths[data_type])
        except KeyError:
            raise KeyError('Data type "%s" not implemented for json data' % data_type)

        if from_date or to_date:
            data = data.filter(ve_funcs.filter_date(from_date, to_date, data_type.value))

        return data

    @staticmethod
    def join_appnexus_feeds(sql_context, standard_feed=None, segment_feed=None, pixel_feed=None,
                            from_date=None, to_date=None, countries=None):
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

        pixel_feed = pixel_feed.select('datetime', 'user_id_64', pixel_feed['pixel_id'].alias('pixel_id_2'))

        # Mapping Users to Segment Feed
        conditions_1 = ((standard_feed.datetime == segment_feed.datetime) &
                        (standard_feed.othuser_id_64 == segment_feed.user_id_64))
        to_drop_1 = ['datetime', 'user_id_64', 'year', 'month', 'day']
        # Change order once updated
        users_1 = ve_funcs.join(standard_feed, segment_feed, conditions_1,
                                how='left_outer', to_drop=to_drop_1, drop_from='right')

        # Mapping Users to the Conversion Pixel feed
        conditions_2 = ((users_1.othuser_id_64 == pixel_feed.user_id_64) &
                        (users_1.datetime == pixel_feed.datetime))

        to_drop_2 = ['datetime', 'user_id_64']
        # Change order once updated
        users_2 = ve_funcs.join(users_1, pixel_feed, conditions_2,
                                how='left_outer', to_drop=to_drop_2, drop_from='right')

        return users_2

    @staticmethod
    def join_categorizer(feed, categorizer_feed=None, sql_context=None):
        customer_ids = feed.select('customer_id').distinct().collect()
        logs.logger.info("%d customer ids found" % len(customer_ids))
        customer_ids = [x.customer_id for x in customer_ids]

        if not sql_context and not categorizer_feed:
            raise ValueError('You must specify a feed or a sql_context')

        categorizer_feed = categorizer_feed or DataFeeds.get_feed_parquet(sql_context, VeCapture.categorizer)
        categorizer = (categorizer_feed
                       .filter(categorizer_feed.customerid.isin(customer_ids))
                       .groupby('customerid').agg(F.collect_list('finalcateg').alias('categs'))
                       )
        categorizer = categorizer.withColumn('categ', ve_utils.most_common_udf('categs'))

        feed = (feed.join(categorizer,
                          feed.customer_id == categorizer.customerid,
                          how='left_outer')
                .drop(categorizer.customerid))

        return feed

    @staticmethod
    def join_page_view(feed, page_view_feed=None, sql_context=None):
        page_view_feed = page_view_feed.withColumn('adnxs', page_view_feed.partner_cookies
                                                   .getField('adnxs')
                                                   .cast('int'))
        if not page_view_feed and not sql_context:
            raise ValueError('You must specify a feed or a sql_context')

        feed = feed.join(page_view_feed,
                         feed.othuser_id_64 == page_view_feed.adnxs)

        return feed

    @staticmethod
    def filter_converted_users(sql_context, standard_feed, country,
                               line_items_mapping=None,
                               pixels_mapping=None):
        # TODO: support multiple countries
        try:
            mapping = ve_funcs.mapping_rules[country]
        except KeyError:
            raise NotImplementedError('Mapping for country %s not implemented' % country)

        feed = ve_funcs.filter_pixel_converted_users(standard_feed, pixels_mapping, mapping['pixel_type'])

        if line_items_mapping:
            logs.logger.info('Mapping line items')
            feed = ve_funcs.map_insertion_orders(feed, sql_context,
                                                 line_items_mapping,
                                                 mapping['campaign_type'])
        if pixels_mapping:
            logs.logger.info('Mapping pixels')
            feed = ve_funcs.map_pixels(feed, sql_context,
                                       pixels_mapping,
                                       mapping['pixel_type'])

        return feed

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
            data = df.groupBy(ve_funcs.get_date('D', data_type=data_type).alias('day')).count()
        elif by == 'M':
            data = df.groupBy(ve_funcs.get_date('M', data_type=data_type).alias('month')).count()
        elif by == 'Y':
            data = df.groupBy(ve_funcs.get_date('Y', data_type=data_type).alias('year')).count()
        elif by == 'W':
            data = df.groupBy(ve_funcs.get_date('W', data_type=data_type).alias('weekofyear')).count()
        else:
            raise NotImplementedError("By '%s' not implemented" % str(by))
        return data
