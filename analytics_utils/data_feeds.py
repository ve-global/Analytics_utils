from analytics_utils import ve_funcs
from analytics_utils.feeds import AppNexus, VeCapture, Events, External, Cookie


class DataFeeds(object):
    """
    Reminder: !hdfs dfs -ls "wasb://derived@du2storvehdp1dn.blob.core.windows.net/PageView/"
    """
    url_blob = "wasb://{container}@du2storvehdp1dn.blob.core.windows.net"
    url_blob_external = "wasb://{container}@du2storhdp1bs01.blob.core.windows.net"

    parquet_paths = {
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
        Events.transaction: "{}/parquet".format(url_blob.format(container=Events.transaction.value)),  # year
        # Events.browser: "{}/raw_parquet/v1".format(url_blob.format(container=Events.browser.value)),  # i_year
        # Events.email: "{}/raw_parquet/v1".format(url_blob.format(container=Events.email.value)),  # i_year
        # Events.apps: "{}/raw_parquet/v1".format(url_blob.format(container=Events.apps.value)),  # i_year
        Cookie.set_cookie: "{}/raw_parquet/SetCookieMessage/v1".format(
            url_blob.format(container='vecapture'))  # i_year,
    }
    json_paths = {
        Events.browser: "{}/{}".format(url_blob.format(container=Events.browser.value),
                                       'raw/v1'), #i_year
        Events.email: "{}/{}".format(url_blob.format(container=Events.email.value),
                                     'raw/v1'), #i_year
        Events.apps: "{}/{}".format(url_blob.format(container=Events.apps.value),
                                    'raw/v1'), #i_year
        Events.transaction: "{}/transaction".format(url_blob.format(container=Events.transaction.value)), # year
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
                                                'Meta/raw/publisher'), #year
        Cookie.sync_cookie:  "{}/{}".format(url_blob.format(container='vecapture'),
                                                'raw/CookieSyncMessage/v1'), #i_year
        Cookie.set_cookie: "{}/{}".format(url_blob.format(container='vecapture'),
                                          'raw/SetCookieMessage/v1')  # i_year,

    }
    avro_paths = {
        External.storm_session: "{}/{}".format(url_blob_external.format(container='psvc-sessions'),
                                               'PSVC-Sessions/raw/v1')
    }

    @staticmethod
    def get_feed_parquet(sql_context, data_type, from_date=None, to_date=None,
                         countries=None):
        """ Date format can either be a datetime or a string of format '%Y-%m-%d'
        Warning: for VeData, the time filter may need to be also based on datetime.
        """
        try:
            data = sql_context.read.parquet(DataFeeds.parquet_paths[data_type])
        except KeyError:
            raise KeyError('Data type "%s" not implemented for parquet data' % data_type)

        if from_date or to_date:
            data = data.filter(ve_funcs.filter_date(from_date, to_date, data_type.value))

        if countries:
            countries = [x.upper() for x in countries]
            if isinstance(data_type, AppNexus):
                data = data.filter(data.geo_country.isin(countries))
            elif isinstance(data_type, VeCapture) and data_type != VeCapture.categorizer:
                data = data.filter(data.geo_info.getField('country_code').isin(countries))
            else:
                raise NotImplementedError('Countries filter not implemented for this type of data')

        return data

    @staticmethod
    def get_feed_json(sql_context, data_type, from_date=None,
                      to_date=None):
        try:
            data = sql_context.read.json(DataFeeds.json_paths[data_type])
        except KeyError:
            raise KeyError('Data type "%s" not implemented for json data' % data_type)

        if from_date or to_date:
            data = data.filter(ve_funcs.filter_date(from_date, to_date, data_type.value))

        return data

    @staticmethod
    def get_feed_avro(sql_context, data_type, from_date=None, to_date=None):
        try:
            data = (sql_context.read
                               .format("com.databricks.spark.avro")
                               .load(DataFeeds.avro_paths[data_type]))
        except KeyError:
            raise KeyError('Data type "%s" not implemented for json data' % data_type)

        if from_date or to_date:
            data = data.filter(ve_funcs.filter_date(from_date, to_date, data_type.value))

        return data

    @staticmethod
    def get_meta(sql_context, data_type, year=2017, month=1, day=10):
        path = '{}/{}/{}/year={}/month={:02}/day={:02}'.format(
            DataFeeds.url_blob.format(container='metadata'), '{}',
            data_type.value, year, month, day)
        schema = sql_context.read.csv(path.format('defs'), sep='\t', header='false')
        data = sql_context.read.csv(path.format('data'), sep='\t', header='false')

        for old_col, new_col in schema.take(1)[0].asDict().items():
            data = data.withColumnRenamed(old_col, new_col)
        return data
