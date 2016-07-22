import pandas as pd
import pyspark.sql.functions as F

from data_feeds import DataFeeds, Feeds
from ve_utils import clock, to_pd
import ve_funcs as VeFuncs


@clock()
@to_pd()
def agg_standard_feed(standard_feed):
    standard_df = DataFeeds.add_columns(standard_feed)

    standard_df = (standard_df.groupby('date', 'advertiser_id').agg(
        F.sum('is_conv').alias('nb_convs'),
        F.sum('is_imp').alias('nb_imps'),
        F.sum('is_click').alias('nb_clicks'),
        F.sum('is_viewed').alias('nb_viewed'),
        VeFuncs.revenue.alias('revenue'), VeFuncs.cpm.alias('cpm')
    ))
    return standard_df


@clock()
def compare_standard_feed(report_path, sql_context, from_date=None, to_date=None):
    """
    Compare the data from the AppNexus API to the LLD feeds.
    """

    # 1. Getting data downloaded from the API
    df = pd.read_csv(report_path, sep=',', parse_dates=['hour'], index_col='hour')
    from_date = from_date or df.index.min()
    to_date = to_date or df.index.max()
    df = df[str(from_date): str(to_date)]

    from_date, to_date = df.index.min(), df.index.max()

    # 2. Getting the corresponding range of time in LLD
    standard_df = DataFeeds.get_feed_parquet(sql_context, Feeds.standard, from_date, to_date)
    standard_df = agg_standard_feed(standard_df)

    # 3. Formatting LLD Data
    standard_df = standard_df[standard_df.advertiser_id.notnull()]
    standard_df.advertiser_id = standard_df.advertiser_id.astype('int64')
    standard_df.date = pd.to_datetime(standard_df.date)

    # 4.Aggregating the results
    df_agg = df.groupby([pd.TimeGrouper('D'), 'advertiser_id']).agg({
        'imps': {'nb_imps': 'sum'},
        'cpm': {'cpm': 'sum'},
        'revenue': {'revenue': 'sum'},
        'imps_viewed': {'nb_viewed': 'sum'},
        'clicks': {'nb_clicks': 'sum'},
        'total_convs': {'nb_convs': 'sum'}
    })
    df_agg.columns = df_agg.columns.droplevel(0)
    df_agg = df_agg.reset_index().rename(columns={'hour': 'date'})
    df_agg.head()

    full_df = pd.merge(df_agg, standard_df, on=['date', 'advertiser_id'],
                       suffixes=('_api', ''), how='outer').fillna(0)

    to_compare = ['nb_convs', 'nb_imps', 'nb_clicks', 'nb_viewed', 'revenue', 'cpm']
    agg_df_bins = []

    for feature in to_compare:
        full_df['%s_diff' % feature] = full_df[feature] - full_df['%s_api' % feature]
        full_df['%s_diff_pc' % feature] = full_df['%s_diff' % feature] * 100. / (full_df[feature] + 1)
        full_df['%s_diff_pc_bins' % feature] = pd.cut(full_df['%s_diff_pc' % feature], bins=range(-100, 110, 10))
        agg_df_bins.append(full_df['%s_diff_pc_bins' % feature].value_counts().sort_index())

    return pd.concat(agg_df_bins, axis=1), full_df
