from enum import Enum


class AppNexus(Enum):
    standard = 'Standard'
    segment = 'Segment'
    pixel = 'ConversionPixel'
    advertiser_meta = 'advertiser'
    campaign_meta = 'campaign'
    device_meta = 'device'
    insertion_order_meta = 'insertion_order'
    line_item_meta = 'line_item'
    pixel_meta = 'pixel'
    publisher_meta = 'publisher'


class VeCapture(Enum):
    category_1d = 'CategoryView_1d'
    category_7d = 'CategoryView_7d'
    category_30d = 'CategoryView_30d'
    page_view = 'PageView'
    categorizer = 'Categorizer'
    cookie_sync = 'CookieSyncMessage'
    new_data = 'NewDataMessage'
    update_abandon_state = 'UpdateAbandonStateMessage'
    update_data = 'UpdateDataMessage'
    journey_stats = 'journey_stats'
    funnel = 'funnel'


class Events(Enum):
    email = 'emailevent'
    browser = 'browserevent'
    apps = 'appsevent'
    transaction = 'transaction'


class External(Enum):
    storm_session = 'storm_session'


class Cookie(Enum):
    set_cookie = 'set_cookie'
    sync_cookie = 'sync_cookie'


class Meta(Enum):
    customer = 'customer'
    category = 'category'
    form = 'form'
    formmapping = 'formmapping'
    formappingid = 'formmappingproactive'
    formmappingtype = 'formmappingtype'
    formurl = 'formurl'
    journey = 'journey'
    journey_extended = 'journey_extended'
    license = 'license'
    salesmanager = 'salesmanager'


class Attribution(Enum):
    converted_sessions = 'converted_sessions'


class CrossDevice(Enum):
    email_blacklist = 'email_blacklist'
    email_cookie = 'email_cookie'