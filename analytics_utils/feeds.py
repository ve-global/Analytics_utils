from enum import Enum



class AppNexus(Enum):
    standard = "Standard"
    segment = "Segment"
    pixel = "ConversionPixel"
    advertiser_meta = "advertiser"
    campaign_meta = "campaign"
    device_meta = "device"
    insertion_order_meta = "insertion_order"
    line_item_meta = "line_item"
    pixel_meta = "pixel"
    publisher_meta = "publisher"


class VeCapture(Enum):
    category_1d = "CategoryView_1d"
    category_7d = "CategoryView_7d"
    category_30d = "CategoryView_30d"
    page_view = "PageView"
    categorizer = "Categorizer"
    cookie_sync = 'CookieSyncMessage'
    new_data = 'NewDataMessage'
    update_abandon_state = 'UpdateAbandonStateMessage'
    update_data = 'UpdateDataMessage'


class Events(Enum):
    email = 'emailevent'
    browser = 'browserevent'
    apps = 'appsevent'
