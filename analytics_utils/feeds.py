from enum import Enum


class AppNexus(Enum):
    standard = "Standard"
    segment = "Segment"
    pixel = "ConversionPixel"


class VeCapture(Enum):
    category_1d = "CategoryView_1d"
    category_7d = "CategoryView_7d"
    category_30d = "CategoryView_30d"
    page = "PageView"
    categorizer = "Categorizer"
