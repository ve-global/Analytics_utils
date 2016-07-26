from enum import Enum


class AppNexus(Enum):
    standard = "Standard"
    segment = "Segment"
    pixel = "ConversionPixel"


class VeCapture(Enum):
    category = "CategoryView"
    page = "PageView"
