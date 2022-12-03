from tap_googleplay.streams.base import GooglePlayBase


class SubscriptionsStream(GooglePlayBase):
    STREAM_NAME = "subscriptions"
    PREFIX = "financial-stats"
    KEY_PROPERTIES = ["country"]
    FILETYPE = "csv"
