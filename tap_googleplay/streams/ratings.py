from tap_googleplay.streams.base import GooglePlayBase


class RatingsStream(GooglePlayBase):
    STREAM_NAME = "ratings"
    PREFIX = "stats"
    KEY_PROPERTIES = ["country"]
    FILETYPE = "csv"
