from tap_googleplay.streams.base import GooglePlayBase


class CrashesStream(GooglePlayBase):
    STREAM_NAME = "crashes"
    PREFIX = "stats"
    DIMENSION = "device"
    FILETYPE = "csv"
