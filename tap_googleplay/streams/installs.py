from tap_googleplay.streams.base import GooglePlayBase


class InstallsStream(GooglePlayBase):
    STREAM_NAME = "installs"
    PREFIX = "stats"
    KEY_PROPERTIES = ["country"]
    FILETYPE = "csv"
