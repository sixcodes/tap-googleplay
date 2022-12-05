"""
Stream base class
"""
import inspect
import os
from datetime import timedelta
from typing import List

import pytz
import singer

from tap_googleplay.tools import Tools

LOGGER = singer.get_logger()
BOOKMARK_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class GooglePlayBase:
    """
    Stream base class
    """

    PREFIX = ""
    STREAM_NAME = ""
    FILETYPE = "csv"
    DIMENSION = "country"
    KEY_PROPERTIES: List[str] = []

    def __init__(self, client, state, catalog):
        self.schema = None

        if catalog:
            stream_details = Tools.stream_details_from_catalog(
                catalog, self.STREAM_NAME
            )
            if stream_details:
                self.schema = stream_details.schema.to_dict()["properties"]
                self.key_properties = stream_details.key_properties

        if not self.schema:
            self.schema = singer.utils.load_json(
                os.path.normpath(
                    os.path.join(
                        self.get_class_path(),
                        "../schemas/{}.json".format(self.STREAM_NAME),
                    )
                )
            )
            self.key_properties = self.KEY_PROPERTIES

        self.client = client
        self.state = state
        self.bookmark_date = singer.bookmarks.get_bookmark(
            state=state, tap_stream_id=self.STREAM_NAME, key="last_record"
        )
        if not self.bookmark_date:
            self.bookmark_date = client.start_date

        self.packages = self.client.get_packages(self.PREFIX, self.STREAM_NAME)

    def sync(self):
        """
        Perform sync action
        These steps are the same for all streams
        Differences between streams are implemented by overriding .do_sync() method
        """
        singer.write_schema(self.STREAM_NAME, self.schema, self.key_properties)
        self.do_sync()
        singer.write_state(self.state)

    def do_sync(self):
        """
        Main sync functionality
        Most of the streams use this
        A few of the streams work differently and o 7-verride this method
        """
        extraction_time = singer.utils.now()
        delta = timedelta(days=1)

        new_bookmark_date = self.bookmark_date
        LOGGER.info(f"Start bookmark: {new_bookmark_date}")
        with singer.metrics.Counter(
            "record_count", {"report": self.STREAM_NAME}
        ) as counter:
            for package_name in self.packages:
                bookmark = Tools.str_to_date(self.bookmark_date).replace(
                    tzinfo=pytz.UTC
                )
                LOGGER.info(f"Starting package : {package_name} sync")
                while bookmark + delta <= extraction_time:
                    try:
                        iterator_str = bookmark.strftime("%Y%m")  # like 201906
                        LOGGER.info(f"The iterator {iterator_str} on package: {package_name}")
                        # README: Neeed a condition for zip files, they have a different style
                        # report_key = f"sales/salesreport_{iterator_str}.zip"
                        report_key = f"{self.PREFIX}/{self.STREAM_NAME}/{self.STREAM_NAME}_{package_name}_{iterator_str}_{self.DIMENSION}.{self.FILETYPE}"
                        response, _ = Tools.csv_to_list(
                            self.client.get_report(report_key, self.FILETYPE)
                        )
                    except FileNotFoundError as ex:
                        LOGGER.error(ex)
                        break

                    for entry in response:
                        new_bookmark_date = max(new_bookmark_date, entry["date"])
                        entry["extracted_at"] = extraction_time.strftime(
                            BOOKMARK_DATE_FORMAT
                        )
                        singer.write_message(
                            singer.RecordMessage(
                                stream=self.STREAM_NAME,
                                record=entry,
                                time_extracted=extraction_time,
                            )
                        )
                    counter.increment()
                    bookmark += delta
        self.state = singer.write_bookmark(
            self.state, self.STREAM_NAME, "last_record", new_bookmark_date
        )
        LOGGER.info(f"New bookmark: {new_bookmark_date}")
        LOGGER.warning(f"state: {self.state}")

    def get_class_path(self):
        """
        The absolute path of the source file for this class
        """
        return os.path.dirname(inspect.getfile(self.__class__))

    def generate_catalog(self):
        """
        Builds the catalog entry for this stream
        """
        return dict(
            tap_stream_id=self.STREAM_NAME,
            stream=self.STREAM_NAME,
            key_properties=self.key_properties,
            schema=self.schema,
            metadata={
                "selected": True,
                "schema-name": self.STREAM_NAME,
                "is_view": False,
            },
        )
