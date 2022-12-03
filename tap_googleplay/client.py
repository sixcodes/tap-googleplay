import codecs
import io
import re
from typing import List, Set
from zipfile import ZipFile

import singer
from google.cloud import storage
import json
LOGGER = singer.get_logger()

REGEXP = r"([\w+]{3}\.\w+\.\w+\.[\w|\.\w]+[\d]{6})"

class GooglePlayClient:
    """
    The Google client
    """

    def __init__(self, config):
        self.start_date = config.get("start_date")
        self.bucket_name = config.get("bucket_name")
        self.bucket = storage.Client.from_service_account_info(
            json.loads(config.get("key_file"))
        ).get_bucket(self.bucket_name)

    def get_report(self, report_key: str, filetype: str = "csv"):
        if filetype == "csv":
            try:
                rep_data = self.bucket.get_blob(report_key).download_as_string()
            except AttributeError as ex:
                LOGGER.error(f"Report {report_key} not found")
                raise FileNotFoundError
            bom = codecs.BOM_UTF16_LE
            # TODO: Check if data exists
            if rep_data.startswith(bom):
                rep_data = rep_data[len(bom) :]
            return rep_data.decode("utf-16le")
        if filetype == "zip":
            # object_bytes: bytes = b''
            try:
                object_bytes = self.bucket.get_blob(report_key).download_as_bytes()
            except AttributeError as ex:
                LOGGER.error(ex)
                raise FileNotFoundError
            archive: io.BytesIO = io.BytesIO()
            archive.write(object_bytes)

            with ZipFile(archive, "r") as zip_archive:
                # TODO: Check if is not empty zip
                file = zip_archive.read(zip_archive.filelist[0].filename).decode(
                    "utf-8"
                )
            return file

    def get_packages(self, prefix: str, stream_name: str) -> Set:
        packages: List[str] = []
        dates: List[str] = []
        regex = re.compile(REGEXP)
        for x in [
            x.name for x in self.bucket.list_blobs(prefix=f"{prefix}/{stream_name}/")
        ]:
            # TODO: Add a flag to extract everything
            if "Test" not in x and "Staging" not in x:
                package_path = regex.findall(x)
                if package_path:
                    pack, ym = package_path[0].rsplit("_", 1)
                    packages.append(pack)
                    # TODO: Rethink if `ym` still needed
                    dates.append(ym)

        return set(packages)
