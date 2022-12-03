import csv

from dateutil.parser import parse


class Tools:
    @staticmethod
    def csv_to_list(content):
        lines = content.split("\n")
        header = [s.lower().replace(" ", "_") for s in lines[0].split(",")]
        data = []
        for row in csv.reader(lines[1:]):
            if len(row) == 0:
                continue
            line_obj = {}
            for i, column in enumerate(header):
                if i < len(row):
                    line_obj[column] = row[i].strip()
            data.append(line_obj)
        return data, header

    @staticmethod
    def str_to_date(value):
        """
        Convert (json) string to date
        """
        return parse(value)

    @staticmethod
    def stream_details_from_catalog(catalog, stream_name):
        """
        Extract details for a single stream from the catalog
        """
        for stream_details in catalog.streams:
            if stream_details.tap_stream_id == stream_name:
                return stream_details
        return None
