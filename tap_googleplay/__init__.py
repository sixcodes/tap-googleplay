"""
tap_googleplay package

Singer tap for the Google Play
Singer: https://github.com/singer-io/getting-started

"""

import singer

import tap_googleplay.client
import tap_googleplay.streams
from tap_googleplay.runner import GooglePlayRunner

LOGGER = singer.get_logger()


@singer.utils.handle_top_exception(LOGGER)
def main():
    """
    Main function - process args, build runner, execute request
    """
    args = singer.utils.parse_args(required_config_keys=["bucket_name", "start_date"])

    runner = GooglePlayRunner(
        client=tap_googleplay.client.GooglePlayClient(args.config),
        state=args.state,
        catalog=args.catalog,
    )

    if args.discover:
        runner.do_discover()
    else:
        runner.do_sync()


if __name__ == "__main__":
    main()
