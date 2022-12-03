from .crashes import CrashesStream
from .installs import InstallsStream
from .ratings import RatingsStream
from .subscriptions import SubscriptionsStream

AVAILABLE_STREAMS = [SubscriptionsStream, InstallsStream, CrashesStream, RatingsStream]
