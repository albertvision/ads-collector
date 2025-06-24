from .base import BaseProvider
from .meta import MetaProvider
from .google import GoogleProvider

PROVIDER_CLASSES = {
    MetaProvider.name: MetaProvider,
    GoogleProvider.name: GoogleProvider,
}

__all__ = ["BaseProvider", "MetaProvider", "GoogleProvider", "PROVIDER_CLASSES"]
