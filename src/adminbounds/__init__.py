from .client import AdminBoundsClient

# Backwards-compatible alias
GeoAdminClient = AdminBoundsClient

__version__ = "0.4.0"
__all__ = ["AdminBoundsClient", "GeoAdminClient"]
