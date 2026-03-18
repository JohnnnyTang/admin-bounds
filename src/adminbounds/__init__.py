from .client import AdminBoundsClient

# Backwards-compatible alias
GeoAdminClient = AdminBoundsClient

__version__ = "0.3.0"
__all__ = ["AdminBoundsClient", "GeoAdminClient"]
