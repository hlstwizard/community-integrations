from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.0.1"

DagsterLibraryRegistry.register(
    "dagster-contrib-azure", __version__, is_dagster_package=False
)
