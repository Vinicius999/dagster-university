from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partition, weekly_partition


trips_by_week = AssetSelection.keys(["trips_by_week"])
adhoc_request = AssetSelection.keys(["adhoc_request"])


trip_update_job = define_asset_job(
    name="trip_update_job",
    selection=AssetSelection.all() - trips_by_week - adhoc_request,
    partitions_def=monthly_partition,
)

weekly_update_job = define_asset_job(
    name="weekly_update_job",
    selection=trips_by_week,
    partitions_def=weekly_partition,
)

adhoc_request_job = define_asset_job(
    name="adhoc_request_job",
    selection=adhoc_request,
)
