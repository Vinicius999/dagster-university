from dagster import asset, Config
from dagster_duckdb import DuckDBResource

import plotly.express as px
import plotly.io as pio

from . import constants


class AdhocRequestConfig(Config):
    filename: str
    borough: str
    start_date: str
    end_date: str


@asset(
    deps=["taxi_zones", "taxi_trips"]
)
def adhoc_request(config: AdhocRequestConfig, database: DuckDBResource):
    """
        The response to an request made in the `requests` directory.
        See `requests/README.md` for more information.
    """
    
    # strip the file extension from the filename, and use it as the output filename
    file_path = constants.REQUEST_DESTINATION_TEMPLATE_FILE_PATH.format(config.filename.split('.')[0])
    
    # count the number of trips that picked up in a given borough, aggregated by time of day and hour of day
    sql_query = f"""
        SELECT
            date_part('hour', pickup_datetime) AS hour_of_day,
            date_part('dayofweek', pickup_datetime) AS day_of_week_num,
            CASE date_part('dayofweek', pickup_datetime)
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END AS day_of_week,
            count(*) AS num_trips
        FROM trips
        LEFT JOIN zones ON trips.pickup_zone_id = zones.zone_id
        WHERE pickup_datetime >= '{config.start_date}'
            AND pickup_datetime < '{config.end_date}'
            AND pickup_zone_id IN (
                SELECT zone_id
                FROM zones
                WHERE borough = '{config.borough}'
            )
        GROUP BY 1, 2
        ORDER BY 1, 2 ASC;
    """
    
    with database.get_connection() as conn:
        results = conn.execute(sql_query).fetch_df()
    
    fig = px.bar(
        results,
        x="hour_of_day",
        y="num_trips",
        color="day_of_week",
        barmode="stack",
        title=f"Number of trips by hour of day in {config.borough}, from {config.start_date} to {config.end_date}",
        labels={
            "hour_of_day": "Hour of Day",
            "day_of_week": "Day of Week",
            "num_trips": "Number of Trips"
        }
    )
    
    pio.write_image(fig, file_path)