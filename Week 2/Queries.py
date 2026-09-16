
def query_2_1(broadcast=False):
    query = f"""
        SELECT {'/*+ BROADCAST(default.taxi_zone_lookup) */' if broadcast else ''}
            zone AS pu_zone,
            month(pu_datetime) AS month, 
            COUNT(*) AS row_count
        FROM default.taxi_trips
        LEFT JOIN default.taxi_zone_lookup
        ON taxi_trips.pu_location_id = taxi_zone_lookup.location_id
        GROUP BY pu_zone, month
    """
    return query

def query_2_2(broadcast=False):
    query = f"""
    SELECT {'/*+ BROADCAST(w) */' if broadcast else ''}
        CASE
            WHEN prcp > 0 THEN 'greater_than_0'
            ELSE 'zero_or_null'
        END AS column_group,
        COUNT(*) AS cnt,
        AVG(trip_distance) AS avg_trip_distance
    FROM default.taxi_trips AS t
    LEFT JOIN default.weather AS w
    ON date_trunc('hour', t.pu_datetime) = w.datetime
    GROUP BY
        CASE
            WHEN prcp > 0 THEN 'greater_than_0'
                ELSE 'zero_or_null'
        END;
    """
    return query

def query_2_3(broadcast=False):
    query = f"""
    SELECT {'/*+ BROADCAST(tzl), BROADCAST(aq) */' if broadcast else ''}
    measurement, COUNT(county) AS trips
    FROM taxi_trips AS t
    LEFT JOIN taxi_zone_lookup AS tzl
    ON t.pu_location_id = tzl.location_id
    LEFT JOIN (
        SELECT *
        FROM (
            SELECT
                measurement,
                county AS aq_county,
                date_trunc("hour", datetime) as hr_datetime,
                ROW_NUMBER() OVER (
                    PARTITION BY date_trunc("hour", datetime)
                    ORDER BY datetime DESC
                ) AS rn
            FROM air_quality r
        )
        WHERE rn = 1
    ) AS aq
    ON date_trunc('hour', pu_datetime) = hr_datetime AND county = aq_county
    GROUP BY measurement
    ORDER BY measurement, trips
    """
    return query

# TODO Query 2.4
def query_2_4():
    query = f""""""
    return query

def query_2_5():
    query = f"""
SELECT date_format(pu_datetime, 'EEE') AS day, hour(pu_datetime) AS hour, COUNT(*) AS trips
    FROM taxi_trips
    GROUP BY hour, day
    ORDER BY day, trips DESC
"""
    return query


def query_2_6():
    query = f"""
SELECT date_format(pu_datetime, 'MMM') AS month, COUNT(*) AS trips
    FROM taxi_trips
    GROUP BY month
    ORDER BY month
"""
    return query