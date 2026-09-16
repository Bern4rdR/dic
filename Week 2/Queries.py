
def query_2_1():
    return """
        SELECT
            zone AS pu_zone,
            month(pu_datetime) AS month, 
            COUNT(*) AS row_count
        FROM default.taxi_trips
        LEFT JOIN default.taxi_zone_lookup
        ON taxi_trips.pu_location_id = taxi_zone_lookup.location_id
        GROUP BY pu_zone, month
    """

def query_2_2():
    return """
    SELECT
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

def query_2_3():
    return """
    SELECT measurement, COUNT(county) AS trips 
    FROM (taxi_trips AS t
    LEFT JOIN taxi_zone_lookup AS tzl
    ON t.pu_location_id = tzl.location_id) t
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
    ) aq
    ON date_trunc('hour', pu_datetime) = hr_datetime AND county = aq_county
    GROUP BY measurement
    ORDER BY measurement, trips
    """