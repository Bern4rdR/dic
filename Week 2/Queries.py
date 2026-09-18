
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

def query_2_4(broadcast=False):
    query = f"""
	   	WITH
	        trips AS (
	            SELECT {'/*+ BROADCAST(taxi_zone_lookup) */' if broadcast else ''} DATE_TRUNC('hour', pu_datetime) AS hour, county, COUNT(*) AS trip_count
	            FROM taxi_trips
	            JOIN taxi_zone_lookup
	                ON pu_location_id = location_id
	            WHERE county IS NOT NULL
	            GROUP BY DATE_TRUNC('hour', pu_datetime), county
	        ),
	        weather_cat AS (
	            SELECT DATE_TRUNC('hour', datetime) AS hour,
	                CASE
	                    WHEN prcp > 3 THEN 'rain'
	                    WHEN temp > 25 THEN 'heatwave'
	                    WHEN temp < 0 THEN 'cold'
	                    WHEN rhum > 65 THEN 'humid'
	                    WHEN rhum < 25 THEN 'dry'
	                    WHEN wspd > 8 THEN 'stormy'
	                    ELSE 'moderate'
	                END AS weather_cond
	            FROM weather
	        ),
	        weather_trips AS (
	            SELECT {'/*+ BROADCAST(w) */' if broadcast else ''} t.county, t.hour, t.trip_count, w.weather_cond
	            FROM trips AS t
	            INNER JOIN weather_cat AS w
	                ON t.hour = w.hour
	        ),
	        demand AS (
	            SELECT county, weather_cond, COUNT(*) AS weather_hours, SUM(trip_count) AS total_trips
	            FROM weather_trips
	            GROUP  BY county, weather_cond
	        )
	    SELECT county, weather_cond, weather_hours, ROUND(CAST(total_trips AS DOUBLE) / NULLIF(weather_hours, 0), 2) AS trips_per_hour
	    FROM demand
	    ORDER BY county, trips_per_hour
	    ;
    """
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
