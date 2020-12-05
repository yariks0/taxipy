from pyspark.sql.types import DecimalType, IntegerType, StringType, StructType, TimestampType


taxi_ride = StructType() \
    .add('vendor_id', IntegerType()) \
    .add('pickup_datetime', TimestampType()) \
    .add('dropoff_datetime', TimestampType()) \
    .add('passenger_count', IntegerType()) \
    .add('trip_distance', DecimalType(5, 2)) \
    .add('rate_code', IntegerType()) \
    .add('store_and_fwd_flag', StringType()) \
    .add('payment_type', IntegerType()) \
    .add('fare_amount', DecimalType(5, 2)) \
    .add('extra', DecimalType(5, 2)) \
    .add('mta_tax', DecimalType(5, 2)) \
    .add('tip_amount', DecimalType(5, 2)) \
    .add('tolls_amount', DecimalType(5, 2)) \
    .add('imp_surcharge', DecimalType(5, 2)) \
    .add('total_amount', DecimalType(5, 2)) \
    .add('pickup_location_id', IntegerType()) \
    .add('dropoff_location_id', IntegerType())
