import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit

from entities.taxi_ride import taxi_ride


def create_delta_table(spark: SparkSession, input: str, output: str) -> None:
    taxi_rides = spark.read.option('header', True).option(
        'mode', 'DROPMALFORMED').csv(input, schema=taxi_ride)

    taxi_rides.write.format('delta').save(output)


def show_records(spark: SparkSession, table: str) -> None:
    df = spark.read.format('delta').load(table)
    df.show()


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='input file path',
    )
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='output delta table path',
    )
    parser.add_argument(
        '--action',
        dest='action',
        default='show',
        help='action to apply',
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName('taxipy') \
        .config('spark.jars.packages', 'io.delta:delta-core_2.12:0.7.0') \
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
        .getOrCreate()

    from delta.tables import DeltaTable
    taxi_ride_table = DeltaTable.forPath(spark, args.output)
    taxi_ride_table.update(condition=expr("vendor_id == 2"),
                           set={"total_amount": "123"})
    show_records(spark, args.output)
    spark.stop()


if __name__ == '__main__':
    run()
