import pytest
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


@pytest.fixture(scope="session")
def spark_session(request):
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("StructuredNetworkWordCount") \
        .getOrCreate()

    request.addfinalizer(lambda: spark_session.sparkContext.stop())

    return spark_session


@pytest.fixture(scope="session")
def streaming_context(scope="session"):
    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext("local[2]", "NetworkWordCount")
    ssc = StreamingContext(sc, 1)
    return ssc
