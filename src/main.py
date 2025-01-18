from utils.spark_helper import SparkHelper
from utils.logger import Logger

def main():
    logger = Logger().get_logger()
    logger.info("Starting the Crash Analysis application.")

    spark_helper = SparkHelper()
    spark = spark_helper.create_spark_session()

    # Example Spark operation
    logger.info("Reading sample data.")
    df = spark.read.csv("data/raw/Charges_use.csv", header=True, inferSchema=True)
    df.show()

    logger.info("Application finished.")

if __name__ == "__main__":
    main()
