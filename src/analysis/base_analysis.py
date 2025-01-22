# from src.utils.logger import get_logger
import logging

class BaseAnalysis:
    """
    Base class for performing analysis.
    Provides common methods for data processing, logging, and writing results.
    """
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        # self.logger = get_logger()
        self.logger = logging.getLogger(__name__)

    def load_data(self, file_path):
        """
        Loads a CSV file into a DataFrame.
        :param file_path: Path to the CSV file.
        :return: Spark DataFrame.
        """
        self.logger.info(f"Loading data from {file_path}")
        return self.spark.read.csv(file_path, header=True, inferSchema=True)

    def save_results(self, df, output_path):
        """
        Saves the DataFrame to the specified path.
        :param df: Spark DataFrame containing results.
        :param output_path: Path to save the results.
        """
        self.logger.info(f"Saving results to {output_path}")
        df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
