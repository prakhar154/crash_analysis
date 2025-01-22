from pyspark.sql import SparkSession
from src.utils.logger import Logger
from src.utils.config_loader import ConfigLoader

class SparkHelper:
    """
    A singleton utility class to manage Spark session creation and reuse.
    Ensures that only one Spark session is created across the application.
    """
    _instance = None  # Singleton instance

    def __new__(cls, *args, **kwargs):
        """
        Implements the singleton pattern to ensure only one instance of the
        SparkHelper class is created.

        :return: SparkHelper
            The singleton instance of the SparkHelper class.
        """
        if cls._instance is None:
            cls._instance = super(SparkHelper, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """
        Initializes the SparkHelper class. Loads configuration and sets up
        logging. Ensures that the Spark session is initialized only when required.
        """
        if not hasattr(self, "_initialized"):  # Ensure initialization runs only once
            self.config = ConfigLoader.load_config()
            self.logger = Logger().get_logger()
            self.spark = None
            self._initialized = True

    def create_spark_session(self, app_name="CrashAnalysisApp"):
        """
        Creates a Spark session if it does not already exist. Configures the
        session using settings from the configuration file.

        :param app_name: str, optional
            The application name for the Spark session. Defaults to "CrashAnalysisApp".
        :return: SparkSession
            The initialized or existing Spark session.
        :raises Exception:
            If the Spark session fails to initialize.
        """
        if self.spark is None:
            try:
                self.logger.info("Initializing Spark session.")
                spark_builder = SparkSession.builder.appName(app_name)

                if self.config.get("spark", {}).get("enable_hive", False):
                    self.logger.info("Enabling Hive support.")
                    spark_builder = spark_builder.enableHiveSupport()

                for key, value in self.config["spark"].get("spark_conf", {}).items():
                    self.logger.debug(f"Setting Spark config: {key} = {value}")
                    spark_builder = spark_builder.config(key, value)

                self.spark = spark_builder.getOrCreate()
                self.logger.info("Spark session initialized successfully.")
            except Exception as e:
                self.logger.error(f"Failed to create Spark session: {e}")
                raise
        else:
            self.logger.info("Reusing existing Spark session.")
        return self.spark
