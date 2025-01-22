from src.utils.spark_helper import SparkHelper
from src.utils.logger import Logger
from src.pipeline.orchestrator import Orchestrator
from src.utils.config_loader import ConfigLoader
from src.analysis.analysis_10 import Analysis10

def main():
    """
    Entry point for the Crash Analysis application.
    Initializes logging, Spark session, configuration loader, and orchestrator.
    Executes all defined analyses in the pipeline.
    """
    # Initialize logger
    logger = Logger().get_logger()
    logger.info("Starting the Crash Analysis application.")  # Log application start

    # Create a Spark session using SparkHelper
    spark_helper = SparkHelper()
    spark = spark_helper.create_spark_session()  # Spark session for distributed data processing

    # Load configuration settings from external sources
    config = ConfigLoader.load_config()

    # Initialize orchestrator with config, logger, and Spark session
    orchestrator = Orchestrator(config, logger, spark)

    # Example of running a single specific analysis (currently commented out)
    # orchestrator.run_analysis(Analysis10)

    # Run all analyses defined in the pipeline
    orchestrator.run_all_analyses()

    # Log application completion
    logger.info("Application finished.")

# Run the main function when the script is executed directly
if __name__ == "__main__":
    main()

