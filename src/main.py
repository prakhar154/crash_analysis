from src.utils.spark_helper import SparkHelper
from src.utils.logger import Logger
from src.pipeline.orchestrator import Orchestrator
from src.utils.config_loader import ConfigLoader
from src.analysis.analysis_2 import Analysis2
from src.analysis.analysis_3 import Analysis3
from src.analysis.analysis_4 import Analysis4
from src.analysis.analysis_5 import Analysis5
from src.analysis.analysis_6 import Analysis6
from src.analysis.analysis_7 import Analysis7
from src.analysis.analysis_8 import Analysis8
from src.analysis.analysis_9 import Analysis9
from src.analysis.analysis_10 import Analysis10



def main():
    logger = Logger().get_logger()
    logger.info("Starting the Crash Analysis application.")

    spark_helper = SparkHelper()
    spark = spark_helper.create_spark_session()

    config = ConfigLoader.load_config()
    
    # Initialize and run the orchestrator
    orchestrator = Orchestrator(config, logger, spark)
    # orchestrator.run_analysis(Analysis10)
    orchestrator.run_all_analyses()


    logger.info("Application finished.")

if __name__ == "__main__":
    main()
