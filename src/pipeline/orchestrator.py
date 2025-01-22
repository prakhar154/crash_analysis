import importlib

class Orchestrator:
    def __init__(self, config, logger, spark):
        """
        Initialize the Orchestrator with the given configuration, logger, and Spark session.

        Args:
            config (dict): The configuration dictionary containing settings and paths.
            logger (Logger): The logger instance for logging messages.
            spark (SparkSession): The Spark session for running Spark jobs.
        """
        self.config = config
        self.logger = logger
        self.spark = spark

    def run_all_analyses(self):
        """
        Run all analyses specified in the configuration file under 'analyses_to_run'.
        
        The method dynamically imports and initializes each analysis class, executes its run method, 
        and handles any errors encountered during the execution.
        """
        self.logger.info("Starting the analysis pipeline...")

        analyses_to_run = self.config.get("analyses_to_run", [])
        
        for analysis_path in analyses_to_run:
            try:
                self.logger.info(f"Loading and running analysis: {analysis_path}")

                # Dynamically import and initialize the analysis class
                module_name, class_name = analysis_path.rsplit('.', 1)
                module = importlib.import_module(module_name)
                analysis_class = getattr(module, class_name)

                # Initialize and run the analysis
                analysis = analysis_class(self.spark, self.config)
                analysis.run()

                self.logger.info(f"Analysis {analysis_class.__name__} completed successfully.")
            except Exception as e:
                self.logger.error(f"Error while running analysis {analysis_path}: {e}")

    def run_analysis(self, analysis_path):
        """
        Run a single analysis specified by its fully qualified name.

        Args:
            analysis_path (str): The fully qualified name of the analysis class to run.

        Example:
            orchestrator.run_analysis('src.analysis.analysis_1.Analysis1')
        
        The method dynamically imports and initializes the specified analysis class, executes its run method,
        and raises any errors encountered during the execution.
        """
        try:
            self.logger.info(f"Loading and running analysis: {analysis_path}")

            # Dynamically import and initialize the analysis class
            module_name, class_name = analysis_path.rsplit('.', 1)
            module = importlib.import_module(module_name)
            analysis_class = getattr(module, class_name)

            # Initialize and run the analysis
            analysis = analysis_class(self.spark, self.config)
            analysis.run()

            self.logger.info(f"Analysis {analysis_class.__name__} completed successfully.")
        except Exception as e:
            self.logger.error(f"Error while running analysis {analysis_path}: {e}")
            raise  # Re-raise the exception to halt execution for this analysis
