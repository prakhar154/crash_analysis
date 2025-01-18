import logging
import os
from utils.config_loader import ConfigLoader

class Logger:
    """
    A utility class for logging across the application. Configurations for
    logging are fetched using ConfigLoader.
    """
    def __init__(self):
        """
        Initializes the Logger class. Sets up logging configurations based
        on the config file and creates log files if not already present.
        """
        self.config = ConfigLoader.load_config()
        log_dir = self.config["logging"]["log_dir"]
        log_file = os.path.join(log_dir, self.config["logging"]["log_file"])
        os.makedirs(log_dir, exist_ok=True)

        logging.basicConfig(
            filename=log_file,
            level=self.config["logging"]["level"],
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            filemode="a",
        )
        self.logger = logging.getLogger("CrashAnalysisLogger")

    def get_logger(self):
        """
        Retrieves the configured logger instance.

        :return: logging.Logger
            The logger instance configured for the application.
        """
        return self.logger
