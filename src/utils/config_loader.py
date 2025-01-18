import yaml
import os

class ConfigLoader:
    """
    A utility class to load and manage configuration from a YAML file.
    Implements lazy loading to ensure the configuration is read only once.
    """
    _config = None  # Singleton instance for the configuration

    @staticmethod
    def load_config(config_path="config/config.yaml"):
        """
        Loads the configuration from the YAML file. Implements lazy loading
        to cache the configuration for subsequent calls.

        :param config_path: str, optional
            The path to the YAML configuration file. Defaults to "config/config.yaml".
        :return: dict
            The loaded configuration as a dictionary.
        :raises FileNotFoundError:
            If the specified configuration file does not exist.
        :raises ValueError:
            If there is an error reading or parsing the YAML file.
        """
        if ConfigLoader._config is None:  # Load config only if it hasn't been loaded yet
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Config file not found at {config_path}")
            try:
                with open(config_path, "r") as file:
                    ConfigLoader._config = yaml.safe_load(file)
            except yaml.YAMLError as error:
                raise ValueError(f"Error reading the YAML file: {error}")
        return ConfigLoader._config
