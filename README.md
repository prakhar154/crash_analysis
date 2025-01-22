# Crash Analysis

### This is a data engineering project where I have performed analysis on vehicle crash data of the United States.

---

## Project Setup

1. Clone the repository:
   ```bash
   git clone <repository_url>
   cd crash_analysis
   ```
2. Make the setup script executable:
   ```bash
   chmod +x setup.sh
   ```
3. Run the setup script:
   ```bash
   ./setup.sh
   ```
   The above steps will set up a Python virtual environment and install the required libraries specified in `requirements.txt`.

---

## Running the Analysis

1. **Input Configurations**:
   - Define Spark configurations, input paths, and output directory locations in the `config/config.yaml` file.
   - Specify the analyses to run by setting `analyses_to_run` in the configuration.
2. **Run the analysis**:
   ```bash
   spark-submit --master local[*] src/main.py
   ```
   This command will execute the analyses as defined in the configuration file.

---

## Project Structure

```
crash_analysis/
├── config/
│   ├── config.yaml                # Configuration file for paths, settings, and constants
├── data/
│   ├── raw/                       # Folder for raw input files (CSV files)
├── output/
│   ├── analysis/                  # Final analysis results
│   ├── logs/                      # Application logs
├── src/
│   ├── main.py                    # Main entry point for the application
│   ├── analysis/
│   │   ├── __init__.py
│   │   ├── base_analysis.py       # Base class for reusable functions
│   │   ├── analysis_1.py          # Module for Analysis 1
│   │   ├── analysis_2.py          # Module for Analysis 2
│   │   └── ...                    # Other analysis modules
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── spark_helper.py        # Spark session initialization
│   │   ├── config_loader.py       # Module to parse and load config.yaml
│   │   ├── df_processor.py        # To perform basic sanity checks on data
│   │   ├── logger.py              # Logging utility
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── orchestrator.py        # Orchestrates the execution of analyses
├── tests/
│   ├── test_analysis.py           # Tests for data analysis modules(WIP)
├── README.md                      # Documentation for the project
├── requirements.txt               # Python dependencies
├── setup.sh                       # Script to set up the environment
└── .gitignore                     # To ignore unnecessary files in the repo
```

### Key Directories and Files:
1. `src/analysis/`: Contains all the analysis logic, each as a separate module.
2. `src/utils/`: Utility files for Spark initialization, configuration loading, and data processing.
3. `src/pipeline/orchestrator.py`: Orchestrates the execution of analyses based on the configuration.
4. `data/raw/`: Stores all raw CSV input files.
5. `output/analysis/`: Stores all generated output files.
