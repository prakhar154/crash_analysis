import pytest
import os
import shutil
# from pyspark.sql import SparkSession
from src.analysis.analysis_1 import Analysis1
from tempfile import mkdtemp
from src.utils.spark_helper import SparkHelper


# Mock config similar to the example YAML config provided
mock_config = {
    "spark": {
        "enable_hive": False,
        "spark_conf": {
            "spark.executor.memory": "4g",
            "spark.driver.memory": "2g",
            "spark.sql.shuffle.partitions": "200"
        }
    },
    "logging": {
        "log_dir": "output/logs",
        "log_file": "application.log",
        "level": "INFO"
    },
    "input_data": {
        "primary_person_data": "data/raw/Primary_Person_use.csv",
        "units_data": "data/raw/Units_use.csv"
    },
    "output_data": {
        "analysis_1": "output/anaysis/analysis_1.csv",
        "analysis_2": "output/anaysis/analysis_2.csv",
        "logs": "output/logs/app.log"
    }
}

# @pytest.fixture(scope="module")
# def spark_session():
#     """Fixture to initialize SparkSession for testing"""
    
#     spark_helper = SparkHelper()
#     spark_session = spark_helper.create_spark_session()

#     return spark_session

# @pytest.fixture(scope="module")
spark_helper = SparkHelper()
spark_session = spark_helper.create_spark_session()


@pytest.fixture(scope="module")
def sample_data():
    """Fixture to create a sample CSV file for testing"""
    temp_dir = mkdtemp()  # Create a temporary directory
    sample_csv_path = os.path.join(temp_dir, "Primary_Person_use.csv")
    
    # Create sample data for testing
    sample_data = [
        ('MALE', 1, 'Crash1'),
        ('MALE', 0, 'Crash2'),
        ('FEMALE', 0, 'Crash3'),
        ('MALE', 0, 'Crash4')
    ]
    


    # Create a Spark DataFrame from the sample data
    df = spark_session.createDataFrame(sample_data, ['PRSN_GNDR_ID', 'DEATH_CNT', 'CRASH_ID'])
    
    # Write the DataFrame to CSV
    df.write.option("header", "true").csv(sample_csv_path)
    
    # Update the config to point to this temporary CSV file
    mock_config['input_data']['primary_person_data'] = sample_csv_path
    
    yield sample_csv_path  # Yield path to the test

    # Cleanup the temporary directory after tests
    shutil.rmtree(temp_dir)

@pytest.fixture(scope="module")
def temp_output_dir():
    """Fixture to create a temporary output directory for result CSV"""
    temp_dir = mkdtemp()  # Create a temporary directory for output
    mock_config['output_data']['analysis_1'] = os.path.join(temp_dir, "analysis_1.csv")
    
    yield mock_config['output_data']['analysis_1']
    
    # Cleanup the temporary directory after tests
    shutil.rmtree(temp_dir)

class TestAnalysis:

    def test_analysis_1(self, sample_data, temp_output_dir):
        """Test the Analysis 1: Crashes with males killed > 2"""
        analysis = Analysis1(spark_session, mock_config)
        
        # Run the analysis with the sample data
        analysis.run()

        # Expecting 2 crashes where MALES_KILLED > 2 (Crash2, Crash4)
        # assert result.count() == 2

        # Optionally, verify the result data saved to the output location
        result_df = spark_session.read.option("header", "true").csv(temp_output_dir)
        assert result_df.count() == 1  # Expect 1 row with the result of crashes > 2
        assert "crashes_with_male_killed_above_2" in result_df.columns  # Check if the column name exists
