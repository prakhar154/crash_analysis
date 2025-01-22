from src.analysis.base_analysis import BaseAnalysis
from pyspark.sql.functions import col

class Analysis2(BaseAnalysis):
    """
    Analysis 2: How many two wheelers are booked for crashes?
    """
    def run(self):
        try:
            # Load data
            data_path = self.config['input_data']['units_data']
            df_units = self.load_data(data_path)

            # Perform analysis: Filtering data for two wheelers

            # drop duplicates
            df_units = df_units.dropDuplicates()

            # filtering data for 2 wheelers
            df_units_2_wheelers = df_units.filter((col('VEH_BODY_STYL_ID') == 'MOTORCYCLE') | (col('VEH_BODY_STYL_ID') == 'POLICE MOTORCYCLE')) 

            # removing null from number plates 
            df_units_2_wheelers = df_units_2_wheelers.filter(col("VIN").isNotNull()) 

            # Count distinct number plates
            distinct_count = df_units_2_wheelers.select("VIN").distinct().count()

            # save this result in csv 
            result_df = self.spark.createDataFrame([(distinct_count,)], ["two_wheelers_booked_for crashes"])

            # Log and save results
            self.logger.info(f"Two wheelers booked for crash: {distinct_count}")
            self.save_results(result_df, self.config['output_data']['analysis_2'])
        except Exception as e:
            # Log the error and raise it to fail the pipeline
            self.logger.error(f"Error occurred in Analysis2: {str(e)}", exc_info=True)
            raise  # Re-raise the exception to stop the pipeline

