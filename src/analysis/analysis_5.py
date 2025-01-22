from src.analysis.base_analysis import BaseAnalysis
from pyspark.sql.functions import col, count

class Analysis5(BaseAnalysis):
    """
    Analysis 5: Which state has the highest number of accidents in which females are not involved?
    """
    def run(self):
        try:
            # Load data

            # Person data
            person_path = self.config['input_data']['primary_person_data']
            df_person = self.load_data(person_path)

            # removing unknown and NA genders 
            df_person_clean = df_person.filter((col('PRSN_GNDR_ID') != 'NA') & (col('PRSN_GNDR_ID') != 'UNKNOWN'))

            # CRASH IDs og accidents involving females 
            df_females = df_person_clean.filter(col('PRSN_GNDR_ID') == 'FEMALE').select('CRASH_ID')

            # Creating another DataFrame with valid license states
            df_valid_license = df_person.filter((col('DRVR_LIC_STATE_ID') != 'NA') & 
                (col('DRVR_LIC_STATE_ID') != 'Other') & 
                (col('DRVR_LIC_STATE_ID') != 'Unknown')).select('CRASH_ID', 'DRVR_LIC_STATE_ID')

            # Get all crashes where females were not involved
            df_crashes = df_valid_license.join(df_females.select('CRASH_ID'), 'CRASH_ID', 'left_anti')

            # Get top state
            result_df = df_crashes.groupBy('DRVR_LIC_STATE_ID') \
                                   .agg(count("*").alias('cnt')) \
                                   .orderBy(col('cnt').desc()) \
                                   .limit(1)

            # Collect the result to log
            top_state = result_df.collect()[0]['DRVR_LIC_STATE_ID']  # Get top state
            top_count = result_df.collect()[0]['cnt']  # Get the count of accidents

            # Rename column for clarity in output
            result_df = result_df.withColumnRenamed('DRVR_LIC_STATE_ID', 'state_with_highest_number_of_accidents_in_which_females_are_not_involved')

            # Log and save results
            self.logger.info(f"State with the highest number of accidents in which females are not involved: {top_state}, Count: {top_count}")
            self.save_results(result_df, self.config['output_data']['analysis_5'])

        except Exception as e:
            # Log the error and raise it to fail the pipeline
            self.logger.error(f"Error occurred in Analysis5: {str(e)}", exc_info=True)
            raise  # Re-raise the exception to stop the pipeline
