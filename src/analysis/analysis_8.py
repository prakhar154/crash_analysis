from src.analysis.base_analysis import BaseAnalysis
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window

class Analysis8(BaseAnalysis):
    """
    Analysis 8`: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash 
    (Use Driver Zip Code) 
    """
    def run(self):
        try:
            # Load data

            # Person data
            person_path = self.config['input_data']['primary_person_data']
            df_person = self.load_data(person_path)

            # unit data
            units_path = self.config['input_data']['units_data']
            df_units = self.load_data(units_path)

            # drop duplicates
            df_units = df_units.dropDuplicates()

            # list of alcohol contributing factors
            alcohol_contrib_facts = ['UNDER INFLUENCE - ALCOHOL', 'HAD BEEN DRINKING']


            # fetch records which have alcohol as contributing factor 
            df_alcohol = df_units.filter(
                (col('CONTRIB_FACTR_1_ID').isin(alcohol_contrib_facts)) |
                (col('CONTRIB_FACTR_2_ID').isin(alcohol_contrib_facts)) |
                (col('CONTRIB_FACTR_P1_ID').isin(alcohol_contrib_facts)) 
            ).select('CRASH_ID')

            # filter out null zip records
            df_driver_zip = df_person.filter(col('DRVR_ZIP').isNotNull()).select('CRASH_ID', 'DRVR_ZIP')
            df_driver_zip = df_person.filter(col('DRVR_ZIP')!=0)

            # getting the top 5 zip 
            df_top_zip = df_alcohol.join(df_driver_zip, 'CRASH_ID', 'inner')
            df_top_zip = df_top_zip.groupBy('DRVR_ZIP').agg(count("*").alias('cnt')).select('DRVR_ZIP').orderBy(col('cnt').desc()).limit(5)
            
            # Rename column for clarity in output
            result_df = df_top_zip.withColumnRenamed('DRVR_ZIP', 'Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash')
            # Log and save results
            self.logger.info(f"Analysis complete. Results stored at {self.config['output_data']['analysis_8']}")
            self.save_results(result_df, self.config['output_data']['analysis_8'])

        except Exception as e:
            # Log the error and raise it to fail the pipeline
            self.logger.error(f"Error occurred in Analysis8: {str(e)}", exc_info=True)
            raise  # Re-raise the exception to stop the pipeline
