from src.analysis.base_analysis import BaseAnalysis
from pyspark.sql.functions import col, count, sum

class Analysis1(BaseAnalysis):
    """
    Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2.
    """
    def run(self):
        try:
            # Load data
            data_path = self.config['input_data']['primary_person_data']
            df_person = self.load_data(data_path)

            # Perform analysis: Filtering where gender is male and death count > 0
            df_male_deaths = df_person.filter((col('PRSN_GNDR_ID') == 'MALE') & (col('DEATH_CNT') >0))
            # df_male_deaths.show()

            # sum of male detahs in a crash 
            df_male_deaths = df_male_deaths.groupBy('CRASH_ID').agg(sum("DEATH_CNT").alias('cnt')).select('CRASH_ID', 'cnt')
            # df_male_deaths.orderBy(col('cnt').desc()).show()
            cnt = df_male_deaths.filter(col('cnt')>2).count()

            # Save this number in a dataframe
            result_df = self.spark.createDataFrame([(cnt,)], ["crashes_with_male_killed_above_2"])

            # Log and save results
            self.logger.info(f"Number of crashes with males killed > 2: {cnt}")
            self.save_results(result_df, self.config['output_data']['analysis_1'])
        except Exception as e:
            # Log the error and raise it to fail the pipeline
            self.logger.error(f"Error occurred in Analysis1: {str(e)}", exc_info=True)
            raise  # Re-raise the exception to stop the pipeline


