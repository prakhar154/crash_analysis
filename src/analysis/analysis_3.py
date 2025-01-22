from src.analysis.base_analysis import BaseAnalysis
from pyspark.sql.functions import col, count 

class Analysis3(BaseAnalysis):
    """
    Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
    """
    def run(self):
        try:
            # Load data

            # person data
            person_path = self.config['input_data']['primary_person_data']
            df_person = self.load_data(person_path)

            # unit data
            units_path = self.config['input_data']['units_data']
            df_units = self.load_data(units_path)


            # filter person df where airbag is not deployed and death count > 0
            df_airbag_death = df_person.filter((col('PRSN_AIRBAG_ID').like("NOT DEPLOYED")) & (col('DEATH_CNT')>0))

            # drop duplicates and remove records where make id is na
            df_units = df_units.dropDuplicates()
            df_units = df_units.filter(~(col('VEH_MAKE_ID')=='NA'))

            # create make df, joining person and units df on CRASH ID 
            df_veh_make = df_airbag_death.select('CRASH_ID', 'DEATH_CNT').join(df_units.select('CRASH_ID', 'VEH_MAKE_ID'), 'CRASH_ID', 'inner')

            # select top 5 vehicle makes by the most number of death counts when airbags were not deployed 
            result_df = df_veh_make.groupBy('VEH_MAKE_ID').agg(count('CRASH_ID').alias('cnt')).select('VEH_MAKE_ID').orderBy(col('cnt').desc()).limit(5)

            # Log and save results
            self.logger.info(f"Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy saved to {self.config['output_data']['analysis_3']}")
            self.save_results(result_df, self.config['output_data']['analysis_3'])
        except Exception as e:
            # Log the error and raise it to fail the pipeline
            self.logger.error(f"Error occurred in Analysis3: {str(e)}", exc_info=True)
            raise  # Re-raise the exception to stop the pipeline


