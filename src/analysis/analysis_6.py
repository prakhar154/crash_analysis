from src.analysis.base_analysis import BaseAnalysis
from pyspark.sql.functions import col, count, dense_rank, sum
from pyspark.sql.window import Window

class Analysis6(BaseAnalysis):
    """
    Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    """
    def run(self):
        try:
            # Load data

            # unit data
            units_path = self.config['input_data']['units_data']
            df_units = self.load_data(units_path)

            # selecting one record of a unit in a single crash
            df_units = df_units.dropDuplicates(['CRASH_ID', 'UNIT_NBR'])

            # drop vales where VEH_MAKE_IDs make is NA
            df_units = df_units.filter(~(col('VEH_MAKE_ID')=='NA'))

            # caculating all injuries includng death; adding count of unknown injuries in total injuries as it is not added in total 
            df_final_injury = df_units.withColumn('final_injury_cnt', col('UNKN_INJRY_CNT')+col('TOT_INJRY_CNT')+col('DEATH_CNT'))

            # aggregating total injuries for of each maker 
            df_veh_make_final_injury = df_final_injury.groupBy('VEH_MAKE_ID').agg(sum('final_injury_cnt').alias('cnt')).select('VEH_MAKE_ID', 'cnt')

            # ranking the VEH_MAKE_IDs by final number of injuries 
            df_veh_make_final_injury = df_veh_make_final_injury.withColumn('rank', dense_rank().over(Window.orderBy(col('cnt').desc())))

            # filtering for 3rd to 5th vehicle makers by injuries 
            result_df = df_veh_make_final_injury.filter((col('rank')>=3)&(col('rank')<=5)).select('VEH_MAKE_ID')

            # Rename column for clarity in output
            result_df = result_df.withColumnRenamed('VEH_MAKE_ID', 'top_3rd_to_5th_vehicle_makers_with most_injuries')

            # Log and save results
            self.logger.info(f"Analysis complete. Results stored at {self.config['output_data']['analysis_6']}")
            self.save_results(result_df, self.config['output_data']['analysis_6'])

        except Exception as e:
            # Log the error and raise it to fail the pipeline
            self.logger.error(f"Error occurred in Analysis6: {str(e)}", exc_info=True)
            raise  # Re-raise the exception to stop the pipeline
