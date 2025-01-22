from src.analysis.base_analysis import BaseAnalysis
from pyspark.sql.functions import col, count, lower, when, regexp_extract

class Analysis9(BaseAnalysis):
    """
    Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    """
    def run(self):
        try:
            # Load data

            # unit data
            units_path = self.config['input_data']['units_data']
            df_units = self.load_data(units_path)

            damages_path = self.config['input_data']['damages_data']
            df_damages = self.load_data(damages_path)

            # drop duplicates
            df_units = df_units.dropDuplicates()

            # condition to check for no damage
            no_damage_condition = (
                (col("DAMAGED_PROPERTY").isNull()) |
                (lower(col("DAMAGED_PROPERTY")).contains("no damage")) |
                (lower(col("DAMAGED_PROPERTY")).contains("no dmg")) |
                (lower(col("DAMAGED_PROPERTY")) == "none")
            )

            # flag to indicate if a record has no damage
            df_damages_with_flag = df_damages.withColumn("NO_DAMAGE", when(no_damage_condition, 1).otherwise(0))

            # Group by CRASH_ID and check if all records for each CRASH_ID have NO_DAMAGE = 1
            df_damages_with_flag = df_damages_with_flag.groupBy("CRASH_ID").agg(
                count(when(col("NO_DAMAGE") == 0, 1)).alias("HAS_DAMAGE")
            ).filter(col("HAS_DAMAGE") == 0)

            # select crash ids
            df_crash_id = df_damages_with_flag.select("CRASH_ID")

            # Extract the numeric part from the "VEH_DMAG_SCL"
            df_units_with_scl_val = df_units.withColumn(
                "VEH_DMAG_SCL_1_VAL", 
                regexp_extract(col("VEH_DMAG_SCL_1_ID"), r'(\d+)', 0).cast('int')
            ).withColumn(
                "VEH_DMAG_SCL_2_VAL", 
                regexp_extract(col("VEH_DMAG_SCL_2_ID"), r'(\d+)', 0).cast('int')
            )


            # Filter rows where either VEH_DMAG_SCL >= 4
            df_units_with_scl_val = df_units_with_scl_val.filter(
                (col("VEH_DMAG_SCL_1_VAL") >= 4) | (col("VEH_DMAG_SCL_2_VAL") >= 4)
            )


            # filter for records with insurance claim 
            df_units_with_insurance = df_units_with_scl_val.filter((col("FIN_RESP_TYPE_ID").isNotNull()) & (col("FIN_RESP_TYPE_ID") != 'NA'))

            # unique crash id count 
            df_final_crash_ids = df_units_with_insurance.join(df_crash_id, 'CRASH_ID', 'inner')
            crash_id_cnt = df_final_crash_ids.select('CRASH_ID').distinct().count()


            # Save this number in a dataframe
            result_df = self.spark.createDataFrame([(crash_id_cnt,)], ["Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance"])
            
    
            # Log and save results
            self.logger.info(f"Analysis complete. Results stored at {self.config['output_data']['analysis_9']}")
            self.save_results(result_df, self.config['output_data']['analysis_9'])

        except Exception as e:
            # Log the error and raise it to fail the pipeline
            self.logger.error(f"Error occurred in Analysis9: {str(e)}", exc_info=True)
            raise  # Re-raise the exception to stop the pipeline
