from src.analysis.base_analysis import BaseAnalysis
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window

class Analysis7(BaseAnalysis):
    """
    Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
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

            # filter out na ethinicity values 
            df_person = df_person.filter(
                ~(col('PRSN_ETHNICITY_ID') == 'NA') &
                ~(col('PRSN_ETHNICITY_ID') == 'OTHER') &
                ~(col('PRSN_ETHNICITY_ID') == 'UNKNOWN')
            )

            # filter out na body styles 
            df_units = df_units.filter(
                ~(col('VEH_BODY_STYL_ID') == 'NA') &
                ~(col('VEH_BODY_STYL_ID') == 'OTHER  (EXPLAIN IN NARRATIVE)') &
                ~(col('VEH_BODY_STYL_ID') == 'NOT REPORTED') &
                ~(col('VEH_BODY_STYL_ID') == 'UNKNOWN')
            )

            # join data to get body styles and ethinicity in a single df
            df_body_type_with_ethinicity = df_units.join(df_person.select('CRASH_ID', "PRSN_ETHNICITY_ID"), 'CRASH_ID', 'inner')

            # Group by BODY_STYLE and PRSN_ETHNICITY_ID to count occurrences
            df_body_type_with_ethinicity_cnt = df_body_type_with_ethinicity.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").agg(count("*").alias("count"))

            # window specification to rank ethnicities by count for each BODY_STYLE
            window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())

            # Add a row number based on the rank of count
            df_body_type_with_ethinicity_ranked = df_body_type_with_ethinicity_cnt.withColumn("rank", row_number().over(window_spec))


            # Filter to get the top ethnic group for each BODY_STYLE
            df_top_ethnic_group = df_body_type_with_ethinicity_ranked.filter(col("rank") == 1).select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")

            # Rename column for clarity in output
            result_df = df_top_ethnic_group.withColumnRenamed('VEH_BODY_STYL_ID', 'body_style').withColumnRenamed('PRSN_ETHNICITY_ID', 'top_ethinicity')

            # Log and save results
            self.logger.info(f"Analysis complete. Results stored at {self.config['output_data']['analysis_7']}")
            self.save_results(result_df, self.config['output_data']['analysis_7'])

        except Exception as e:
            # Log the error and raise it to fail the pipeline
            self.logger.error(f"Error occurred in Analysis7: {str(e)}", exc_info=True)
            raise  # Re-raise the exception to stop the pipeline
