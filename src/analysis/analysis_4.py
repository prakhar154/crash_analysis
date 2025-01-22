from src.analysis.base_analysis import BaseAnalysis
from pyspark.sql.functions import col, sum 

class Analysis4(BaseAnalysis):
    """
    Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
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


            # filter person data to remove records of people with no license and where the person is the driver 
            df_licensed_drivers = df_person.filter((col('DRVR_LIC_TYPE_ID')!='NA') & (col('DRVR_LIC_TYPE_ID')!='UNLICENSED') & (col('PRSN_TYPE_ID').like("DRIVER")))

            # filter duplicate records and unit data to get records of hit and run 
            df_units = df_units.dropDuplicates()
            df_hnr = df_units.filter(col('VEH_HNR_FL')=='Y')

            # create hit and run df, joining person and units df on CRASH ID 
            df_licensed_drivers_hnr = df_licensed_drivers.select('CRASH_ID').join(df_hnr.select('CRASH_ID', 'VIN'), 'CRASH_ID', 'inner')

            # removing null, unknown and numerical formulas from number plates of vehicles
            df_licensed_drivers_hnr = df_licensed_drivers_hnr.filter((col('VIN').isNotNull()))

            excluded_values = ['0', 'UN', 'UNK', '1E+17', 'UNKOWN', 'UNKNOWN']

            df_licensed_drivers_hnr = df_licensed_drivers_hnr.filter(~(col('VIN').isin(excluded_values)))
            df_licensed_drivers_hnr = df_licensed_drivers_hnr.filter(~(col('VIN').contains('E+')))


            # number of Vehicles with driver having valid licences involved in hit and run 
            cnt = df_licensed_drivers_hnr.select('VIN').distinct().count()

            # Save this number in a dataframe
            result_df = self.spark.createDataFrame([(cnt,)], ["Number_of_Vehicles_with_driver_having_valid_licences_involved_in_hit_and_run"])



            # Log and save results
            self.logger.info(f"Number of Vehicles with driver having valid licences involved in hit and run {cnt}")
            self.save_results(result_df, self.config['output_data']['analysis_4'])
        except Exception as e:
            # Log the error and raise it to fail the pipeline
            self.logger.error(f"Error occurred in Analysis4: {str(e)}", exc_info=True)
            raise  # Re-raise the exception to stop the pipeline


