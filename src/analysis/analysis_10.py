from src.analysis.base_analysis import BaseAnalysis
from pyspark.sql.functions import col, sum, lower, count

class Analysis10(BaseAnalysis):
    """
    Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, 
    used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    """

    def get_top_10_colors(self, df_units):
        """
        function to get top 10 vehicle colors 
        param: units data
        returns: list of top 10 colors
        """

        # dropping NA values from color
        df_valid_color = df_units.filter(col('VEH_COLOR_ID')!='NA')

        # top 10 colors
        df_top_10_colors = df_valid_color.groupBy(
            col('VEH_COLOR_ID')).agg(count("*").alias('cnt')).select('VEH_COLOR_ID', 'cnt').orderBy(col('cnt').desc()).limit(10)

        # Collect the top 10 color IDs and counts into a list
        top_10_colors = df_top_10_colors.collect()

        # Create a list of just the VEH_COLOR_ID values
        top_10_color_ids_list = [row['VEH_COLOR_ID'] for row in top_10_colors]

        return top_10_color_ids_list

    def get_top_25_states(self, df_units):
        """
        Top 25 states with highest number of offences
        param: units data
        returns: list of top 25 states
        """

        # top 25 states with highest number of offences 
        df_top_25_states = df_units.filter((col('VEH_LIC_STATE_ID')!='NA') & (col('VEH_LIC_STATE_ID')!='98'))
        df_top_25_states = df_top_25_states.groupBy('VEH_LIC_STATE_ID').agg(count("*").alias('cnt')).select('VEH_LIC_STATE_ID').orderBy(col('cnt').desc()).limit(25)

        # Collect the top 25 states and their counts
        top_25_states = df_top_25_states.collect()

        # Create a list of just the VEH_LIC_STATE_ID values
        top_25_state_ids_list = [row['VEH_LIC_STATE_ID'] for row in top_25_states]

        return top_25_state_ids_list


    def run(self):
        try:
            # Load data

            # person data
            person_path = self.config['input_data']['primary_person_data']
            df_person = self.load_data(person_path)

            # unit data
            units_path = self.config['input_data']['units_data']
            df_units = self.load_data(units_path)


            # charges data 
            charges_path = self.config['input_data']['charges_data']
            df_charges = self.load_data(charges_path)            


            # filter person data to remove records of people with no license and where the person is the driver 
            df_person_licensed = df_person.select('CRASH_ID', 'DRVR_LIC_TYPE_ID', 'PRSN_TYPE_ID').filter(
                (col('DRVR_LIC_TYPE_ID')!='NA') & 
                (col('DRVR_LIC_TYPE_ID')!='UNLICENSED') & 
                (col('PRSN_TYPE_ID').like("DRIVER")))

            # crash id of people drivers with license 
            df_person_licensed_charges = df_person_licensed.select('CRASH_ID').distinct()

            # filter charges data for speeding related offences 
            df_speed_charges = df_charges.filter(lower(col('CHARGE')).contains('speed')).select('CRASH_ID').distinct()


            # crash ids of licensed drivers with speed related offence
            df_speed_related_licenses = df_person_licensed_charges.join(
                df_speed_charges, on='CRASH_ID', how='inner'
            )

            # selecting one record of a unit in a single crash
            df_units = df_units.dropDuplicates(['CRASH_ID', 'UNIT_NBR'])

            # get top 10 vehicle colors
            top_10_color_ids_list = self.get_top_10_colors(df_units)
            
            # get top 25 states with most offences 
            top_25_state_ids_list = self.get_top_25_states(df_units)

            df_vehicle_makes = df_units.filter(~(col('VEH_MAKE_ID')=='NA')).select('CRASH_ID', 'VEH_COLOR_ID', 'VEH_LIC_STATE_ID', 'VEH_MAKE_ID')

            # get all relevant crash ids based on speed offence and driver license 
            df_vehicle_makes_crash_ids = df_vehicle_makes.join(df_speed_related_licenses, 'CRASH_ID', 'inner')
            
            #filter data to get vehicles with top 5 colors and top 25 states 
            df_vehicle_makes_crash_ids = df_vehicle_makes_crash_ids.filter((col('VEH_COLOR_ID').isin(top_10_color_ids_list)) & 
                (col('VEH_LIC_STATE_ID').isin(top_25_state_ids_list))).select('VEH_MAKE_ID')

            # top 5 veh makes based on crashes 
            df_top_5_veh_makes = df_vehicle_makes_crash_ids.groupBy('VEH_MAKE_ID').agg(count('*').alias('cnt')).select('VEH_MAKE_ID').orderBy(col('cnt').desc()).limit(5)

            df_top_5_veh_makes = df_top_5_veh_makes.withColumnRenamed('VEH_MAKE_ID', 'Top 5 Vehicle Makes')

            # Log and save results
            self.logger.info(f"Analysis complete. Results stored at {self.config['output_data']['analysis_10']}")
            self.save_results(df_top_5_veh_makes, self.config['output_data']['analysis_10'])
        except Exception as e:
            # Log the error and raise it to fail the pipeline
            self.logger.error(f"Error occurred in Analysis10: {str(e)}", exc_info=True)
            raise  # Re-raise the exception to stop the pipeline


