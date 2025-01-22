from pyspark.sql import DataFrame, functions as F

class DataFrameProcessor:
    """
    A class to perform common DataFrame processing tasks such as calculating null statistics.
    This can be extended with more DataFrame processing methods.
    """

    def __init__(self, df: DataFrame):
        """
        Initializes the DataFrameProcessor with the DataFrame to be processed.

        Args:
            df (DataFrame): The input DataFrame to process.
        """
        self.df = df

    def get_null_stats(self) -> DataFrame:
        """
        Computes the null count and null percentage for each column in the DataFrame.

        Returns:
            DataFrame: A DataFrame with columns 'column_name', 'null_count', and 'null_percentage'.
        """
        total_rows = self.df.count()  # Total number of rows in the DataFrame

        # Calculate null count and null percentage for each column
        null_stats = [
            (
                col,
                self.df.filter(F.col(col).isNull()).count(),
                (self.df.filter(F.col(col).isNull()).count() / total_rows) * 100
            )
            for col in self.df.columns
        ]
        
        # Create a DataFrame from the null stats list
        null_stats_df = self.df.sparkSession.createDataFrame(
            null_stats, ["column_name", "null_count", "null_percentage"]
        )

        return null_stats_df

    def show_null_stats(self) -> None:
        """
        Displays the null statistics of the DataFrame.

        This method uses the get_null_stats method to calculate and show the null stats.
        """
        null_stats_df = self.get_null_stats()
        null_stats_df.show(truncate=False)

    # Other DataFrame processing methods can be added here as needed
    # For example:
    def drop_nulls(self, subset: list = None) -> DataFrame:
        """
        Drops rows with null values in the specified columns or all columns.

        Args:
            subset (list, optional): List of columns to check for null values. Defaults to None, which checks all columns.

        Returns:
            DataFrame: A new DataFrame with rows containing null values dropped.
        """
        return self.df.dropna(subset=subset)

    def fill_nulls(self, value: dict) -> DataFrame:
        """
        Fills null values in the DataFrame with specified values for each column.

        Args:
            value (dict): A dictionary where keys are column names and values are the fill value for nulls.

        Returns:
            DataFrame: A new DataFrame with null values filled.
        """
        return self.df.fillna(value)

