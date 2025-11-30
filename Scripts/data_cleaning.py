import pandas as pd
import numpy as np

class DataCleaning:
    def handle_missing_values(self, df, strategy="mean"):
        if strategy == "mean":
            df.fillna(df.mean(numeric_only=True), inplace=True)
        elif strategy == "median":
            df.filna(df.median(numeric_only=True), inplace=True )
        elif strategy == "mode":
            df.fillna(df.mode().iloc[0], inplace=True)
        elif strategy == "drop":
            df.dropna(inplace=True)
        return df
    
    def remove_duplicates(self, df):
        return df.drop_duplicates()
    
    def fix_data_types(self, df):
        for col in df.columns:
            try:
                df[col]= pd.to_numeric(df[col])
            except ValueError:
                pass
        return df
    
    def normalize_data(self, df):
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = (df[numeric_cols] - df[numeric_cols].min()) / (df[numeric_cols].max() - df[numeric_cols].min())
        return df
    
    def clean_data(self, df, missing_value_strategy="mean"):
        df = self.handle_missing_values(df, strategy=missing_value_strategy)
        df = self.remove_duplicates(df)
        df = self.fix_data_types(df)
        df = self.normalize_data(df)
        print("Data cleaning completed successfully.")
        return df
