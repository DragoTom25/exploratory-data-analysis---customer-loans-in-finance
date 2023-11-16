import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
from dateutil.parser import parse

class RDSDatabaseConnector:

        def __init__(self, yaml_file):
                self.yaml_file = yaml_file
                self.db_creds = self.read_db_creds()
                self.db_engine = self.init_db_engine()
                self.db_table_list = self.list_db_tables()

        def read_db_creds(self):
                with open(self.yaml_file, 'r') as f:
                        db_creds = yaml.safe_load(f)
                return db_creds
    
        def init_db_engine(self):
                db_engine = create_engine(f"postgresql://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}")
                return db_engine
        
        def list_db_tables(self):
                inspector = inspect(self.db_engine)
                table_names = inspector.get_table_names()
                return table_names
        
        def extract_loan_payments_data(self):
       
                table_name = "loan_payments"

       
                query = f"SELECT * FROM {table_name}"

        
                try:
                        data_frame = pd.read_sql_query(query, self.db_engine)
                        return data_frame
                except Exception as e:
                        print(f"Error extracting data from the database: {str(e)}")
                        return None
        def save_loan_payments_data_to_csv(self, file_path):
                loan_payments_data = self.extract_loan_payments_data()
                if loan_payments_data is not None:
                        try:
                                loan_payments_data.to_csv(file_path, index=False)
                                print(f"Data saved to {file_path}")
                        except Exception as e:
                                print(f"Error saving data to CSV file: {str(e)}")
                else:
                        print("No data to save.")
        def load_data_from_csv(self, file_path):
                try:
                        data_frame = pd.read_csv(file_path)
                        print(f"Data loaded from {file_path}")
                        return data_frame
                except Exception as e:
                        print(f"Error loading data from CSV file: {str(e)}")
                        return None
if __name__ == "__main__":
        yaml_file_path = r"C:\Users\ra184\AICore Financial Data\credentials.yaml"
        db_connector = RDSDatabaseConnector(yaml_file_path)

    # Specify the directory and file name for saving and loading the data
        csv_file_path = r"C:\Users\ra184\AICore Financial Data\loans_data.csv"

    # Save the data to a CSV file
        db_connector.save_loan_payments_data_to_csv(csv_file_path)

    # Load the data from the CSV file
        loan_payments_data = db_connector.load_data_from_csv(csv_file_path)

if loan_payments_data is not None:
        # Print the shape of the data
        print("Data shape:", loan_payments_data.shape)

        # Print a sample of the data
        print("Sample of the data:")
        print(loan_payments_data.head())
