import pandas as pd
import os
import json
import yaml
import argparse
from typing import Dict
from google.cloud import bigquery
from datetime import datetime

def load_column_mappings(mapping_file: str) -> Dict:
    """Load column mappings from YAML/JSON file."""
    try:
        with open(mapping_file, 'r') as f:
            if mapping_file.endswith(('.yaml', '.yml')):
                return yaml.safe_load(f)
            elif mapping_file.endswith('.json'):
                return json.load(f)
            else:
                raise ValueError("Mapping file must be either YAML or JSON")
    except Exception as e:
        raise Exception(f"Error loading mapping file: {str(e)}")

def validate_mapping_config(mapping: Dict, filename: str) -> bool:
    """Validate mapping configuration for a file."""
    if not isinstance(mapping, dict):
        return False
    if 'columns' not in mapping:
        return False
    if not isinstance(mapping['columns'], dict):
        return False
    if 'attributes' in mapping and not isinstance(mapping['attributes'], dict):
        return False
    return True

def process_files(directory: str, mapping_file: str, project_id: str, dataset_id: str, table_id: str) -> Dict:
    """
    Process files with specified column mappings and load to BigQuery.
    Returns a dictionary with detailed statistics for each file.
    """
    client = bigquery.Client(project=project_id)
    table_ref = f'{project_id}.{dataset_id}.{table_id}'
    mappings = load_column_mappings(mapping_file)
    stats = {}
    
    schema = [
        bigquery.SchemaField("Id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FirstName", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("LastName", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Mobile", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("PostCode", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("DataSource", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SourceCreatedDate", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SourceModifiedDate", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SourceFile", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Attributes", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("Key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Value", "STRING", mode="NULLABLE")
        ]),
        bigquery.SchemaField("BQInsertedDate", "TIMESTAMP", mode="REQUIRED")
    ]
    
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)

        stats[filename] = {
            'total_rows': 0,
            'processed_rows': 0,
            'failed_rows': 0,
            'status': 'not_processed',
            'error_message': None,
            'start_time': datetime.now().isoformat(),
            'end_time': None
        }
        
        if not filename.endswith(('.xlsx', '.csv')):
            stats[filename]['status'] = 'skipped'
            stats[filename]['error_message'] = 'Not a supported file type'
            continue
        
        if filename not in mappings:
            stats[filename]['status'] = 'skipped'
            stats[filename]['error_message'] = 'No mapping configuration found'
            continue
        
        file_mapping = mappings[filename]
        
        if not validate_mapping_config(file_mapping, filename):
            stats[filename]['status'] = 'failed'
            stats[filename]['error_message'] = 'Invalid mapping configuration'
            continue
        
        try:
            if filename.endswith(".xlsx"):
                df = pd.read_excel(filepath)
            elif filename.endswith(".csv"):
                df = pd.read_csv(filepath)
            
            stats[filename]['total_rows'] = len(df)
            
            # Initialise the new DataFrame with the same number of rows as the input file
            new_df = pd.DataFrame(index=range(len(df)))
            
            # Fill in defauls for dataframe including required columns
            new_df['DataSource'] = file_mapping.get('data_source', 'unknown')
            new_df['SourceCreatedDate'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            new_df['SourceModifiedDate'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            new_df['SourceFile'] = filename
            new_df['BQInsertedDate'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            new_df['Id'] = None
            new_df['FirstName'] = None
            new_df['LastName'] = None
            new_df['Email'] = None
            new_df['Mobile'] = None
            new_df['PostCode'] = None
            
            for source_col, target_col in file_mapping['columns'].items():
                if source_col in df.columns:
                    new_df[target_col] = df[source_col]
                else:
                    print(f"Warning: Column {source_col} not found in {filename}")
            
            attributes_list = []
            
            if 'attributes' in file_mapping:
                for idx in range(len(df)):
                    row_attributes = []
                    for source_col, attr_name in file_mapping['attributes'].items():
                        if source_col in df.columns:
                            value = df[source_col].iloc[idx]
                            if pd.notna(value):
                                row_attributes.append({
                                    'Key': attr_name,
                                    'Value': str(value)
                                })
                    attributes_list.append(row_attributes)
            else:
                attributes_list = [[] for _ in range(len(df))]
            
            new_df['Attributes'] = attributes_list
            
            json_rows = []
            for idx in range(len(new_df)):
                row_dict = new_df.iloc[idx].to_dict()
                for key in row_dict:
                    if key not in ['BQInsertedDate', 'SourceFile', 'Attributes']:
                        if isinstance(row_dict[key], pd.Series) or isinstance(row_dict[key], pd.DataFrame):
                            row_dict[key] = None
                        elif pd.isna(row_dict[key]):
                            row_dict[key] = None
                json_rows.append(row_dict)
            
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
            
            job = client.load_table_from_json(json_rows, table_ref, job_config=job_config)
            job.result()
            
            stats[filename]['status'] = 'success'
            stats[filename]['processed_rows'] = len(json_rows)
            
        except Exception as e:
            stats[filename]['status'] = 'failed'
            stats[filename]['error_message'] = str(e)
            print(f"Error processing file {filename}: {e}")
        
        finally:
            stats[filename]['end_time'] = datetime.now().isoformat()
    
    return stats

def save_statistics(stats: Dict, output_file: str):
    """Save processing statistics to a file."""
    with open(output_file, 'w') as f:
        json.dump(stats, f, indent=2)

def main():
    parser = argparse.ArgumentParser(description='Process files and load to BigQuery')
    parser.add_argument('--directory', required=True, help='Directory containing the files to process')
    parser.add_argument('--mapping-file', required=True, help='YAML/JSON file containing column mappings')
    parser.add_argument('--project-id', help='BigQuery project ID')
    parser.add_argument('--dataset-id', help='BigQuery dataset ID')
    parser.add_argument('--table-id', help='BigQuery table ID')
    parser.add_argument('--output-file', default='processing_stats.json', help='Output file for processing statistics')
    
    args = parser.parse_args()
    
    try:
        stats = process_files(
            args.directory,
            args.mapping_file,
            args.project_id,
            args.dataset_id,
            args.table_id
        )
        
        save_statistics(stats, args.output_file)
        
        print("\nProcessing Summary:")
        for filename, file_stats in stats.items():
            print(f"\nFile: {filename}")
            print(f"Status: {file_stats['status']}")
            print(f"Total rows: {file_stats['total_rows']}")
            print(f"Processed rows: {file_stats['processed_rows']}")
            print(f"Failed rows: {file_stats['failed_rows']}")
            if file_stats['error_message']:
                print(f"Error: {file_stats['error_message']}")
                    
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()