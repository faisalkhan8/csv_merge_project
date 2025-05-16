#!/usr/bin/env python3
"""
FAC Data Integration Pipeline

This script fetches and merges Federal Audit Clearinghouse (FAC) data from SAM.gov,
using report_id as the primary key for alignment across different data types.
Optimized for handling very large datasets (500MB+) efficiently.
"""

import os
import logging
from datetime import datetime
import yaml
from pathlib import Path
import duckdb
import pandas as pd
from typing import Dict, List, Optional, Generator
import requests
from tqdm import tqdm
import sys
from contextlib import contextmanager
import gc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)

@contextmanager
def memory_tracker():
    """Track memory usage before and after operations."""
    import psutil
    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024 / 1024
    yield
    mem_after = process.memory_info().rss / 1024 / 1024
    logging.info(f"Memory usage: {mem_after - mem_before:.2f}MB")

class FACDataPipeline:
    CHUNK_SIZE = 100_000  # Process data in chunks of 100k rows
    
    def __init__(self, config_path: str = 'config.yaml'):
        """Initialize the FAC data pipeline with configuration."""
        self.config = self._load_config(config_path)
        self.temp_dir = Path(self.config['settings']['download_directory'])
        self.temp_dir.mkdir(exist_ok=True)
        
        # Use temporary directory for DuckDB
        self.db_path = self.temp_dir / "temp.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        
        # Enable compression for DuckDB
        self.conn.execute("PRAGMA enable_compression")
        self.conn.execute("PRAGMA memory_limit='4GB'")  # Adjust based on available RAM
        
    def _load_config(self, config_path: str) -> dict:
        """Load and validate configuration."""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self._validate_config(config)
        return config
    
    def _validate_config(self, config: dict) -> None:
        """Validate configuration structure and required fields."""
        required_settings = ['output_filename', 'download_directory', 'primary_join_key']
        if not all(key in config['settings'] for key in required_settings):
            raise ValueError(f"Missing required settings: {required_settings}")
    
    def fetch_data_stream(self, source_config: dict) -> Generator[pd.DataFrame, None, None]:
        """Stream data from API in chunks to handle large datasets."""
        api_key = os.getenv('SAM_API_KEY', 'DEMO_KEY')
        headers = {
            'accept': 'application/json',
            'x-api-key': api_key
        }
        
        params = source_config['api_params'].copy()
        total_records = 0
        
        while True:
            try:
                response = requests.get(
                    source_config['url'],
                    headers=headers,
                    params=params,
                    timeout=self.config['settings']['api_timeout_seconds'],
                    stream=True  # Enable streaming response
                )
                response.raise_for_status()
                data = response.json()
                
                if not data.get('results'):
                    break
                
                # Process in smaller chunks
                df = pd.DataFrame(data['results'])
                total_records += len(df)
                
                # Ensure report_id is string type
                if 'report_id' in df.columns:
                    df['report_id'] = df['report_id'].astype(str)
                
                yield df
                
                # Update pagination
                params['from'] += params['size']
                if len(data['results']) < params['size']:
                    break
                    
            except Exception as e:
                logging.error(f"Error fetching data: {str(e)}")
                raise
            
            finally:
                # Force garbage collection after each chunk
                gc.collect()
    
    def process_source(self, source_config: dict) -> None:
        """Process a single data source with memory-efficient streaming."""
        table_name = source_config['name']
        logging.info(f"Processing {table_name}...")
        
        # Create table with first chunk to establish schema
        first_chunk = True
        rows_processed = 0
        
        with memory_tracker():
            for chunk in self.fetch_data_stream(source_config):
                if first_chunk:
                    # Create table with optimal types
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    self.conn.execute(f"""
                        CREATE TABLE {table_name} AS 
                        SELECT * FROM chunk
                    """)
                    first_chunk = False
                else:
                    # Append to existing table
                    self.conn.execute(f"""
                        INSERT INTO {table_name}
                        SELECT * FROM chunk
                    """)
                
                rows_processed += len(chunk)
                logging.info(f"Processed {rows_processed:,} rows for {table_name}")
    
    def merge_data(self) -> None:
        """Merge all data sources using report_id as the key with optimization for large datasets."""
        try:
            # Process each source
            for source in self.config['sources']:
                self.process_source(source)
            
            # Build the merge query
            tables = [s['name'] for s in self.config['sources']]
            primary_key = self.config['settings']['primary_join_key']
            
            # Create the merged table incrementally
            merged_table = "merged_data"
            self.conn.execute(f"DROP TABLE IF EXISTS {merged_table}")
            
            query = f"""
            CREATE TABLE {merged_table} AS
            WITH incremental_join AS (
                SELECT * FROM {tables[0]}
            """
            
            for table in tables[1:]:
                query += f"""
                LEFT JOIN {table} 
                USING ({primary_key})
                """
            
            query += ")"
            
            # Execute merge in chunks
            self.conn.execute(query)
            
            # Export in chunks
            output_path = Path(self.config['settings']['output_filename'])
            chunk_size = self.CHUNK_SIZE
            
            logging.info("Exporting merged data...")
            self.conn.execute(f"""
                COPY (SELECT * FROM {merged_table}) 
                TO '{output_path}' 
                WITH (FORMAT 'csv', HEADER true, COMPRESSION 'gzip')
            """)
            
        except Exception as e:
            logging.error(f"Error during merge: {str(e)}")
            raise
            
        finally:
            # Cleanup
            if self.config['settings'].get('cleanup_temp_files', True):
                self.conn.close()
                if self.db_path.exists():
                    self.db_path.unlink()
                for file in self.temp_dir.glob('*.csv'):
                    file.unlink()
                if self.temp_dir.exists():
                    self.temp_dir.rmdir()
                logging.info("Cleaned up temporary files")

def main():
    """Execute the FAC data pipeline."""
    try:
        pipeline = FACDataPipeline()
        pipeline.merge_data()
        logging.info("Pipeline completed successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()