"""
Main entry point for the Metadata Database Tagger.
"""

import os
import json
import logging
import argparse
from typing import Dict, List, Optional, Any

from connectors.base import DatabaseConnector
from connectors.snowflake import SnowflakeConnector
# Import other connectors as they're implemented
# from connectors.azure import AzureSQLConnector
# from connectors.bigquery import BigQueryConnector
# from connectors.databricks import DatabricksConnector

from detection.detector import PIIDetector
from detection.rule_loader import RuleLoader
from utils.override_handler import OverrideHandler
from utils.export import export_results

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_connector(db_type: str, config: Dict[str, str]) -> DatabaseConnector:
    """Factory function to create the appropriate database connector"""
    if db_type.lower() == 'snowflake':
        return SnowflakeConnector(config)
    # Uncomment as other connectors are implemented
    # elif db_type.lower() == 'azure_sql':
    #     return AzureSQLConnector(config)
    # elif db_type.lower() == 'bigquery':
    #     return BigQueryConnector(config)
    # elif db_type.lower() == 'databricks':
    #     return DatabricksConnector(config)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def process_database(connector: DatabaseConnector, detector: PIIDetector, rule_loader: RuleLoader,
                     overrides: Dict[str, str], schemas: Optional[List[str]] = None,
                     sample_size: int = 100) -> Dict[str, List[Dict[str, str]]]:
    """
    Process the database and assign tags to columns
    Returns a dictionary of applied tags
    """
    # Connect to the database
    connector.connect()
    
    # Get tag configuration from rule loader
    tag_name = rule_loader.get_tag_name()
    tag_schema = rule_loader.get_tag_schema()
    
    # Get the database name from the connector if available
    database_name = connector.config.get('database', '')
    
    logger.info(f"Using tag name: {tag_name} and tag schema: {tag_schema or 'default'}")
    
    results = {}
    
    try:
        # Get schemas to process
        if not schemas:
            schemas = connector.get_schemas()
        
        # Process each schema
        for schema in schemas:
            logger.info(f"Processing schema: {schema}")
            results[schema] = []
            
            # Get tables in schema
            tables = connector.get_tables(schema)
            
            # Process each table
            for table in tables:
                logger.info(f"Processing table: {schema}.{table}")
                
                # Get columns in table
                columns = connector.get_columns(schema, table)
                
                # Process each column
                for column_info in columns:
                    column_name = column_info['name']
                    logger.info(f"Processing column: {schema}.{table}.{column_name}")
                    
                    # Get sample data
                    sample_data = connector.get_sample_data(schema, table, column_name, sample_size)
                    
                    # Create different column key formats for override lookup
                    column_key = f"{schema}.{table}.{column_name}".lower()
                    db_column_key = f"{database_name}.{schema}.{table}.{column_name}".lower() if database_name else None
                    
                    # Check for overrides using both key formats
                    tag_value = None
                    if db_column_key and db_column_key in overrides:
                        # First try with the database prefix
                        tag_value = overrides.get(db_column_key)
                        logger.info(f"Found database-qualified override for {db_column_key}: {tag_value}")
                    elif column_key in overrides:
                        # Then try without database prefix
                        tag_value = overrides.get(column_key)
                        logger.info(f"Found override for {column_key}: {tag_value}")
                        
                    if tag_value:
                        # We have an override, apply it directly
                        success = connector.apply_tag(schema, table, column_name, tag_name, tag_value, tag_schema)
                        
                        if success:
                            result = {
                                'schema': schema,
                                'table': table,
                                'column': column_name,
                                'tag_name': tag_name, 
                                'tag_value': tag_value,
                                'reason': "Manual override"
                            }
                            results[schema].append(result)
                    else:
                        # No override, use the detector
                        tag_info = detector.get_tag_for_column(column_name, sample_data)
                        
                        if tag_info:
                            tag_value, reason = tag_info
                            # Apply tag to column using tag name from configuration
                            success = connector.apply_tag(schema, table, column_name, tag_name, tag_value, tag_schema)
                            
                            if success:
                                result = {
                                    'schema': schema,
                                    'table': table,
                                    'column': column_name,
                                    'tag_name': tag_name,
                                    'tag_value': tag_value,
                                    'reason': reason
                                }
                                results[schema].append(result)
    finally:
        # Close the connection
        connector.close()
    
    return results

def main():
    """Main entry point for the script"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Metadata Data Tagger for database columns')
    parser.add_argument('--config', default='config/database_config.json', 
                        help='Path to database configuration file (JSON)')
    parser.add_argument('--rules', default='config/tag_rules.yaml', 
                        help='Path to tag rules configuration file (YAML)')
    parser.add_argument('--db-type', default='snowflake', 
                        choices=['snowflake', 'azure_sql', 'bigquery', 'databricks'], 
                        help='Database type')
    parser.add_argument('--db-name', 
                        help='Name of the database to process (as defined in config file)')
    parser.add_argument('--schemas', nargs='+', 
                        help='Schemas to process (default: all schemas)')
    parser.add_argument('--override', default='config/overrides.json', 
                        help='Path to override file (CSV or JSON)')
    parser.add_argument('--override-format', default='json', choices=['json', 'csv'], 
                        help='Override file format')
    parser.add_argument('--sample-size', type=int, default=100, 
                        help='Number of sample rows to check per column')
    parser.add_argument('--output', default='tagging_results.json', 
                        help='Output file for tagging results')
    parser.add_argument('--output-format', default='json', choices=['json', 'csv'], 
                        help='Output file format')
    
    args = parser.parse_args()
    
    try:
        # Load database configuration
        with open(args.config, 'r') as f:
            db_configs = json.load(f)
        
        # Create rule loader and detector
        rule_loader = RuleLoader(args.rules)
        detector = PIIDetector(rule_loader)
        
        # Load tag overrides
        override_handler = OverrideHandler()
        if os.path.exists(args.override):
            overrides = (override_handler.load_from_csv(args.override) 
                        if args.override_format.lower() == 'csv' 
                        else override_handler.load_from_json(args.override))
            logger.info(f"Loaded {len(overrides)} override mappings from {args.override}")
        else:
            overrides = {}
            logger.info(f"No override file found at {args.override}, proceeding without overrides")
        
        # Handle multiple database configurations
        all_results = {}
        
        # Determine which databases to process
        databases_to_process = []
        
        if 'databases' in db_configs:
            # New multi-database config format
            if args.db_name:
                # Process only the specified database
                for db in db_configs['databases']:
                    if db['name'] == args.db_name:
                        databases_to_process.append(db)
                        break
                if not databases_to_process:
                    raise ValueError(f"Database '{args.db_name}' not found in configuration")
            else:
                # Use the default database if defined, otherwise process all
                default_db_name = db_configs.get('default_database')
                if default_db_name:
                    for db in db_configs['databases']:
                        if db['name'] == default_db_name:
                            databases_to_process.append(db)
                            break
                else:
                    databases_to_process = db_configs['databases']
        else:
            # Legacy single database config format
            databases_to_process = [{'name': 'default', 'config': db_configs}]
        
        # Process each database
        for db_config in databases_to_process:
            logger.info(f"Processing database: {db_config['name']}")
            
            # Create connector for this database
            connector = create_connector(args.db_type, db_config['config'])
            
            # Process this database - pass rule_loader to process_database
            results = process_database(connector, detector, rule_loader, overrides, args.schemas, args.sample_size)
            
            # Store results for this database
            all_results[db_config['name']] = results
        
        # Export combined results
        # Flatten nested dictionary for export
        flat_results = []
        for db_name, db_results in all_results.items():
            for schema, schema_results in db_results.items():
                for result in schema_results:
                    # Add database name to each result
                    result['database'] = db_name
                    flat_results.append(result)
        
        # Use the export function but with our flattened structure
        if args.output_format.lower() == 'json':
            with open(args.output, 'w') as f:
                json.dump(flat_results, f, indent=2)
        else:  # CSV format
            import csv
            with open(args.output, 'w', newline='') as f:
                if flat_results:
                    fieldnames = flat_results[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(flat_results)
                else:
                    writer = csv.writer(f)
                    writer.writerow(['No results found'])
        
        logger.info(f"Successfully processed {len(databases_to_process)} database(s) and saved results to {args.output}")
        print(f"Successfully processed {len(databases_to_process)} database(s) and saved results to {args.output}")
    
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        print(f"Error: File not found: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        print(f"Error: Invalid JSON in configuration file: {e}")
    except ImportError as e:
        logger.error(f"Dependency error: {e}")
        print(f"Error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"Unexpected error occurred: {e}")
        print("Check the logs for more details.")

if __name__ == "__main__":
    main()