#!/usr/bin/env python3

"""
Snowflake Policy Manager - A tool for managing Snowflake security policies.
"""

import argparse
import logging
import os
import json
import sys
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Import from existing project
from connectors.snowflake import SnowflakeConnector

# Import policy manager modules
from policy_manager.policy_loader import PolicyLoader
from policy_manager.policy_engine import PolicyEngine
from policy_manager.policy_applier import PolicyApplier

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_connector(db_type: str, config: Dict[str, str]):
    """Factory function to create the appropriate database connector (reused from main.py)"""
    if db_type.lower() == 'snowflake':
        return SnowflakeConnector(config)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def load_environment_variables(env_file=".env"):
    """Load environment variables from .env file if it exists"""
    if os.path.exists(env_file):
        logger.info(f"Loading environment variables from {env_file}")
        # Force reload to ensure variables are available
        load_dotenv(env_file, override=True)
        
        # Verify critical environment variables
        critical_vars = ['SNOWFLAKE_DATABASE', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
        for var in critical_vars:
            if not os.environ.get(var):
                logger.warning(f"Critical environment variable '{var}' not found in {env_file}")
    else:
        logger.warning(f"Environment file {env_file} not found, using existing environment variables")

def main():
    """Main entry point for the Snowflake Policy Manager"""
    parser = argparse.ArgumentParser(description='Snowflake Policy Manager')
    parser.add_argument('--config', default='config/database_config.json',
                        help='Path to database configuration file (JSON)')
    parser.add_argument('--policy-config', default='config/policy_config.yaml',
                        help='Path to policy configuration file (YAML)')
    parser.add_argument('--db-name', help='Name of the database to use (as defined in config file)')
    parser.add_argument('--apply', action='store_true', help='Apply policies to the database')
    parser.add_argument('--validate', action='store_true', help='Validate policy configuration without applying')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--env-file', default='.env', help='Path to environment variables file')
    parser.add_argument('--schema', help='Schema to use for policy creation (overrides dynamic detection)')
    
    # Policy-specific options
    parser.add_argument('--row-access-only', action='store_true', help='Apply only row access policies')
    parser.add_argument('--masking-only', action='store_true', help='Apply only masking policies')
    parser.add_argument('--tags-only', action='store_true', help='Apply only tag policies')
    parser.add_argument('--pii-only', action='store_true', help='Apply only PII detection')
    
    args = parser.parse_args()
    
    # Set log level based on verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load environment variables - do this first!
    load_environment_variables(args.env_file)
    
    try:
        # Load policies
        policy_loader = PolicyLoader(args.policy_config)
        policies = policy_loader.load_policies()
        
        if args.validate:
            logger.info(f"Policy configuration validated successfully for {len(policies.get('row_access', []))} row access policies, "
                       f"{len(policies.get('category_policies', []))} category policies")
            return 0
        
        if not args.apply:
            logger.info("No action requested. Use --apply to apply policies or --validate to validate configuration.")
            parser.print_help()
            return 0
        
        # Load database configuration
        with open(args.config, 'r') as f:
            db_configs = json.load(f)
        
        # Determine which database to use
        selected_db = None
        if 'databases' in db_configs:
            if args.db_name:
                # Use the specified database
                for db in db_configs['databases']:
                    if db['name'] == args.db_name:
                        selected_db = db
                        break
                if not selected_db:
                    raise ValueError(f"Database '{args.db_name}' not found in configuration")
            else:
                # Use the default database
                default_db_name = db_configs.get('default_database')
                if not default_db_name:
                    raise ValueError("No database specified and no default database in configuration")
                
                for db in db_configs['databases']:
                    if db['name'] == default_db_name:
                        selected_db = db
                        break
                if not selected_db:
                    raise ValueError(f"Default database '{default_db_name}' not found in configuration")
        else:
            # Legacy single database config format
            selected_db = {'name': 'default', 'config': db_configs}
        
        logger.info(f"Using database configuration: {selected_db['name']}")
        
        # Check if the database name from the environment variable exists in the config
        # If not, update it with the environment variable
        database_name = os.environ.get('SNOWFLAKE_DATABASE')
        if database_name and 'database' in selected_db['config'] and selected_db['config']['database'] != database_name:
            # Use the environment variable value
            logger.info(f"Updating database name in configuration from {selected_db['config']['database']} to {database_name}")
            selected_db['config']['database'] = database_name
        elif 'database' in selected_db['config']:
            database_name = selected_db['config']['database']
        
        # Create Snowflake connector
        connector = create_connector('snowflake', selected_db['config'])
        connector.connect()
        
        try:
            # Create policy engine and applier
            policy_engine = PolicyEngine(connector)
            policy_applier = PolicyApplier(policy_engine)
            
            # If a specific schema was provided, set it as active
            if args.schema:
                logger.info(f"Setting active schema to: {args.schema}")
                policy_engine.set_active_schema(args.schema)
                # Also update the global policy schema setting
                if 'global' in policies:
                    policies['global']['policy_schema'] = args.schema
            
            # Make sure global policy settings are in sync with our actual database
            if 'global' in policies and database_name:
                # Update the database name in the policies to match what we're actually using
                if policies['global'].get('database') != database_name:
                    logger.info(f"Updating database name in policy config to {database_name}")
                    policies['global']['database'] = database_name
            
            # Determine which policies to apply
            if args.row_access_only:
                policy_applier.apply_row_access_policies(policies.get('row_access', []))
            elif args.masking_only:
                policy_applier.apply_category_policies(
                    policies.get('category_policies', []), 
                    database_name, 
                    policies.get('global', {}).get('default_tag', 'PII')
                )
            elif args.tags_only:
                policy_applier.apply_tag_policies(policies.get('tags', []))
            elif args.pii_only:
                policy_applier.apply_pii_detection(policies.get('pii_detection', {}))
            else:
                # Apply all policies
                policy_applier.apply_all_policies(policies)
                
            logger.info("Successfully applied policies to Snowflake")
            
        finally:
            # Close the connection
            connector.close()
        
        return 0
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())