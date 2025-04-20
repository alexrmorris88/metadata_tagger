"""
Override handler module for managing manual tagging overrides.
"""

import csv
import json
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class OverrideHandler:
    """Handles manual tagging overrides from files"""
    
    def load_from_csv(self, filepath: str) -> Dict[str, str]:
        """
        Load override mappings from a CSV file
        Expected format: schema,table,column,tag
        Or: database,schema,table,column,tag
        """
        overrides = {}
        try:
            with open(filepath, 'r') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                
                # Check if 'database' is in the fieldnames
                has_database = 'database' in fieldnames if fieldnames else False
                
                for row in reader:
                    if has_database and 'database' in row:
                        # Create both keys - with and without database
                        db_key = f"{row['database']}.{row['schema']}.{row['table']}.{row['column']}".lower()
                        key = f"{row['schema']}.{row['table']}.{row['column']}".lower()
                        overrides[db_key] = row['tag']
                        overrides[key] = row['tag']  # Add simplified key too
                    else:
                        key = f"{row['schema']}.{row['table']}.{row['column']}".lower()
                        overrides[key] = row['tag']
                        
            logger.info(f"Loaded {len(overrides)} overrides from CSV file: {filepath}")
            return overrides
        except Exception as e:
            logger.error(f"Error loading CSV overrides from {filepath}: {e}")
            return {}
    
    def load_from_json(self, filepath: str) -> Dict[str, str]:
        """
        Load override mappings from a JSON file
        Expected format: {"schema.table.column": "tag", ...}
        Or: {"database.schema.table.column": "tag", ...}
        """
        try:
            with open(filepath, 'r') as f:
                json_overrides = json.load(f)
                
                # Process keys to handle both formats
                overrides = {}
                for key, value in json_overrides.items():
                    key_lower = key.lower()
                    overrides[key_lower] = value
                    
                    # Parse the key to handle database-qualified names
                    parts = key_lower.split('.')
                    if len(parts) == 4:  # database.schema.table.column
                        # Add simplified key too (without database)
                        simplified_key = f"{parts[1]}.{parts[2]}.{parts[3]}"
                        overrides[simplified_key] = value
                        logger.info(f"Added simplified override key: {simplified_key} from {key_lower}")
            
            logger.info(f"Loaded {len(json_overrides)} overrides from JSON file: {filepath}")
            return overrides
        except Exception as e:
            logger.error(f"Error loading JSON overrides from {filepath}: {e}")
            return {}
    
    def save_to_csv(self, overrides: Dict[str, str], filepath: str) -> bool:
        """
        Save override mappings to a CSV file
        """
        try:
            # Convert the dict of "schema.table.column" -> "tag" to rows with separate columns
            rows = []
            for key, tag in overrides.items():
                parts = key.split('.')
                if len(parts) == 3:  # schema.table.column
                    rows.append({
                        'schema': parts[0],
                        'table': parts[1],
                        'column': parts[2],
                        'tag': tag
                    })
                elif len(parts) == 4:  # database.schema.table.column
                    rows.append({
                        'database': parts[0],
                        'schema': parts[1],
                        'table': parts[2],
                        'column': parts[3],
                        'tag': tag
                    })
                
            with open(filepath, 'w', newline='') as f:
                # Determine fieldnames based on the first row
                fieldnames = list(rows[0].keys()) if rows else ['schema', 'table', 'column', 'tag']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            
            logger.info(f"Saved {len(rows)} overrides to CSV file: {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error saving CSV overrides to {filepath}: {e}")
            return False
    
    def save_to_json(self, overrides: Dict[str, str], filepath: str) -> bool:
        """
        Save override mappings to a JSON file
        """
        try:
            with open(filepath, 'w') as f:
                json.dump(overrides, f, indent=2)
            
            logger.info(f"Saved {len(overrides)} overrides to JSON file: {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error saving JSON overrides to {filepath}: {e}")
            return False
    
    def add_override(self, overrides: Dict[str, str], schema: str, table: str, column: str, tag: str, 
                     database: str = None) -> Dict[str, str]:
        """
        Add a new override to the dictionary
        """
        if database:
            key = f"{database}.{schema}.{table}.{column}".lower()
            simplified_key = f"{schema}.{table}.{column}".lower()
            overrides[key] = tag
            overrides[simplified_key] = tag  # Add simplified key too
            logger.info(f"Added override with database: {key} -> {tag}")
        else:
            key = f"{schema}.{table}.{column}".lower()
            overrides[key] = tag
            logger.info(f"Added override: {key} -> {tag}")
        
        return overrides
    
    def remove_override(self, overrides: Dict[str, str], schema: str, table: str, column: str, 
                        database: str = None) -> Dict[str, str]:
        """
        Remove an override from the dictionary
        """
        keys_to_remove = []
        
        if database:
            full_key = f"{database}.{schema}.{table}.{column}".lower()
            simplified_key = f"{schema}.{table}.{column}".lower()
            keys_to_remove.extend([full_key, simplified_key])
        else:
            key = f"{schema}.{table}.{column}".lower()
            keys_to_remove.append(key)
            
            # Also look for any database-qualified keys that match
            for existing_key in list(overrides.keys()):
                parts = existing_key.split('.')
                if len(parts) == 4 and parts[1] == schema.lower() and parts[2] == table.lower() and parts[3] == column.lower():
                    keys_to_remove.append(existing_key)
        
        # Remove all identified keys
        for key in keys_to_remove:
            if key in overrides:
                del overrides[key]
                logger.info(f"Removed override: {key}")
        
        if not keys_to_remove:
            logger.warning(f"Override not found for removal: {schema}.{table}.{column}")
            
        return overrides