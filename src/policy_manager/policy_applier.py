"""
Policy applier module for applying Snowflake security policies.
"""

import logging
import re
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)

class PolicyApplier:
    """Applies Snowflake security policies based on configuration"""
    
    def __init__(self, policy_engine):
        """Initialize with a policy engine"""
        self.policy_engine = policy_engine
    
    def apply_all_policies(self, policies: Dict[str, Any]) -> bool:
        """Apply all policies from configuration"""
        success = True
        
        # Process global settings
        global_settings = policies.get('global', {})
        database = global_settings.get('database', '')
        admin_role = global_settings.get('admin_role', 'ACCOUNTADMIN')
        default_tag = global_settings.get('default_tag', 'PII')
        
        # Get the policy schema - if not specified, use the active schema
        policy_schema = global_settings.get('policy_schema')
        if not policy_schema:
            policy_schema = self.policy_engine.get_active_schema(database)
            if not policy_schema:  # Add check for None
                policy_schema = "PUBLIC"  # Default to PUBLIC if no schema detected
                logger.warning("No active schema detected, defaulting to PUBLIC")
            else:
                logger.info(f"Using active schema as policy schema: {policy_schema}")
            # Update the global settings with the active schema
            global_settings['policy_schema'] = policy_schema
        
        # Replace variables in policy expressions
        variables = {
            'admin_role': admin_role,
            'database': database,
            'policy_schema': policy_schema,
            'default_tag': default_tag
        }
        self._replace_variables(policies, variables)
        
        # Apply category-based policies
        if 'category_policies' in policies:
            success = success and self.apply_category_policies(
                policies['category_policies'], 
                database, 
                default_tag,
                global_settings
            )
        
        # Apply row access policies
        if 'row_access' in policies:
            success = success and self.apply_tag_based_row_access_policies(
                policies['row_access'], 
                database,
                default_tag
            )
        
        # Apply PII detection
        if 'pii_detection' in policies:
            success = success and self.apply_pii_detection(policies['pii_detection'])
        
        return success
    
    def _replace_variables(self, obj: Any, variables: Dict[str, str]) -> None:
        """Recursively replace variables in string values within an object"""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, (dict, list)):
                    self._replace_variables(value, variables)
                elif isinstance(value, str):
                    # First handle template variables with ${var_name} syntax
                    for var_name, var_value in variables.items():
                        # Skip None values to prevent TypeError
                        if var_value is None:
                            logger.warning(f"Variable '{var_name}' has None value, skipping replacement")
                            continue
                            
                        if f"${{{var_name}}}" in value:
                            obj[key] = value.replace(f"${{{var_name}}}", var_value)
                    
                    # Then handle simplified variable syntax $var_name
                    # Using regex to ensure we only replace whole variables and not partial matches
                    for var_name, var_value in variables.items():
                        # Skip None values to prevent TypeError
                        if var_value is None:
                            continue
                            
                        pattern = r'\$' + var_name + r'\b'
                        obj[key] = re.sub(pattern, var_value, obj[key])
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, (dict, list)):
                    self._replace_variables(item, variables)
                elif isinstance(item, str):
                    # First handle template variables with ${var_name} syntax
                    for var_name, var_value in variables.items():
                        # Skip None values to prevent TypeError
                        if var_value is None:
                            logger.warning(f"Variable '{var_name}' has None value, skipping replacement")
                            continue
                            
                        if f"${{{var_name}}}" in item:
                            obj[i] = item.replace(f"${{{var_name}}}", var_value)
                    
                    # Then handle simplified variable syntax $var_name
                    # Using regex to ensure we only replace whole variables and not partial matches
                    for var_name, var_value in variables.items():
                        # Skip None values to prevent TypeError
                        if var_value is None:
                            continue
                            
                        pattern = r'\$' + var_name + r'\b'
                        obj[i] = re.sub(pattern, var_value, obj[i])
    
    def apply_category_policies(self, category_policies: List[Dict[str, Any]], 
                              database: str, tag_name: str, global_settings: Dict[str, Any] = None) -> bool:
        """Apply masking policies based on tagged categories"""
        success = True
        global_settings = global_settings or {}
        
        logger.info(f"Applying category-based policies for {len(category_policies)} categories")
        
        for policy_config in category_policies:
            category = policy_config.get('category')
            if not category:
                logger.warning("Skipping policy with missing category")
                continue
                
            masking_policy = policy_config.get('masking_policy', {})
            if not masking_policy:
                logger.warning(f"No masking policy defined for category: {category}")
                continue
                
            # Get policy details
            policy_name = masking_policy.get('name')
            if not policy_name:
                # Generate a policy name from the category if not specified
                policy_name = self._sanitize_name(category)
                
            # Get policy schema from global settings or specific policy
            policy_schema = masking_policy.get('schema')
            if not policy_schema:
                # Use global policy schema setting
                policy_schema = global_settings.get('policy_schema', 'PUBLIC')
                
            comment = masking_policy.get('comment', '')
            data_types = masking_policy.get('data_types', {})
            
            if not policy_name or not policy_schema or not data_types:
                logger.warning(f"Incomplete masking policy configuration for category: {category}")
                continue
            
            # Create masking policies for each data type
            for data_type, expression in data_types.items():
                # Replace admin_role directly as a failsafe
                admin_role = global_settings.get('admin_role', 'ACCOUNTADMIN')
                
                # Special handling for expressions with $admin_role or ${admin_role}
                if '${admin_role}' in expression:
                    expression = expression.replace('${admin_role}', admin_role)
                elif '$admin_role' in expression:
                    expression = expression.replace('$admin_role', admin_role)
                elif "current_role() = 'ACCOUNTADMIN'" in expression:
                    # If using a hardcoded ACCOUNTADMIN, check if it needs to be replaced
                    if admin_role != 'ACCOUNTADMIN':
                        expression = expression.replace("current_role() = 'ACCOUNTADMIN'", f"current_role() = '{admin_role}'")
                
                if not self.policy_engine.create_category_masking_policy(
                    database, policy_schema, policy_name, data_type, expression, comment
                ):
                    success = False
                    continue
            
            # Get columns with this tag category
            tagged_columns = self.policy_engine.get_tagged_columns(database, tag_name, [category])
            
            # Apply appropriate masking policy to each column based on its data type
            for column in tagged_columns:
                # Normalize data type to match our policy data types
                col_data_type = self._normalize_data_type(column['data_type'])
                
                # Skip if we don't have a policy for this data type
                if col_data_type not in data_types:
                    logger.warning(f"No masking policy for data type {col_data_type} in category {category}")
                    continue
                
                # Apply the policy
                if not self.policy_engine.apply_masking_policy_by_data_type(
                    database, policy_schema, policy_name,
                    column['schema'], column['table'], column['column'],
                    col_data_type
                ):
                    success = False
        
        return success
    
    def _sanitize_name(self, name: str) -> str:
        """Convert a category or tag value to a valid policy name"""
        # Replace spaces and special characters with underscores
        sanitized = name.lower().replace(' ', '_').replace('-', '_')
        # Remove any non-alphanumeric characters except underscores
        sanitized = ''.join(c for c in sanitized if c.isalnum() or c == '_')
        # Ensure it starts with a letter
        if sanitized and not sanitized[0].isalpha():
            sanitized = 'policy_' + sanitized
        return sanitized
    
    def _normalize_data_type(self, data_type: str) -> str:
        """Normalize Snowflake data type to match our policy data types"""
        # Convert data type to uppercase
        upper_type = data_type.upper()
        
        # Handle VARCHAR variants
        if 'VARCHAR' in upper_type or 'CHAR' in upper_type or 'TEXT' in upper_type or 'STRING' in upper_type:
            return 'VARCHAR'
        
        # Handle numeric types
        if 'INT' in upper_type:
            return 'INTEGER'
        if 'NUMBER' in upper_type or 'NUMERIC' in upper_type or 'DECIMAL' in upper_type:
            return 'NUMBER'
        if 'FLOAT' in upper_type or 'DOUBLE' in upper_type or 'REAL' in upper_type:
            return 'NUMBER'  # Map to NUMBER which is Snowflake's general numeric type
        
        # Handle date/time types
        if 'DATE' in upper_type:
            return 'DATE'
        if 'TIME' in upper_type and 'TIMESTAMP' not in upper_type:
            return 'TIME'
        if 'TIMESTAMP' in upper_type:
            return 'TIMESTAMP'
        
        # Default case
        return upper_type
    
    def apply_tag_based_row_access_policies(self, row_policies: List[Dict[str, Any]], 
                                          database: str, tag_name: str) -> bool:
        """Apply row access policies to tables based on tagged columns"""
        success = True
        
        logger.info(f"Applying {len(row_policies)} tag-based row access policies")
        
        try:
            # First, check what tags are available in the database
            available_tags = self.policy_engine.list_available_tags(database)
            
            # Verify the specified tag exists
            if tag_name not in [tag.upper() for tag in available_tags]:
                logger.warning(f"Tag '{tag_name}' not found in database. Available tags: {available_tags}")
                logger.info("To check which columns are already tagged, you need to run the metadata tagging process first.")
                return False
        except Exception as e:
            logger.error(f"Error checking available tags: {e}")
            logger.info("Continuing without tag verification...")
        
        for policy in row_policies:
            # Get policy details
            name = policy.get('name')
            schema = policy.get('schema')
            policy_expr = policy.get('policy_expression')
            categories = policy.get('apply_to_categories', [])
            comment = policy.get('comment', '')
            
            if not name or not schema or not policy_expr or not categories:
                logger.warning("Incomplete row access policy configuration")
                continue
            
            # Create a policy in the policy schema - we'll use a single policy for all tables
            # rather than table-specific policies
            row_policy = {
                'name': name,
                'database': database,
                'schema': schema,
                'policy_schema': schema,  # Use the same schema for the policy
                'policy_expression': policy_expr,
                'comment': comment
            }
            
            if not self.policy_engine.create_row_access_policy(row_policy):
                success = False
                continue
            
            # Find tables with columns tagged with any of the specified categories
            tables = self.policy_engine.get_tables_with_tagged_columns(database, tag_name, categories)
            
            if not tables:
                logger.warning(f"No tables found with columns tagged with categories: {categories}")
                continue
            
            # Apply the policy to each table
            for table in tables:
                table_policy = {
                    'name': name,
                    'database': database,
                    'schema': table['schema'],
                    'table': table['table'],
                    'policy_schema': schema  # The schema where the policy is defined
                }
                
                if not self.policy_engine.apply_row_access_policy(table_policy):
                    success = False
        
        return success
    
    def apply_row_access_policies(self, policies: List[Dict[str, Any]]) -> bool:
        """Apply legacy row access policies (direct specification)"""
        success = True
        
        logger.info(f"Applying {len(policies)} row access policies")
        
        for policy in policies:
            # Create the policy
            if not self.policy_engine.create_row_access_policy(policy):
                success = False
                continue
            
            # Apply the policy to the specified table
            if not self.policy_engine.apply_row_access_policy(policy):
                success = False
        
        return success
    
    def apply_tag_policies(self, policies: List[Dict[str, Any]]) -> bool:
        """Apply tag policies"""
        # Legacy method - maintained for compatibility
        logger.warning("Tag policies method called but not implemented - use category policies instead")
        return True
    
    def apply_pii_detection(self, pii_config: Dict[str, Any]) -> bool:
        """Apply PII detection and tagging"""
        success = True
        
        if not pii_config:
            logger.info("No PII detection configuration found")
            return success
        
        rules = pii_config.get('rules', [])
        auto_tagging = pii_config.get('auto_tagging', {})
        
        if not rules:
            logger.warning("No PII detection rules found")
            return False
        
        logger.info(f"Running PII detection with {len(rules)} rule categories")
        
        # Run PII detection
        findings = self.policy_engine.run_pii_detection(rules, auto_tagging)
        
        # Apply tags if auto-tagging is enabled
        if auto_tagging.get('enabled', False):
            tagged_count = self.policy_engine.apply_pii_tags(findings, auto_tagging)
            logger.info(f"Auto-tagged {tagged_count} columns with PII tags")
        else:
            # Just log the findings
            total_findings = sum(len(schema_findings) for schema_findings in findings.values())
            logger.info(f"Found {total_findings} PII columns (auto-tagging disabled)")
        
        return success