"""
Policy loader module for loading Snowflake policy configurations from YAML files.
"""

import os
import yaml
import logging
import re
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class PolicyLoader:
    """Loads and manages Snowflake policies from YAML configuration files"""
    
    def __init__(self, config_path: str):
        """Initialize with path to policy configuration file"""
        self.config_path = config_path
        
    def load_policies(self) -> Dict[str, Any]:
        """Load policies from the configuration file"""
        try:
            # Ensure environment variables are loaded
            if os.path.exists('.env'):
                load_dotenv('.env')
                
            with open(self.config_path, 'r') as f:
                # Load YAML file as string first to process env vars
                yaml_content = f.read()
                
                # Process environment variables in the YAML content
                yaml_content = self._process_env_vars(yaml_content)
                
                # Then parse the processed YAML content
                config = yaml.safe_load(yaml_content)
            
            if not config or 'policies' not in config:
                logger.warning("No policies found in configuration file")
                return {}
            
            policies = config['policies']
            
            # Process any remaining environment variables in the loaded object
            self._process_env_vars_in_object(policies)
            
            # Validate the policy structure
            self._validate_policies(policies)
            
            # Count policies by type
            row_access_count = len(policies.get('row_access', []))
            category_count = len(policies.get('category_policies', []))
            
            logger.info(f"Loaded {row_access_count} row access policies and {category_count} category policies")
            
            return policies
        except FileNotFoundError:
            logger.error(f"Policy configuration file not found: {self.config_path}")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error loading policies: {e}")
            return {}
    
    def _process_env_vars(self, content: str) -> str:
        """Replace environment variable references with their values"""
        # Pattern to match ${ENV_VAR} or $ENV_VAR format
        pattern = r'\${([A-Za-z0-9_]+)}|\$([A-Za-z0-9_]+)'
        
        def replace_env_var(match):
            # Get the env var name from either group 1 (${VAR}) or group 2 ($VAR)
            env_var = match.group(1) or match.group(2)
            env_value = os.environ.get(env_var)
            
            if env_value is None:
                logger.warning(f"Environment variable '{env_var}' not found")
                # Return the original reference if env var not found
                return match.group(0)
            
            return env_value
        
        # Replace all environment variable references
        processed_content = re.sub(pattern, replace_env_var, content)
        return processed_content
    
    def _process_env_vars_in_object(self, obj: Any) -> None:
        """Recursively process environment variables in an object"""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, (dict, list)):
                    self._process_env_vars_in_object(value)
                elif isinstance(value, str):
                    # Pattern to match ${ENV_VAR} or $ENV_VAR format
                    pattern = r'\${([A-Za-z0-9_]+)}|\$([A-Za-z0-9_]+)'
                    
                    # Find all environment variable references
                    matches = re.findall(pattern, value)
                    if matches:
                        # Process each match
                        for match in matches:
                            env_var = match[0] or match[1]  # First or second group
                            env_value = os.environ.get(env_var)
                            
                            if env_value is not None:
                                # Replace in the value
                                if match[0]:  # ${VAR} format
                                    value = value.replace(f"${{{env_var}}}", env_value)
                                else:  # $VAR format
                                    value = value.replace(f"${env_var}", env_value)
                        
                        # Update the dict
                        obj[key] = value
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, (dict, list)):
                    self._process_env_vars_in_object(item)
                elif isinstance(item, str):
                    # Pattern to match ${ENV_VAR} or $ENV_VAR format
                    pattern = r'\${([A-Za-z0-9_]+)}|\$([A-Za-z0-9_]+)'
                    
                    # Find all environment variable references
                    matches = re.findall(pattern, item)
                    if matches:
                        # Process each match
                        for match in matches:
                            env_var = match[0] or match[1]  # First or second group
                            env_value = os.environ.get(env_var)
                            
                            if env_value is not None:
                                # Replace in the value
                                if match[0]:  # ${VAR} format
                                    item = item.replace(f"${{{env_var}}}", env_value)
                                else:  # $VAR format
                                    item = item.replace(f"${env_var}", env_value)
                        
                        # Update the list
                        obj[i] = item
    
    def _validate_policies(self, policies: Dict[str, Any]) -> None:
        """Validate policy configuration structure"""
        # Validate global settings
        global_settings = policies.get('global', {})
        if not global_settings.get('database'):
            logger.warning("No database specified in global settings, will use connection database")
        
        # Validate category policies
        for policy in policies.get('category_policies', []):
            if 'category' not in policy:
                raise ValueError(f"Category policy missing 'category' field")
            
            masking_policy = policy.get('masking_policy', {})
            if not masking_policy:
                raise ValueError(f"No masking policy defined for category: {policy.get('category')}")
                
            # Name can be missing, will be auto-generated
            # Schema can be missing, will use global policy_schema or default
            
            # Validate data_types in masking_policy
            if 'data_types' not in masking_policy or not masking_policy['data_types']:
                raise ValueError(f"No data types defined for masking policy in category {policy.get('category')}")
        
        # Validate row access policies
        for policy in policies.get('row_access', []):
            if not all(k in policy for k in ['name', 'schema', 'policy_expression']):
                raise ValueError(f"Invalid row access policy configuration: {policy}")
            
            if 'apply_to_categories' not in policy or not policy['apply_to_categories']:
                raise ValueError(f"Row access policy missing 'apply_to_categories': {policy}")
        
        # Validate PII detection
        pii_detection = policies.get('pii_detection', {})
        if pii_detection.get('enabled', False):
            if not pii_detection.get('scan_schemas'):
                logger.warning("PII detection enabled but no schemas specified for scanning")