"""
Rule loader module for loading tagging rules from configuration files.
Support for category IDs to improve maintainability.
"""

import os
import yaml
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class RuleLoader:
    """Loads and manages tag rules from configuration files"""
    
    def __init__(self, config_path: str = None):
        """Initialize with path to rule configuration file"""
        self.config_path = config_path or os.path.join('config', 'tag_rules.yaml')
        self.name_patterns = {}
        self.data_patterns = {}
        self.categories = []
        self.category_map = {}  # Maps category_id to category name
        self.thresholds = {}
        self.tag_configuration = {
            'tag_name': 'GDPR_CLASSIFICATION',  # Default value
            'tag_schema': ''
        }
        self.loaded = False
    
    def load_rules(self) -> bool:
        """Load rules from the configuration file"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Load tag configuration
            if 'tag_configuration' in config:
                tag_config = config.get('tag_configuration', {})
                self.tag_configuration = {
                    'tag_name': tag_config.get('tag_name', 'GDPR_CLASSIFICATION'),
                    'tag_schema': tag_config.get('tag_schema', '')
                }
            
            # Load categories and build category map
            self.categories = []
            self.category_map = {}
            for cat in config.get('categories', []):
                category_name = cat.get('name')
                category_id = cat.get('id', category_name)  # Use name as ID if ID is not specified
                self.categories.append(category_name)
                self.category_map[category_id] = category_name
                logger.debug(f"Loaded category: {category_id} -> {category_name}")
            
            # Load column name patterns
            for pattern_config in config.get('name_patterns', []):
                pattern = pattern_config.get('pattern')
                
                # Check for category_id first, then fall back to category
                if 'category_id' in pattern_config and pattern_config['category_id'] in self.category_map:
                    category_id = pattern_config.get('category_id')
                    category = self.category_map.get(category_id)
                    logger.debug(f"Mapping pattern using category_id: {category_id} -> {category}")
                else:
                    category = pattern_config.get('category')
                    logger.debug(f"Using direct category: {category}")
                
                if pattern and category:
                    self.name_patterns[pattern] = category
            
            # Load data content patterns
            for pattern_config in config.get('data_patterns', []):
                pattern = pattern_config.get('pattern')
                
                # Check for category_id first, then fall back to category
                if 'category_id' in pattern_config and pattern_config['category_id'] in self.category_map:
                    category_id = pattern_config.get('category_id')
                    category = self.category_map.get(category_id)
                    logger.debug(f"Mapping data pattern using category_id: {category_id} -> {category}")
                else:
                    category = pattern_config.get('category')
                    logger.debug(f"Using direct category: {category}")
                
                if pattern and category:
                    self.data_patterns[pattern] = category
            
            # Load thresholds
            self.thresholds = config.get('thresholds', {})
            
            self.loaded = True
            logger.info(f"Loaded {len(self.name_patterns)} name patterns and {len(self.data_patterns)} data patterns")
            return True
        except Exception as e:
            logger.error(f"Failed to load rules: {e}")
            return False
    
    def get_name_patterns(self) -> Dict[str, str]:
        """Get the column name pattern rules"""
        if not self.loaded:
            self.load_rules()
        return self.name_patterns
    
    def get_data_patterns(self) -> Dict[str, str]:
        """Get the data content pattern rules"""
        if not self.loaded:
            self.load_rules()
        return self.data_patterns
    
    def get_categories(self) -> List[str]:
        """Get the list of tag categories"""
        if not self.loaded:
            self.load_rules()
        return self.categories
    
    def get_threshold(self, threshold_name: str, default_value: float = 0.05) -> float:
        """Get a threshold value by name"""
        if not self.loaded:
            self.load_rules()
        return self.thresholds.get(threshold_name, default_value)
    
    def get_tag_name(self) -> str:
        """Get the configured tag name"""
        if not self.loaded:
            self.load_rules()
        return self.tag_configuration.get('tag_name', 'GDPR_CLASSIFICATION')
    
    def get_tag_schema(self) -> str:
        """Get the configured tag schema"""
        if not self.loaded:
            self.load_rules()
        return self.tag_configuration.get('tag_schema', '')
    
    def reload(self) -> bool:
        """Force reload of the rules"""
        self.loaded = False
        return self.load_rules()