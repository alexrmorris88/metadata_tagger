"""
PII detector module for identifying sensitive data based on patterns.
"""

import re
import logging
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)

class PIIDetector:
    """Detects PII in database columns based on rules"""
    
    def __init__(self, rules: List[Dict[str, Any]], threshold: float = 0.05):
        """Initialize with detection rules and match threshold"""
        self.rules = rules
        self.threshold = threshold
        self.name_patterns = {}
        self.data_patterns = {}
        
        # Process rules into usable dictionaries
        self._process_rules()
    
    def _process_rules(self):
        """Process rules into pattern dictionaries"""
        for rule in self.rules:
            category = rule.get('category')
            if not category:
                continue
            
            # Process name patterns
            for pattern_config in rule.get('name_patterns', []):
                pattern = pattern_config.get('pattern')
                if pattern:
                    self.name_patterns[pattern] = category
            
            # Process data patterns
            for pattern_config in rule.get('patterns', []):
                pattern = pattern_config.get('pattern')
                if pattern:
                    self.data_patterns[pattern] = category
        
        logger.debug(f"Loaded {len(self.name_patterns)} name patterns and {len(self.data_patterns)} data patterns")
    
    def detect_from_name(self, column_name: str) -> Optional[str]:
        """Detect PII category based on column name"""
        for pattern, category in self.name_patterns.items():
            if re.search(pattern, column_name, re.IGNORECASE):
                logger.debug(f"Column name '{column_name}' matches pattern '{pattern}'")
                return category
        return None
    
    def detect_from_data(self, data_samples: List[Any]) -> Dict[str, int]:
        """
        Detect PII categories based on data content
        Returns a dictionary of categories and their match counts
        """
        # Convert all sample data to strings for regex matching
        str_samples = [str(sample) for sample in data_samples if sample is not None]
        
        results = {}
        for sample in str_samples:
            for pattern, category in self.data_patterns.items():
                if re.search(pattern, sample):
                    if category in results:
                        results[category] += 1
                    else:
                        results[category] = 1
        
        return results
    
    def detect_pii(self, column_name: str, sample_data: List[Any]) -> Optional[str]:
        """
        Detect PII in a column
        Returns the category of PII if detected, None otherwise
        """
        # Check column name patterns first
        name_category = self.detect_from_name(column_name)
        if name_category:
            return name_category
        
        # Check data patterns if we have sample data
        if sample_data:
            data_categories = self.detect_from_data(sample_data)
            
            # Only return a category if enough samples match (based on threshold)
            if data_categories:
                sample_size = len(sample_data)
                for category, count in sorted(data_categories.items(), key=lambda x: x[1], reverse=True):
                    if count / sample_size >= self.threshold:
                        return category
        
        # No PII detected
        return None