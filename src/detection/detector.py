"""
Detector module for finding PII and sensitive data in database columns.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any

from .rule_loader import RuleLoader

logger = logging.getLogger(__name__)

class PIIDetector:
    """Detects PII and sensitive data based on rules loaded from configuration"""
    
    def __init__(self, rule_loader: Optional[RuleLoader] = None):
        """Initialize the detector with rules"""
        self.rule_loader = rule_loader or RuleLoader()
        self.rule_loader.load_rules()
        self.threshold_percent = self.rule_loader.get_threshold('data_pattern_match', 0.05)
    
    def detect_from_name(self, column_name: str) -> Optional[str]:
        """Detect PII category based on column name"""
        name_patterns = self.rule_loader.get_name_patterns()
        
        for pattern, category in name_patterns.items():
            if re.search(pattern, column_name):
                logger.debug(f"Column name '{column_name}' matches pattern '{pattern}'")
                return category
        return None
    
    def detect_from_data(self, data_samples: List[Any]) -> Dict[str, int]:
        """
        Detect PII categories based on data content
        Returns a dictionary of categories and their match counts
        """
        data_patterns = self.rule_loader.get_data_patterns()
        
        # Convert all sample data to strings for regex matching
        str_samples = [str(sample) for sample in data_samples if sample is not None]
        
        results = {}
        for sample in str_samples:
            for pattern, category in data_patterns.items():
                if re.search(pattern, sample):
                    if category in results:
                        results[category] += 1
                    else:
                        results[category] = 1
        
        return results
    
    def get_tag_for_column(self, column_name: str, sample_data: List[Any], 
                         overrides: Optional[Dict[str, str]] = None) -> Optional[Tuple[str, str]]:
        """
        Determine the appropriate tag for a column
        Returns a tuple of (tag_category, tag_reason) or None if no tag applies
        
        Args:
            column_name: The name of the column
            sample_data: Sample data from the column
            overrides: Optional manual overrides by column name
        """
        # Check overrides first if provided
        if overrides and column_name.lower() in overrides:
            override_tag = overrides[column_name.lower()]
            return (override_tag, "Manual override")
        
        # Check column name patterns
        name_tag = self.detect_from_name(column_name)
        if name_tag:
            return (name_tag, f"Column name pattern: {column_name}")
        
        # Check data patterns if we have sample data
        if sample_data:
            data_tags = self.detect_from_data(sample_data)
            # Only apply a tag if enough samples match (based on threshold)
            if data_tags:
                sample_size = len(sample_data)
                for tag, count in data_tags.items():
                    if count / sample_size >= self.threshold_percent:
                        return (tag, f"Data pattern match: {count}/{sample_size} samples")
        
        # No tag assigned
        return None