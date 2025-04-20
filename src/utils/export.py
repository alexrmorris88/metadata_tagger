"""
Export module for saving tagging results.
"""

import csv
import json
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

def export_results(results: Dict[str, List[Dict[str, str]]], 
                  filepath: str, format_type: str = 'json') -> bool:
    """
    Export tagging results to a file
    
    Args:
        results: Dictionary of results by schema
        filepath: Path to output file
        format_type: Output format ('json' or 'csv')
    
    Returns:
        bool: Success status
    """
    try:
        # Flatten results for easier export
        flat_results = []
        for schema, schema_results in results.items():
            flat_results.extend(schema_results)
        
        if format_type.lower() == 'csv':
            return export_to_csv(flat_results, filepath)
        elif format_type.lower() == 'json':
            return export_to_json(flat_results, filepath)
        else:
            logger.error(f"Unsupported export format: {format_type}")
            return False
    except Exception as e:
        logger.error(f"Error exporting results: {e}")
        return False

def export_to_csv(results: List[Dict[str, str]], filepath: str) -> bool:
    """Export results to CSV file"""
    try:
        with open(filepath, 'w', newline='') as f:
            # Extract all possible fieldnames from the results
            fieldnames = set()
            for result in results:
                fieldnames.update(result.keys())
            
            writer = csv.DictWriter(f, fieldnames=sorted(fieldnames))
            writer.writeheader()
            writer.writerows(results)
        
        logger.info(f"Exported {len(results)} tagged columns to CSV file: {filepath}")
        return True
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        return False

def export_to_json(results: List[Dict[str, str]], filepath: str) -> bool:
    """Export results to JSON file"""
    try:
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Exported {len(results)} tagged columns to JSON file: {filepath}")
        return True
    except Exception as e:
        logger.error(f"Error exporting to JSON: {e}")
        return False