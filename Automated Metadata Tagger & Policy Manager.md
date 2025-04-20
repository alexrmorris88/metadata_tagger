## Table of Contents

1. [Overview](https://claude.ai/chat/4a0e0f58-4743-47a9-9466-21b39fc2d4ac#overview)
2. [System Architecture](https://claude.ai/chat/4a0e0f58-4743-47a9-9466-21b39fc2d4ac#system-architecture)
3. [Installation and Setup](https://claude.ai/chat/4a0e0f58-4743-47a9-9466-21b39fc2d4ac#installation-and-setup)
4. [Configuration Files](https://claude.ai/chat/4a0e0f58-4743-47a9-9466-21b39fc2d4ac#configuration-files)
5. [Using the Metadata Tagger](https://claude.ai/chat/4a0e0f58-4743-47a9-9466-21b39fc2d4ac#using-the-metadata-tagger)
6. [Using the Policy Manager](https://claude.ai/chat/4a0e0f58-4743-47a9-9466-21b39fc2d4ac#using-the-policy-manager)
7. [Working with Tags and Policies](https://claude.ai/chat/4a0e0f58-4743-47a9-9466-21b39fc2d4ac#working-with-tags-and-policies)
8. [Extending the Tools](https://claude.ai/chat/4a0e0f58-4743-47a9-9466-21b39fc2d4ac#extending-the-tools)
9. [Troubleshooting](https://claude.ai/chat/4a0e0f58-4743-47a9-9466-21b39fc2d4ac#troubleshooting)
10. [Authentication Options](https://claude.ai/chat/4a0e0f58-4743-47a9-9466-21b39fc2d4ac#authentication-options)

## Overview

The Snowflake Metadata Tagger and Policy Manager are a suite of tools designed for automating data privacy, security, and compliance within Snowflake environments. These tools work together to identify sensitive data and apply appropriate security controls.

### Key Features

- **Metadata Tagger**:
    
    - Automated detection of PII and sensitive data
    - Customizable tagging rules via patterns and data sampling
    - Support for manual overrides
    - Detailed logging and reporting
- **Policy Manager**:
    
    - Automated creation of Snowflake security policies
    - Support for column-level masking policies
    - Row-level security based on tagged data
    - Policy validation functionality

### Use Cases

- GDPR, CCPA, HIPAA, and other regulatory compliance
- Data classification and cataloging
- Implementation of security controls based on data classification
- Data governance and stewardship programs
- Auditing and documentation of sensitive data locations

## System Architecture

The tools are built with a modular architecture to facilitate extensibility and maintainability:

```
/
├── config/                       # Configuration files
│   ├── database_config.json      # Database connection settings
│   ├── tag_rules.yaml            # Tag definitions and rules
│   ├── policy_config.yaml        # Security policy definitions
│   └── overrides.json            # Manual tagging overrides
├── connectors/                   # Database connectors
│   ├── base.py                   # Base connector interface
│   ├── snowflake.py              # Snowflake implementation
│   └── [other connectors]        # For other databases (future)
├── detection/                    # PII detection logic
│   ├── detector.py               # Main detection engine
│   └── rule_loader.py            # Loads rules from YAML
├── policy_manager/               # Policy management components
│   ├── policy_loader.py          # Loads policy configurations
│   ├── policy_engine.py          # Core policy implementation
│   └── policy_applier.py         # Applies policies to database
├── utils/                        # Utility functions
│   ├── override_handler.py       # Handles manual overrides
│   └── export.py                 # Export results
├── metadata_tagger.py            # Metadata tagger main script
├── policy_manager.py             # Policy manager main script
└── .env                          # Environment variables
```

### Component Descriptions

1. **Database Connectors**: Abstract interfaces to database systems, ensuring consistency across platforms while encapsulating database-specific implementations.
    
2. **Detection Engine**: The core logic that identifies PII and sensitive data through pattern matching on column names and data samples.
    
3. **Rule Loader**: Loads and manages tagging rules from YAML configuration.
    
4. **Policy Engine**: Creates and manages Snowflake security policies based on tagged columns.
    
5. **Policy Applier**: Applies the appropriate policies to tables and columns based on their tags.
    
6. **Override Handler**: Manages manual tagging overrides for scenarios where automated detection isn't sufficient.
    
7. **Main Scripts**: Orchestrates the process flow, connecting to databases, applying rules, and managing policies.
    

## Installation and Setup

### Prerequisites

- Python 3.8 or newer
- Access to a Snowflake database
- Appropriate database permissions:
    - For tagging: SELECT on tables, CREATE TAG, and APPLY TAG permissions
    - For policy management: ACCOUNTADMIN role (or similar with policy creation rights)

### Installation Steps

1. **Clone the repository**:

```bash
git clone https://github.com/your-org/snowflake-metadata-tools.git
cd snowflake-metadata-tools
```

2. **Create a virtual environment**:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install required packages**:

```bash
pip install -r requirements.txt
```

4. **Create configuration files**:

```bash
# Create directories if they don't exist
mkdir -p config

# Create the configuration files
touch config/database_config.json
touch config/tag_rules.yaml
touch config/policy_config.yaml
touch config/overrides.json
touch .env
```

5. **Configure your environment**:

Edit the `.env` file with your Snowflake credentials:

```
# Snowflake Credentials
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_ROLE=your_role

# For SSO authentication (optional)
OKTA_URL=https://your-company.okta.com
```

### Required Packages

```
snowflake-connector-python>=2.7.0
PyYAML>=6.0
python-dotenv>=0.19.0
```

## Configuration Files

### 1. Database Configuration (`database_config.json`)

This file defines the connection parameters for your database(s). It supports multiple databases and authentication methods:

```json
{
  "databases": [
    {
      "name": "PasswordAuth",
      "config": {
        "user": "${SNOWFLAKE_USER}",
        "password": "${SNOWFLAKE_PASSWORD}",
        "account": "${SNOWFLAKE_ACCOUNT}",
        "warehouse": "${SNOWFLAKE_WAREHOUSE}",
        "database": "${SNOWFLAKE_DATABASE}",
        "role": "${SNOWFLAKE_ROLE}",
        "authentication": {
          "method": "password"
        }
      }
    },
    {
      "name": "BrowserSSO",
      "config": {
        "account": "${SNOWFLAKE_ACCOUNT}",
        "user": "${SNOWFLAKE_USER}",
        "warehouse": "${SNOWFLAKE_WAREHOUSE}",
        "database": "${SNOWFLAKE_DATABASE}",
        "role": "${SNOWFLAKE_ROLE}",
        "authentication": {
          "method": "sso",
          "type": "externalbrowser"
        }
      }
    }
  ],
  "default_database": "PasswordAuth"
}
```

### 2. Tag Rules (`tag_rules.yaml`)

This file defines the tag structure and detection rules:

```yaml
# Tag structure settings
tag_configuration:
  # The name of the tag to create in the database
  tag_name: "PII"
  # Optional: Use a dedicated schema for tags (leave empty to use table's schema)
  tag_schema: ""

# Tag categories
categories:
  - id: "customer_pii"
    name: "PII - Customer Information"
    description: "Personally Identifiable Information about customers"
  
  - id: "financial_pii"
    name: "PII - Financial Data" 
    description: "Financial information that must be protected"
  
  - id: "internal_sensitive"
    name: "Sensitive - Internal Only"
    description: "Company confidential information"

# Column name pattern rules
name_patterns:
  # Customer Name Fields
  - pattern: '(?i)(^|_)(first|last|full|customer|user)(_)?name(s)?($|_)'
    category_id: "customer_pii"
  
  # Customer Email Address
  - pattern: '(?i)(^|_)(email|e-mail|mail)(_)?address(es)?($|_)'
    category_id: "customer_pii"
  
  # Financial data
  - pattern: '(?i)(^|_)(credit_card|cc|card)(_)?(number|nbr|no|#)?($|_)'
    category_id: "financial_pii"

# Data content pattern rules
data_patterns:
  # Email pattern
  - pattern: '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    category_id: "customer_pii"
  
  # Credit card numbers (basic pattern)
  - pattern: '\b(?:\d{4}[-\s]?){3}\d{4}\b'
    category_id: "financial_pii"

# Detection thresholds
thresholds:
  # Minimum percentage of data samples that must match a pattern
  data_pattern_match: 0.05  # 5%
```

### 3. Policy Configuration (`policy_config.yaml`)

This file defines the security policies to be applied:

```yaml
# Snowflake Policy Configuration
policies:
  # Global settings for all policies
  global:
    database: "${SNOWFLAKE_DATABASE}"  # Read from environment variable
    admin_role: "ACCOUNTADMIN"         # Role that can see unmasked data
    default_tag: "PII"                 # Tag name applied by the metadata tagger
    
  # Category-based Masking Policies
  category_policies:
    - category: "PII - Customer Information"
      masking_policy:
        name: "pii_customer_information"
        data_types:
          VARCHAR: "CASE WHEN current_role() = 'ACCOUNTADMIN' THEN VAL ELSE '********' END"
          INTEGER: "CASE WHEN current_role() = 'ACCOUNTADMIN' THEN VAL ELSE 999999 END"
          DATE: "CASE WHEN current_role() = 'ACCOUNTADMIN' THEN VAL ELSE '0000-00-00' END"
        comment: "Mask customer PII data except for admin role"
        
    - category: "PII - Financial Data"
      masking_policy:
        name: "pii_financial_data"
        data_types:
          VARCHAR: "CASE WHEN current_role() = 'ACCOUNTADMIN' THEN VAL ELSE '********' END"
          NUMBER: "CASE WHEN current_role() = 'ACCOUNTADMIN' THEN VAL ELSE 999999 END"
        comment: "Mask financial PII data except for admin role"

  # Row Access Policies
  row_access:
    - name: "customer_data_access"
      schema: "PUBLIC"
      policy_expression: "current_role() IN ('ACCOUNTADMIN', 'DATA_SCIENTIST')"
      apply_to_categories: ["PII - Customer Information"]
      comment: "Restrict access to customer data to specified roles"
```

### 4. Overrides (`overrides.json`)

This file allows for manual specification of tags for particular columns:

```json
{
  "MY_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER.C_BIRTH_YEAR": "PII - Customer Information",
  "MY_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER.C_BIRTH_MONTH": "PII - Customer Information",
  "MY_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER.C_BIRTH_DAY": "PII - Customer Information"
}
```

## Using the Metadata Tagger

The Metadata Tagger scans your Snowflake database and applies tags to columns containing sensitive data.

### How It Works

1. **Detection**: The tagger analyzes columns using:
    
    - Column name pattern matching (e.g., columns with "email" in the name)
    - Data content analysis (e.g., looking for email patterns in the data)
    - Manual overrides for specific columns
2. **Tagging**: When sensitive data is found, the tool applies the appropriate tag to the column.
    
3. **Reporting**: A detailed report is generated showing which columns were tagged and why.
    

### Command Line Options

```bash
python metadata_tagger.py [options]
```

Available options:

- `--config`: Path to database configuration file (default: `config/database_config.json`)
- `--rules`: Path to tag rules configuration (default: `config/tag_rules.yaml`)
- `--db-type`: Database type (default: `snowflake`)
- `--db-name`: Specific database to process (default: process all or default from config)
- `--schemas`: List of specific schemas to process (default: all schemas)
- `--override`: Path to override file (default: `config/overrides.json`)
- `--override-format`: Format of override file: `json` or `csv` (default: `json`)
- `--sample-size`: Number of data samples to check per column (default: `100`)
- `--output`: Output file path (default: `tagging_results.json`)
- `--output-format`: Output format: `json` or `csv` (default: `json`)

### Common Usage Scenarios

#### 1. Basic Usage (Default Options)

```bash
python metadata_tagger.py
```

#### 2. Process Specific Schemas

```bash
python metadata_tagger.py --schemas PUBLIC SALES MARKETING
```

#### 3. Use Specific Database Configuration

```bash
python metadata_tagger.py --db-name BrowserSSO
```

#### 4. Increase Sample Size for Better Detection

```bash
python metadata_tagger.py --sample-size 500
```

#### 5. Apply Manual Overrides

```bash
python metadata_tagger.py --override custom_overrides.json
```

#### 6. Generate CSV Output

```bash
python metadata_tagger.py --output tagging_results.csv --output-format csv
```

## Using the Policy Manager

The Policy Manager creates and applies Snowflake security policies based on the tags applied by the Metadata Tagger.

### How It Works

1. **Policy Loading**: The manager loads policy definitions from the configuration file.
    
2. **Tag Analysis**: It identifies which columns have specific tags and need policies.
    
3. **Policy Creation**: Creates masking policies and row access policies in Snowflake.
    
4. **Policy Application**: Applies the appropriate policies to tables and columns.
    

### Command Line Options

```bash
python policy_manager.py [options]
```

Available options:

- `--config`: Path to database configuration file (default: `config/database_config.json`)
- `--policy-config`: Path to policy configuration file (default: `config/policy_config.yaml`)
- `--db-name`: Name of the database to use (as defined in config file)
- `--apply`: Apply policies to the database (required to make changes)
- `--validate`: Validate policy configuration without applying
- `--schema`: Schema to use for policy creation (overrides dynamic detection)
- `--row-access-only`: Apply only row access policies
- `--masking-only`: Apply only masking policies
- `--tags-only`: Apply only tag policies
- `--verbose`: Enable verbose logging

### Common Usage Scenarios

#### 1. Validate Policy Configuration Without Applying

```bash
python policy_manager.py --validate
```

#### 2. Apply All Policies

```bash
python policy_manager.py --apply
```

#### 3. Apply Only Masking Policies

```bash
python policy_manager.py --apply --masking-only
```

#### 4. Apply Policies to a Specific Database

```bash
python policy_manager.py --apply --db-name PasswordAuth
```

#### 5. Set a Specific Schema for Policy Creation

```bash
python policy_manager.py --apply --schema SECURITY_SCHEMA
```

#### 6. Verbose Mode for Troubleshooting

```bash
python policy_manager.py --apply --verbose
```

## Working with Tags and Policies

### Tag Lifecycle

1. **Creation**: Tags are created in Snowflake by the Metadata Tagger.
2. **Application**: Tags are applied to specific columns based on detection or overrides.
3. **Usage**: The Policy Manager uses tags to determine which policies to apply.
4. **Viewing**: You can view tags in Snowflake using system views (see below).

### Snowflake Tag Structure

Tags in Snowflake consist of:

- **Tag Name**: The identifier of the tag (e.g., "PII")
- **Tag Value**: The value assigned to the tag (e.g., "PII - Customer Information")
- **Tagged Object**: The database object (column) that has the tag applied

### Viewing Tags in Snowflake

You can query applied tags using Snowflake's system views:

```sql
-- View all tags in the account
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
WHERE OBJECT_DATABASE = 'YOUR_DATABASE';

-- View tags for a specific table
SELECT 
    COLUMN_NAME,
    TAG_NAME,
    TAG_VALUE
FROM 
    TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('PII'))
WHERE
    OBJECT_DATABASE = 'YOUR_DATABASE'
    AND OBJECT_SCHEMA = 'YOUR_SCHEMA'
    AND OBJECT_NAME = 'YOUR_TABLE';
```

### Policy Types

#### 1. Column-Level Masking Policies

These policies control how sensitive data is displayed based on the user's role:

```sql
-- Example of a masking policy in Snowflake
CREATE MASKING POLICY email_mask AS (val VARCHAR) RETURNS VARCHAR ->
    CASE 
        WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
        ELSE REGEXP_REPLACE(val, '.+(@.+)', '****$1')
    END;
```

#### 2. Row-Level Access Policies

These policies control which rows a user can see:

```sql
-- Example of a row access policy in Snowflake
CREATE ROW ACCESS POLICY sales_data_access AS (
    ROLE VARCHAR
) RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SALES_ANALYST');
```

## Extending the Tools

### Adding New Tag Rules

To add new tag detection patterns, edit the `tag_rules.yaml` file:

1. **Add a new category**:

```yaml
categories:
  - id: "health_pii"
    name: "PII - Health Data"
    description: "Protected health information (PHI)"
```

2. **Add new name patterns**:

```yaml
name_patterns:
  - pattern: '(?i)(^|_)(medical|health|patient|diagnosis)($|_)'
    category_id: "health_pii"
```

3. **Add new data patterns**:

```yaml
data_patterns:
  - pattern: '\b\d{3}-\d{2}-\d{4}\b'  # SSN pattern
    category_id: "customer_pii"
```

### Supporting Additional Database Platforms

To add support for a new database platform:

1. Create a new connector class in the `connectors/` directory that implements the `DatabaseConnector` interface:

```python
# connectors/my_database.py
from .base import DatabaseConnector

class MyDatabaseConnector(DatabaseConnector):
    def __init__(self, config):
        self.config = config
        self.conn = None
    
    def connect(self):
        # Implement connection logic
        pass
    
    # Implement other required methods
```

2. Update the `create_connector()` function in `metadata_tagger.py` to include your new connector:

```python
def create_connector(db_type, config):
    if db_type.lower() == 'snowflake':
        return SnowflakeConnector(config)
    elif db_type.lower() == 'my_database':
        return MyDatabaseConnector(config)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
```

### Creating Custom Policy Types

To add a new policy type:

1. Update the `policy_config.yaml` with your new policy type:

```yaml
policies:
  # Existing policies
  
  # New policy type
  custom_policies:
    - name: "my_custom_policy"
      # Policy-specific configurations
```

2. Implement the policy creation logic in `policy_engine.py`:

```python
def create_custom_policy(self, policy_config):
    # Implementation
    pass
```

3. Update the `policy_applier.py` to apply your custom policy:

```python
def apply_custom_policies(self, custom_policies):
    # Implementation
    pass
```

4. Add the call to your new method in `apply_all_policies()`:

```python
def apply_all_policies(self, policies):
    # Existing policy applications
    
    # Apply custom policies
    if 'custom_policies' in policies:
        self.apply_custom_policies(policies['custom_policies'])
```

## Troubleshooting

### Common Issues and Solutions

#### Connection Issues

**Problem**: Unable to connect to Snowflake.

**Solutions**:

- Verify credentials in your `.env` file
- Check if your account identifier is correct (includes region/cloud provider)
- Ensure your IP is allowed to connect to Snowflake
- Verify that the warehouse exists and is running

#### Permission Issues

**Problem**: Permission denied errors when applying tags or policies.

**Solutions**:

- Ensure your role has the necessary privileges:
    - `ACCOUNTADMIN` role is required for policy creation
    - `MODIFY` privilege on tables for tag application
    - `CREATE TAG` privilege for creating tags
- Check privilege grants using:
    
    ```sql
    SHOW GRANTS TO ROLE your_role;
    ```
    

#### No Tags Being Applied

**Problem**: The tagger runs without errors, but no tags are applied.

**Solutions**:

- Increase the sample size (`--sample-size`) to improve detection
- Check if your patterns match the actual column names/data
- Review the log output for pattern matching information
- Try adding manual overrides for known sensitive columns
- Verify the threshold isn't too high (default is 5% of data must match)

#### Policy Application Failures

**Problem**: Policies aren't being applied correctly.

**Solutions**:

- Use the `--validate` option to check your policy configuration
- Make sure the categories in policies match your tag values exactly
- Check for proper permissions in Snowflake
- Run in verbose mode (`--verbose`) for detailed logging
- Verify that the tagged columns exist with:
    
    ```sql
    SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCESWHERE TAG_NAME = 'PII';
    ```
    

### Logging and Debugging

The tools have detailed logging to help diagnose issues:

1. **Console Output**: Basic progress and errors are displayed on the console.
    
2. **Log Messages**: More detailed logs use Python's logging system:
    
    ```python
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ```
    
3. **Debug Mode**: Run with `--verbose` for maximum logging detail.
    
4. **Snowflake History**: Check query history in Snowflake for SQL errors:
    
    ```sql
    SELECT *
    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
    ORDER BY START_TIME DESC
    LIMIT 20;
    ```
    

## Authentication Options

### Password Authentication

The simplest method using username and password:

```json
{
  "name": "PasswordAuth",
  "config": {
    "user": "${SNOWFLAKE_USER}",
    "password": "${SNOWFLAKE_PASSWORD}",
    "account": "${SNOWFLAKE_ACCOUNT}",
    "warehouse": "${SNOWFLAKE_WAREHOUSE}",
    "database": "${SNOWFLAKE_DATABASE}",
    "role": "${SNOWFLAKE_ROLE}",
    "authentication": {
      "method": "password"
    }
  }
}
```

### SSO Authentication

For environments using single sign-on:

#### External Browser (Most Common)

```json
{
  "name": "BrowserSSO",
  "config": {
    "account": "${SNOWFLAKE_ACCOUNT}",
    "user": "${SNOWFLAKE_USER}",
    "warehouse": "${SNOWFLAKE_WAREHOUSE}",
    "database": "${SNOWFLAKE_DATABASE}",
    "role": "${SNOWFLAKE_ROLE}",
    "authentication": {
      "method": "sso",
      "type": "externalbrowser"
    }
  }
}
```

#### Okta-Specific SSO

```json
{
  "name": "OktaSSO",
  "config": {
    "account": "${SNOWFLAKE_ACCOUNT}",
    "user": "${SNOWFLAKE_USER}",
    "warehouse": "${SNOWFLAKE_WAREHOUSE}",
    "database": "${SNOWFLAKE_DATABASE}",
    "role": "${SNOWFLAKE_ROLE}",
    "authentication": {
      "method": "sso",
      "type": "okta",
      "okta_url": "${OKTA_URL}"
    }
  }
}
```

### Important Notes on SSO Authentication

1. **Username Requirement**: All SSO methods require a username to be set in the configuration or environment variables.
    
2. **Browser Requirements**: External browser authentication requires:
    
    - A default browser configured on your system
    - The ability to open a browser window (challenging on headless servers)
3. **Timeout Issues**: Be aware that SSO requests can timeout if not completed promptly.
    
4. **Pre-authentication**: Sometimes being pre-authenticated in your browser can allow seamless login without username/password entry.
    

For server environments where browser authentication isn't possible, you can consider:

- Key-pair authentication
- OAuth token-based authentication
- Using the password authentication method with a service account