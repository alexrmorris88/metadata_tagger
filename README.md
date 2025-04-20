# Snowflake Metadata Tagger & Policy Manager

This repository contains two main tools for managing data privacy and security in Snowflake:

1. **Metadata Tagger**: Automatically identifies and tags PII and sensitive data in your Snowflake database
2. **Policy Manager**: Creates and applies masking and access policies based on those tags

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

Requirements include:
- snowflake-connector-python
- pyyaml
- python-dotenv

### 2. Configure Environment Variables

Create a `.env` file in the root directory with your Snowflake credentials:

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

## Using the Metadata Tagger

The Metadata Tagger scans your Snowflake database and applies tags to columns containing sensitive data.

### 1. Configure Tag Rules

Review and modify `config/tag_rules.yaml` to define:
- Tag categories (e.g., "PII - Customer Information")
- Column name patterns (e.g., matching "email" or "address" in column names)
- Data content patterns (e.g., regex for finding email addresses in data samples)

### 2. Configure Database Connection

Review `config/database_config.json` to ensure it has the correct connection information. The default configuration uses environment variables from your `.env` file.

### 3. Run the Tagger

Basic usage:
```bash
python metadata_tagger.py
```

Advanced options:
```bash
python metadata_tagger.py --db-type snowflake --sample-size 200 --schemas PUBLIC SALES --output tagging_results.json
```

Options:
- `--config`: Path to database configuration file (default: `config/database_config.json`)
- `--rules`: Path to tag rules file (default: `config/tag_rules.yaml`)
- `--db-type`: Database type (default: `snowflake`)
- `--db-name`: Name of the database to process (as defined in config file)
- `--schemas`: List of schemas to process (default: all schemas)
- `--override`: Path to override file (default: `config/overrides.json`)
- `--sample-size`: Number of rows to check per column (default: 100)
- `--output`: Output file for tagging results (default: `tagging_results.json`)

### 4. Review Results

After running, check the output file (`tagging_results.json` by default) to see which columns were tagged and why.

## Using the Policy Manager

The Policy Manager creates and applies Snowflake security policies based on the tags applied by the Metadata Tagger.

### 1. Configure Policies

Review and modify `config/policy_config.yaml` to define:
- Global settings (database, admin role, etc.)
- Category-based masking policies
- Row access policies

### 2. Run the Policy Manager

Basic usage:
```bash
python policy_manager.py --apply
```

Advanced options:
```bash
python policy_manager.py --apply --db-name PasswordAuth --schema PUBLIC --masking-only
```

Options:
- `--config`: Path to database configuration file (default: `config/database_config.json`)
- `--policy-config`: Path to policy configuration file (default: `config/policy_config.yaml`)
- `--db-name`: Name of the database to use (as defined in config file)
- `--apply`: Apply policies to the database
- `--validate`: Validate policy configuration without applying
- `--schema`: Schema to use for policy creation
- `--row-access-only`: Apply only row access policies
- `--masking-only`: Apply only masking policies
- `--tags-only`: Apply only tag policies

## Custom Tag Overrides

If you need to manually specify tags for certain columns, create or edit `config/overrides.json`:

```json
{
  "SCHEMA.TABLE.COLUMN": "PII - Customer Information",
  "DATABASE.SCHEMA.TABLE.COLUMN": "PII - Financial Data"
}
```

## Complete Workflow Example

A typical workflow uses both tools in sequence:

1. Run the Metadata Tagger to scan and tag data:
```bash
python metadata_tagger.py
```

2. Review the results and adjust overrides if needed in `config/overrides.json`

3. Run the tagger again with your overrides:
```bash
python metadata_tagger.py --override config/overrides.json
```

4. Apply security policies based on the tags:
```bash
python policy_manager.py --apply
```

5. Verify in Snowflake that the appropriate policies have been applied

## Troubleshooting

- **Connection issues**: Verify your credentials in the `.env` file
- **Permission errors**: Ensure your Snowflake role has sufficient privileges
- **Missing tags**: Check the tag rules and increase the sample size for better detection
- **Policy errors**: Run with `--validate` first to check your policy configuration

For detailed logs, check the application logs in your terminal output.