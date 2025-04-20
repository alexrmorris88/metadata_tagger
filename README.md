# Metadata Database Tagger

An automated Python-based tool for detecting and tagging sensitive or business-critical metadata in databases using configurable patterns and overrides.

## Table of Contents

1. [Overview](#overview)  
2. [System Architecture](#system-architecture)  
3. [Installation and Setup](#installation-and-setup)  
4. [Configuration Files](#configuration-files)  
5. [Usage Instructions](#usage-instructions)  
6. [Working with Tags](#working-with-tags)  
7. [Extending the Tool](#extending-the-tool)  
8. [Troubleshooting](#troubleshooting)  

---

## ✅ Overview

The Metadata Database Tagger automates the detection and tagging of sensitive data or business-critical fields across databases using name-based and value-based pattern matching. It is designed for enterprise-scale metadata management, supporting custom rules, override mechanisms, and multiple database platforms.

### Key Features

- Automated tagging using column name and data pattern matching  
- YAML-based configurable detection rules  
- Manual tagging overrides (JSON or CSV)  
- Multi-database and multi-schema scanning  
- Secure credential handling via `.env`  
- JSON or CSV output support  

### Use Cases

- Data classification and documentation  
- Metadata discovery for data governance  
- Policy tagging for compliance or internal standards  
- Tagging sensitive or business-critical columns  

---

## ⚙️ System Architecture

```
metadata_tagger/
├── config/
│   ├── snowflake_database_config.json
│   ├── tag_rules.yaml
│   └── overrides.json
├── src/
│   ├── main.py
│   ├── connectors/
│   ├── detection/
│   └── utils/
└── .env
```

Modular design supports database extensibility, custom logic, and clean configuration management.

---

## ⚙️ Installation and Setup

### Prerequisites

- Python ≥ 3.8  
- Snowflake credentials  
- Permissions to read schemas and create/apply tags  

### Setup Instructions

```bash
git clone https://github.com/your-org/metadata-tagger.git
cd metadata_tagger
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Then configure:

- `config/snowflake_database_config.json` – connection settings  
- `.env` – credentials (excluded from Git)  
- `config/tag_rules.yaml` – tagging rules  
- `config/overrides.json` – manual overrides (optional)  

---

## ⚙️ Configuration Files

### `snowflake_database_config.json`

Supports multiple environments using env variables:
```json
{
  "databases": [
    {
      "name": "Production",
      "config": {
        "user": "${SNOWFLAKE_USER}",
        "password": "${SNOWFLAKE_PASSWORD}",
        ...
      }
    }
  ]
}
```

### `.env`
```env
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
...
```

### `tag_rules.yaml` – Main tagging logic

Define categories, name-based patterns, and data patterns.  
Includes thresholds like `data_pattern_match: 0.05`.

### `overrides.json` or `overrides.csv`

Manual tag assignments by column.

---

## 🚀 Usage Instructions

### Basic Command

```bash
python src/main.py
```

### Advanced Options

```bash
python src/main.py --config config/database_config.json \
  --rules config/tag_rules.yaml \
  --override config/overrides.json \
  --db-name Production \
  --schemas PUBLIC \
  --sample-size 200 \
  --output results.csv \
  --output-format csv
```

---

## 🏷️ Working with Tags

### Tag Application Order

1. Manual override  
2. Column name regex match  
3. Data sample pattern match  
4. Apply tag using Snowflake's `ALTER TABLE`  

### Query Tags in Snowflake

```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES(...))
WHERE TAG_NAME = 'PII';
```

---

## 🧩 Extending the Tool

### Add New Rules

In `tag_rules.yaml`, add to:
- `name_patterns`
- `data_patterns`
- `categories`

### Support New Databases

Implement a new connector under `src/connectors/` with the required methods:
- `connect()`, `get_schemas()`, `get_tables()`, etc.

Update the `create_connector()` logic to recognize your database.

### Add ML Detection

Customize `PIIDetector` in `src/detection/detector.py` with ML logic.

---

## 🧠 Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| Env vars not working | `.env` missing or wrong format | Ensure dotenv is loaded and names match |
| Connection error | Invalid credentials or config | Double-check `.env` and Snowflake URL |
| Tags not applied | Rule mismatch or low match rate | Check sample size, regex, and thresholds |
| Permission denied | Missing privileges | Grant USAGE, SELECT, CREATE TAG, MODIFY |

---

## 📓 Logging

To enable verbose logging:

```python
logging.basicConfig(level=logging.DEBUG, ...)
```
# metadata_tagger
