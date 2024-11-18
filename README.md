# Python File Ingestion Pipeline

A Python-based file ingestion pipeline that processes CSV and Excel files into BigQuery, supporting flexible column mapping and dynamic attribute handling.

## Features

- Support for CSV and Excel (.xlsx) files
- Flexible column mapping via YAML/JSON configuration
- Dynamic attribute handling for key-value pairs
- NULL value handling with configurable defaults
- Detailed processing statistics and error reporting
- BigQuery destination configuration
- Automatic data source tracking

## Prerequisites

- [Python](https://www.python.org/downloads/) (v3.10 or higher)
- [Google Cloud SDK](https://cloud.google.com/sdk/docs)
- Required Python packages (see requirements.txt)

## Installation

1. Install required packages:
```bash
pip install -r requirements.txt
```
2. Authenticate with Google Cloud SDK using the following commands:
   1. `gcloud auth login` (This will open a browser window to authenticate with your Google Account)
   2. `gcloud config set project <PROJECT_ID>` (replace <PROJECT_ID> with your Google Cloud Project ID you created earlier)
   3. `gcloud auth application-default login` (This sets up the application default credentials for your project)
   4. `gcloud auth application-default set-quota-project <PROJECT_ID>` (This sets the quota project for your project)

## Configuration

### Column Mapping File

Create a YAML or JSON file to specify column mappings and attribute definitions.

#### YAML Example (mappings.yaml):
```yaml
"customer_data_2024.xlsx":
  columns:
    "First Name": "FirstName"
    "Last Name": "LastName"
    "Email Address": "Email"
    "Phone Number": "Mobile"
    "Post Code": "PostCode"
  attributes:
    "Subscription Status": "SubscriptionStatus"
    "Customer Type": "CustomerType"
    "Preference": "ContactPreference"
  data_source: "tangerine"

"contacts_export.csv":
  columns:
    "fname": "FirstName"
    "lname": "LastName"
    "email": "Email"
    "mob": "Mobile"
    "pcode": "PostCode"
  attributes:
    "sub_status": "SubscriptionStatus"
    "cust_type": "CustomerType"
  data_source: "orange"
```

#### JSON Example (mappings.yaml):
```json
{
  "customer_data_2024.xlsx": {
    "columns": {
      "First Name": "FirstName",
      "Last Name": "LastName",
      "Email Address": "Email",
      "Phone Number": "Mobile",
      "Post Code": "PostCode"
    },
    "attributes": {
      "Subscription Status": "SubscriptionStatus",
      "Customer Type": "CustomerType",
      "Preference": "ContactPreference",
      "Account Status": "AccountStatus",
      "Last Login Date": "LastLoginDate",
      "Registration Source": "RegistrationSource",
      "Marketing Preferences": "MarketingPrefs",
      "Loyalty Tier": "LoyaltyTier"
    },
    "data_source": "tangerine"
  },
  "newsletter_subscribers.csv": {
    "columns": {
      "subscriber_first": "FirstName",
      "subscriber_last": "LastName",
      "subscriber_email": "Email"
    },
    "attributes": {
      "subscription_tier": "SubscriptionTier",
      "opt_in_date": "OptInDate",
      "newsletter_frequency": "NewsletterFrequency",
      "interest_categories": "InterestCategories"
    },
    "data_source": "newsletter_system"
  },
  "legacy_data.xlsx": {
    "columns": {
      "contact_fname": "FirstName",
      "contact_lname": "LastName",
      "contact_email": "Email",
      "mobile_num": "Mobile",
      "postal": "PostCode"
    },
    "attributes": {
      "legacy_id": "LegacyIdentifier",
      "account_creation": "AccountCreationDate",
      "last_purchase": "LastPurchaseDate",
      "total_purchases": "TotalPurchases",
      "preferred_contact": "PreferredContact"
    },
    "data_source": "legacy_system"
  }
}
```

### BigQuery Schema

The script uses the following BigQuery schema:

| Field Name         | Type      | Mode     | Description                               |
| ------------------ | --------- | -------- | ----------------------------------------- |
| Id                 | STRING    | NULLABLE | Unique identifier                         |
| FirstName          | STRING    | NULLABLE | First name                                |
| LastName           | STRING    | NULLABLE | Last name                                 |
| Email              | STRING    | NULLABLE | Email address                             |
| Mobile             | STRING    | NULLABLE | Mobile number                             |
| PostCode           | STRING    | NULLABLE | Postal code                               |
| DataSource         | STRING    | NULLABLE | Data source identifier                    |
| SourceCreatedDate  | STRING    | NULLABLE | Creation date from source                 |
| SourceModifiedDate | STRING    | NULLABLE | Last modified date from source            |
| SourceFile         | STRING    | REQUIRED | Source filename                           |
| Attributes         | RECORD    | REPEATED | Key-value pairs for additional attributes |
| BQInsertedDate     | TIMESTAMP | REQUIRED | BigQuery insertion timestamp              |

#### Attributes Structure
Each attribute in the Attributes array has the following structure:
```json
{
  "Key": "AttributeName",
  "Value": "AttributeValue"
}
```

## Usage

Run the script using command line arguments:

```bash
python main.py \
  --directory /path/to/files \
  --mapping-file mappings.yaml \
  --project-id your-project-id \
  --dataset-id your-dataset \
  --table-id your-table \
  --output-file processing_stats.json
```

### Command Line Arguments

| Argument       | Required | Default               | Description                        |
| -------------- | -------- | --------------------- | ---------------------------------- |
| --directory    | Yes      | None                  | Path to directory containing files |
| --mapping-file | Yes      | None                  | Path to YAML/JSON mapping file     |
| --project-id   | No       | None                  | BigQuery project ID                |
| --dataset-id   | No       | None                  | BigQuery dataset ID                |
| --table-id     | No       | None                  | BigQuery table ID                  |
| --output-file  | No       | processing_stats.json | Statistics output file             |

## Output

### BigQuery Data
The script loads processed data into the specified BigQuery table, including:
- Standard mapped columns
- Dynamic attributes as key-value pairs
- System-generated timestamps and metadata

### Processing Statistics
Generates a JSON file containing:
```json
{
  "filename.csv": {
    "total_rows": 100,
    "processed_rows": 98,
    "failed_rows": 2,
    "status": "success",
    "error_message": null,
    "start_time": "2024-01-01T10:00:00",
    "end_time": "2024-01-01T10:00:05"
  }
}
```

## Error Handling

- Validates file types before processing
- Reports missing mapping configurations
- Logs missing columns from source files
- Captures and reports JSON parsing errors
- Handles BigQuery loading failures
- Validates attribute key-value pairs

## Limitations

- Supports only CSV and Excel (.xlsx) files
- Attribute values are converted to strings
- File names must match mapping configuration exactly
- Cannot handle nested attribute structures
- Maximum of 1000 attributes per row (BigQuery limitation)

## Future Enhancements / Roadmap / Ideas
- [ ] Pre-Validate file formatting before processing (e.g., incorrect data layout, nested new lines, bad characters, non-UTF-8 encoding, etc.)
- [ ] Support for nested attribute structures (e.g., JSON objects)
- [ ] Enhanced error handling and reporting (e.g., error log files for each file and detailed error messages)
- [ ] Support for additional source system (e.g., Google Cloud Storage)
- [ ] Support for default data definitions for common source systems (e.g., Salesforce, HubSpot)
- [ ] Support for best match data definition based on existing YAML / JSON mappings (e.g., Provide random CSV file and get best match data definition, column mapping, and attribute mapping, then allow the user to decide if they want to use it or not)

## License
This repository is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Authors
- [Benjamin Western](https://benjaminwestern.io)