# Collibra Bulk Exporter

This project helps in bulk exporting assets along with their related attributes, relations, and responsibilities from Collibra. It allows you to specify asset type IDs in a configuration file and exports the data into your desired format (CSV, JSON, Excel).

## Features

- Export assets by asset type ID
- Include all related attributes, relations, and responsibilities
- Support for multiple output formats (CSV, JSON, Excel)
- Parallel processing for faster exports
- Automatic pagination for large datasets
- Comprehensive logging

---

## Project Structure

The project has been organized into a proper Python package structure:

```
collibra-bulk-exporter/
├── config/                      # Configuration files
│   └── Collibra_Asset_Type_Id_Manager.json
├── logs/                        # Log files
├── outputs/                     # Exported data files
├── src/                         # Source code
│   ├── collibra_exporter/       # Main package
│   │   ├── api/                 # API-related modules
│   │   │   ├── fetcher.py       # Data fetching functionality
│   │   │   ├── graphql_query.py # GraphQL query generation
│   │   │   └── oauth_auth.py    # OAuth authentication
│   │   ├── models/              # Data models
│   │   │   ├── exporter.py      # Data export functionality
│   │   │   └── transformer.py   # Data transformation
│   │   ├── utils/               # Utility functions
│   │   │   ├── asset_type.py    # Asset type utilities
│   │   │   └── logging_config.py # Logging configuration
│   │   ├── processor.py         # Core processing logic
│   │   └── __init__.py          # Package initialization
│   └── main.py                  # Entry point
├── .env                         # Environment variables
├── setup.py                     # Package setup
└── requirements.txt             # Dependencies
```

---

## Setup Instructions

### 1. Setting Up OAuth in Your Collibra Instance

To connect the tool with your Collibra instance, you need to set up OAuth credentials:

1. **Log in to Collibra**: 
   - Navigate to your Collibra instance.

2. **Access OAuth Settings**: 
   - Go to **Settings** -> **OAuth Applications**.

3. **Register a New Application**:
   - Click on **Register Application**.
   - Set the integration type to **"Integration"** and give the name of the **application**.

4. **Generate Client Credentials**:
   - Copy the `clientId` and `clientSecret`.
   - Add them to the `.env` file as shown below.

### 2. Clone the Repository

```bash
# Clone the repository from GitHub
$ git clone https://github.com/sayan123234/Collibra-Bulk-Exporter-To-File.git

# Navigate into the project directory
$ cd Collibra-Bulk-Exporter-To-File
```

### 3. Create a Python Virtual Environment

```bash
# Create a virtual environment
$ python -m venv env

# Activate the virtual environment
# On Windows
$ env\Scripts\activate

# On macOS/Linux
$ source env/bin/activate
```

### 4. Install the Package

```bash
# Install the package in development mode
$ pip install -e .
```

### 5. Set Up the `.env` File

Create a `.env` file in the root directory of the project and add the following environment variables:

```env
# Environment Variables for Collibra-Bulk-Exporter

# Collibra instance URL (e.g., your_instance_name.collibra.com)
COLLIBRA_INSTANCE_URL=your_instance_name.collibra.com

# Path to your output directory
FILE_SAVE_LOCATION=outputs

# Path to your configuration file
CONFIG_PATH=config/Collibra_Asset_Type_Id_Manager.json

# Output format (choose: csv, json, excel)
OUTPUT_FORMAT=csv

# Client ID and Secret of your registered application in Collibra
CLIENT_ID=your_client_id
CLIENT_SECRET=your_client_secret
```

### 6. Update Asset Type IDs

Edit the `config/Collibra_Asset_Type_Id_Manager.json` file to include the asset type IDs you want to export:

```json
{
    "ids": [
        "asset_type_id_1",
        "asset_type_id_2"
    ]
}
```

### 7. Run the Application

Run the script to export the assets:

```bash
# Run using the Python module
$ python src/main.py

# Or, if installed with pip
$ collibra-exporter
```

The output files will be saved in the `outputs` directory in the format specified in the `.env` file.

---

## Troubleshooting

1. **Dependency Errors**:
   - Ensure you are using the correct Python version (recommended: Python 3.8 or higher).
   - Reinstall dependencies using: `pip install -e .`.

2. **Connection Issues**:
   - Verify that the `COLLIBRA_INSTANCE_URL` and credentials in the `.env` file are correct.

3. **Permission Errors**:
   - Ensure that the registered OAuth application has the necessary permissions to access asset data in Collibra.

4. **Configuration Issues**:
   - Check that the `CONFIG_PATH` environment variable points to the correct configuration file.
   - Verify that the configuration file contains valid asset type IDs.

---

## Additional Notes

- Make sure to keep your `.env` file and credentials secure.
- For large datasets, exporting might take some time; monitor the progress in the terminal.
- Check the logs in the `logs` directory for detailed information about the export process.

Enjoy using the Collibra Bulk Exporter!
