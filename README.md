# Collibra-Bulk-Exporter

This project helps in bulk exporting assets along with their related attributes, relations, and responsibilities by providing `assetTypeId(s)`. The `Collibra_Asset_Type_Id_Manager.json` is the configuration file where you can update the asset type IDs to specify what you want to export. The tool exports the data into the desired file format (CSV, JSON, Excel) in a separate folder called `outputs`. Files are auto-generated based on the `assetTypeName(s)`.

---

## Setup Instructions

## Setting Up OAuth in Your Collibra Instance

To connect the tool with your Collibra instance, you need to set up OAuth credentials.

1. **Log in to Collibra**: 
   - Navigate to your Collibra instance.

2. **Access OAuth Settings**: 
   - Go to **Settings** -> **OAuth Applications**.

3. **Register a New Application**:
   - Click on **Register Application**.
   - Set the integration type to **"Integration"** and give the name of the **application**.

4. **Generate Client Credentials**:
   - Copy the `clientId` and `clientSecret`.
   - Add them to the `.env` file as shown above.

---

Follow the steps below to set up and use the Collibra-Bulk-Exporter:

### 1. Clone the Repository

```bash
# Clone the repository from GitHub
$ git clone <repository-url>

# Navigate into the project directory
$ cd Collibra-Bulk-Exporter
```

### 2. Create a Python Virtual Environment

```bash
# Create a virtual environment
$ python -m venv env

# Activate the virtual environment
# On Windows
$ env\Scripts\activate

# On macOS/Linux
$ source env/bin/activate
```

### 3. Install Dependencies

```bash
# Install the required Python packages
$ pip install -r requirements.txt
```

### 4. Set Up the `.env` File

Create a `.env` file in the root directory of the project and add the following environment variables:

```env
# Environment Variables for Collibra-Bulk-Exporter

# Collibra instance URL (e.g., your_instance_name.collibra.com)
COLLIBRA_INSTANCE_URL=your_instance_name.collibra.com

# Path to your output directory
FILE_SAVE_LOCATION=outputs

# Output format (choose: csv, json, excel)
OUTPUT_FORMAT=csv

# Client ID and Secret of your registered application in Collibra
CLIENT_ID=your_client_id
CLIENT_SECRET=your_client_secret
```

### 5. Update Asset Type IDs

Edit the `Collibra_Asset_Type_Id_Manager.json` file in the root directory to include the asset type IDs you want to export. For example:

```json
{
    "assetTypeIds": [
        "asset_type_id_1",
        "asset_type_id_2"
    ]
}
```

### 6. Run the Application

Run the script to export the assets:

```bash
$ python main.py
```

The output files will be saved in the `outputs` directory in the format specified in the `.env` file.

---

## Troubleshooting

1. **Dependency Errors**:
   - Ensure you are using the correct Python version (recommended: Python 3.8 or higher).
   - Reinstall dependencies using: `pip install -r requirements.txt`.

2. **Connection Issues**:
   - Verify that the `COLLIBRA_INSTANCE_URL` and credentials in the `.env` file are correct.

3. **Permission Errors**:
   - Ensure that the registered OAuth application has the necessary permissions to access asset data in Collibra.

---

## Additional Notes

- Make sure to keep your `.env` file and credentials secure.
- For large datasets, exporting might take some time; monitor the progress in the terminal.

Enjoy using the Collibra-Bulk-Exporter!
