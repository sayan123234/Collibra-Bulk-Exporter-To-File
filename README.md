# Collibra-Bulk-Exporter

This project helps in bulk exporting assets along with their related attributes, relations, and responsibilities by providing `assetTypeId(s)`. The `Collibra_Asset_Type_Id_Manager` is the JSON file where you can update the asset IDs. The tool then exports the data in the desired format (CSV, JSON, Excel). Additionally, the file or files are auto-generated based on the `assetTypeName(s)`.

Follow the instructions below for creating the `.env` file:

---

### Environment Variables:
- `ENVIRONMENT`  
  Set the environment of your Collibra instance (e.g., `dev`, `test`, `prod`).

- `COLLIBRA_INSTANCE_URL`  
  Example: `https://your_instance_name.collibra.com`

- `FILE_SAVE_LOCATION`  
  Path to your output directory.

- `OUTPUT_FORMAT`  
  Options: `csv`, `json`, or `excel`.

- `CLIENT_ID`  
  Client ID of your registered application in Collibra.

- `CLIENT_SECRET`  
  Client secret of your registered application in Collibra.

---

### Setting Up OAuth in Your Collibra Instance:

1. Go to **Settings** -> **OAuth Applications**.
2. Register a new application:
   - Choose integration type as **"Integration"**.
3. Use the `clientSecret` and `clientId` in the `.env` file.

---

# Collibra-Bulk-Exporter
