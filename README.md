# Collibra-Bulk-Exporter

Follow the instruction below for creating .env file:
---------------------------------------------------------------

ENVIRONMENT= Set your environment of your collibra instance (dev/test/prod)

COLLIBRA_INSTANCE_URL = https://your_instance_name.collibra.com

FILE_SAVE_LOCATION=/path/to/your/output/directory
OUTPUT_FORMAT=csv  # or 'json', 'excel'


CLIENT_ID= Client ID of your registered application in Collibra
CLIENT_SECRET= Client secret of your registered application in Collibra

----------------------------------------------------------------

Follow the instruction below for setting up Oauth in your Collibra instance:
---------------------------------------------------------------
1. Go to settings -> Oauth Applications 
2.Register a new application -> Choose integration type as "Integration"
3.Use the the clientSecret and ClientId in env file
---------------------------------------------------------------


#   C o l l i b r a - B u l k - E x p o r t e r  
 