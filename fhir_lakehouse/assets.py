import os
import glob
import boto3
import snowflake.connector
import json
import io
from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PrivateFormat, NoEncryption
from dagster import asset, AssetExecutionContext
from dotenv import load_dotenv
from fhir.resources.patient import Patient
from fhir.resources.humanname import HumanName
from fhir.resources.address import Address

# Load .env relative to this file so it works regardless of the working directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

AWS_BUCKET_NAME = "vc-healthcare-datalake"
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

# Column definitions for each Snowflake table, keyed by CSV filename (without extension)
TABLE_SCHEMAS = {
    "patients": """
        Id STRING, BIRTHDATE DATE, DEATHDATE DATE, SSN STRING,
        DRIVERS STRING, PASSPORT STRING, PREFIX STRING, FIRST STRING,
        LAST STRING, SUFFIX STRING, MAIDEN STRING, MARITAL STRING,
        RACE STRING, ETHNICITY STRING, GENDER STRING, BIRTHPLACE STRING,
        ADDRESS STRING, CITY STRING, STATE STRING, COUNTY STRING, ZIP STRING,
        LAT FLOAT, LON FLOAT, HEALTHCARE_EXPENSES FLOAT, HEALTHCARE_COVERAGE FLOAT
    """,
    "conditions": """
        "START" DATE, "STOP" DATE, PATIENT STRING, ENCOUNTER STRING,
        CODE STRING, DESCRIPTION STRING
    """,
    "encounters": """
        Id STRING, "START" TIMESTAMP_NTZ, "STOP" TIMESTAMP_NTZ, PATIENT STRING,
        ORGANIZATION STRING, PROVIDER STRING, PAYER STRING, ENCOUNTERCLASS STRING,
        CODE STRING, DESCRIPTION STRING, BASE_ENCOUNTER_COST FLOAT,
        TOTAL_CLAIM_COST FLOAT, PAYER_COVERAGE FLOAT,
        REASONCODE STRING, REASONDESCRIPTION STRING
    """,
    "medications": """
        "START" DATE, "STOP" DATE, PATIENT STRING, PAYER STRING, ENCOUNTER STRING,
        CODE STRING, DESCRIPTION STRING, BASE_COST FLOAT, PAYER_COVERAGE FLOAT,
        DISPENSES INT, TOTALCOST FLOAT, REASONCODE STRING, REASONDESCRIPTION STRING
    """,
}


@asset(group_name="cloud_ingestion")
def upload_raw_to_s3(context: AssetExecutionContext) -> list:
    """
    Step 1: Uploads all CSV files from data/raw/ to S3.
    """
    s3_client = boto3.client("s3")
    uploaded = []

    csv_files = glob.glob(os.path.join(RAW_DATA_DIR, "*.csv"))
    context.log.info(f"Found {len(csv_files)} CSV file(s) to upload: {[os.path.basename(f) for f in csv_files]}")

    for local_path in csv_files:
        filename = os.path.basename(local_path)
        s3_key = f"raw/{filename}"
        context.log.info(f"Uploading {filename} to s3://{AWS_BUCKET_NAME}/{s3_key} ...")
        s3_client.upload_file(local_path, AWS_BUCKET_NAME, s3_key)
        uploaded.append(s3_key)

    context.log.info(f"S3 upload complete. {len(uploaded)} file(s) uploaded.")
    return uploaded


@asset(deps=[upload_raw_to_s3], group_name="cloud_ingestion")
def s3_to_snowflake_raw(context: AssetExecutionContext) -> str:
    """
    Step 2: Creates a Snowflake table for each CSV and bulk-copies the data from S3.
    """
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as f:
        private_key = load_pem_private_key(f.read(), password=None)
    private_key_bytes = private_key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption()
    )

    ctx = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=private_key_bytes,
        database="CLINICAL_LAKEHOUSE",
        schema="PUBLIC"
    )
    cs = ctx.cursor()
    cs.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

    for table_name, columns_ddl in TABLE_SCHEMAS.items():
        context.log.info(f"Setting up table raw.{table_name} ...")
        cs.execute(f"CREATE TABLE IF NOT EXISTS raw.{table_name} ({columns_ddl});")

        context.log.info(f"Copying s3://{AWS_BUCKET_NAME}/raw/{table_name}.csv -> raw.{table_name} ...")
        cs.execute(f"""
            COPY INTO raw.{table_name}
            FROM 's3://{AWS_BUCKET_NAME}/raw/{table_name}.csv'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')
            FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """)
        result = cs.fetchone()
        context.log.info(f"raw.{table_name} load result: {result}")

    cs.close()
    ctx.close()

    context.log.info("All tables synced from S3 to Snowflake.")
    return f"Loaded {len(TABLE_SCHEMAS)} table(s) into Snowflake raw schema."

@asset(deps=[s3_to_snowflake_raw],group_name = "fhir_standardization")
def generate_fhir_patients(context: AssetExecutionContext) -> str:
    """
    Step3: Extracts raw database records from snowflake, maps them to strict
    HL7 FHIR R4 JSON structures, and saves them to AWS s3
    """ 
    #1. Connect to snowflake to read the raw inputs
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as f:
        private_key = load_pem_private_key(f.read(), password=None)
    private_key_bytes = private_key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption()
    )

    ctx = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=private_key_bytes,
        database="CLINICAL_LAKEHOUSE",
        schema="PUBLIC"
    )
    cs = ctx.cursor()

    #Grab a smaple of 100 patients to test the transforamtion loop
    context.log.info("Fetching raw patient data from snowflake")
    cs.execute("SELECT ID, FIRST, LAST, GENDER, BIRTHDATE, CITY, STATE FROM raw.patients LIMIT 100")
    rows =cs.fetchall()
    cs.close()
    ctx.close()

    #2. Initialize AWS s3 client to store our transformed data
    s3_client = boto3.client('s3')
    success_count = 0
    context.log.info(f"Beginning FHIR R4 transformation loop for {len(rows)} patients...")

    for row in rows:
        p_id, first_name, last_name, gender, birthdate, city, state = row

        try:
            #3. Construct a strictly compliant FHIR patient resource using python models
            fhir_patient= Patient()
            fhir_patient.id = str(p_id)
            # fhir_patient.resourceType = "Patient"
            fhir_patient.active =True
            fhir_patient.birthDate = birthdate.strftime("%Y-%m-%d")if birthdate else None
            fhir_patient.gender = gender.lower()

        #Construct complex nested human name
            name = HumanName()
            name.use = "official"
            name.family = last_name
            name.given = [first_name]
            fhir_patient.name = [name]
        
        # Construct complex nested address
            addr = Address()
            addr.use = "home"
            addr.city = city
            addr.state = state
            fhir_patient.address = [addr]
        
        # Convert the strict object into a standard Python Dictionary/JSON string
            patient_json = fhir_patient.json(indent=2)
        
        # 4. Stream the JSON file directly to AWS S3 (Standardized Layer)
            # S3 Key structure acts as our cloud file directory path
            s3_key = f"standardized/fhir/patients/{p_id}.json"

            s3_client.put_object(
                Bucket=AWS_BUCKET_NAME,
                Key=s3_key,
                Body=patient_json,
                ContentType="application/json"
            )
            success_count += 1
        except Exception as e:
            context.log.error (f"Failed to map patient {p_id} to FHIR: {str(e)}")
            continue

    context.log.info(f"Successfully serialized and uploaded {success_count} FHIR patient bundles to S3")
    return f"Uploaded {success_count} FHIR JSON objects to s3://{AWS_BUCKET_NAME}/standardized/fhir/patients/"