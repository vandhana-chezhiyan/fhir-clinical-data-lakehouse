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
from fhir.resources.encounter import Encounter
from fhir.resources.period import Period
from fhir.resources.coding import Coding
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.reference import Reference
from fhir.resources.medicationrequest import MedicationRequest
from fhir.resources.codeablereference import CodeableReference
from fhir.resources.condition import Condition

# Load .env relative to this file so it works regardless of the working directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

AWS_BUCKET_NAME = "vc-healthcare-datalake"
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

# Column definitions for each Snowflake table, keyed by CSV filename (without extension)
TABLE_SCHEMAS = {
    "patients": """
        Id STRING, BIRTHDATE DATE, DEATHDATE DATE, SSN STRING,
        DRIVERS STRING, PASSPORT STRING, PREFIX STRING, "FIRST" STRING,
        "LAST" STRING, SUFFIX STRING, MAIDEN STRING, MARITAL STRING,
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
    Step3: Extracts raw patient database records from snowflake, maps them to strict
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
    cs.execute('SELECT ID, "FIRST", "LAST", GENDER, BIRTHDATE, CITY, STATE FROM raw.patients LIMIT 100')
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
            # Pass all fields at construction — Pydantic v2 validates on __init__
            fhir_patient = Patient(
                id=str(p_id),
                active=True,
                birthDate=birthdate.strftime("%Y-%m-%d") if birthdate else None,
                gender=gender.lower(),
                name=[HumanName(use="official", family=last_name, given=[first_name])],
                address=[Address(use="home", city=city, state=state)]
            )
            patient_json = fhir_patient.model_dump_json(indent=2)
        
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

@asset(deps=[s3_to_snowflake_raw],group_name ="fhir_standardization")
def generate_fhir_encounter(context:AssetExecutionContext)->str:
    """
    Ste4: Extracts raw encounter records from snowflake, maps them to strict
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
    context.log.info("Fetching raw encounter data from snowflake")
    # Fetch core columns including the foreign key linking to the Patient
    cs.execute('SELECT ID, "START", "STOP", PATIENT, ENCOUNTERCLASS, DESCRIPTION FROM raw.encounters LIMIT 100;')
    rows = cs.fetchall()
    cs.close()
    ctx.close()

    s3_client = boto3.client('s3')
    success_count = 0

    for row in rows:
        enc_id, start_time, stop_time, patient_id, enc_class, description = row

        try:
            # Pass all fields at construction — Pydantic v2 validates on __init__
            fhir_enc = Encounter(
                id=str(enc_id),
                status="finished",
                class_fhir=[CodeableConcept(
                    coding=[Coding(
                        code=enc_class.lower(),
                        system="http://terminology.hl7.org/CodeSystem/v3-ActCode"
                    )]
                )],
                actualPeriod=Period(
                    start=start_time.isoformat() + "+00:00" if start_time else None,
                    end=stop_time.isoformat() + "+00:00" if stop_time else None
                ),
                subject=Reference(reference=f"Patient/{patient_id}")
            )

            s3_key = f"standardized/fhir/encounters/{enc_id}.json"
            s3_client.put_object(
                Bucket=AWS_BUCKET_NAME,
                Key=s3_key,
                Body=fhir_enc.model_dump_json(indent=2),
                ContentType="application/json"
            )
            success_count += 1
            
        except Exception as e:
            context.log.error(f"Failed to map encounter {enc_id}: {str(e)}")
            continue

    return f"Uploaded {success_count} FHIR Encounter JSON objects to S3."

@asset(deps=[s3_to_snowflake_raw], group_name="fhir_standardization")
def generate_fhir_medications(context: AssetExecutionContext) -> str:
    """
    Step 5: Extracts raw Medications from Snowflake, maps them
    to HL7 FHIR R5 MedicationRequest JSON profiles, and streams them to S3.
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

    context.log.info("Fetching raw medications from Snowflake...")
    cs.execute('SELECT "START", PATIENT, ENCOUNTER, CODE, DESCRIPTION FROM raw.medications LIMIT 100;')
    rows = cs.fetchall()
    cs.close()
    ctx.close()

    s3_client = boto3.client('s3')
    success_count = 0

    for row in rows:
        start_time, patient_id, encounter_id, code, description = row

        try:
            unique_id = f"{patient_id}-{code}"
            fhir_med = MedicationRequest(
                id=unique_id,
                status="active",
                intent="order",
                medication=CodeableReference(
                    concept=CodeableConcept(
                        coding=[Coding(
                            system="http://www.nlm.nih.gov/research/umls/rxnorm",
                            code=str(code),
                            display=description
                        )],
                        text=description
                    )
                ),
                subject=Reference(reference=f"Patient/{patient_id}"),
                encounter=Reference(reference=f"Encounter/{encounter_id}") if encounter_id else None,
                authoredOn=start_time.isoformat() if start_time else None
            )

            s3_key = f"standardized/fhir/medications/{unique_id}.json"
            s3_client.put_object(
                Bucket=AWS_BUCKET_NAME,
                Key=s3_key,
                Body=fhir_med.model_dump_json(indent=2),
                ContentType="application/json"
            )
            success_count += 1

        except Exception as e:
            context.log.error(f"Failed to map medication for patient {patient_id}: {str(e)}")
            continue

    return f"Uploaded {success_count} FHIR MedicationRequest JSON objects to S3."


@asset(deps=[s3_to_snowflake_raw], group_name="fhir_standardization")
def generate_fhir_conditions(context: AssetExecutionContext) -> str:
    """
    Step 6: Extracts raw Conditions from Snowflake, maps them
    to HL7 FHIR R5 Condition JSON profiles, and streams them to S3.
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

    context.log.info("Fetching raw conditions from Snowflake...")
    cs.execute('SELECT "START", PATIENT, ENCOUNTER, CODE, DESCRIPTION FROM raw.conditions LIMIT 100;')
    rows = cs.fetchall()
    cs.close()
    ctx.close()

    s3_client = boto3.client('s3')
    success_count = 0

    for row in rows:
        start_date, patient_id, encounter_id, code, description = row

        try:
            unique_id = f"{patient_id}-{code}"
            fhir_cond = Condition(
                id=unique_id,
                clinicalStatus=CodeableConcept(
                    coding=[Coding(
                        system="http://terminology.hl7.org/CodeSystem/condition-clinical",
                        code="active"
                    )]
                ),
                code=CodeableConcept(
                    coding=[Coding(
                        system="http://snomed.info/sct",
                        code=str(code),
                        display=description
                    )],
                    text=description
                ),
                subject=Reference(reference=f"Patient/{patient_id}"),
                encounter=Reference(reference=f"Encounter/{encounter_id}") if encounter_id else None,
                onsetDateTime=start_date.isoformat() if start_date else None
            )

            s3_key = f"standardized/fhir/conditions/{unique_id}.json"
            s3_client.put_object(
                Bucket=AWS_BUCKET_NAME,
                Key=s3_key,
                Body=fhir_cond.model_dump_json(indent=2),
                ContentType="application/json"
            )
            success_count += 1

        except Exception as e:
            context.log.error(f"Failed to map condition for patient {patient_id}: {str(e)}")
            continue

    return f"Uploaded {success_count} FHIR Condition JSON objects to S3."