import pandas as pd
import requests
import json
import os
from datetime import datetime, timezone
from tqdm import tqdm
import xml.etree.ElementTree as ET
import re
from bs4 import BeautifulSoup
import time
from google.cloud import storage
from google.cloud import bigquery
from validation.validator import validate_dataframe, DataValidationError
import sys

# ---- ISRCTN FUNCTION ----
 
ISRCTN_BASE_URL = "https://www.isrctn.com/api/query/format/default"

PARAMS = {
    "q": "heart",
    "limit": 5
}

def fetch_isrctn_xml():
    response = requests.get(ISRCTN_BASE_URL, params=PARAMS, timeout=30)

    if response.status_code != 200:
        print("ISRCTN API failed:", response.status_code)
        return None

    content_type = response.headers.get("Content-Type", "")

    if "xml" not in content_type.lower():
        print("ISRCTN did not return XML. Content-Type:", content_type)
        return None

    return response.content

NS = {"ns": "http://www.67bricks.com/isrctn"}

def parse_isrctn_xml(xml_content):
    root = ET.fromstring(xml_content)
    
    records = []
    
    for full_trial in root.findall("ns:fullTrial", NS):
        trial = full_trial.find("ns:trial", NS)
        if trial is None:
            continue
        
        record = {}
        
        record["registry_id"] = trial.findtext("ns:isrctn", default=None, namespaces=NS)
        
        # Title
        desc = trial.find("ns:trialDescription", NS)
        record["title"] = (
            desc.findtext("ns:title", default=None, namespaces=NS)
            if desc is not None else None
        )
        
        # Condition
        conditions = trial.find("ns:conditions", NS)
        if conditions is not None:
            condition_node = conditions.find("ns:condition", NS)
            
            if condition_node is not None:
                description_node = condition_node.find("ns:description", NS)
                
                record["condition"] = (
                    description_node.text.strip()
                    if description_node is not None and description_node.text
                    else None
                )
            else:
                record["condition"] = None
        else:
            record["condition"] = None

#Had to dive deep into the folder to find conditions and country
      
        # Recruitment Country
        participants = trial.find("ns:participants", NS)
        if participants is not None:
            recruitment_countries = participants.find("ns:recruitmentCountries", NS)
            
            if recruitment_countries is not None:
                country_node = recruitment_countries.find("ns:country", NS)
                
                record["recruitment_country"] = (
                    country_node.text.strip()
                    if country_node is not None and country_node.text
                    else None
                )
            else:
                record["recruitment_country"] = None
        else:
            record["recruitment_country"] = None
        
        record["trial_id"] = f"isrctn:{record['registry_id']}"
        record["ingestion_ts"] = datetime.now(timezone.utc)

        record["source"] = "isrctn"
        
        records.append(record)
    
    return records

# ---- CTGOV FUNCTION ----

def fetch_ctgov_all(condition="heart attack"):
    
    CTGOV_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
    
    all_rows = []
    next_page_token = None
    
    while True:
        params = {
            "query.cond": condition,
            "pageSize": 100
        }
        
        if next_page_token:
            params["pageToken"] = next_page_token
        
        response = requests.get(CTGOV_BASE_URL, params=params)
        response.raise_for_status()
        
        data = response.json()
        studies = data.get("studies", [])
        
        for study in studies:
            protocol = study.get("protocolSection", {})
            id_module = protocol.get("identificationModule", {})
            conditions_module = protocol.get("conditionsModule", {})
            contacts_module = protocol.get("contactsLocationsModule", {})
            
            registry_id = id_module.get("nctId")
            trial_id = f"CTGOV:{registry_id}" if registry_id else None
            
            locations = contacts_module.get("locations", [])
            countries = list(
                set(loc.get("country") for loc in locations if loc.get("country"))
            )
            country_value = ", ".join(countries) if countries else None
            
            all_rows.append({
                "trial_id": trial_id,
                "source": "CTGOV",
                "registry_id": registry_id,
                "title": id_module.get("briefTitle"),
                "condition": ", ".join(conditions_module.get("conditions", [])),
                "country": country_value
            })
        
        next_page_token = data.get("nextPageToken")
        
        if not next_page_token:
            break
    
    return pd.DataFrame(all_rows)

# ---- EUCTR FUNCTION ----

EUCTR_BASE_URL = "https://www.clinicaltrialsregister.eu"

def fetch_euctr_sample(keyword="heart attack", max_trials=5):
    
    search_url = f"{EUCTR_BASE_URL}/ctr-search/search?query={keyword}"
    response = requests.get(search_url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    trial_links = []
    
    for link in soup.find_all("a", href=True):
        if "trial/" in link["href"]:
            full_url = link["href"] if link["href"].startswith("http") else EUCTR_BASE_URL + link["href"]
            trial_links.append(full_url)
    
    trial_links = list(set(trial_links))[:max_trials]
    
    rows = []
    
    for url in trial_links:
        try:
            trial_response = requests.get(url)
            trial_response.raise_for_status()
            
            trial_soup = BeautifulSoup(trial_response.text, "html.parser")
            text = trial_soup.get_text()
            
            # VERY basic extraction for sample
            eudract_number = None
            if "EudraCT Number:" in text:
                eudract_number = text.split("EudraCT Number:")[1].split("\n")[0].strip()
            
            rows.append({
                "trial_id": f"EUCTR:{eudract_number}" if eudract_number else None,
                "source": "EUCTR",
                "registry_id": eudract_number,
                "title": None,  # refine later
                "condition": None,
                "country": None,
                "ingestion_ts": datetime.now(timezone.utc)
            })
            
            time.sleep(1)
            
        except Exception as e:
            print("Error scraping:", url, e)
            continue
    
    return pd.DataFrame(rows)

# ---- EMA FUNCTION ----

EMA_CDP_HOME = "https://clinicaldata.ema.europa.eu/web/cdp"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ClinicalTrialProject/1.0)"}

def fetch_ema_sample_from_latest_news(max_items=25):
    """
    Pulls the publicly visible 'Clinical data published' items from EMA CDP home page.
    No login required. Produces canonical columns ready for Parquet/BigQuery staging.
    """
    r = requests.get(EMA_CDP_HOME, headers=HEADERS, timeout=60)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text("\n")

    # Pattern like: 19/02/2026 Clinical data published
    pattern = re.compile(r"(\d{2}/\d{2}/\d{4})\s+Clinical data published", re.IGNORECASE)

    matches = list(pattern.finditer(text))
    rows = []

    for i, m in enumerate(matches[:max_items]):
        pub_date = m.group(1)

        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        block = text[start:end].strip()

        # Take first non-empty line as a "title-ish" summary
        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        summary = lines[0] if lines else None

        # Try to extract medicine name from "refer to <Medicine>, a ..."
        med = None
        med_match = re.search(r"refer to\s+([A-Za-z0-9][A-Za-z0-9\- ]+?),\s+a\s", block, re.IGNORECASE)
        if med_match:
            med = med_match.group(1).strip()

        # Build a stable-ish registry_id for this feed item
        registry_id = f"{pub_date}:{med or (summary or 'clinical-data')}".strip()

        rows.append({
            "trial_id": f"EMA_CDP:{registry_id}",
            "source": "EMA_CDP",
            "registry_id": registry_id,
            "title": med or summary,
            "condition": None,      # optional: can NLP this later, but not needed for pipeline proof
            "country": "EU",
            "ingestion_ts": datetime.now(timezone.utc)
        })

    return pd.DataFrame(rows)


#---MAIN---

def main():
    print("Pipeline started...")

    # --- RUN INGESTION ---

    # ISRCTN
    xml_data = fetch_isrctn_xml()

    if xml_data:
        try:
            isrctn_records = parse_isrctn_xml(xml_data)
            df_isrctn = pd.DataFrame(isrctn_records)
        except Exception as e:
            print("ISRCTN parse failed:", e)
            df_isrctn = pd.DataFrame()
    else:
        df_isrctn = pd.DataFrame()
    if not df_isrctn.empty:
        df_isrctn = df_isrctn.rename(columns={"recruitment_country": "country"})

    # CTGov
    df_ctgov = fetch_ctgov_all("heart attack")
    df_ctgov["ingestion_ts"] = datetime.now(timezone.utc)

    # EUCTR
    df_euctr = fetch_euctr_sample("heart attack", max_trials=50)

    # EMA
    df_ema = fetch_ema_sample_from_latest_news(max_items=20)

    # --- COMBINE ---
    df_all = pd.concat([df_isrctn, df_ctgov, df_euctr, df_ema], ignore_index=True)
    df_all = df_all[df_all["trial_id"].notna()]
    df_all = df_all.drop_duplicates(subset=["trial_id"], keep="last")

    print("Total unified rows:", len(df_all))

    # --- DATA VALIDATION ---
    try:
        print("Running data validation...")
        validate_dataframe(df_all)
        print("Data validation passed.")

    except DataValidationError as e:
        print(f"DATA VALIDATION FAILED: {str(e)}")
        sys.exit(1)

    # --- SAVE LOCAL PARQUET ---
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    local_file = f"clinical_trials_staging.parquet"
    df_all.to_parquet(local_file, index=False)

    print("Local parquet saved.")

    # --- UPLOAD TO GCS ---
    bucket_name = "clinical-trials-staging"
    destination_blob = f"staging/unified/ingestion_date={ingestion_date}/clinical_trials_staging.parquet"

    storage_client = storage.Client(project="clinical-trial-project")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(local_file)

    print("Uploaded to GCS:", destination_blob)

    # --- LOAD INTO BIGQUERY STAGING TABLE ---
    bq_client = bigquery.Client(project="clinical-trial-project")

    table_id = "clinical-trial-project.clinical_trials.stg_trials"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_APPEND",
    )

    uri = f"gs://{bucket_name}/{destination_blob}"

    load_job = bq_client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config,
    )

    load_job.result()

    print("Loaded into BigQuery staging table.")

    # --- MERGE INTO ANALYTICS TABLE ---
    merge_query = """
    MERGE `clinical-trial-project.clinical_trials.analytics_trials` T
    USING (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT
        trial_id, source, registry_id, title, condition, country, ingestion_ts,
        ROW_NUMBER() OVER (PARTITION BY trial_id ORDER BY ingestion_ts DESC) AS rn
        FROM `clinical-trial-project.clinical_trials.stg_trials`
        WHERE trial_id IS NOT NULL
    )
    WHERE rn = 1
    ) S
    ON T.trial_id = S.trial_id
    WHEN MATCHED THEN
    UPDATE SET
        source = S.source,
        registry_id = S.registry_id,
        title = S.title,
        condition = S.condition,
        country = S.country,
        ingestion_ts = S.ingestion_ts
    WHEN NOT MATCHED THEN
    INSERT (trial_id, source, registry_id, title, condition, country, ingestion_ts)
    VALUES (S.trial_id, S.source, S.registry_id, S.title, S.condition, S.country, S.ingestion_ts);
    """

    merge_job = bq_client.query(merge_query)
    merge_job.result()

    print("Merge completed.")

    # --- CLEAN STAGING TABLE ---
    bq_client.query(
        "TRUNCATE TABLE `clinical-trial-project.clinical_trials.stg_trials`").result()

    print("Staging table truncated.")

    print("Pipeline finished successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("FATAL ERROR:")
        traceback.print_exc()
        raise