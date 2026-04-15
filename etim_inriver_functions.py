#!/usr/bin/env python3
"""
09.03.2026 Updated, added logging for upsert error
28.11.2025 Support functions for ETIM updates
"""
from service_logger import logger

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import json
import sys
import time
from datetime import datetime
import os

LIMIT_ITEMS_FETCH = 1000 # Possible to limit number of Items for testing etc. purpose

ETIM_CLASS_FILE = "ETIMClassCVL.json" # For testing

#APIKEY = '63df2bfe3df0236c479467a8e3b33d74' #TEST
APIKEY = '3a7ccd9efd61adcfc26a7868825c0c52' #PROD
endpoint_fetchdata = 'https://apieuw.productmarketingcloud.com/api/v1.0.1/entities:fetchdata'
endpoint_query = 'https://apieuw.productmarketingcloud.com/api/v1.0.0/query'
endpoint_etimclasses = 'https://apieuw.productmarketingcloud.com/api/v1.0.0/model/cvls/ETIMClass/values'
endpoint_upsert = 'https://apieuw.productmarketingcloud.com/api/v1.0.0/entities:upsert'

UPSERT_CHUNK_SIZE = 10
UPSERT_WAIT_UNTIL_NEXT = 10 # Wait until send next Upsert
UPSERT_TIME_CONTINUE = 60  # Register unusual delay, but continue upserts
UPSERT_TIME_HALT = 200     # Register unusual delay, and stop upserts

def post_response(url, json=None) -> requests.Response:
    retry_strategy = Retry(
        total=1,  # Total number of retries
        status_forcelist=[429, 502, 503, 504],  # Status codes to retry
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],  # Include POST in HTTP methods to retry
        backoff_factor=2,  # Backoff factor for retries
        raise_on_status=False,  # Do not raise exceptions for retryable responses
    )
    session = requests.Session()
    session.headers.update({
        'X-inRiver-APIKey': APIKEY,
        'Content-Type': 'application/json'
    })
    session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    try:
        # Using session.post instead of session.get
        response = session.post(url, json=json, timeout=300)
        return response

    except requests.exceptions.RequestException as e:
        # Handle any request-related exceptions here
        # Replace 'logger' with your logger's name if it's different
        logger.error(f"An error occurred: {e}")
        return None


def put_response(url, json) -> requests.Response:
    retry_strategy = Retry(
        total=2,  # Total number of retries
        status_forcelist=[429, 500, 502, 503, 504],  # Status codes to retry
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],  # Include POST in HTTP methods to retry
        backoff_factor=3,  # Backoff factor for retries
        raise_on_status=False,  # Do not raise exceptions for retryable responses
    )
    session = requests.Session()
    session.headers.update({
        'X-inRiver-APIKey': APIKEY,
        'Content-Type': 'application/json'
    })
    session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    try:
        # Using session.post instead of session.get
        response = session.put(url, json=json)
        return response

    except requests.exceptions.RequestException as e:
        # Handle any request-related exceptions here
        # Replace 'logger' with your logger's name if it's different
        logger.error(f"An error occurred: {e}")
        return None

def get_response(url) -> requests.Response:
    retry_strategy = Retry(
        total=2,  # Total number of retries
        status_forcelist=[429, 500, 502, 503, 504],  # Status codes to retry
        allowed_methods=["HEAD", "GET", "OPTIONS"],  # HTTP methods to retry
        backoff_factor=2,  # Backoff factor for retries
        raise_on_status=False,  # Do not raise exceptions for retryable responses
    )
    session = requests.Session()
    session.headers.update({
        'X-inRiver-APIKey': APIKEY,
        'Content-Type': 'application/json'
    })
    session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    try:
        response = session.get(url)
        return response

    except requests.exceptions.RequestException as e:
        # Handle any request-related exceptions here
        logger.error(f"An error occurred: {e}")
        return None


def load_etimclass_from_file(file_path: str) -> list:
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            suppliers = json.load(file)
            return suppliers
    except Exception as e:
        logger.info(f"Exception occurred while reading suppliers file: {str(e)}")
        return []
    
def get_etim_classes():
    if 0:
        etim_class = load_etimclass_from_file(ETIM_CLASS_FILE)
        if not etim_class:  # Check if empty
            logger.error("No etim_class found.")
            raise ValueError("ETIM class data is empty.")
        return etim_class
    else:
        return load_etimclass_from_inriver()


def load_etimclass_from_inriver():
    resp = get_response(endpoint_etimclasses)
    if ( resp.status_code == 200):
        return resp.json()
    else:
        logger.error (f"get_etim_classes {resp.status_code}")
        return None


# Using PUT update single entity data with ItemETIMClassGroup, ItemETIMClass
# NOT TO BE USED, SINCE BETTER JUST TO UPSERT ALL ETIM VALUES AT ONCE
def update_etimclasses(items, supplierVat):
    logger.info(f"update_etimclasses {supplierVat} items {len(items)}")
    for ResultRow in items:
        try:
            entityId = ResultRow.get("entityId","")
            fieldValues = ResultRow.get("fieldValues","")

            ## Remove extra fields from Inriver update
            fields_to_remove = {'ItemSupplierProductCode', 'ItemElectricityNumber', 'ItemLVINumber'}
            fieldValues = [field for field in fieldValues if field["fieldTypeId"] not in fields_to_remove]

            endpoint_put = f"https://apieuw.productmarketingcloud.com/api/v1.0.0/entities/{entityId}/fieldvalues"
            resp = put_response (endpoint_put, fieldValues)
            if ( resp.status_code == 200):
                logger.info (f"updateOK {round(resp.elapsed.total_seconds(), 1)}: {entityId} {fieldValues}")
            else:
                logger.info (f"updateFAIL {round(resp.elapsed.total_seconds(), 1)}: {entityId} {fieldValues}")

        except:
            errortext = str('Error  {}. {}, line: {}'.format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
            logger.error(errortext)


# Item Upsert request should not have both (Item: ItemETIMClass + ItemETIMClassGroup) and specificationData
# ALWAYS ENSURE THAT ONLY EITHER ONE IS INCLUDED, ELSE CAN CREATE DOUBLE LINKS TO SPECIFICATION
def upsert_etimvalues(items, keyfield, supplierVat, include_specification=False):
    all_items = []
    logger.info(f"upsert_etimvalues {supplierVat} items {len(items)} include_specification:{include_specification}")
    for item in items:
        try:
            fieldValues = item.get("fieldValues", [])
            
            # Extract values for required fields
            field_map = {fv["fieldTypeId"]: fv["value"] for fv in fieldValues}
            
            # Create JSON structure
            if include_specification:
                # Group and Class cannot be define simultaneously with potential new specification linking
                item_json = {
                    "entityTypeId": "Item",
                    "keyFieldTypeIds": [keyfield, "ItemSupplierNumber"],
                    "fieldvalues": [
                        ["ItemSupplierNumber", field_map.get("ItemSupplierNumber", "")],
                        [keyfield, field_map.get(keyfield, "")],
                        ["ItemMemo", field_map.get("ItemMemo", "")]
                    ]
                }
                etim_specification = item.get("specificationData", None)
                if etim_specification:
                    item_json["specificationData"] = etim_specification

            else:
                # Update/add only Group and Class, which shall create new Specification linking
                item_json = {
                    "entityTypeId": "Item",
                    "keyFieldTypeIds": [keyfield, "ItemSupplierNumber"],
                    "fieldvalues": [
                        ["ItemSupplierNumber", field_map.get("ItemSupplierNumber", "")],
                        [keyfield, field_map.get(keyfield, "")],
                        ["ItemETIMClass", field_map.get("ItemETIMClass", "")],
                        ["ItemETIMClassGroup", field_map.get("ItemETIMClassGroup", "")],
                        ["ItemMemo", field_map.get("ItemMemo", "")]
                    ]
                }

            # Add item JSON to the list
            all_items.append(item_json)

        except Exception as e:
            errortext = f"Error {sys.exc_info()[0]}. {sys.exc_info()[1]}, line: {sys.exc_info()[2].tb_lineno}"
            logger.error(errortext)

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    os.makedirs("logs", exist_ok=True)
    upsert_filename = os.path.join("logs", f"{supplierVat}_{timestamp}.json")
    with open(upsert_filename, 'w', encoding='utf-8') as f:
        json.dump(all_items, f, ensure_ascii=False, indent=4)
        logger.info(f'inriver upsert data dumped to {upsert_filename}')

    if all_items:
        total_items = len(all_items)
        processed = 0
        for i in range(0, len(all_items), UPSERT_CHUNK_SIZE):
            chunk = all_items[i:i + UPSERT_CHUNK_SIZE]
            processed += len(chunk)
            logger.info(f"Sending upsert {supplierVat} {len(chunk)} items ({processed}/{total_items})")
            resp = post_response(endpoint_upsert, chunk)
            
            if resp is None or resp.status_code != 200:
                logger.error(f"Upsert failed {supplierVat} StatusCode {resp.status_code if resp else 'N/A'}")
                logger.error(chunk)
                continue

            response_time = resp.elapsed.total_seconds()
            try:
                response_data = resp.json()
            except ValueError:
                logger.error(f"Upsert returned invalid JSON for {supplierVat}: {resp.text}")
                continue

            error_count = response_data.get("errorCount", 0)
            errors = response_data.get("errors", [])
            updatedEntities = len(response_data.get("updatedEntities", 0))

            logger.info(f"Upsert success {supplierVat} items {len(chunk)}; updatedEntities {updatedEntities}; ResponseTime {response_time}; {resp.text}")
            if error_count > 0:
                logger.error(f"Upsert had {error_count} errors: {errors}")
                if error_count == len(chunk):
                    logger.error(f"Upsert aborted chunk for {supplierVat}")
                    continue

            if response_time > UPSERT_TIME_HALT:
                logger.warning(f"Response time ({response_time}s) exceeded {UPSERT_TIME_HALT}, continuing with next chunk")
                continue

            elif response_time > UPSERT_TIME_CONTINUE:
                logger.warning(f"Response time ({response_time}s) exceeded {UPSERT_TIME_CONTINUE}. Sleep 60s before next batch.")
                time.sleep(60)

            else:
                logger.info(f"wait {UPSERT_WAIT_UNTIL_NEXT}s before next...")
                time.sleep(UPSERT_WAIT_UNTIL_NEXT)

    else:
        logger.warning(f'Nothing to Upsert for {supplierVat}')


def fetch_items(itemEntityIds):
    try:
        params = {
            "entityIds": itemEntityIds,
            "objects": "FieldValues",
            "fieldTypeIds": "ItemSupplierNumber, ItemElectricityNumber,ItemLVINumber,ItemGTINCode,ItemSupplierProductCode,ItemETIMClass,ItemETIMClassGroup,ItemMemo"
        }

        resp = post_response(endpoint_fetchdata, params)
        if resp is None:
            logger.error("Abort, fetch_items response is None")
            return []

        if resp.status_code != 200:
            logger.error(f"Abort, invalid status_code: {resp.status_code} {resp.text}")
            return []

        return resp.json()
    except Exception:
        errortext = str('Error  {}. {}, line: {}'.format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
        logger.error(errortext)
        return []


def fetch_items_unlimited(entityRemaining):
    try:
        BATCH_SIZE = 1000
        combined_json = []
        fetched_items = 0

        while entityRemaining:
            if len(entityRemaining) > BATCH_SIZE:
                entitySubset = entityRemaining[:BATCH_SIZE]
                entityRemaining = entityRemaining[BATCH_SIZE:]
            else:
                entitySubset = entityRemaining
                entityRemaining = []

            logger.debug(f'inriver_functions fetch_items: {len(entitySubset)}, remaining {len(entityRemaining)}')

            # Fetch data for the batch
            json_data = fetch_items(entitySubset)
            if json_data:
                combined_json.extend(json_data)

            # Track number of fetched items, check if iterations are limited
            fetched_items += len(entitySubset)
            if LIMIT_ITEMS_FETCH <= fetched_items:
                entityRemaining = []
                logger.info('inriver_functions LIMIT_ITEMS_FETCH enabled')

        return combined_json

    except Exception as e:
        logger.error(f"FATAL ERROR: {str(e)}")
        sys.exit("FATAL ERROR")


def query_items(SupplierVATNumber):
    try:
        params = {
            "systemCriteria": [ 
                { 
                "type": "EntityTypeId", 
                "value": "Item",
                "operator": "Equal" 
                }
            ],
            "dataCriteria": [
                {
                    "fieldTypeId": "ItemStatus",
                    "value": "Active",
                    "operator": "Equal"
                },
                {
                    "fieldTypeId": "ItemETIMClassGroup",
                    "operator": "IsEmpty"
                },
                {
                    "fieldTypeId": "ItemETIMClass",
                    "operator": "IsEmpty"
                }
            ],
            "linkCriterion": {
                "dataCriteria": [
                {
                    "fieldTypeId": "SupplierStatus",
                    "value": "Active"
                },
                {
                    "fieldTypeId": "SupplierVATNumber",
                    "value": SupplierVATNumber
                }
                ],
                "linkTypeId": "SupplierItems",
                "direction": "inbound",
                "linkExists": True
                }
        }
        resp = post_response(endpoint_query, params)
        if resp is None:
            logger.error(f"query_items: no response for supplier {SupplierVATNumber}")
            return []

        if resp.status_code != 200:
            logger.error(f"query_items failed {SupplierVATNumber} status {resp.status_code}: {resp.text}")
            return []

        data = resp.json()
        logger.info(f"inriver_functions {SupplierVATNumber} Active Items ItemETIMClassGroup IsEmpty {data.get('count', 0)}")
        return data.get('entityIds', [])

    except Exception:
        errortext = str('Error  {}. {}, line: {}'.format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
        print(errortext)
        logger.error(errortext)


def get_items(suppliervat):
    entities = query_items(suppliervat)
    inriver_items = fetch_items_unlimited(entities)

    # for debugging save the data from Inriver
    tmp_filename = 'inriver_items_data_fetched.json'
    os.makedirs("data", exist_ok=True)
    file_path = os.path.join("data", tmp_filename)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(inriver_items, f, ensure_ascii=False, indent=4)

    return inriver_items


if __name__ == '__main__':
    #get_items('FI0000000')
    get_items('FI01149018')
