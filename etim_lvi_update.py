#!/usr/bin/env python3
"""
09.02.2026
Update missing ETIMClass/Groups to Inriver
# For each Supplier
    # Get ETIMClass CVL from Inriver
    # Get Inriver Active Items with empty ItemETIMClassGroup
    # Get etim json from Azure blob (updated by LVI/STK etim-monitor)
    # Transform etim-stk json to inriver format
    # Update ETIM classes/groups to Inriver
"""
from service_logger import logger
import etim_inriver_functions
import traceback

from openpyxl import Workbook

import json
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime
import pandas as pd
import os


def load_env_file():
    env_path = '.env'
    if not os.path.exists(env_path):
        return

    with open(env_path, 'r', encoding='utf-8') as env_file:
        for line in env_file:
            stripped = line.strip()
            if not stripped or stripped.startswith('#'):
                continue
            if '=' not in stripped:
                continue
            key, value = stripped.split('=', 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


load_env_file()

TAG = 'lvi'
UPDATE_INRIVER = True
LVI_SUPPLIERS_FILE = 'lvi_suppliers_update.json'    # local file to define which suppliers

AZURE_STORAGE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING', '')
BLOB_CONTAINER_NAME = "tuotetieto-logging-test"
BLOB_DIRECTORY_PATH = "suppliers/etim/"

# COMMON
def load_suppliers_from_file(file_path: str) -> list:
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            suppliers = json.load(file)
            return suppliers
    except Exception as e:
        logger.info(f"Exception occurred while reading suppliers file: {str(e)}")
        return []

# COMMON
def save_to_json_file(data, filename):
    os.makedirs("data", exist_ok=True)
    file_path = os.path.join("data", filename)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# COMMON
def download_from_blob(container_name, blob_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_data = blob_client.download_blob().readall()
        return json.loads(blob_data)
    except Exception as e:
        logger.error(f"Failed to download {blob_name} from blob: {e}")
        return None

# COMMON
def log_inriver_items(filtered_inriver_items_json, supplier_vat):
    # Build timestamped log filename
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    os.makedirs("logs", exist_ok=True)
    log_path = os.path.join("logs", f"{supplier_vat}_{timestamp}.log")

    # Open file once, write everything
    with open(log_path, "w", encoding="utf-8") as f:
        for row in filtered_inriver_items_json:
            entity_id = row.get("entityId", "")
            field_values = row.get("fieldValues", [])
            spec_data = row.get("specificationData", {})

            item_lvis_number = ""
            etim_class_group = ""
            etim_class = ""

            for fv in field_values:
                key = fv.get("fieldTypeId")
                val = fv.get("value", "")
                if key in ("ItemElectricityNumber", "ItemLVINumber"):
                    item_lvis_number = val
                elif key == "ItemETIMClassGroup":
                    etim_class_group = val
                elif key == "ItemETIMClass":
                    etim_class = val

            features = len(spec_data.get("specificationValues", "")) if spec_data else 0
            f.write(
                f"{datetime.now():%Y-%m-%d %H:%M:%S} "
                f"etim_update entityId: {entity_id} {item_lvis_number} "
                f"{etim_class_group} {etim_class} features: {features}\n"
            )


# Function to process products in chunks and dynamically extract columns
def process_products(products):
    # Extract all relevant TT keys dynamically
    all_keys = set()
    for product in products:
        for key in product.keys():
            if key != "etimFeatureValues":  # Ignore the etimFeatureValues key
                all_keys.add(key)

    # Filter out keys where all values are empty in the chunk
    valid_keys = {key for key in all_keys if any(product.get(key) for product in products)}

    # Sort columns by the TT number (assuming keys follow the pattern "TT###")
    sorted_keys = sorted(valid_keys, key=lambda x: int(x[2:]))

    # Prepare the final list of product records with filtered keys
    filtered_products = [
        {key: product.get(key, '') for key in valid_keys} for product in products
    ]

    # Convert the filtered products to a pandas DataFrame
    df = pd.DataFrame(filtered_products)

    # Reorder the DataFrame according to the sorted keys
    df = df[sorted_keys]

    return df


# LVI ETIM
def init_etim_writer():
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "ETIM Data"

    #etim_file = open(ETIM_FILENAME, 'w', newline='', encoding='utf-8')
    headers = [
        "TT100",
        "TT020",
        "TT200",
        #"etimFeatureValueId",
        "etimClassId",
        "etimFeatureId",
        "etimFeatureType",
        "value1",
        "value2"
        #"unitOfMeasureAbbreviation",
        #"etimUnitOfMeasureId"
    ]
    #etim_writer = csv.DictWriter(etim_file, fieldnames=headers, dialect='excel', delimiter=';')
    #etim_writer.writeheader()
    #return etim_writer, etim_file

    sheet.append(headers)
    return workbook, sheet


# LVI specific
def transform_lvi_etim_specification(item) -> json:
    etim_features = 0
    specificationValues = []
    try:
        if "etimFeatureValues" in item:
            etim_class = item.get("TT060", None)
            etim_feature_values = item.get("etimFeatureValues", None)
            for feature in etim_feature_values:
                etim_features += 1
                #etim_class= feature.get("etimClassId", None) # Should always be the same as upper level
                if feature.get("etimClassId", None) != etim_class:
                    logger.error(f'TT020 {item.get("TT020","")} TT060 {etim_class} different from etimFeatureValues etimClassId {feature.get("etimClassId", None)}')
                    return []

                etim_feature = feature.get("etimFeatureId", None)
                etim_type = feature.get("etimFeatureType", None)
                value1 = feature.get("value1", None)

                if etim_class and etim_feature and etim_type and value1:
                    if etim_type in ("ALPHANUMERIC", "NUMERIC"):
                        specificationValues.append([f'{etim_class}{etim_feature}', value1])
                    elif etim_type in ("LOGICAL"):
                        specificationValues.append([f'{etim_class}{etim_feature}', value1.lower() == "true"])
                    elif etim_type in ("RANGE"):
                        value2 = feature.get("value2", None)
                        specificationValues.append([f'{etim_class}{etim_feature}Min', value1])
                        specificationValues.append([f'{etim_class}{etim_feature}Max', value2])
                    else:
                        logger.error(f'ItemLVINumber {item.get("ItemLVINumber","")} etimFeatureType {etim_type} not recognised')
                        return []
            
            if specificationValues:
                specification_data = {
                    "specification": etim_class,
                    "specificationValues": specificationValues
                }
                return specification_data
            else:
                return []


    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)
        filename, line, func, text = tb[-1]  # Get last traceback entry
        errortext = f"Error in {filename}, function {func}, line {line}: {e}"
        logger.error(errortext)
        return []


# LVI specific
#    "TT010": "ItemElectricityNumber",
#    "TT020": "ItemLVINumber",
#    "TT050": "ItemSupplierProductCode",
#    "TT052": "ItemGTINCode",
#    "TT060": "ItemSetETIMClass",
# Based on Inriver items add ETIM classes/groups to it
# Remove Items which do not have ItemETIMClassGroup (ETIMClass has been deleted from dynamic database)
# Remove Items which do not have any ETIM features
# Return updated Inriver json with ItemETIMClass/ItemETIMClassGroup
def update_and_filter_inriver_items(tt_items_json, inriver_items_json, etim_classes_json) -> tuple:
    if not isinstance(etim_classes_json, list) or not etim_classes_json:
        logger.error("ETIM class CVL is missing or invalid.")
        return [], [], []
    if not isinstance(tt_items_json, list):
        logger.error("TT items JSON is not a list.")
        return [], [], []

    etim_class_map = {item.get("key"): item.get("parentKey") for item in etim_classes_json if isinstance(item, dict)}
    tt_items_dict = {
        item.get("TT020"): item
        for item in tt_items_json if isinstance(item, dict) and item.get("TT020")
    }
    changed_items = []
    missing_lvis_numbers = []
    items_to_remove = []  # Inriver entity IDs that should be removed
    datenow = datetime.now().strftime("%Y-%m-%d")

    # Iterate through inriver_items_json and update ItemETIMClassGroup and ItemETIMClass
    # These Inriver Items should not have yet any ETIM Class/Groups existing, or it should MUST be the same
    for result_row in inriver_items_json:
        field_values = result_row.get("fieldValues")
        if not isinstance(field_values, list):
            logger.warning(f'Invalid fieldValues in Inriver item: {result_row.get("entityId", "unknown")}')
            continue

        item_lvis_number = None
        for field in field_values:
            if field.get("fieldTypeId") == "ItemLVINumber":
                item_lvis_number = field.get("value", "")

        # If a matching LVIS-number is found, update the fields
        if item_lvis_number and item_lvis_number in tt_items_dict:
            matched_item = tt_items_dict[item_lvis_number]
            do_update = False  # Track if any value changes

            # Step1: Check if there is ETIM class defined, stop processing if not
            new_etim_class = matched_item.get("TT060", None)
            if new_etim_class == None:
                #logger.info(f'{item_lvi_number} ETIMClass TT060 missing from LVIS')
                do_update = False
                continue

            # Step2: Get and add corresponding ETIM Group, if not found, then class has been deprecated
            new_etim_group = etim_class_map.get(new_etim_class, None)
            if new_etim_group == None:
                logger.warning(f'{item_lvis_number} ETIMClass TT060 {new_etim_class} deprecated, no matching ItemETIMClassGroup')
                do_update = False
                continue

            # Step3: Cross-check and filter and update fieldvalues
            for field in result_row["fieldValues"]:

                if field["fieldTypeId"] == "ItemETIMClass":
                    if new_etim_class is not None:
                        inriver_value = field.get("value", None)
                        if inriver_value is None:
                            field["value"] = new_etim_class
                            do_update = True
                        elif inriver_value == new_etim_class:
                            #logger.info(f'{item_lvis_number} ItemETIMClass same: Source {new_etim_class} Inriver {inriver_value}')
                            do_update = False
                        else:  # query should have returned only Items with missing ETIM class/group
                            logger.warning(f'{item_lvis_number} ItemETIMClass different: Source {new_etim_class} Inriver {inriver_value}')
                            do_update = False
                            break
                    else:
                        logger.error(f'{item_lvis_number} ItemETIMClass compare')
                        do_update = False
                        break

                elif field["fieldTypeId"] == "ItemETIMClassGroup":
                    if new_etim_group is not None:
                        inriver_value = field.get("value", None)
                        if inriver_value is None:
                            field["value"] = new_etim_group
                            do_update = True
                        elif inriver_value == new_etim_group:
                            #logger.info(f'{item_lvi_number} ItemETIMClassGroup same: Source {new_etim_class} Inriver {inriver_value}')
                            do_update = False
                        else:  # query should have returned only Items with missing ETIM class/group
                            logger.warning(f'{item_lvis_number} ItemETIMClassGroup different: Source {new_etim_group} Inriver {inriver_value}')
                            do_update = False
                            break
                    else:
                        logger.error(f'{item_lvis_number} ItemETIMClassGroup compare')
                        do_update = False
                        break

                # DO NOT include IF ItemSupplierProductCode does not match
                elif field["fieldTypeId"] == "ItemSupplierProductCode":
                    source_value = matched_item.get("TT050", None)
                    def normalize(value):
                        return value.strip() if isinstance(value, str) else "" if value is None else value
                    if normalize(field["value"]) != normalize(source_value):
                        logger.error(f'{item_lvis_number} ItemSupplierProductCode does not match: TT050 {source_value} Inriver {field["value"]}')
                        do_update = False
                        break

                # DO NOT include IF ItemGTINCode does not match
                elif field["fieldTypeId"] == "ItemGTINCode":
                    source_value = matched_item.get("TT052", None)
                    def normalize(value):
                        return value.strip() if isinstance(value, str) else "" if value is None else value
                    if normalize(field["value"]) != normalize(source_value):
                        logger.error(f'{item_lvis_number} ItemGTINCode does not match: TT052 {source_value} Inriver {field["value"]}')
                        do_update = False
                        break

                elif field["fieldTypeId"] == "ItemMemo":
                    field["value"] = f"ETIM Added {datenow}"


            if do_update:
                # AddInriver compatible ETIM specificationData
                specification_data = transform_lvi_etim_specification(matched_item)
                if specification_data:
                    # Add only changed items with ETIM specification data
                    result_row["specificationData"] = specification_data
                    changed_items.append(result_row)
                else:
                    #logger.debug(f'{item_lvis_number} ETIM_Features missing')
                    do_update = False

        else:
            # Collect missing LVIS numbers and entity IDs for removal
            if item_lvis_number:
                missing_lvis_numbers.append(item_lvis_number)
                entity_id = result_row.get("entityId")
                if entity_id:
                    items_to_remove.append(entity_id)
            do_update = False

    return changed_items, missing_lvis_numbers, items_to_remove


def get_duplicate_key_pairs(inriver_items_json, keyfield):
    counts = {}
    for row in inriver_items_json:
        field_values = row.get('fieldValues')
        if not isinstance(field_values, list):
            continue
        values = {fv.get('fieldTypeId'): fv.get('value') for fv in field_values if isinstance(fv, dict)}
        key = (values.get(keyfield), values.get('ItemSupplierNumber'))
        if not all(key):
            continue
        counts[key] = counts.get(key, 0) + 1
    return {key for key, count in counts.items() if count > 1}


def filter_out_duplicate_key_rows(items_json, keyfield, duplicate_keys):
    filtered = []
    for row in items_json:
        field_values = row.get('fieldValues')
        if not isinstance(field_values, list):
            continue
        values = {fv.get('fieldTypeId'): fv.get('value') for fv in field_values if isinstance(fv, dict)}
        key = (values.get(keyfield), values.get('ItemSupplierNumber'))
        if key in duplicate_keys:
            continue
        filtered.append(row)
    return filtered


# LVI
# Updates missing ItemETIMClass and ItemETIMClassGroup to Active Inriver Items
def handler() -> bool:
    try:
        suppliers = load_suppliers_from_file(LVI_SUPPLIERS_FILE)
        if not suppliers:
            logger.info('No suppliers found.')
            return

        # Get ETIMClass CVL from Inriver
        etim_classes_json = etim_inriver_functions.get_etim_classes()
        if not isinstance(etim_classes_json, list) or not etim_classes_json:
            logger.error('No ETIM classes loaded from Inriver; aborting update.')
            return False
        logger.info(f'etim_classes CVL {len(etim_classes_json)} loaded from Inriver')

        # For each Supplier
        for supplier in suppliers:
            supplierVat = supplier.get('SupplierVATNumber', '')
            supplierName = supplier.get('SupplierName', '')
    
            logger.info(f'etim_classes Supplier {supplierVat} {supplierName}')

            # Get Inriver Active Items with empty ItemETIMClassGroup
            inriver_items_json = etim_inriver_functions.get_items(supplierVat)
            if not isinstance(inriver_items_json, list):
                logger.error(f'No valid Inriver items loaded for {supplierVat}')
                continue
            logger.info(f'inriver_items_json {len(inriver_items_json)} loaded from Inriver')
            if not inriver_items_json:
                logger.info(f'No Inriver items to update for {supplierVat}')
                continue

            duplicate_keys = get_duplicate_key_pairs(inriver_items_json, 'ItemLVINumber')
            if duplicate_keys:
                logger.error(f'Supplier {supplierVat} has {len(duplicate_keys)} duplicate ItemLVINumber+ItemSupplierNumber keys in Inriver; ambiguous items will be skipped.')
                logger.error(f'Duplicate keys sample: {list(duplicate_keys)[:10]}')
                inriver_items_json = filter_out_duplicate_key_rows(inriver_items_json, 'ItemLVINumber', duplicate_keys)

            # Get etim-stk json from Azure blob (updated by etim-monitor)
            etim_filename = f'etim-{TAG}-{supplierVat}.json'
            blob_name = f"{BLOB_DIRECTORY_PATH}{etim_filename}"
            if tt_json is None:
                logger.error(f"Skipping supplier {supplierVat} due to missing blob data: {blob_name}")
                continue
            if not isinstance(tt_json, list):
                logger.error(f"Skipping supplier {supplierVat}: downloaded ETIM data is not a list")
                continue
            save_to_json_file(tt_json, etim_filename)

            # TEST WITH FILE
            #etim_filename = f'etim-{TAG}-{supplierVat}.json'
            #with open(etim_filename, 'r', encoding='utf-8') as infile:
            #    tt_json = json.load(infile)  # Load JSON data from the file

            filtered_inriver_items_json, missing_lvis_numbers, items_to_remove = update_and_filter_inriver_items(tt_json, inriver_items_json, etim_classes_json)
            inriver_items_len = len(filtered_inriver_items_json)
            logger.info(f'filtered_inriver_items_json length: {inriver_items_len}')
            
            # Log summary of missing LVIS numbers
            if missing_lvis_numbers:
                logger.warning(f'{len(missing_lvis_numbers)} items from Inriver not found in LVIS data: {missing_lvis_numbers[:10]}{"..." if len(missing_lvis_numbers) > 10 else ""}')
            
            # Log items that should be removed from Inriver
            if items_to_remove:
                logger.info(f'{len(items_to_remove)} Inriver items should be removed (no longer in source data): {items_to_remove[:10]}{"..." if len(items_to_remove) > 10 else ""}')
            
            save_to_json_file(filtered_inriver_items_json, f'etim-{TAG}-{supplierVat}-inriver.json')
            #save_to_excel_file(filtered_inriver_items_json, f'etim-{TAG}-inriver-{supplierVat}.xlsx')

            # Update ETIM classes/groups to Inriver
            if UPDATE_INRIVER and inriver_items_len:
                log_inriver_items(filtered_inriver_items_json, supplierVat)
                logger.info(f'UPDATE_INRIVER:{UPDATE_INRIVER} items:{inriver_items_len}')
                etim_inriver_functions.upsert_etimvalues(filtered_inriver_items_json, 'ItemLVINumber', supplierVat, include_specification=True)

        return True


    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)
        filename, line, func, text = tb[-1]  # Get last traceback entry
        errortext = f"Error in {filename}, function {func}, line {line}: {e}"
        logger.error(errortext)
        return False


def main():
    logger.info(f'START ETIM_{TAG}_UPDATE')
    handler ()
    logger.info(f'END ETIM_{TAG}_UPDATE')

if __name__ == '__main__':
    main()