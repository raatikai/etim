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

TAG = 'stk'
UPDATE_INRIVER = True
STK_SUPPLIERS_FILE = 'stk_suppliers_update.json'    # local file to define which suppliers

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
def save_to_excel_file(json_data, filename):
    # Extract relevant fields and convert to a structured DataFrame
    excel_data = []
    for item in json_data:
        entity_id = item["entityId"]
        item_data = {
            "sys_id": entity_id,
            #"ItemSupplierProductCode": None,
            #"ItemElectricityNumber": None,
            "ItemETIMClassGroup": None,
            "ItemETIMClass": None,
            "ItemMemo": None,
        }

        for field in item["fieldValues"]:
            if field["fieldTypeId"] in item_data:
                item_data[field["fieldTypeId"]] = field["value"]

        excel_data.append(item_data)

    # Convert to DataFrame
    df = pd.DataFrame(excel_data)
    # Save to Excel file
    excel_filename = filename
    df.to_excel(excel_filename, index=False)


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


# STK specific transformation from TT ETIM data to Inriver ETIM data model
# INPUT TT item
#    "TT010": "ItemElectricityNumber",
#    "TT020": "ItemLVINumber",
#    "TT050": "ItemSupplierProductCode",
#    "TT052": "ItemGTINCode",
#    "TT060": "ItemSetETIMClass",
# OUTPUT: Inriver compatible ETIM specification_data
def transform_stk_etim_specification(item) -> json:
    etim_features = 0
    specificationValues = []
    try:
        if "Features" in item:
            etim_class = item.get("TT060", None)
            etim_feature_values = item.get("Features", None)
            for feature in etim_feature_values:
                etim_features += 1

                etim_feature = feature.get("Identifier", None)
                etim_type = feature.get("Type", None)
                value1 = feature.get("Value", None)

                if etim_class and etim_feature and etim_type and value1:
                    if etim_type in ("A", "N", "ALPHANUMERIC", "NUMERIC"):
                        specificationValues.append([f'{etim_class}{etim_feature}', value1])
                    elif etim_type in ("L", "LOGICAL"):
                        specificationValues.append([f'{etim_class}{etim_feature}', value1])
                    elif etim_type in ("R", "RANGE"):
                        value2 = feature.get("ValueEnd", None)
                        specificationValues.append([f'{etim_class}{etim_feature}Min', value1])
                        specificationValues.append([f'{etim_class}{etim_feature}Max', value2])
                    else:
                        logger.error(f'TT010 ItemElectricityNumber {item.get("ItemElectricityNumber","")} etimFeatureType {etim_type} not recognised')
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


# STK specific
# Based on Inriver items add ETIM classes/groups to it
# Remove Items which do not have ItemETIMClassGroup (ETIMClass has been deleted from dynamic database)
# Remove Items which do not have any ETIM features
# Return updated Inriver json with ItemETIMClass/ItemETIMClassGroup
def update_and_filter_inriver_items(tt_items_json, inriver_items_json, etim_classes_json) -> json:
    if not isinstance(etim_classes_json, list) or not etim_classes_json:
        logger.error("ETIM class CVL is missing or invalid.")
        return []
    if not isinstance(tt_items_json, list):
        logger.error("TT items JSON is not a list.")
        return []

    etim_class_map = {item.get("key"): item.get("parentKey") for item in etim_classes_json if isinstance(item, dict)}
    tt_items_dict = {
        item.get("TT010"): item
        for item in tt_items_json if isinstance(item, dict) and item.get("TT010")
    }
    changed_items = []
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
            if field.get("fieldTypeId") == "ItemElectricityNumber":
                item_lvis_number = field.get("value", "")
                #logger.info(f'{item_lvis_number}')

        # If a matching LVIS-number is found, update the fields
        if item_lvis_number and item_lvis_number in tt_items_dict:
            matched_item = tt_items_dict[item_lvis_number]
            do_update = False  # Track if any value changes

            # Step1: Check if there is ETIM class defined, stop processing if not
            new_etim_class = matched_item.get("TT060", None)
            if new_etim_class == None:
                logger.warning(f'{item_lvis_number} ETIMClass TT060 missing from LVIS')
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
                            #logger.info(f'{item_lvis_number} ItemETIMClass new: Source {new_etim_class} Inriver {inriver_value}')
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
                        logger.warning(f'{item_lvis_number} ItemETIMClassGroup compare')
                        do_update = False
                        break

                # DO NOT include IF ItemSupplierProductCode does not match
                elif field["fieldTypeId"] == "ItemSupplierProductCode":
                    source_value = matched_item.get("TT050", None)
                    def normalize(value):
                        return value.strip() if isinstance(value, str) else "" if value is None else value
                    if normalize(field["value"]) != normalize(source_value):
                        logger.warning(f'{item_lvis_number} ItemSupplierProductCode does not match: TT050 {source_value} Inriver {field["value"]}')
                        do_update = False
                        break

                # DO NOT include IF ItemGTINCode does not match
                elif field["fieldTypeId"] == "ItemGTINCode":
                    source_value = matched_item.get("TT052", None)
                    def normalize(value):
                        return value.strip() if isinstance(value, str) else "" if value is None else value
                    if normalize(field["value"]) != normalize(source_value):
                        logger.warning(f'{item_lvis_number} ItemGTINCode does not match: TT052 {source_value} Inriver {field["value"]}')
                        do_update = False
                        break

                elif field["fieldTypeId"] == "ItemMemo":
                    field["value"] = f"ETIM Added {datenow}"


            if do_update:
                # AddInriver compatible ETIM specificationData
                specification_data = transform_stk_etim_specification(matched_item)
                if specification_data:
                    # Add only changed items with ETIM specification data
                    result_row["specificationData"] = specification_data
                    changed_items.append(result_row)
                else:
                    #logger.debug(f'{item_lvis_number} ETIM_Features missing')
                    do_update = False

        else:
            logger.warning(f'item_lvis_number {item_lvis_number} from Inriver not found from LVIS data')
            do_update = False

    return changed_items


# STK ETIM handler
# Updates missing ItemETIMClass and ItemETIMClassGroup to Active Inriver Items
def handler() -> bool:
    try:
        suppliers = load_suppliers_from_file(STK_SUPPLIERS_FILE)
        if not suppliers:
            logger.error('No suppliers found.')
            return False

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

            # Get etim-stk json from Azure blob (updated by etim-monitor)
            etim_filename = f'etim-{TAG}-{supplierVat}.json'
            blob_name = f"{BLOB_DIRECTORY_PATH}{etim_filename}"
            tt_json = download_from_blob(BLOB_CONTAINER_NAME, blob_name)
            if tt_json is None:
                logger.error(f"Skipping supplier {supplierVat} due to missing blob data: {blob_name}")
                continue
            if not isinstance(tt_json, list):
                logger.error(f"Skipping supplier {supplierVat}: downloaded ETIM data is not a list")
                continue
            save_to_json_file(tt_json, f'etim-{TAG}-{supplierVat}.json')

            # TEST WITH FILE
            #etim_filename = f'etim-{TAG}-{supplierVat}.json'
            #with open(etim_filename, 'r', encoding='utf-8') as infile:
            #    tt_json = json.load(infile)  # Load JSON data from the file

            filtered_inriver_items_json = update_and_filter_inriver_items(tt_json, inriver_items_json, etim_classes_json)
            inriver_items_len = len(filtered_inriver_items_json)
            logger.info(f'filtered_inriver_items_json length: {inriver_items_len}')
            save_to_json_file(filtered_inriver_items_json, f'etim-{TAG}-{supplierVat}-inriver.json')
            #save_to_excel_file(filtered_inriver_items_json, f'etim-{TAG}-inriver-{supplierVat}.xlsx')

            log_inriver_items(filtered_inriver_items_json, supplierVat)

            # Update ETIM classes/groups to Inriver
            if UPDATE_INRIVER and inriver_items_len:
                log_inriver_items(filtered_inriver_items_json, supplierVat)
                logger.info(f'UPDATE_INRIVER:{UPDATE_INRIVER} items:{inriver_items_len}')
                etim_inriver_functions.upsert_etimvalues(filtered_inriver_items_json, 'ItemElectricityNumber', supplierVat, include_specification=True)

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