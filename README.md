# ETIM Update Scripts

This workspace contains ETIM update utilities for syncing ETIM metadata from Azure blob storage into Inriver.

## Purpose

- `etim_lvi_update.py`: Updates ETIM class/group data for LVI suppliers.
- `etim_stk_update.py`: Updates ETIM class/group data for STK suppliers.
- `etim_inriver_functions.py`: Shared helper functions for Inriver API access, ETIM CVL loading, item querying, and upserts.
- `service_logger.py`: Central logging configuration.

## How it works

1. Supplier definitions are loaded from `lvi_suppliers_update.json` or `stk_suppliers_update.json`.
2. The script fetches current active Inriver items missing `ItemETIMClassGroup`.
3. It downloads ETIM export JSON from Azure blob storage.
4. It transforms the ETIM payload to Inriver specification format.
5. It uploads updated items back to Inriver using the upsert API.

## Improvements made

- Added validation for downloaded ETIM blob data before processing.
- Added validation for Inriver item payloads and field values.
- Improved error handling in API calls:
  - `query_items()` checks HTTP response validity.
  - `fetch_items()` now returns safely on missing or bad responses.
  - `upsert_etimvalues()` logs failures per chunk and continues processing.
- Prevented the update scripts from aborting on a single bad supplier or bad input payload.

## Running

Copy `.env.example` to `.env` and set your Azure storage connection string. The project `.gitignore` excludes `.env` so secrets are not committed.

```bash
copy .env.example .env
# then update .env with your connection string
python etim_lvi_update.py
python etim_stk_update.py
```

Alternatively, set the environment variable directly:

```bash
set AZURE_STORAGE_CONNECTION_STRING="<your-connection-string>"
python etim_lvi_update.py
python etim_stk_update.py
```

## Notes

- This workspace currently does not contain a Git repository in the folder.
- If you want changes tracked in Git, initialize a repository in the root directory before committing.
