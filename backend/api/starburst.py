from flask import Blueprint, request, jsonify, abort
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import requests
import base64
import json
import os
import re
import time
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

starburst_bp = Blueprint('starburst_bp', __name__)

def retry_api_call(func, max_retries=5, initial_delay=1, max_delay=30):
    for attempt in range(max_retries):
        try:
            response = func()
            if response.status_code in [429, 500, 502, 503, 504]:
                if attempt < max_retries - 1:
                    delay = min(initial_delay * (2 ** attempt), max_delay)
                    print(f" API returned {response.status_code}, retrying in {delay}s (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(delay)
                    continue
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_retries - 1:
                delay = min(initial_delay * (2 ** attempt), max_delay)
                print(f" Request timeout/connection error, retrying in {delay}s (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(delay)
                continue
            raise
        except Exception as e:
            raise
    return response

class ConnectionTestRequest(BaseModel):
    url: str
    username: Optional[str] = None
    password: Optional[str] = None
    accessToken: Optional[str] = None
    customHeaders: Optional[Dict[str, str]] = None
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    connectorId: Optional[str] = None

class ConnectionTestResponse(BaseModel):
    success: bool
    message: str

class StarburstQueryRequest(BaseModel):
    connectorId: str
    sqlQuery: str

class StarburstQueryResponse(BaseModel):
    data: List[Dict[str, Any]]
    columns: List[str]
    message: str = ""

class StarburstTableDetailRequest(BaseModel):
    connectorId: str
    catalog: str
    schema_name: str
    tableName: str

class StarburstTableColumn(BaseModel):
    name: str
    type: str
    comment: Optional[str] = None

class StarburstTableDetailResponse(BaseModel):
    catalog: str
    schema_name: str
    tableName: str
    columns: List[StarburstTableColumn]
    comment: Optional[str] = None

class StarburstSchemaRequest(BaseModel):
    connectorId: str
    catalog: str

class StarburstSchemaResponse(BaseModel):
    schemas: List[str]

class StarburstCatalogResponse(BaseModel):
    catalogs: List[str]

class StarburstLineageRequest(BaseModel):
    connectorId: str
    catalog: str
    schema_name: str
    tableName: str

class StarburstLineageResponse(BaseModel):
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]

class StarburstTableSearchRequest(BaseModel):
    connectorId: str
    query: str
    limit: int = 100
    catalog: Optional[str] = None
    schema_name: Optional[str] = None

class StarburstTableSearchResponse(BaseModel):
    tables: List[Dict[str, Any]]
    total: int

class StarburstColumnSearchRequest(BaseModel):
    connectorId: str
    query: str
    limit: int = 100
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    tableName: Optional[str] = None

class StarburstColumnSearchResponse(BaseModel):
    columns: List[Dict[str, Any]]
    total: int

def get_starburst_access_token(account_domain: str, client_id: str, client_secret: str) -> Optional[str]:
    try:
        base_url = f"https://{account_domain}"
        token_url = f"{base_url}/oauth/v2/token"
        token_data = 'grant_type=client_credentials'
        auth_string = f"{client_id}:{client_secret}"
        auth_b64 = base64.b64encode(auth_string.encode()).decode()
        token_headers = {
            'Authorization': f'Basic {auth_b64}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        token_response = requests.post(token_url, headers=token_headers, data=token_data, timeout=30)
        if token_response.status_code == 200:
            token_response_data = token_response.json()
            return token_response_data.get('access_token')
        else:
            print(f" Failed to get access token: {token_response.status_code} - {token_response.text}")
            return None
    except Exception as e:
        print(f" Error getting Starburst access token: {e}")
        return None

def discover_all_starburst_connectors(account_domain: str, access_token: str) -> List[Dict[str, Any]]:
    try:
        base_url = f"https://{account_domain}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        catalogs_url = f"{base_url}/public/api/v1/catalog"
        catalogs_response = requests.get(catalogs_url, headers=headers, timeout=60)
        catalogs_response.raise_for_status()
        catalogs_data = catalogs_response.json()
        catalogs = catalogs_data.get('result', [])

        print(f" Found {len(catalogs)} catalogs from Starburst")

        connectors_info = []

        for catalog_idx, catalog in enumerate(catalogs, 1):
            catalog_name = catalog.get('catalogName', catalog.get('name'))
            catalog_id = catalog.get('catalogId')
            catalog_type = catalog.get('catalogType')
            connector_type = catalog.get('connectorType')

            if catalog_name and catalog_name.lower() in ['galaxy', 'galaxy_telemetry', 'system', 'information_schema']:
                print(f" Skipping system catalog: {catalog_name}")
                continue

            print(f"Processing catalog {catalog_idx}/{len(catalogs)}: {catalog_name}")
            sys.stdout.flush()

            schemas_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema"
            schemas = []
            try:
                schemas_response = requests.get(schemas_url, headers=headers, timeout=60)
                schemas_response.raise_for_status()
                schemas_data = schemas_response.json()
                schemas_list = schemas_data.get('result', [])

                print(f" Found {len(schemas_list)} schemas in {catalog_name}")

                schema_infos = []
                for schema_idx, schema in enumerate(schemas_list, 1):
                    schema_name = (schema.get('schemaName') or 
                                   schema.get('name') or 
                                   schema.get('schema') or
                                   schema.get('schemaIdentifier', {}).get('schema') or
                                   schema.get('schemaId') or
                                   'UnknownSchema')
                    schema_id = (schema.get('schemaId') or 
                                schema.get('id') or 
                                schema.get('schemaIdentifier', {}).get('schema') or
                                schema_name)

                    print(f"Processing schema {schema_idx}/{len(schemas_list)}: {schema_name}")
                    sys.stdout.flush()

                    tables_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table"
                    tables = []
                    try:
                        tables_response = retry_api_call(
                            lambda: requests.get(tables_url, headers=headers, timeout=60),
                            max_retries=3,
                            initial_delay=1,
                            max_delay=30
                        )
                        if tables_response.text and tables_response.text.strip():
                            tables_response.raise_for_status()
                            tables_data = tables_response.json()
                            tables_list = tables_data.get('result', [])

                            print(f" Found {len(tables_list)} tables in {schema_name}")

                            def fetch_table_columns(table):
                                try:
                                    table_name = table.get('tableName') or table.get('name') or table.get('tableIdentifier', {}).get('table') or str(table.get('tableId', 'Unknown'))
                                    table_id = table.get('tableId') or table.get('id') or table_name
                                    table_type = table.get('tableType') or table.get('type') or 'BASE TABLE'

                                    columns_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table/{table_id}/column"
                                    columns = []
                                    col_attempt = 0
                                    max_col_attempts = 3

                                    while col_attempt < max_col_attempts:
                                        try:
                                            columns_response = retry_api_call(
                                                lambda: requests.get(columns_url, headers=headers, timeout=30),
                                                max_retries=2,
                                                initial_delay=1,
                                                max_delay=10
                                            )

                                            if columns_response.text and columns_response.text.strip():
                                                columns_response.raise_for_status()
                                                columns_data = columns_response.json()
                                                for col in columns_data.get('result', []):
                                                    col_name = col.get('columnId') or col.get('name') or col.get('columnName') or 'unknown_column'
                                                    columns.append({
                                                        "name": col_name,
                                                        "type": col.get('dataType') or col.get('type') or 'STRING',
                                                        "nullable": col.get('nullable', True),
                                                        "description": col.get('description', ''),
                                                        "tags": [tag.get('name') for tag in col.get('tags', [])] if col.get('tags') else []
                                                    })
                                                break
                                            else:
                                                break
                                        except requests.exceptions.Timeout:
                                            col_attempt += 1
                                            if col_attempt < max_col_attempts:
                                                wait_time = min(2 ** col_attempt, 10)
                                                print(f" Timeout fetching columns for {catalog_name}.{schema_name}.{table_name}, retry {col_attempt}/{max_col_attempts} in {wait_time}s...")
                                                time.sleep(wait_time)
                                            else:
                                                print(f" Failed to fetch columns for {catalog_name}.{schema_name}.{table_name} after {max_col_attempts} attempts")
                                                break
                                        except Exception as col_error:
                                            col_attempt += 1
                                            if col_attempt < max_col_attempts:
                                                wait_time = min(2 ** col_attempt, 10)
                                                print(f" Error fetching columns for {catalog_name}.{schema_name}.{table_name}: {col_error}, retry {col_attempt}/{max_col_attempts} in {wait_time}s...")
                                                time.sleep(wait_time)
                                            else:
                                                print(f" Failed to fetch columns for {catalog_name}.{schema_name}.{table_name} after {max_col_attempts} attempts: {col_error}")
                                                break

                                    return {
                                        "table_name": table_name,
                                        "table_id": table_id,
                                        "table_type": table_type,
                                        "columns": columns
                                    }
                                except Exception as table_error:
                                    print(f" Error processing table: {table_error}")
                                    return None

                            print(f" Fetching {len(tables_list)} tables in parallel (20 concurrent)...")
                            sys.stdout.flush()
                            table_infos = []
                            completed_count = 0

                            with ThreadPoolExecutor(max_workers=20) as executor:
                                futures = {executor.submit(fetch_table_columns, table): table for table in tables_list}

                                for future in as_completed(futures):
                                    try:
                                        result = future.result(timeout=60)
                                        if result:
                                            table_infos.append(result)
                                            completed_count += 1
                                            if completed_count % 10 == 0 or completed_count == len(tables_list):
                                                print(f" Progress: {completed_count}/{len(tables_list)} tables completed")
                                                sys.stdout.flush()
                                    except Exception as e:
                                        print(f" Error in future result: {e}")
                                        sys.stdout.flush()

                            print(f" Completed {len(table_infos)} tables in {schema_name}")
                            tables = table_infos
                        else:
                            print(f" No tables found in {schema_name}")
                    except Exception as table_error:
                        print(f" Error fetching tables for {catalog_name}.{schema_name}: {table_error}")
                        tables = []

                    schema_infos.append({
                        "schema_name": schema_name,
                        "schema_id": schema_id,
                        "tables": tables
                    })

                schemas = schema_infos
            except Exception as schema_error:
                print(f" Error fetching schemas for catalog {catalog_name}: {schema_error}")
                schemas = []

            total_tables_in_catalog = sum(len(s.get('tables', [])) for s in schemas)
            print(f" Completed catalog {catalog_name}: {len(schemas)} schemas, {total_tables_in_catalog} tables")

            connectors_info.append({
                "catalog_id": catalog_id,
                "catalog_name": catalog_name,
                "catalog_type": catalog_type,
                "connector_type": connector_type,
                "schemas": schemas
            })

        print(f" Discovered {len(connectors_info)} catalogs with tables")
        return connectors_info

    except Exception as e:
        print(f" Error discovering Starburst connectors: {e}")
        import traceback
        traceback.print_exc()
        return []

def _get_starburst_client(connector_id: str):
    try:
        from flask import current_app
        active_connectors = current_app.config.get('active_connectors', [])

        connector = next((c for c in active_connectors if c["id"] == connector_id and c["type"] == "Starburst Galaxy" and c["enabled"]), None)
        if not connector:
            print(f"ERROR: Starburst connector {connector_id} not found!")
            print(f"Available connectors: {[c['id'] for c in active_connectors]}")
            raise ValueError(f"Starburst connector with ID {connector_id} not found or not enabled.")

        print(f" Found connector: {connector['name']}")

        config = connector.get("config", {})

        if "account_domain" in config:
            base_url = f"https://{config['account_domain']}"
        elif "url" in config:
            base_url = config["url"]
        else:
            raise ValueError(f"Starburst connector config missing url or account_domain")


        username = config.get("username")
        password = config.get("password")
        access_token = config.get("accessToken") or config.get("access_token")
        client_id = config.get("client_id")
        client_secret = config.get("client_secret")


        if client_id and client_secret and not access_token:
            import requests
            try:
                token_url = f"{base_url}/oauth/v2/token"
                token_data = 'grant_type=client_credentials'
                auth_string = f"{client_id}:{client_secret}"
                auth_b64 = base64.b64encode(auth_string.encode()).decode()
                token_headers = {
                    'Authorization': f'Basic {auth_b64}',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
                print(f" Getting Starburst access token from {token_url}")
                token_response = requests.post(token_url, headers=token_headers, data=token_data, timeout=30)
                print(f"Token response status: {token_response.status_code}")
                if token_response.status_code == 200:
                    token_response_data = token_response.json()
                    access_token = token_response_data.get('access_token')
                    print(f" Successfully got Starburst access token (first 20 chars): {access_token[:20] if access_token else 'None'}...")
                else:
                    print(f" Failed to get access token: {token_response.status_code} - {token_response.text}")
            except Exception as token_error:
                print(f" Error getting Starburst access token: {token_error}")
                import traceback
                traceback.print_exc()

        custom_headers = config.get("customHeaders", {})
        catalog = config.get("catalog")
        schema_name = config.get("schema_name")

        return {
            "base_url": base_url,
            "username": username,
            "password": password,
            "access_token": access_token,
            "custom_headers": custom_headers,
            "catalog": catalog,
            "schema_name": schema_name,
        }
    except Exception as e:
        print(f"Error getting Starburst client: {e}")
        import traceback
        traceback.print_exc()
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"Failed to get Starburst client: {e}")

def _starburst_request(
    connector_id: str,
    method: str,
    path: str,
    json_data: Optional[Dict] = None,
    params: Optional[Dict] = None,
    headers: Optional[Dict] = None
):
    from fastapi import HTTPException
    client_config = _get_starburst_client(connector_id)
    base_url = client_config["base_url"].rstrip('/')
    url = f"{base_url}{path}"
    auth = None
    request_headers = client_config["custom_headers"].copy()
    if headers:
        request_headers.update(headers)
    if client_config["username"] and client_config["password"]:
        auth = (client_config["username"], client_config["password"])
    elif client_config['access_token']:
        request_headers["Authorization"] = f"Bearer {client_config['access_token']}"
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, params=params, headers=request_headers, auth=auth, verify=False)
        elif method.upper() == "POST":
            response = requests.post(url, json=json_data, params=params, headers=request_headers, auth=auth, verify=False)
        elif method.upper() == "PUT":
            response = requests.put(url, json=json_data, params=params, headers=request_headers, auth=auth, verify=False)
        elif method.upper() == "DELETE":
            response = requests.delete(url, json=json_data, params=params, headers=request_headers, auth=auth, verify=False)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        response.raise_for_status()
        return response.json() if response.text else {}
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text}")
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Starburst API error: {http_err}. Details: {response.text}"
        )
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
        raise HTTPException(
            status_code=503,
            detail=f"Could not connect to Starburst. Please check the URL and network connectivity: {conn_err}"
        )
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
        raise HTTPException(
            status_code=408,
            detail=f"Starburst request timed out: {timeout_err}"
        )
    except requests.exceptions.RequestException as req_err:
        print(f"An unexpected request error occurred: {req_err}")
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred with Starburst: {req_err}"
        )

def _execute_starburst_query(connector_id: str, sql_query: str) -> Dict[str, Any]:
    from fastapi import HTTPException
    client_config = _get_starburst_client(connector_id)
    base_url = client_config["base_url"].rstrip('/')
    headers = client_config["custom_headers"].copy()
    if client_config["access_token"]:
        headers["Authorization"] = f"Bearer {client_config['access_token']}"
    statement_url = f"{base_url}/v1/statement"
    auth = (client_config["username"], client_config["password"]) if client_config["username"] else None
    try:
        response = requests.post(statement_url, data=sql_query, headers=headers, auth=auth, verify=False)
        response.raise_for_status()
        query_status = response.json()
        while query_status.get("nextUri"):
            if query_status.get("stats", {}).get("state") == "FAILED":
                error = query_status.get("error", {}).get("message", "Unknown error")
                raise HTTPException(status_code=500, detail=f"Starburst query failed: {error}")
            if query_status.get("stats", {}).get("state") == "FINISHED":
                break
            time.sleep(1)
            response = requests.get(query_status["nextUri"], headers=headers, auth=auth, verify=False)
            response.raise_for_status()
            query_status = response.json()
        columns = [col["name"] for col in query_status.get("columns", [])]
        data = query_status.get("data", [])
        return {"columns": columns, "data": data}
    except requests.exceptions.RequestException as e:
        print(f"Error executing Starburst query: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response content: {e.response.text}")
            raise HTTPException(status_code=e.response.status_code, detail=f"Starburst query execution failed: {e.response.text}")
        else:
            raise HTTPException(status_code=500, detail=f"Starburst query execution failed: {e}")

def get_starburst_catalogs(connector_id: str) -> List[str]:
    from fastapi import HTTPException
    try:
        response = _starburst_request(connector_id, "GET", "/v1/metadata/catalog")
        return response
    except Exception as e:
        print(f"Error fetching Starburst catalogs: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch Starburst catalogs: {e}")

def get_starburst_schemas(connector_id: str, catalog: str) -> List[str]:
    from fastapi import HTTPException
    try:
        response = _starburst_request(connector_id, "GET", f"/v1/metadata/catalog/{catalog}/schema")
        return response
    except Exception as e:
        print(f"Error fetching Starburst schemas for catalog {catalog}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch Starburst schemas: {e}")

def get_starburst_tables(connector_id: str, catalog: str, schema_name: str) -> List[str]:
    from fastapi import HTTPException
    try:
        response = _starburst_request(connector_id, "GET", f"/v1/metadata/catalog/{catalog}/schema/{schema_name}")
        return response
    except Exception as e:
        print(f"Error fetching Starburst tables for {catalog}.{schema_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch Starburst tables: {e}")

def get_starburst_table_details(connector_id: str, catalog: str, schema_name: str, table_name: str) -> Dict[str, Any]:
    from fastapi import HTTPException
    try:
        response = _starburst_request(connector_id, "GET", f"/v1/metadata/catalog/{catalog}/schema/{schema_name}/{table_name}")
        return response
    except Exception as e:
        print(f"Error fetching Starburst table details for {catalog}.{schema_name}.{table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch Starburst table details: {e}")

def get_starburst_table_lineage(connector_id: str, catalog: str, schema_name: str, table_name: str) -> Dict[str, Any]:
    from fastapi import HTTPException
    try:
        response = _starburst_request(connector_id, "GET", f"/v1/lineage/catalog/{catalog}/schema/{schema_name}/table/{table_name}")
        return response
    except Exception as e:
        print(f"Error fetching Starburst table lineage for {catalog}.{schema_name}.{table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch Starburst table lineage: {e}")

def search_starburst_tables(connector_id: str, query: str, limit: int = 100, catalog: Optional[str] = None, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
    from fastapi import HTTPException
    try:
        all_tables = []
        catalogs = [catalog] if catalog else get_starburst_catalogs(connector_id)
        for cat in catalogs:
            schemas = [schema_name] if schema_name else get_starburst_schemas(connector_id, cat)
            for sch in schemas:
                tables_in_schema = get_starburst_tables(connector_id, cat, sch)
                for table in tables_in_schema:
                    if query.lower() in table.lower():
                        all_tables.append({"catalog": cat, "schema": sch, "name": table})
                        if len(all_tables) >= limit:
                            return all_tables
        return all_tables
    except Exception as e:
        print(f"Error searching Starburst tables: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to search Starburst tables: {e}")

def search_starburst_columns(connector_id: str, query: str, limit: int = 100, catalog: Optional[str] = None, schema_name: Optional[str] = None, tableName: Optional[str] = None) -> List[Dict[str, Any]]:
    from fastapi import HTTPException
    try:
        all_columns = []
        catalogs = [catalog] if catalog else get_starburst_catalogs(connector_id)
        for cat in catalogs:
            schemas = [schema_name] if schema_name else get_starburst_schemas(connector_id, cat)
            for sch in schemas:
                tables = [tableName] if tableName else get_starburst_tables(connector_id, cat, sch)
                for table in tables:
                    table_details = get_starburst_table_details(connector_id, cat, sch, table)
                    for col in table_details.get("columns", []) or []:
                        if query.lower() in col.get("name", "").lower():
                            all_columns.append({"catalog": cat, "schema": sch, "table": table, "name": col["name"], "type": col["type"]})
                            if len(all_columns) >= limit:
                                return all_columns
        return all_columns
    except Exception as e:
        print(f"Error searching Starburst columns: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to search Starburst columns: {e}")
