from flask import Blueprint, request, jsonify, abort, current_app
from werkzeug.exceptions import HTTPException
from pydantic import BaseModel
from google.cloud import bigquery
from google.cloud import datacatalog_v1
from google.oauth2 import service_account
import os
import json
import time
import re
from typing import List, Optional, Dict
bigquery_bp = Blueprint('bigquery_bp', __name__)
def create_policy_tag_taxonomy(project_id: str, credentials, taxonomy_name: str = "DataClassification") -> str:
    try:
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        parent = f"projects/{project_id}/locations/us"
        taxonomies = datacatalog_client.list_taxonomies(parent=parent)
        for taxonomy in taxonomies:
            if taxonomy.display_name == taxonomy_name:
                print(f" Taxonomy '{taxonomy_name}' already exists: {taxonomy.name}")
                return taxonomy.name
        taxonomy = datacatalog_v1.Taxonomy()
        taxonomy.display_name = taxonomy_name
        taxonomy.description = "Data classification taxonomy for policy tags"
        created_taxonomy = datacatalog_client.create_taxonomy(
            parent=parent,
            taxonomy=taxonomy
        )
        print(f" Created taxonomy '{taxonomy_name}': {created_taxonomy.name}")
        return created_taxonomy.name
    except Exception as e:
        print(f" Could not create taxonomy: {str(e)}")
        print(f" Error type: {type(e).__name__}")
        print(f" Full error details: {e}")
        print(f" This might be due to missing Data Catalog API permissions or API not enabled")
        raise e  
def create_policy_tag(taxonomy_name: str, tag_name: str, credentials) -> str:
    try:
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        policy_tags = datacatalog_client.list_policy_tags(parent=taxonomy_name)
        for tag in policy_tags:
            if tag.display_name == tag_name:
                print(f" Policy tag '{tag_name}' already exists: {tag.name}")
                return tag.name
        policy_tag = datacatalog_v1.PolicyTag()
        policy_tag.display_name = tag_name
        policy_tag.description = f"Policy tag for {tag_name}"
        created_tag = datacatalog_client.create_policy_tag(
            parent=taxonomy_name,
            policy_tag=policy_tag
        )
        print(f" Created policy tag '{tag_name}': {created_tag.name}")
        return created_tag.name
    except Exception as e:
        print(f" Could not create policy tag '{tag_name}': {str(e)}")
        print(f" Error type: {type(e).__name__}")
        print(f" Full error details: {e}")
        raise e  
def delete_policy_tag(policy_tag_name: str, credentials) -> bool:
    try:
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        datacatalog_client.delete_policy_tag(name=policy_tag_name)
        print(f" Deleted policy tag: {policy_tag_name}")
        return True
    except Exception as e:
        print(f" Could not delete policy tag '{policy_tag_name}': {str(e)}")
        return False
def get_policy_tag_by_name(taxonomy_name: str, tag_name: str, credentials) -> str:
    try:
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        policy_tags = datacatalog_client.list_policy_tags(parent=taxonomy_name)
        for tag in policy_tags:
            if tag.display_name == tag_name:
                return tag.name
        return None
    except Exception as e:
        print(f" Could not find policy tag '{tag_name}': {str(e)}")
        return None
def create_table_taxonomy(project_id: str, credentials, taxonomy_name: str = "TableClassification") -> str:
    try:
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        parent = f"projects/{project_id}/locations/us"
        taxonomies = datacatalog_client.list_taxonomies(parent=parent)
        for taxonomy in taxonomies:
            if taxonomy.display_name == taxonomy_name:
                print(f" Table taxonomy '{taxonomy_name}' already exists: {taxonomy.name}")
                return taxonomy.name
        taxonomy = datacatalog_v1.Taxonomy()
        taxonomy.display_name = taxonomy_name
        taxonomy.description = "Table-level classification taxonomy for policy tags"
        created_taxonomy = datacatalog_client.create_taxonomy(
            parent=parent,
            taxonomy=taxonomy
        )
        print(f" Created table taxonomy '{taxonomy_name}': {created_taxonomy.name}")
        return created_taxonomy.name
    except Exception as e:
        print(f" Could not create table taxonomy: {str(e)}")
        return None
def is_pii_column(column: Dict) -> bool:
    tags = column.get('tags', [])
    tags_str = str(tags).upper()
    
    non_pii_tags = ['ANALYTICAL', 'OPERATIONAL', 'PUBLIC', 'INTERNAL', 'METADATA', 
                    'BUSINESS', 'REFERENCE', 'DIMENSION', 'FACT', 'METRIC']
    
    pii_tags = ['PII', 'SENSITIVE', 'DATA_PRIVACY', 'CRITICAL_PII', 'FINANCIAL', 
                'PAYMENT_INFO', 'CREDENTIALS', 'EMAIL', 'PHONE', 'SSN', 'PERSONAL_INFO',
                'CONFIDENTIAL', 'SECRET', 'RESTRICTED']
    
    if any(non_pii_tag in tags_str for non_pii_tag in non_pii_tags):
        return False
    
    return any(pii_tag in tags_str for pii_tag in pii_tags)
def detect_pii_type(column_name: str) -> tuple:
    col_name = column_name.lower()
    if 'ssn' in col_name or 'social' in col_name or 'social_security' in col_name:
        return (True, "SSN")
    elif 'credit' in col_name and 'card' in col_name:
        return (True, "Credit Card")
    elif 'card' in col_name and ('number' in col_name or 'exp' in col_name):
        return (True, "Credit Card")
    elif 'bank' in col_name or ('account' in col_name and 'number' in col_name):
        return (True, "Bank Account")
    elif 'email' in col_name or 'e_mail' in col_name:
        return (True, "Email")
    elif 'phone' in col_name or 'mobile' in col_name or 'cell' in col_name:
        return (True, "Phone Number")
    elif 'address' in col_name or 'street' in col_name or 'zipcode' in col_name or 'postal' in col_name:
        return (True, "Address")
    elif ('name' in col_name or 'first' in col_name or 'last' in col_name) and ('full' in col_name or 'first' in col_name or 'last' in col_name):
        return (True, "Name")
    elif 'passport' in col_name or 'license' in col_name or 'national_id' in col_name:
        return (True, "Government ID")
    elif ('birth' in col_name or 'dob' in col_name) and 'date' in col_name:
        return (True, "Date of Birth")
    elif 'password' in col_name or 'secret' in col_name or 'token' in col_name:
        return (True, "Credentials")
    elif 'id' in col_name and ('user' in col_name or 'customer' in col_name or 'person' in col_name):
        return (True, "User ID")
    elif 'ip' in col_name and 'address' in col_name:
        return (True, "IP Address")
    elif 'payment' in col_name or ('financi' in col_name and 'account' in col_name):
        return (True, "Financial Info")
    elif 'pii' in col_name or 'personal' in col_name:
        return (True, "Personal Info")
    else:
        return (False, "")
def generate_masked_view_sql_bigquery(project_id: str, dataset_id: str, table_id: str, 
                                     column_tags: List, client) -> str:
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        select_columns = []
        pii_columns_found = []
        business_columns_skipped = []
        
        for field in table.schema:
            col_tag = next((col for col in column_tags if col.name == field.name), None)
            if col_tag and is_pii_column({'tags': col_tag.tags}):
                pii_columns_found.append(f"{field.name} (tags: {col_tag.tags})")
                if field.field_type in ['STRING', 'BYTES']:
                    select_columns.append(f"CAST('***MASKED***' AS STRING) AS {field.name}")
                elif field.field_type == 'INTEGER':
                    select_columns.append(f"CAST(NULL AS INTEGER) AS {field.name}")
                elif field.field_type == 'FLOAT64':
                    select_columns.append(f"CAST(NULL AS FLOAT64) AS {field.name}")
                elif field.field_type == 'FLOAT':
                    select_columns.append(f"CAST(NULL AS FLOAT64) AS {field.name}")
                elif field.field_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                    select_columns.append(f"CAST(NULL AS {field.field_type}) AS {field.name}")
                else:
                    select_columns.append(f"CAST(NULL AS STRING) AS {field.name}")
            else:
                select_columns.append(field.name)
                if col_tag and col_tag.tags:
                    business_columns_skipped.append(f"{field.name} (tags: {col_tag.tags})")
        
        if business_columns_skipped:
            print(f"  Skipped masking for business/operational columns: {', '.join(business_columns_skipped)}")
        
        if not pii_columns_found:
            print(f"  No PII columns found - masked view will NOT be created")
            return ""
        
        print(f"🔒 Creating masked view for PII columns: {', '.join(pii_columns_found)}")
        view_name = f"{table_id}_masked"
        select_str = ',\n  '.join(select_columns)
        sql = f"CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.{view_name}` AS\n"
        sql += "SELECT\n"
        sql += f"  {select_str}\n"
        sql += f"FROM\n"
        sql += f"  `{project_id}.{dataset_id}.{table_id}`"
        return sql
    except Exception as e:
        print(f"Error generating masked view SQL: {str(e)}")
        return ""
class TableDetailsRequest(BaseModel):
    projectId: str
    datasetId: str
    tableId: str
    connectorId: Optional[str] = None
class TagInfo(BaseModel):
    displayName: str
    tagId: str
    resourceName: str
class ColumnInfo(BaseModel):
    name: str
    type: str
    mode: str
    description: Optional[str] = None
    piiFound: bool = False
    piiType: str = ""  
    tags: List[str] = []
    tagDetails: List[TagInfo] = []  
class TableDetailsResponse(BaseModel):
    tableName: str
    columns: List[ColumnInfo]
    tableTags: List[str] = []  
class ColumnTag(BaseModel):
    name: str
    tags: List[str]
    piiFound: bool = False
    piiType: str = ""  
class PublishTagsRequest(BaseModel):
    projectId: str
    datasetId: str
    tableId: str
    columns: List[ColumnTag]
    tableTags: List[str] = []  
    connectorId: Optional[str] = None  
class PublishTagsResponse(BaseModel):
    success: bool
    message: str
    sqlCommands: List[str] = []
    requiresBilling: bool = False
    billingMessage: str = ""
    maskedViewSQL: str = ""  
@bigquery_bp.route("/table-details", methods=["POST"])
def get_table_details():
    request_data = request.get_json()
    request_obj = TableDetailsRequest(**request_data)
    try:
        active_connectors = current_app.config.get('active_connectors', [])
        
        if not active_connectors:
            abort(500, "No active connectors found. Please set up a connector first.")
        
        bigquery_connector = None
        if request_obj.connectorId:
            bigquery_connector = next((c for c in active_connectors if c["id"] == request_obj.connectorId), None)
        else:
            bigquery_connector = next((c for c in active_connectors if c["type"].lower() == "bigquery" and c["enabled"]), None)
        if not bigquery_connector:
            abort(404, "No active BigQuery connector found. Please set up a BigQuery connection first.")
        service_account_json_str = bigquery_connector.get("service_account_json") or bigquery_connector.get("config", {}).get("service_account_json")
        if not service_account_json_str:
            abort(400, "BigQuery connector missing service account credentials.")
        try:
            if isinstance(service_account_json_str, str):
                service_account_info = json.loads(service_account_json_str)
            else:
                service_account_info = service_account_json_str
            service_account_str = str(service_account_info)
            if "your-project-id" in service_account_str or "YOUR_PRIVATE_KEY" in service_account_str or "your-service-account@your-project" in service_account_str:
                abort(400, " BigQuery connector has placeholder credentials. Please reconfigure the connector with valid Google Cloud service account credentials (JSON key file).")
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
            )
            client = bigquery.Client(credentials=credentials, project=request_obj.projectId)
        except json.JSONDecodeError as e:
            abort(400, f"Invalid service account JSON in connector: {str(e)}")
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e) if str(e) else f"{error_type} occurred"
            print(f"BigQuery authentication error (type={error_type}): {error_msg}")
            abort(500, f"Failed to authenticate with BigQuery: {error_type}: {error_msg}. Please check your service account credentials in the connector settings.")
        table_ref = client.dataset(request_obj.datasetId).table(request_obj.tableId)
        table = client.get_table(table_ref)
        table_labels = table.labels or {}
        columns = []
        for field in table.schema:
            pii_found, pii_type = detect_pii_type(field.name)
            column_tags = []
            tag_details = []
            if field.policy_tags and len(field.policy_tags.names) > 0:
                print(f" Column {field.name} has {len(field.policy_tags.names)} policy tags")
                for policy_tag_resource in field.policy_tags.names:
                    print(f" Processing policy tag resource: {policy_tag_resource}")
                    tag_id = policy_tag_resource.split('/')[-1]
                    try:
                        from google.cloud import datacatalog_v1
                        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
                        policy_tag = datacatalog_client.get_policy_tag(name=policy_tag_resource)
                        if policy_tag.display_name:
                            display_name = policy_tag.display_name
                            column_tags.append(display_name)
                            tag_details.append(TagInfo(
                                displayName=display_name,
                                tagId=tag_id,
                                resourceName=policy_tag_resource
                            ))
                            print(f" Found REAL policy tag display name: {display_name}")
                        else:
                            display_name = f"TAG_{tag_id}"
                            column_tags.append(display_name)
                            tag_details.append(TagInfo(
                                displayName=display_name,
                                tagId=tag_id,
                                resourceName=policy_tag_resource
                            ))
                            print(f" Policy tag has no display name, using fallback: {display_name}")
                    except Exception as e:
                        print(f" Could not get policy tag display name for {policy_tag_resource}: {e}")
                        taxonomy_parts = policy_tag_resource.split('/')
                        if len(taxonomy_parts) >= 6:
                            taxonomy_name = taxonomy_parts[4]  
                            display_name = f"TAG_{taxonomy_name}_{tag_id}"
                        else:
                            display_name = f"TAG_{tag_id}"
                        column_tags.append(display_name)
                        tag_details.append(TagInfo(
                            displayName=display_name,
                            tagId=tag_id,
                            resourceName=policy_tag_resource
                        ))
                        print(f" Using fallback name: {display_name}")
            print(f" Column {field.name}: Found {len(column_tags)} REAL policy tags: {column_tags}")
            column_info = ColumnInfo(
                name=field.name,
                type=field.field_type,
                mode=field.mode,
                description=field.description if field.description else "",  
                piiFound=pii_found,
                piiType=pii_type,
                tags=column_tags if column_tags else [],  
                tagDetails=tag_details if tag_details else []
            )
            columns.append(column_info)
        table_tags = []
        print(f" Table {request_obj.tableId}: No cached table tags, showing empty array")
        return jsonify(TableDetailsResponse(
            tableName=request_obj.tableId,
            columns=columns,
            tableTags=table_tags
        ).model_dump())
    except HTTPException as e:
        raise
    except Exception as e:
        error_msg = str(e) if str(e) else "Unknown error occurred"
        import traceback
        traceback.print_exc()
        print(f"BigQuery error: {error_msg}")
        abort(500, f"Failed to fetch table details from BigQuery: {error_msg}. Please ensure you have proper BigQuery credentials and the table exists.")
@bigquery_bp.route("/publish-tags", methods=["POST"])
def publish_tags():
    request_data = request.get_json()
    request_obj = PublishTagsRequest(**request_data)
    try:
        print(f" No local caching - only storing real policy tags in BigQuery")
        sql_commands = []
        table_tags_list = request_obj.tableTags or []
        if table_tags_list:
            label_pairs = []
            for i, tag in enumerate(table_tags_list[:64]):  
                clean_tag = tag.lower().replace(' ', '_').replace(':', '_').replace('-', '_')
                clean_tag = ''.join(c for c in clean_tag if c.isalnum() or c in '_-')
                if clean_tag and len(clean_tag) <= 63:
                    label_pairs.append(f"policy_tag_{i} = '{clean_tag}'")
            if label_pairs:
                sql_commands.insert(0, f"ALTER TABLE `{request_obj.projectId}.{request_obj.datasetId}.{request_obj.tableId}`\nSET OPTIONS (\n  labels = [{', '.join(label_pairs)}]\n);")
        requires_billing = False
        billing_message = ""
        success = False
        masked_view_sql = ""  
        try:
            active_connectors = current_app.config.get('active_connectors', [])
            if request_obj.connectorId:
                bigquery_connector = next((c for c in active_connectors if c["id"] == request_obj.connectorId), None)
            else:
                bigquery_connector = next((c for c in active_connectors if c["type"].lower() == "bigquery" and c["enabled"]), None)
            if bigquery_connector:
                service_account_json_str = bigquery_connector.get("service_account_json") or bigquery_connector.get("config", {}).get("service_account_json")
                if service_account_json_str:
                    try:
                        service_account_info = json.loads(service_account_json_str)
                        credentials = service_account.Credentials.from_service_account_info(
                            service_account_info,
                            scopes=["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
                        )
                        client = bigquery.Client(credentials=credentials, project=request_obj.projectId)
                    except (json.JSONDecodeError, TypeError):
                        client = bigquery.Client(project=request_obj.projectId)
                else:
                    client = bigquery.Client(project=request_obj.projectId)
                table_ref = client.dataset(request_obj.datasetId).table(request_obj.tableId)
                table = client.get_table(table_ref)
                if request_obj.tableTags:
                    try:
                        table_taxonomy_name = create_table_taxonomy(request_obj.projectId, credentials, "TableClassification")
                        if table_taxonomy_name:
                            table_policy_tag_map = {}
                            for tag in request_obj.tableTags:
                                policy_tag_name = create_policy_tag(table_taxonomy_name, tag, credentials)
                                if policy_tag_name:
                                    table_policy_tag_map[tag] = policy_tag_name
                            new_labels = {}
                            for i, tag in enumerate(request_obj.tableTags[:64]):  
                                clean_tag = tag.lower().replace(' ', '_').replace(':', '_').replace('-', '_')
                                clean_tag = ''.join(c for c in clean_tag if c.isalnum() or c in '_-')
                                if clean_tag and len(clean_tag) <= 63:
                                    new_labels[f"table_policy_tag_{i}"] = clean_tag
                            existing_labels = table.labels or {}
                            existing_labels.update(new_labels)
                            table.labels = existing_labels
                            table = client.update_table(table, ["labels"])
                            print(f" Applied table-level policy tags: {request_obj.tableTags}")
                        else:
                            print(" Could not create table taxonomy, using labels only")
                            new_labels = {}
                            for i, tag in enumerate(request_obj.tableTags[:64]):  
                                clean_tag = tag.lower().replace(' ', '_').replace(':', '_').replace('-', '_')
                                clean_tag = ''.join(c for c in clean_tag if c.isalnum() or c in '_-')
                                if clean_tag and len(clean_tag) <= 63:
                                    new_labels[f"table_policy_tag_{i}"] = clean_tag
                            existing_labels = table.labels or {}
                            existing_labels.update(new_labels)
                            table.labels = existing_labels
                            table = client.update_table(table, ["labels"])
                    except Exception as table_policy_error:
                        print(f" Could not apply table-level policy tags: {str(table_policy_error)}")
                        new_labels = {}
                        for i, tag in enumerate(request_obj.tableTags[:64]):  
                            clean_tag = tag.lower().replace(' ', '_').replace(':', '_').replace('-', '_')
                            clean_tag = ''.join(c for c in clean_tag if c.isalnum() or c in '_-')
                            if clean_tag and len(clean_tag) <= 63:
                                new_labels[f"table_policy_tag_{i}"] = clean_tag
                        existing_labels = table.labels or {}
                        existing_labels.update(new_labels)
                        table.labels = existing_labels
                        table = client.update_table(table, ["labels"])
                for column in request_obj.columns:
                    if column.tags:
                        desc_parts = []
                        if column.tags:
                            desc_parts.append(f"Policy Tags: {', '.join(column.tags)}")
                        if column.piiFound:
                            pii_info = f"PII: {column.piiType}" if column.piiType else "PII: Yes"
                            desc_parts.append(pii_info)
                        desc_str = ' | '.join(desc_parts)
                        print(f" Creating REAL policy tags for column {column.name} with tags: {column.tags}")
                        try:
                            taxonomy_name = create_policy_tag_taxonomy(request_obj.projectId, credentials)
                            if not taxonomy_name:
                                raise Exception(f" FAILED: Could not create taxonomy for project {request_obj.projectId}")
                            print(f" Using taxonomy: {taxonomy_name}")
                            policy_tag_map = {}
                            for tag in column.tags:
                                print(f" Creating policy tag: {tag}")
                                policy_tag_name = create_policy_tag(taxonomy_name, tag, credentials)
                                if policy_tag_name:
                                    policy_tag_map[tag] = policy_tag_name
                                    print(f" Created policy tag '{tag}': {policy_tag_name}")
                                else:
                                    raise Exception(f" FAILED: Could not create policy tag '{tag}'")
                            if not policy_tag_map:
                                raise Exception(f" FAILED: No policy tags were created for column {column.name}")
                            
                            all_policy_tag_resources = list(policy_tag_map.values())
                            print(f" Applying {len(all_policy_tag_resources)} policy tags to column {column.name}: {list(policy_tag_map.keys())}")
                            
                            table_ref = client.dataset(request_obj.datasetId).table(request_obj.tableId)
                            table = client.get_table(table_ref)
                            updated_fields = []
                            for field in table.schema:
                                if field.name == column.name:
                                    new_field = bigquery.SchemaField(
                                        name=field.name,
                                        field_type=field.field_type,
                                        mode=field.mode,
                                        description=field.description,
                                        policy_tags=bigquery.PolicyTagList(names=all_policy_tag_resources)
                                    )
                                    updated_fields.append(new_field)
                                else:
                                    updated_fields.append(field)
                            table.schema = updated_fields
                            table = client.update_table(table, ["schema"])
                            print(f" SUCCESS! Applied {len(all_policy_tag_resources)} REAL policy tags to column {column.name}: {list(policy_tag_map.keys())}")
                            billing_message = f" BigQuery Policy Tags Update Successful: Table {request_obj.projectId}.{request_obj.datasetId}.{request_obj.tableId} updated successfully with REAL policy tags!"
                            billing_message += f"\n\n REAL POLICY TAGS APPLIED:"
                            billing_message += f"\n• Column {column.name}: {', '.join(list(policy_tag_map.keys()))}"
                            billing_message += f"\n• Total tags applied: {len(all_policy_tag_resources)}"
                            billing_message += f"\n• Check BigQuery UI 'Policy tags' column to see all the tags!"
                            success = True
                        except Exception as policy_tag_error:
                            print(f" POLICY TAG CREATION FAILED: {policy_tag_error}")
                            raise Exception(f" FAILED to create real policy tags: {policy_tag_error}")
                if not success:
                    raise Exception(f" FAILED: Policy tags were not applied to any columns")
                print(f"Publish policy tags success: Table {request_obj.projectId}.{request_obj.datasetId}.{request_obj.tableId} updated successfully")
                masked_view_sql = generate_masked_view_sql_bigquery(
                    request_obj.projectId, request_obj.datasetId, request_obj.tableId, request_obj.columns, client
                )
                if masked_view_sql:
                    try:
                        query_job = client.query(masked_view_sql)
                        query_job.result()  
                        billing_message += f"\n\n🔒 SECURITY: Created masked view with PII columns automatically masked!"
                        billing_message += f"\n   (Business/operational tags like 'analytical', 'operational' do NOT trigger masking)"
                        print(f" Masked view created for {request_obj.projectId}.{request_obj.datasetId}.{request_obj.tableId}")
                    except Exception as mv_error:
                        print(f" Could not create masked view: {str(mv_error)}")
                        masked_view_sql = f"-- Could not create masked view: {str(mv_error)}\n\n{masked_view_sql}"
                else:
                    print(f"  No masked view created - no PII columns detected (business tags don't trigger masking)")
                    billing_message += f"\n\n  No masked view created - only business/operational tags found (not PII)"
        except Exception as e:
            error_msg = str(e)
            print(f"Publish tags error: {error_msg}")
            if "billing" in error_msg.lower() or "sandbox" in error_msg.lower():
                requires_billing = True
                billing_message = f" BigQuery Update Failed: {error_msg}"
            elif "permission" in error_msg.lower() or "access" in error_msg.lower():
                requires_billing = False
                billing_message = f" Permission Error: {error_msg}"
            elif "invalid characters" in error_msg.lower():
                requires_billing = False
                billing_message = f" Invalid Label Format: {error_msg}"
            else:
                requires_billing = False
                billing_message = f" BigQuery Error: {error_msg}"
        return jsonify(PublishTagsResponse(
            success=success,
            message="BigQuery operation completed. See details below.",
            sqlCommands=sql_commands,
            requiresBilling=requires_billing,
            billingMessage=billing_message,
            maskedViewSQL=masked_view_sql
        ).model_dump())
    except Exception as e:
        print(f"Publish tags error: {str(e)}")
        abort(500, f"Failed to save tags: {str(e)}")
class DeleteTagsRequest(BaseModel):
    projectId: str
    datasetId: str
    tableId: str
    columnName: str
    tagToDelete: str
    connectorId: Optional[str] = None  
class AllTagsResponse(BaseModel):
    tags: List[str]
    totalCount: int
class TaxonomyRequest(BaseModel):
    projectId: str
    taxonomyName: str
    description: str = ""
    connectorId: Optional[str] = None
class PolicyTagRequest(BaseModel):
    projectId: str
    taxonomyName: str
    tagName: str
    description: str = ""
    connectorId: Optional[str] = None
class DeletePolicyTagRequest(BaseModel):
    projectId: str
    taxonomyName: str
    tagName: str
    connectorId: Optional[str] = None
class TaxonomyResponse(BaseModel):
    success: bool
    message: str
    taxonomyName: Optional[str] = None
class PolicyTagResponse(BaseModel):
    success: bool
    message: str
    policyTagName: Optional[str] = None
@bigquery_bp.route("/all-tags", methods=["GET"])
def get_all_tags():
    try:
        import os
        active_connectors = current_app.config.get('active_connectors', [])
        all_tags = set()
        bigquery_connector = next((c for c in active_connectors if c["type"].lower() == "bigquery" and c["enabled"]), None)
        if bigquery_connector:
            try:
                service_account_json_str = bigquery_connector.get("service_account_json") or bigquery_connector.get("config", {}).get("service_account_json")
                if service_account_json_str:
                    try:
                        service_account_info = json.loads(service_account_json_str)
                        credentials = service_account.Credentials.from_service_account_info(
                            service_account_info,
                            scopes=["https://www.googleapis.com/auth/bigquery"]
                        )
                        client = bigquery.Client(credentials=credentials, project=bigquery_connector.get("project_id", "default"))
                    except (json.JSONDecodeError, TypeError):
                        client = bigquery.Client(project=bigquery_connector.get("project_id", "default"))
                else:
                    client = bigquery.Client(project=bigquery_connector.get("project_id", "default"))
                project_id = bigquery_connector.get("project_id", client.project)
                datasets = list(client.list_datasets())
                for dataset in datasets:
                    dataset_id = dataset.dataset_id
                    dataset_ref = client.dataset(dataset_id, project=project_id)
                    try:
                        tables = client.list_tables(dataset_ref)
                        for table_item in tables:
                            table_ref = dataset_ref.table(table_item.table_id)
                            try:
                                table = client.get_table(table_ref)
                                if table.labels:
                                    for label_value in table.labels.values():
                                        if label_value:
                                            all_tags.add(label_value)
                            except Exception as table_error:
                                continue
                    except Exception as dataset_error:
                        continue
                print(f" Fetched {len(all_tags)} real tags from BigQuery")
            except Exception as bigquery_error:
                print(f" Could not fetch tags from BigQuery: {bigquery_error}")
        print(f" No cached tags - only fetching real tags from BigQuery")
        tags_list = sorted(list(all_tags))
        return jsonify(AllTagsResponse(
            tags=tags_list,
            totalCount=len(tags_list)
        ).model_dump())
    except Exception as e:
        print(f"Error getting all tags: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify(AllTagsResponse(tags=[], totalCount=0).model_dump())
@bigquery_bp.route("/delete-tags", methods=["POST"])
def delete_tags():
    request_data = request.get_json()
    request_obj = DeleteTagsRequest(**request_data)
    try:
        print(f" No local caching - only working with real policy tags in BigQuery")
        try:
            active_connectors = current_app.config.get('active_connectors', [])
            if request_obj.connectorId:
                bigquery_connector = next((c for c in active_connectors if c["id"] == request_obj.connectorId), None)
            else:
                bigquery_connector = next((c for c in active_connectors if c["type"].lower() == "bigquery" and c["enabled"]), None)
            service_account_json_str = bigquery_connector.get("service_account_json") or bigquery_connector.get("config", {}).get("service_account_json") if bigquery_connector else None
            if bigquery_connector and service_account_json_str:
                service_account_info = json.loads(service_account_json_str)
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info,
                    scopes=["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
                )
                client = bigquery.Client(credentials=credentials, project=request_obj.projectId)
                try:
                    taxonomy_name = create_policy_tag_taxonomy(request_obj.projectId, credentials, "DataClassification")
                    if taxonomy_name:
                        policy_tag_name = get_policy_tag_by_name(taxonomy_name, request_obj.tagToDelete, credentials)
                        if policy_tag_name:
                            print(f" Removing policy tag '{request_obj.tagToDelete}' from column {request_obj.columnName}")
                            table_ref = client.dataset(request_obj.datasetId).table(request_obj.tableId)
                            table = client.get_table(table_ref)
                            updated_fields = []
                            for field in table.schema:
                                if field.name == request_obj.columnName:
                                    new_field = bigquery.SchemaField(
                                        name=field.name,
                                        field_type=field.field_type,
                                        mode=field.mode,
                                        description=field.description,
                                        policy_tags=None  
                                    )
                                    updated_fields.append(new_field)
                                else:
                                    updated_fields.append(field)
                            table.schema = updated_fields
                            table = client.update_table(table, ["schema"])
                            print(f" Removed policy tag '{request_obj.tagToDelete}' from column {request_obj.columnName}")
                        else:
                            print(f" Policy tag '{request_obj.tagToDelete}' not found in taxonomy")
                    else:
                        print(" Could not access taxonomy for policy tag removal")
                except Exception as policy_error:
                    print(f" Could not remove policy tag from BigQuery: {str(policy_error)}")
                    try:
                        table_ref = client.dataset(request_obj.datasetId).table(request_obj.tableId)
                        table = client.get_table(table_ref)
                        for field in table.schema:
                            if field.name == request_obj.columnName:
                                current_desc = field.description or ""
                                if f"Policy Tags: {request_obj.tagToDelete}" in current_desc:
                                    new_desc = current_desc.replace(f"Policy Tags: {request_obj.tagToDelete}", "").replace("Policy Tags: , ", "Policy Tags: ").replace("Policy Tags: ", "").strip()
                                    if new_desc.startswith("|"):
                                        new_desc = new_desc[1:].strip()
                                    if new_desc.endswith("|"):
                                        new_desc = new_desc[:-1].strip()
                                else:
                                    import re
                                    pattern = rf"Policy Tags: ([^|]*{re.escape(request_obj.tagToDelete)}[^|]*)"
                                    match = re.search(pattern, current_desc)
                                    if match:
                                        tags_part = match.group(1)
                                        tags_list = [tag.strip() for tag in tags_part.split(',')]
                                        tags_list = [tag for tag in tags_list if tag != request_obj.tagToDelete]
                                        if tags_list:
                                            new_desc = current_desc.replace(match.group(0), f"Policy Tags: {', '.join(tags_list)}")
                                        else:
                                            new_desc = current_desc.replace(match.group(0), "")
                                    else:
                                        new_desc = current_desc
                                alter_sql = f"ALTER TABLE `{request_obj.projectId}.{request_obj.datasetId}.{request_obj.tableId}` ALTER COLUMN {request_obj.columnName} SET OPTIONS (description = '{new_desc}')"
                                query_job = client.query(alter_sql)
                                query_job.result()
                                print(f" Fallback: Updated column {request_obj.columnName} description to remove tag")
                                break
                    except Exception as desc_error:
                        print(f" Could not update column description: {str(desc_error)}")
        except Exception as bigquery_error:
            print(f" Could not access BigQuery for tag deletion: {str(bigquery_error)}")
        return jsonify(PublishTagsResponse(
            success=True,
            message=f" Tag '{request_obj.tagToDelete}' successfully deleted from column '{request_obj.columnName}'",
            sqlCommands=[],
            requiresBilling=False,
            billingMessage=""
        ).model_dump())
    except Exception as e:
        print(f"Delete tags error: {str(e)}")
        abort(500, f"Failed to delete tag: {str(e)}")
@bigquery_bp.route("/create-taxonomy", methods=["POST"])
def create_taxonomy_endpoint():
    request_data = request.get_json()
    request_obj = TaxonomyRequest(**request_data)
    try:
        active_connectors = current_app.config.get('active_connectors', [])
        if request_obj.connectorId:
            bigquery_connector = next((c for c in active_connectors if c["id"] == request_obj.connectorId), None)
        else:
            bigquery_connector = next((c for c in active_connectors if c["type"].lower() == "bigquery" and c["enabled"]), None)
        if not bigquery_connector:
            abort(404, "No active BigQuery connector found.")
        service_account_json_str = bigquery_connector.get("service_account_json") or bigquery_connector.get("config", {}).get("service_account_json")
        service_account_info = json.loads(service_account_json_str)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        taxonomy_name = create_policy_tag_taxonomy(request_obj.projectId, credentials, request_obj.taxonomyName)
        if taxonomy_name:
            return jsonify(TaxonomyResponse(
                success=True,
                message=f" Taxonomy '{request_obj.taxonomyName}' created successfully",
                taxonomyName=taxonomy_name
            ).model_dump())
        else:
            return jsonify(TaxonomyResponse(
                success=False,
                message=f" Failed to create taxonomy '{request_obj.taxonomyName}'"
            ).model_dump())
    except Exception as e:
        print(f"Create taxonomy error: {str(e)}")
        abort(500, f"Failed to create taxonomy: {str(e)}")
@bigquery_bp.route("/create-policy-tag", methods=["POST"])
def create_policy_tag_endpoint():
    request_data = request.get_json()
    request_obj = PolicyTagRequest(**request_data)
    try:
        active_connectors = current_app.config.get('active_connectors', [])
        if request_obj.connectorId:
            bigquery_connector = next((c for c in active_connectors if c["id"] == request_obj.connectorId), None)
        else:
            bigquery_connector = next((c for c in active_connectors if c["type"].lower() == "bigquery" and c["enabled"]), None)
        if not bigquery_connector:
            abort(404, "No active BigQuery connector found.")
        service_account_json_str = bigquery_connector.get("service_account_json") or bigquery_connector.get("config", {}).get("service_account_json")
        service_account_info = json.loads(service_account_json_str)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        parent = f"projects/{request_obj.projectId}/locations/us"
        taxonomies = datacatalog_client.list_taxonomies(parent=parent)
        taxonomy_name = None
        for taxonomy in taxonomies:
            if taxonomy.display_name == request_obj.taxonomyName:
                taxonomy_name = taxonomy.name
                break
        if not taxonomy_name:
            abort(404, f"Taxonomy '{request_obj.taxonomyName}' not found")
        policy_tag_name = create_policy_tag(taxonomy_name, request_obj.tagName, credentials)
        if policy_tag_name:
            return jsonify(PolicyTagResponse(
                success=True,
                message=f" Policy tag '{request_obj.tagName}' created successfully in taxonomy '{request_obj.taxonomyName}'",
                policyTagName=policy_tag_name
            ).model_dump())
        else:
            return jsonify(PolicyTagResponse(
                success=False,
                message=f" Failed to create policy tag '{request_obj.tagName}'"
            ).model_dump())
    except Exception as e:
        print(f"Create policy tag error: {str(e)}")
        abort(500, f"Failed to create policy tag: {str(e)}")
@bigquery_bp.route("/delete-policy-tag", methods=["POST"])
def delete_policy_tag_endpoint():
    request_data = request.get_json()
    request_obj = DeletePolicyTagRequest(**request_data)
    try:
        active_connectors = current_app.config.get('active_connectors', [])
        if request_obj.connectorId:
            bigquery_connector = next((c for c in active_connectors if c["id"] == request_obj.connectorId), None)
        else:
            bigquery_connector = next((c for c in active_connectors if c["type"].lower() == "bigquery" and c["enabled"]), None)
        if not bigquery_connector:
            abort(404, "No active BigQuery connector found.")
        service_account_json_str = bigquery_connector.get("service_account_json") or bigquery_connector.get("config", {}).get("service_account_json")
        service_account_info = json.loads(service_account_json_str)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        parent = f"projects/{request_obj.projectId}/locations/us"
        taxonomies = datacatalog_client.list_taxonomies(parent=parent)
        taxonomy_name = None
        for taxonomy in taxonomies:
            if taxonomy.display_name == request_obj.taxonomyName:
                taxonomy_name = taxonomy.name
                break
        if not taxonomy_name:
            abort(404, f"Taxonomy '{request_obj.taxonomyName}' not found")
        policy_tag_name = get_policy_tag_by_name(taxonomy_name, request_obj.tagName, credentials)
        if not policy_tag_name:
            abort(404, f"Policy tag '{request_obj.tagName}' not found in taxonomy '{request_obj.taxonomyName}'")
        success = delete_policy_tag(policy_tag_name, credentials)
        if success:
            return jsonify(PolicyTagResponse(
                success=True,
                message=f" Policy tag '{request_obj.tagName}' deleted successfully from taxonomy '{request_obj.taxonomyName}'"
            ).model_dump())
        else:
            return jsonify(PolicyTagResponse(
                success=False,
                message=f" Failed to delete policy tag '{request_obj.tagName}'"
            ).model_dump())
    except Exception as e:
        print(f"Delete policy tag error: {str(e)}")
        abort(500, f"Failed to delete policy tag: {str(e)}")
