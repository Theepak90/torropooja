from flask import Blueprint, request, jsonify, abort, current_app
from werkzeug.exceptions import HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import re
from google.cloud import bigquery, datacatalog_v1
from google.oauth2 import service_account
from google.api_core import exceptions as google_exceptions
from urllib.parse import urlparse
import json
import requests
import base64
from datetime import datetime
import os
from typing import Tuple
import hmac
import hashlib
try:
    import sqlglot
    from sqlglot import parse_one
    HAS_SQLGLOT = True
except Exception:
    HAS_SQLGLOT = False
lineage_bp = Blueprint('lineage_bp', __name__)
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import db_helpers
from db_helpers import _require_role
def generate_signature(payload: Dict[str, Any]) -> str:
    signing_key = current_app.config.get('TORRO_LINEAGE_SIGNING_KEY')
    if not signing_key:
        raise ValueError("TORRO_LINEAGE_SIGNING_KEY not set in environment or config.")
    encoded_payload = json.dumps(payload, sort_keys=True).encode('utf-8')
    return hmac.new(signing_key.encode('utf-8'), encoded_payload, hashlib.sha256).hexdigest()
@lineage_bp.route("/lineage/ingest", methods=["POST"])
@_require_role('admin')
def ingest_lineage_artifact():
    payload = request.get_json()
    try:
        signature = request.headers.get('X-Torro-Signature')
        if signature:
            signing_key = current_app.config.get('TORRO_LINEAGE_SIGNING_KEY')
            if not signing_key:
                print("WARNING: X-Torro-Signature provided but TORRO_LINEAGE_SIGNING_KEY is not set. Skipping signature validation.")
            else:
                expected_signature = generate_signature(payload)
                if not hmac.compare_digest(expected_signature, signature):
                    abort(403, "Invalid signature.")
        db_helpers.save_integration_data('lineage_artifact', payload)
        return jsonify({"status": "ok", "stored": True})
    except Exception as e:
        print(f"Error ingesting lineage artifact: {str(e)}")
        abort(500, f"Failed to ingest lineage artifact: {str(e)}")
def save_lineage_snapshot(snapshot: Dict[str, Any]):
    signing_key = current_app.config.get('TORRO_LINEAGE_SIGNING_KEY')
    signature = None
    signature_alg = None
    if signing_key:
        signature = hmac.new(
            signing_key.encode('utf-8'), 
            json.dumps(snapshot, sort_keys=True).encode('utf-8'), 
            hashlib.sha256
        ).hexdigest()
        signature_alg = "HMAC-SHA256"
    db_helpers.save_lineage_snapshot(snapshot, signature, signature_alg)
def sign_edge(edge: 'LineageEdge') -> Optional[str]:
    try:
        signing_key = current_app.config.get('TORRO_LINEAGE_SIGNING_KEY')
        if not signing_key:
            return None
        payload = json.dumps({
            'source': edge.source,
            'target': edge.target,
            'relationship': edge.relationship,
            'created_at': edge.created_at,
        }, sort_keys=True).encode('utf-8')
        return hmac.new(signing_key.encode('utf-8'), payload, hashlib.sha256).hexdigest()
    except Exception:
        return None
def _reconcile_openlineage_to_edges() -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []
    integrations = db_helpers.load_integration_data('openlineage')
    for entry in integrations:
        evt = entry.get('data', {})
        inputs = (evt.get('inputs') or [])
        outputs = (evt.get('outputs') or [])
        for inp in inputs:
            s = inp.get('name') or inp.get('namespace')
            if not s:
                continue
            for out in outputs:
                t = out.get('name') or out.get('namespace')
                if not t:
                    continue
                now_iso = datetime.now().isoformat()
                edge = LineageEdge(
                    source=s,
                    target=t,
                    relationship='openlineage_job',
                    column_lineage=[],
                    total_pii_columns=0,
                    avg_data_quality=95.0,
                    last_validated=now_iso,
                    validation_status='valid',
                    confidence_score=0.8,
                    evidence=['openlineage'],
                    sources=['openlineage'],
                    created_at=now_iso,
                    updated_at=now_iso,
                )
                edge.edge_signature = sign_edge(edge)
                edges.append(edge.model_dump())
    return edges
def _reconcile_dbt_to_edges() -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []
    integrations = db_helpers.load_integration_data('dbt')
    for entry in integrations:
        batch = entry.get('data', {})
        for node in batch.get('nodes', []):
            deps = node.get('depends_on') or []
            target = node.get('name')
            if not target:
                continue
            for dep in deps:
                if not dep:
                    continue
                now_iso = datetime.now().isoformat()
                edge = LineageEdge(
                    source=dep,
                    target=target,
                    relationship='dbt_dependency',
                    column_lineage=[],
                    total_pii_columns=0,
                    avg_data_quality=95.0,
                    last_validated=now_iso,
                    validation_status='valid',
                    confidence_score=0.75,
                    evidence=['dbt'],
                    sources=['dbt'],
                    created_at=now_iso,
                    updated_at=now_iso,
                )
                edge.edge_signature = sign_edge(edge)
                edges.append(edge.model_dump())
    return edges
def _reconcile_airflow_to_edges() -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []
    integrations = db_helpers.load_integration_data('airflow')
    for entry in integrations:
        batch = entry.get('data', {})
        for task in batch.get('tasks', []):
            target = task.get('task_id')
            for upstream in (task.get('upstream') or []):
                if not upstream or not target:
                    continue
                now_iso = datetime.now().isoformat()
                edge = LineageEdge(
                    source=upstream,
                    target=target,
                    relationship='airflow_upstream',
                    column_lineage=[],
                    total_pii_columns=0,
                    avg_data_quality=95.0,
                    last_validated=now_iso,
                    validation_status='valid',
                    confidence_score=0.6,
                    evidence=['airflow'],
                    sources=['airflow'],
                    created_at=now_iso,
                    updated_at=now_iso,
                )
                edge.edge_signature = sign_edge(edge)
                edges.append(edge.model_dump())
    return edges
def _reconcile_metadata_to_edges() -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []
    integrations = db_helpers.load_integration_data('metadata')
    for entry in integrations:
        payload = entry.get('data', {}).get('payload') or {}
        rels = payload.get('relationships') or []
        for r in rels:
            s = r.get('source')
            t = r.get('target')
            rel = r.get('type', 'metadata_relationship')
            if not s or not t:
                continue
            now_iso = datetime.now().isoformat()
            edge = LineageEdge(
                source=s,
                target=t,
                relationship=rel,
                column_lineage=[],
                total_pii_columns=0,
                avg_data_quality=95.0,
                last_validated=now_iso,
                validation_status='valid',
                confidence_score=0.7,
                evidence=['metadata'],
                sources=['metadata'],
                created_at=now_iso,
                updated_at=now_iso,
            )
            edge.edge_signature = sign_edge(edge)
            edges.append(edge.model_dump())
    return edges
def _logs_imply_relationship(source_id: str, target_id: str) -> bool:
    logs = db_helpers.load_query_logs(limit=1000)
    s_short = source_id.split('.')[-1].lower()
    t_short = target_id.split('.')[-1].lower()
    for entry in logs:
        sql = (entry.get("sql") or "").lower()
        if s_short in sql and t_short in sql:
            return True
    return False
class ColumnLineage(BaseModel):
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: str  
    contains_pii: Optional[bool] = False  
    data_quality_score: Optional[int] = 95  
    impact_score: Optional[int] = 1  
class LineageNode(BaseModel):
    id: str
    name: str
    type: str
    catalog: str
    connector_id: str
    source_system: str
    columns: List[Dict[str, Any]]  
    dataset: Optional[str] = None  
    project_id: Optional[str] = None  
    schema: Optional[str] = None  
    account_domain: Optional[str] = None  
class LineageEdge(BaseModel):
    source: str
    target: str
    relationship: str
    column_lineage: Optional[List[ColumnLineage]] = []
    total_pii_columns: Optional[int] = 0  
    avg_data_quality: Optional[float] = 95.0  
    last_validated: Optional[str] = None  
    validation_status: Optional[str] = "unknown"  
    confidence_score: Optional[float] = 0.0  
    evidence: Optional[List[str]] = []  
    sources: Optional[List[str]] = []  
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    edge_signature: Optional[str] = None
class LineageResponse(BaseModel):
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    column_relationships: int  
    total_pii_columns: Optional[int] = 0  
    avg_data_quality: Optional[float] = 0.0  
    lineage_completeness: Optional[float] = 0.0  
    avg_confidence: Optional[float] = 0.0  
def compute_edge_confidence(relationship: str, column_lineage: List[ColumnLineage], transformations: Optional[List[Dict[str, Any]]] = None) -> tuple:
    base = 0.4
    evidence: List[str] = []
    if relationship in ["foreign_key", "etl_pipeline", "elt_pipeline", "id_relationship"]:
        base = 0.6
        evidence.append(f"relationship_type:{relationship}")
    if column_lineage:
        mappings = len(column_lineage)
        avg_impact = sum((cl.impact_score or 1) for cl in column_lineage) / max(1, mappings)
        base += min(0.3, mappings * 0.03)
        base += min(0.2, (avg_impact / 10.0) * 0.2)
        evidence.append(f"column_mappings:{mappings}")
    if transformations:
        trans_types = {t.get('type') for t in transformations}
        if any(t in trans_types for t in ["FOREIGN_KEY", "ETL_PIPELINE", "ELT_PIPELINE", "ID_RELATIONSHIP"]):
            base += 0.15
            evidence.append("transformations:strong")
        if any(t in trans_types for t in ["COUNT", "SUM", "JOIN", "DISTINCT"]):
            evidence.append("transformations:sql_ops")
    return (max(0.0, min(1.0, base)), evidence)
def detect_pii_in_column(column_name: str, description: str = '') -> tuple:
    pii_patterns = {
        'HIGH': ['ssn', 'social_security', 'passport', 'national_id', 'license_number', 
                'credit_card', 'account_number', 'password', 'secret', 'private_key'],
        'MEDIUM': ['email', 'phone', 'mobile', 'address', 'zip', 'postal', 'birth_date', 
                  'birthday', 'age', 'gender', 'race', 'ethnicity'],
        'LOW': ['name', 'first_name', 'last_name', 'full_name', 'username', 'user_id']
    }
    combined = f"{column_name} {description}".lower()
    for sensitivity, patterns in pii_patterns.items():
        if any(pattern in combined for pattern in patterns):
            return True, sensitivity
    return False, 'NONE'
def get_enterprise_data_quality_score(column: Dict, table_metadata: Dict = None) -> int:
    score = 50  
    if column.get('nullable') == False:
        score += 10  
    if column.get('unique'):
        score += 5   
    if column.get('primary_key'):
        score += 15  
    description = column.get('description', '')
    if description and description.strip() and description != '-':
        score += 20
        if len(description) > 50:  
            score += 5
    col_type = column.get('type', '').upper()
    col_name = column.get('name', '').lower()
    if 'email' in col_name and 'VARCHAR' in col_type:
        score += 5
    elif 'date' in col_name and any(dt in col_type for dt in ['DATE', 'TIMESTAMP']):
        score += 5
    elif 'id' in col_name and any(dt in col_type for dt in ['INTEGER', 'BIGINT']):
        score += 5
    if table_metadata:
        if table_metadata.get('row_count', 0) > 1000:
            score += 5  
        if table_metadata.get('last_modified'):
            score += 5  
    return min(100, max(0, score))  
def get_column_quality_score(column: Dict) -> int:
    has_desc = bool(column.get('description', '').strip())
    return 95 if has_desc else 80
def extract_column_usage_from_sql(sql: str, table_name: str) -> Dict[str, List[str]]:
    if not sql:
        return {}
    column_usage = {}
    column_patterns = [
        r'\bSELECT\s+([^,]+?)(?:\s+FROM|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
        r'\bWHERE\s+([^,]+?)(?:\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
        r'\bGROUP\s+BY\s+([^,]+?)(?:\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
        r'\bORDER\s+BY\s+([^,]+?)(?:\s+UNION|\s*$)',
        r'\bHAVING\s+([^,]+?)(?:\s+UNION|\s*$)',
    ]
    for pattern in column_patterns:
        matches = re.finditer(pattern, sql, re.IGNORECASE | re.DOTALL)
        for match in matches:
            columns_text = match.group(1)
            columns = [col.strip().split('.')[-1].strip('`"\'') for col in columns_text.split(',')]
            columns = [col for col in columns if col and not col.upper() in ['*', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'DISTINCT']]
            if table_name not in column_usage:
                column_usage[table_name] = []
            column_usage[table_name].extend(columns)
    join_patterns = [
        r'\bJOIN\s+[^\s]+\s+ON\s+([^,]+?)(?:\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
        r'\bLEFT\s+JOIN\s+[^\s]+\s+ON\s+([^,]+?)(?:\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
        r'\bRIGHT\s+JOIN\s+[^\s]+\s+ON\s+([^,]+?)(?:\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
        r'\bINNER\s+JOIN\s+[^\s]+\s+ON\s+([^,]+?)(?:\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
    ]
    for pattern in join_patterns:
        matches = re.finditer(pattern, sql, re.IGNORECASE | re.DOTALL)
        for match in matches:
            join_condition = match.group(1)
            columns = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', join_condition)
            columns = [col for col in columns if col.upper() not in ['ON', 'AND', 'OR', '=', '!=', '<', '>', '<=', '>=']]
            if table_name not in column_usage:
                column_usage[table_name] = []
            column_usage[table_name].extend(columns)
    for table in column_usage:
        column_usage[table] = list(set(column_usage[table]))
    return column_usage
def analyze_cross_table_sql_relationships(source_asset: Dict, target_asset: Dict, discovered_assets: List[Dict]) -> List[ColumnLineage]:
    column_relationships = []
    source_columns = source_asset.get('columns', [])
    target_columns = target_asset.get('columns', [])
    if not source_columns or not target_columns:
        return []
    source_sql = source_asset.get('sql', '') or source_asset.get('definition', '')
    target_sql = target_asset.get('sql', '') or target_asset.get('definition', '')
    source_table_name = source_asset.get('name', '').lower()
    target_table_name = target_asset.get('name', '').lower()
    if target_sql and source_table_name in target_sql.lower():
        target_usage = extract_column_usage_from_sql(target_sql, target_table_name)
        for target_col in target_columns:
            target_col_name = target_col['name'].lower()
            for source_col in source_columns:
                source_col_name = source_col['name'].lower()
                if (f"{source_table_name}.{source_col_name}" in target_sql.lower() or 
                    f"`{source_table_name}`.`{source_col_name}`" in target_sql.lower() or
                    f"{source_table_name}.{source_col_name}" in target_sql.lower()):
                    contains_pii, _ = detect_pii_in_column(source_col['name'], source_col.get('description', ''))
                    source_quality = get_column_quality_score(source_col)
                    target_quality = get_column_quality_score(target_col)
                    avg_quality = (source_quality + target_quality) // 2
                    column_relationships.append(ColumnLineage(
                        source_table=source_asset['id'],
                        source_column=source_col['name'],
                        target_table=target_asset['id'],
                        target_column=target_col['name'],
                        relationship_type="cross_table_sql",
                        contains_pii=bool(contains_pii),
                        data_quality_score=avg_quality,
                        impact_score=9
                    ))
    elif source_sql and target_table_name in source_sql.lower():
        source_usage = extract_column_usage_from_sql(source_sql, source_table_name)
        for source_col in source_columns:
            source_col_name = source_col['name'].lower()
            for target_col in target_columns:
                target_col_name = target_col['name'].lower()
                if (f"{target_table_name}.{target_col_name}" in source_sql.lower() or 
                    f"`{target_table_name}`.`{target_col_name}`" in source_sql.lower() or
                    f"{target_table_name}.{target_col_name}" in source_sql.lower()):
                    contains_pii, _ = detect_pii_in_column(source_col['name'], source_col.get('description', ''))
                    source_quality = get_column_quality_score(source_col)
                    target_quality = get_column_quality_score(target_col)
                    avg_quality = (source_quality + target_quality) // 2
                    column_relationships.append(ColumnLineage(
                        source_table=source_asset['id'],
                        source_column=source_col['name'],
                        target_table=target_asset['id'],
                        target_column=target_col['name'],
                        relationship_type="cross_table_sql",
                        contains_pii=bool(contains_pii),
                        data_quality_score=avg_quality,
                        impact_score=9
                    ))
    return column_relationships
def build_column_lineage_from_usage(source_asset: Dict, target_asset: Dict, discovered_assets: List[Dict]) -> List[ColumnLineage]:
    column_relationships = []
    source_columns = source_asset.get('columns', [])
    target_columns = target_asset.get('columns', [])
    if not source_columns or not target_columns:
        return []
    source_sql = source_asset.get('sql', '') or source_asset.get('definition', '')
    target_sql = target_asset.get('sql', '') or target_asset.get('definition', '')
    source_usage = extract_column_usage_from_sql(source_sql, source_asset.get('name', ''))
    target_usage = extract_column_usage_from_sql(target_sql, target_asset.get('name', ''))
    cross_table_relationships = analyze_cross_table_sql_relationships(source_asset, target_asset, discovered_assets)
    column_relationships.extend(cross_table_relationships)
    source_col_map = {col['name'].lower(): col for col in source_columns}
    target_col_map = {col['name'].lower(): col for col in target_columns}
    for target_col in target_columns:
        target_col_name = target_col['name'].lower()
        for source_col in source_columns:
            source_col_name = source_col['name'].lower()
            is_used = False
            relationship_type = "inferred"
            if target_sql and source_col_name in target_sql.lower():
                is_used = True
                relationship_type = "sql_reference"
            elif source_sql and target_col_name in source_sql.lower():
                is_used = True
                relationship_type = "sql_derived"
            elif any(pattern in target_sql.lower() for pattern in [
                f"select {source_col_name}",
                f"from {source_col_name}",
                f"join {source_col_name}",
                f"where {source_col_name}",
                f"group by {source_col_name}",
                f"order by {source_col_name}"
            ]):
                is_used = True
                relationship_type = "sql_transformation"
            elif any(pattern in target_sql.lower() for pattern in [
                f"count({source_col_name})",
                f"sum({source_col_name})",
                f"avg({source_col_name})",
                f"min({source_col_name})",
                f"max({source_col_name})"
            ]):
                is_used = True
                relationship_type = "aggregation"
            elif any(pattern in target_sql.lower() for pattern in [
                f"upper({source_col_name})",
                f"lower({source_col_name})",
                f"trim({source_col_name})",
                f"substring({source_col_name}",
                f"concat({source_col_name}"
            ]):
                is_used = True
                relationship_type = "string_transform"
            elif any(pattern in target_sql.lower() for pattern in [
                f"date({source_col_name})",
                f"extract({source_col_name}",
                f"format_date({source_col_name}"
            ]):
                is_used = True
                relationship_type = "date_transform"
            if is_used:
                contains_pii, _ = detect_pii_in_column(source_col['name'], source_col.get('description', ''))
                source_quality = get_column_quality_score(source_col)
                target_quality = get_column_quality_score(target_col)
                avg_quality = (source_quality + target_quality) // 2
                impact_score = 10 if relationship_type == "sql_reference" else 8 if relationship_type == "sql_derived" else 6
                column_relationships.append(ColumnLineage(
                    source_table=source_asset['id'],
                    source_column=source_col['name'],
                    target_table=target_asset['id'],
                    target_column=target_col['name'],
                    relationship_type=relationship_type,
                    contains_pii=bool(contains_pii),
                    data_quality_score=avg_quality,
                    impact_score=impact_score
                ))
    return column_relationships
def build_column_lineage_from_metadata(source_asset: Dict, target_asset: Dict) -> List[ColumnLineage]:
    column_relationships = build_column_lineage_from_usage(source_asset, target_asset, [])
    
    if not column_relationships:
        column_relationships = build_column_lineage_from_naming(source_asset, target_asset)
    
    return column_relationships

def build_column_lineage_from_naming(source_asset: Dict, target_asset: Dict) -> List[ColumnLineage]:
    column_relationships = []
    source_columns = source_asset.get('columns', [])
    target_columns = target_asset.get('columns', [])
    
    if not source_columns or not target_columns:
        return []
    
    source_col_map = {col['name'].lower(): col for col in source_columns}
    target_col_map = {col['name'].lower(): col for col in target_columns}
    
    matched_source_cols = set()
    matched_target_cols = set()
    for target_col in target_columns:
        target_col_name = target_col['name']
        target_col_name_lower = target_col_name.lower()
        
        if target_col_name_lower in source_col_map:
            source_col = source_col_map[target_col_name_lower]
            matched_source_cols.add(target_col_name_lower)
            matched_target_cols.add(target_col_name_lower)
            
            contains_pii, pii_sensitivity = detect_pii_in_column(source_col['name'], source_col.get('description', ''))
            source_quality = get_column_quality_score(source_col)
            target_quality = get_column_quality_score(target_col)
            avg_quality = (source_quality + target_quality) // 2
            
            column_relationships.append(ColumnLineage(
                source_table=source_asset['id'],
                source_column=source_col['name'],
                target_table=target_asset['id'],
                target_column=target_col['name'],
                relationship_type="direct_mapping",
                contains_pii=contains_pii,
                data_quality_score=avg_quality,
                impact_score=7
            ))
    
    for target_col in target_columns:
        target_col_name_lower = target_col['name'].lower()
        if target_col_name_lower in matched_target_cols:
            continue
        
        target_base = target_col_name_lower
        for suffix in ['_id', '_key', '_name', '_code', '_type', '_status']:
            if target_base.endswith(suffix):
                target_base = target_base[:-len(suffix)]
                break
        
        for source_col_name_lower, source_col in source_col_map.items():
            if source_col_name_lower in matched_source_cols:
                continue
            
            source_base = source_col_name_lower
            for suffix in ['_id', '_key', '_name', '_code', '_type', '_status']:
                if source_base.endswith(suffix):
                    source_base = source_base[:-len(suffix)]
                    break
            
            if source_base == target_base and source_base:
                matched_source_cols.add(source_col_name_lower)
                matched_target_cols.add(target_col_name_lower)
                
                contains_pii, pii_sensitivity = detect_pii_in_column(source_col['name'], source_col.get('description', ''))
                source_quality = get_column_quality_score(source_col)
                target_quality = get_column_quality_score(target_col)
                avg_quality = (source_quality + target_quality) // 2
                
                column_relationships.append(ColumnLineage(
                    source_table=source_asset['id'],
                    source_column=source_col['name'],
                    target_table=target_asset['id'],
                    target_column=target_col['name'],
                    relationship_type="inferred_mapping",
                    contains_pii=contains_pii,
                    data_quality_score=avg_quality,
                    impact_score=5
                ))
                break
    
    return column_relationships
def extract_table_references_from_sql(sql: str, project_id: str = None) -> Dict[str, Any]:
    if not sql:
        return {'tables': [], 'transformations': [], 'column_usage': {}}
    if HAS_SQLGLOT:
        try:
            node = parse_one(sql)
            tables = {".".join([p for p in map(str, t.parts) if p]) for t in node.find_all(sqlglot.expressions.Table)}
            functions = {f.name.upper() for f in node.find_all(sqlglot.expressions.Func)}
            transformations = []
            for fn in ["COUNT", "SUM", "AVG", "MIN", "MAX", "COALESCE", "CASE", "DISTINCT"]:
                if fn in functions:
                    cat = 'aggregation' if fn in ["COUNT","SUM","AVG","MIN","MAX"] else 'data_quality' if fn=="COALESCE" else 'conditional' if fn=="CASE" else 'distinct'
                    transformations.append({'type': fn, 'category': cat})
            return {
                'tables': list(tables),
                'transformations': transformations,
                'aliases': {},
                'column_usage': extract_column_usage_from_sql(sql, "query"),
                'has_joins': any(j for j in ["JOIN"] if j in sql.upper()),
                'has_unions': "UNION" in sql.upper(),
                'has_subqueries': "SELECT" in sql.upper() and "(" in sql and ")" in sql
            }
        except Exception:
            pass  
    patterns = [
        r'`([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`',
        r'([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'`([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`',
        r'\bFROM\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'\bJOIN\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'\bLEFT\s+JOIN\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'\bRIGHT\s+JOIN\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'\bINNER\s+JOIN\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'\bOUTER\s+JOIN\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'UNION\s+ALL\s+SELECT.*?FROM\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'"([a-zA-Z0-9_-]+)"\."([a-zA-Z0-9_-]+)"\."([a-zA-Z0-9_-]+)"',
        r'([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'"([a-zA-Z0-9_-]+)"\."([a-zA-Z0-9_-]+)"',
        r'\bFROM\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'\bJOIN\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'\bLEFT\s+JOIN\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'\bRIGHT\s+JOIN\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'\bINNER\s+JOIN\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'\bOUTER\s+JOIN\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'UNION\s+ALL\s+SELECT.*?FROM\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'\bFROM\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'\bJOIN\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'\bLEFT\s+JOIN\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'\bRIGHT\s+JOIN\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'\bINNER\s+JOIN\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'\bOUTER\s+JOIN\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
    ]
    tables = set()
    transformations = []
    column_usage = {}
    for pattern in patterns:
        matches = re.finditer(pattern, sql, re.IGNORECASE)
        for match in matches:
            groups = match.groups()
            if len(groups) >= 2:
                if project_id:
                    table_ref = f"{project_id}.{groups[0]}.{groups[1]}" if len(groups) == 2 else f"{groups[0]}.{groups[1]}.{groups[2]}"
                else:
                    table_ref = f"{groups[0]}.{groups[1]}" if len(groups) == 2 else f"{groups[0]}.{groups[1]}.{groups[2]}"
                tables.add(table_ref)
    column_usage = extract_column_usage_from_sql(sql, "query")
    transformation_patterns = [
        (r'\bCOUNT\s*\(', 'aggregation', 'COUNT'),
        (r'\bSUM\s*\(', 'aggregation', 'SUM'),
        (r'\bAVG\s*\(', 'aggregation', 'AVG'),
        (r'\bMIN\s*\(', 'aggregation', 'MIN'),
        (r'\bMAX\s*\(', 'aggregation', 'MAX'),
        (r'\bCOALESCE\s*\(', 'data_quality', 'COALESCE'),
        (r'\bCASE\s+WHEN', 'conditional', 'CASE'),
        (r'\bCROSS\s+JOIN', 'join_type', 'CROSS_JOIN'),
        (r'\bGROUP\s+BY', 'aggregation', 'GROUP_BY'),
        (r'\bDISTINCT', 'distinct', 'DISTINCT'),
        (r'\bDATE\s*\(', 'date_transform', 'DATE'),
        (r'\bTRIM\s*\(', 'string_transform', 'TRIM'),
        (r'\bUPPER\s*\(', 'string_transform', 'UPPER'),
        (r'\bLOWER\s*\(', 'string_transform', 'LOWER'),
    ]
    for pattern, cat, trans_type in transformation_patterns:
        if re.search(pattern, sql, re.IGNORECASE):
            transformations.append({'type': trans_type, 'category': cat})
    alias_patterns = [
        r'FROM\s+`?([a-zA-Z0-9_.-]+)`?\s+AS\s+([a-zA-Z0-9_]+)',
        r'FROM\s+`?([a-zA-Z0-9_.-]+)`?\s+([a-zA-Z0-9_]+)',
    ]
    aliases = {}
    for pattern in alias_patterns:
        for match in re.finditer(pattern, sql, re.IGNORECASE):
            aliases[match.group(2) if len(match.groups()) == 2 else match.group(1)] = match.group(1)
    return {
        'tables': list(tables),
        'transformations': transformations,
        'aliases': aliases,
        'column_usage': column_usage,
        'has_joins': bool(re.search(r'\bJOIN\b', sql, re.IGNORECASE)),
        'has_unions': bool(re.search(r'\bUNION\b', sql, re.IGNORECASE)),
        'has_subqueries': bool(re.search(r'\(.*SELECT.*\)', sql, re.IGNORECASE)),
    }
def get_starburst_view_lineage(asset: Dict[str, Any], connector_config: Dict[str, Any]) -> Dict[str, Any]:
    if asset.get('type') != 'View':
        return {'tables': [], 'transformations': []}
    try:
        asset_id = asset.get('id', '')
        catalog = asset.get('catalog', '')
        schema = asset.get('schema', '')
        table_name = asset.get('name', '')
        sql_definition = asset.get('sql', '') or asset.get('definition', '') or asset.get('view_definition', '')
        if not sql_definition:
            return {'tables': [], 'transformations': []}
        result = extract_table_references_from_sql(sql_definition, catalog)
        return result
    except Exception as e:
        print(f"Error getting Starburst view lineage for {asset.get('id')}: {str(e)}")
        return {'tables': [], 'transformations': []}
def get_starburst_table_lineage(asset: Dict[str, Any], discovered_assets: List[Dict]) -> Dict[str, Any]:
    if asset.get('type') != 'Table':
        return {'tables': [], 'transformations': []}
    try:
        asset_id = asset.get('id', '')
        catalog = asset.get('catalog', '')
        schema = asset.get('schema', '')
        table_name = asset.get('name', '')
        related_tables = []
        transformations = []
        constraints = asset.get('constraints', [])
        foreign_keys = asset.get('foreign_keys', [])
        indexes = asset.get('indexes', [])
        for fk in foreign_keys:
            if 'referenced_table' in fk:
                ref_table = fk['referenced_table']
                for other_asset in discovered_assets:
                    if (other_asset.get('catalog') == catalog and 
                        other_asset.get('schema') == schema and 
                        other_asset.get('name') == ref_table):
                        related_tables.append(f"{catalog}.{schema}.{ref_table}")
                        transformations.append({
                            'type': 'FOREIGN_KEY',
                            'category': 'constraint',
                            'source_table': f"{catalog}.{schema}.{ref_table}",
                            'target_table': asset_id,
                            'columns': fk.get('columns', [])
                        })
                        break
        columns = asset.get('columns', [])
        for col in columns:
            col_name = col.get('name', '').lower()
            col_type = col.get('type', '')
            if any(pattern in col_name for pattern in ['_id', '_key', 'id_', 'key_']):
                for other_asset in discovered_assets:
                    if (other_asset.get('id') != asset_id and 
                        other_asset.get('type') == 'Table' and
                        other_asset.get('catalog') == catalog):
                        other_columns = other_asset.get('columns', [])
                        for other_col in other_columns:
                            other_col_name = other_col.get('name', '').lower()
                            other_col_type = other_col.get('type', '')
                            if (col_type == other_col_type and 
                                any(pattern in other_col_name for pattern in ['_id', '_key', 'id_', 'key_']) and
                                col_name != other_col_name):
                                related_tables.append(other_asset.get('id'))
                                transformations.append({
                                    'type': 'ID_RELATIONSHIP',
                                    'category': 'metadata',
                                    'source_table': other_asset.get('id'),
                                    'target_table': asset_id,
                                    'columns': [col_name, other_col_name]
                                })
        table_name_lower = table_name.lower()
        for other_asset in discovered_assets:
            if (other_asset.get('id') != asset_id and 
                other_asset.get('type') == 'Table' and
                other_asset.get('catalog') == catalog):
                other_name = other_asset.get('name', '').lower()
                if (('raw' in table_name_lower or 'source' in table_name_lower) and 
                    ('processed' in other_name or 'stage' in other_name)):
                    related_tables.append(other_asset.get('id'))
                    transformations.append({
                        'type': 'ETL_PIPELINE',
                        'category': 'pipeline',
                        'source_table': asset_id,
                        'target_table': other_asset.get('id'),
                        'stage': 'extract_load'
                    })
                elif (('stage' in table_name_lower or 'staging' in table_name_lower) and 
                      ('analytics' in other_name or 'final' in other_name or 'prod' in other_name)):
                    related_tables.append(other_asset.get('id'))
                    transformations.append({
                        'type': 'ELT_PIPELINE',
                        'category': 'pipeline',
                        'source_table': asset_id,
                        'target_table': other_asset.get('id'),
                        'stage': 'load_transform'
                    })
        return {
            'tables': related_tables,
            'transformations': transformations,
            'constraints': constraints,
            'foreign_keys': foreign_keys
        }
    except Exception as e:
        print(f"Error getting Starburst table lineage for {asset.get('id')}: {str(e)}")
        return {'tables': [], 'transformations': []}
def get_bigquery_view_lineage(asset: Dict[str, Any], connector_config: Dict[str, Any]) -> Dict[str, Any]:
    if asset.get('type') != 'View':
        return {'tables': [], 'transformations': []}
    try:
        asset_id = asset.get('id', '')
        parts = asset_id.split('.')
        if len(parts) < 3:
            return {'tables': [], 'transformations': []}
        project_id = parts[0]
        dataset_id = parts[1]
        table_id = parts[2]
        if 'service_account_json' in connector_config:
            service_account_info = json.loads(connector_config['service_account_json'])
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
            )
            client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            client = bigquery.Client(project=project_id)
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        if table.view_query:
            result = extract_table_references_from_sql(table.view_query, project_id)
            return result
        return {'tables': [], 'transformations': []}
    except Exception as e:
        print(f"Error getting BigQuery view lineage for {asset.get('id')}: {str(e)}")
        return {'tables': [], 'transformations': []}
@lineage_bp.route("/lineage", methods=["GET"])
def get_data_lineage():
    page = int(request.args.get("page", 0))
    page_size = int(request.args.get("page_size", 1000))
    use_cache = request.args.get("use_cache", "true").lower() == "true"
    as_of = request.args.get("as_of")
    snapshot = request.args.get("snapshot", "false").lower() == "true"
    try:
        from flask import current_app
        discovered_assets = current_app.config.get('discovered_assets', [])
        active_connectors = current_app.config.get('active_connectors', [])
        
        if not discovered_assets or not active_connectors:
            discovered_assets = db_helpers.load_assets()
            active_connectors = db_helpers.load_connectors()
        
        nodes = []
        edges = []
        column_relationship_count = 0
        connector_map = {conn['id']: conn for conn in active_connectors}
        active_connector_ids = set(conn['id'] for conn in active_connectors)
        asset_map = {}
        filtered_count = 0
        for asset in discovered_assets:
            asset_connector_id = asset.get('connector_id', '')
            if asset_connector_id in active_connector_ids:
                pass  
            else:
                filtered_count += 1
                continue
            asset_id = asset.get('id')
            connector_id = asset.get('connector_id', '')
            
            source_system = asset.get('source_system') or asset.get('technical_metadata', {}).get('source_system')
            
            if not source_system:
                if connector_id.startswith('bq_'):
                    source_system = 'BigQuery'
                elif connector_id.startswith('starburst_'):
                    source_system = 'Starburst Galaxy'
                else:
                    source_system = 'Unknown'
            node = LineageNode(
                id=asset_id,
                name=asset.get('name', 'Unknown'),
                type=asset.get('type', 'Table'),
                catalog=asset.get('catalog', 'Unknown'),
                connector_id=connector_id,
                source_system=source_system,
                columns=asset.get('columns', []),
                dataset=asset.get('dataset'),  
                project_id=asset.get('project_id'),  
                schema=asset.get('schema'),  
                account_domain=asset.get('account_domain')  
            )
            nodes.append(node)
            asset_map[asset_id] = asset
        starburst_assets = [a for a in discovered_assets if a.get('connector_id', '').startswith('starburst_')]
        starburst_nodes = [n for n in nodes if n.connector_id.startswith('starburst_')]
        for asset in discovered_assets:
            asset_connector_id = asset.get('connector_id', '')
            if asset_connector_id not in active_connector_ids:
                continue
            if asset.get('type') == 'View':
                asset_id = asset.get('id')
                connector_id = asset.get('connector_id', '')
                connector_config = connector_map.get(connector_id, {})
                lineage_result = {'tables': [], 'transformations': []}
                if connector_id.startswith('bq_'):
                    lineage_result = get_bigquery_view_lineage(asset, connector_config)
                elif connector_id.startswith('starburst_'):
                    lineage_result = get_starburst_view_lineage(asset, connector_config)
                upstream_tables = lineage_result.get('tables', [])
                transformations = lineage_result.get('transformations', [])
                for upstream_table_id in upstream_tables:
                    if upstream_table_id in asset_map:
                        source_asset = asset_map[upstream_table_id]
                        target_asset = asset
                        column_lineage = build_column_lineage_from_metadata(source_asset, target_asset)
                        column_relationship_count += len(column_lineage)
                        relationship = 'feeds_into'
                        if transformations:
                            trans_types = [t['type'] for t in transformations]
                            relationship = f"{relationship} (transforms: {', '.join(trans_types[:3])})"
                        pii_count = sum(1 for col in column_lineage if col.contains_pii)
                        avg_quality = sum(col.data_quality_score for col in column_lineage) / len(column_lineage) if column_lineage else 95.0
                        trans_plus = list(transformations) if transformations else []
                        if _logs_imply_relationship(upstream_table_id, asset_id):
                            trans_plus.append({'type': 'QUERY_LOG'})
                        confidence, evidence = compute_edge_confidence(relationship='feeds_into', column_lineage=column_lineage, transformations=trans_plus)
                        now_iso = datetime.now().isoformat()
                        edge = LineageEdge(
                            source=upstream_table_id,
                            target=asset_id,
                            relationship=relationship,
                            column_lineage=column_lineage,
                            total_pii_columns=pii_count,
                            avg_data_quality=round(avg_quality, 2),
                            last_validated=now_iso,
                            validation_status="valid",
                            confidence_score=round(confidence, 3),
                            evidence=evidence,
                            sources=["view_sql"],
                            created_at=now_iso,
                            updated_at=now_iso
                        )
                        edge.edge_signature = sign_edge(edge)
                        edges.append(edge)
        for asset in discovered_assets:
            asset_connector_id = asset.get('connector_id', '')
            if asset_connector_id not in active_connector_ids:
                continue
            if asset.get('type') == 'Table' and asset_connector_id.startswith('starburst_'):
                asset_id = asset.get('id')
                connector_id = asset.get('connector_id', '')
                table_lineage = get_starburst_table_lineage(asset, discovered_assets)
                upstream_tables = table_lineage.get('tables', [])
                transformations = table_lineage.get('transformations', [])
                for upstream_table_id in upstream_tables:
                    if upstream_table_id in asset_map:
                        source_asset = asset_map[upstream_table_id]
                        target_asset = asset
                        column_lineage = []
                        for transformation in transformations:
                            if transformation.get('target_table') == asset_id and transformation.get('source_table') == upstream_table_id:
                                columns = transformation.get('columns', [])
                                if len(columns) >= 2:
                                    for i in range(min(len(columns), 2)):
                                        source_col = columns[0] if i == 0 else f"{columns[0]}_ref"
                                        target_col = columns[1] if i == 0 else f"{columns[1]}_ref"
                                        contains_pii, pii_sensitivity = detect_pii_in_column(source_col, '')
                                        source_quality = get_enterprise_data_quality_score({'name': source_col, 'type': 'VARCHAR'}, source_asset)
                                        target_quality = get_enterprise_data_quality_score({'name': target_col, 'type': 'VARCHAR'}, target_asset)
                                        avg_quality = (source_quality + target_quality) // 2
                                        column_lineage.append(ColumnLineage(
                                            source_table=upstream_table_id,
                                            source_column=source_col,
                                            target_table=asset_id,
                                            target_column=target_col,
                                            relationship_type=transformation.get('type', 'foreign_key').lower(),
                                            contains_pii=contains_pii,
                                            data_quality_score=avg_quality,
                                            impact_score=10  
                                        ))
                        if not column_lineage:
                            source_cols = {c.get('name',''): c for c in (source_asset.get('columns', []) or [])}
                            target_cols = {c.get('name',''): c for c in (target_asset.get('columns', []) or [])}
                            inferred_pairs = []
                            for name in source_cols.keys():
                                lname = name.lower()
                                if lname == 'id' or lname.endswith('_id') or 'id_' in lname:
                                    if name in target_cols:
                                        inferred_pairs.append((name, name))
                            for s_name, t_name in inferred_pairs[:3]:  
                                contains_pii, _ = detect_pii_in_column(s_name, source_cols[s_name].get('description',''))
                                source_quality = get_enterprise_data_quality_score({'name': s_name, 'type': source_cols[s_name].get('type','')}, source_asset)
                                target_quality = get_enterprise_data_quality_score({'name': t_name, 'type': target_cols[t_name].get('type','')}, target_asset)
                                avg_quality = (source_quality + target_quality) // 2
                                column_lineage.append(ColumnLineage(
                                    source_table=upstream_table_id,
                                    source_column=s_name,
                                    target_table=asset_id,
                                    target_column=t_name,
                                    relationship_type='id_inference',
                                    contains_pii=contains_pii,
                                    data_quality_score=avg_quality,
                                    impact_score=4
                                ))
                        if column_lineage:
                            column_relationship_count += len(column_lineage)
                            pii_count = sum(1 for col in column_lineage if col.contains_pii)
                            avg_quality = sum(col.data_quality_score for col in column_lineage) / len(column_lineage)
                            relationship_type = 'foreign_key'
                            if any(t.get('type') == 'ETL_PIPELINE' for t in transformations):
                                relationship_type = 'etl_pipeline'
                            elif any(t.get('type') == 'ELT_PIPELINE' for t in transformations):
                                relationship_type = 'elt_pipeline'
                            elif any(t.get('type') == 'ID_RELATIONSHIP' for t in transformations):
                                relationship_type = 'id_relationship'
                            trans_plus = list(transformations) if transformations else []
                            if _logs_imply_relationship(upstream_table_id, asset_id):
                                trans_plus.append({'type': 'QUERY_LOG'})
                            confidence, evidence = compute_edge_confidence(relationship=relationship_type, column_lineage=column_lineage, transformations=trans_plus)
                            now_iso = datetime.now().isoformat()
                            edge = LineageEdge(
                                source=upstream_table_id,
                                target=asset_id,
                                relationship=relationship_type,
                                column_lineage=column_lineage,
                                total_pii_columns=pii_count,
                                avg_data_quality=round(avg_quality, 2),
                                last_validated=now_iso,
                                validation_status="valid",
                                confidence_score=round(confidence, 3),
                                evidence=evidence,
                                sources=["starburst_metadata"],
                                created_at=now_iso,
                                updated_at=now_iso
                            )
                            edge.edge_signature = sign_edge(edge)
                            edges.append(edge)
        if len(edges) < 50:  
            tables_by_catalog = {}
            for asset in discovered_assets:
                asset_connector_id = asset.get('connector_id', '')
                if asset_connector_id not in active_connector_ids:
                    continue
                asset_type = (asset.get('type') or '').upper()
                if asset_type in ['TABLE', 'VIEW']:
                    catalog = asset.get('catalog', asset_connector_id)
                    if catalog not in tables_by_catalog:
                        tables_by_catalog[catalog] = []
                    tables_by_catalog[catalog].append(asset)
            for catalog, assets_in_catalog in tables_by_catalog.items():
                for i, asset1 in enumerate(assets_in_catalog):
                    for asset2 in assets_in_catalog[i+1:]:
                        column_lineage = build_column_lineage_from_usage(asset1, asset2, discovered_assets)
                        if len(column_lineage) >= 2:
                            asset1_name = asset1.get('name', '').lower()
                            asset2_name = asset2.get('name', '').lower()
                            asset1_catalog = asset1.get('catalog', '').lower()
                            asset2_catalog = asset2.get('catalog', '').lower()
                            is_etl = False
                            is_elt = False
                            pipeline_stage = None
                            if 'raw' in asset1_name or 'landing' in asset1_name or 'source' in asset1_name:
                                if 'processed' in asset2_name or 'stage' in asset2_name or 'staged' in asset2_name:
                                    is_etl = True
                                    pipeline_stage = "extract_load"
                                elif 'analytics' in asset2_name or 'report' in asset2_name or 'summary' in asset2_name:
                                    is_elt = True
                                    pipeline_stage = "extract_load_transform"
                            if 'stage' in asset1_name or 'staging' in asset1_name:
                                if 'analytics' in asset2_name or 'final' in asset2_name or 'production' in asset2_name:
                                    is_elt = True
                                    pipeline_stage = "load_transform"
                            source_id = asset1['id']
                            target_id = asset2['id']
                            if ('raw' in asset2_name or 'source' in asset2_name) and ('prod' in asset1_name or 'analytics' in asset1_name):
                                source_id, target_id = target_id, source_id
                                is_etl = True
                                pipeline_stage = "extract_load_transform"
                            elif asset1.get('type') == 'View' and asset2.get('type') == 'Table':
                                source_id = asset2['id']
                                target_id = asset1['id']
                            edge_exists = any(e.source == source_id and e.target == target_id for e in edges)
                            if not edge_exists:
                                column_relationship_count += len(column_lineage)
                                pii_count = sum(1 for col in column_lineage if col.contains_pii)
                                avg_quality = sum(col.data_quality_score for col in column_lineage) / len(column_lineage) if column_lineage else 95.0
                                relationship_type = 'inferred_from_metadata'
                                if is_etl:
                                    relationship_type = 'etl_pipeline'
                                elif is_elt:
                                    relationship_type = 'elt_pipeline'
                                trans_plus = []
                                if _logs_imply_relationship(source_id, target_id):
                                    trans_plus.append({'type': 'QUERY_LOG'})
                                confidence, evidence = compute_edge_confidence(relationship=relationship_type, column_lineage=column_lineage, transformations=trans_plus)
                                now_iso = datetime.now().isoformat()
                                edge = LineageEdge(
                                    source=source_id,
                                    target=target_id,
                                    relationship=relationship_type,
                                    column_lineage=column_lineage,
                                    total_pii_columns=pii_count,
                                    avg_data_quality=round(avg_quality, 2),
                                    last_validated=now_iso,
                                    validation_status="inferred",
                                    confidence_score=round(confidence, 3),
                                    evidence=evidence,
                                    sources=["metadata_inference"],
                                    created_at=now_iso,
                                    updated_at=now_iso
                                )
                                edge.edge_signature = sign_edge(edge)
                                edges.append(edge)
                                stage_icon = "" if is_etl else "⚡" if is_elt else ""
        if as_of:
            try:
                as_of_dt = datetime.fromisoformat(as_of.replace('Z', '+00:00'))
                pre_filter_edge_count = len(edges)
                edges = [e for e in edges if e.created_at and datetime.fromisoformat(e.created_at.replace('Z', '+00:00')) <= as_of_dt]
                node_ids_kept = {e.source for e in edges} | {e.target for e in edges}
                nodes = [n for n in nodes if n.id in node_ids_kept]
            except Exception as _:
                print("WARN: Invalid as_of; skipping temporal filter")
        try:
            saved_relations = db_helpers.get_lineage_relations()
            print(f"🔍 Loaded {len(saved_relations)} saved lineage relations from database")
            if saved_relations:
                print(f"🔍 Sample relation: {saved_relations[0]}")
        except Exception as e:
            print(f"WARN: Failed to load saved lineage relations: {e}")
            import traceback
            traceback.print_exc()
            saved_relations = []
        existing_edge_keys = {(e.source, e.target) for e in edges}
        print(f"🔍 Existing edges before manual relations: {len(existing_edge_keys)}")
        
        # First, ensure we have assets for manual lineage relations even if not in asset_map
        # Load missing assets from database - check ALL relations, not just missing ones
        missing_asset_ids = set()
        all_relation_asset_ids = set()
        for relation in saved_relations:
            source_id = relation.get('source_id')
            target_id = relation.get('target_id')
            if source_id:
                all_relation_asset_ids.add(source_id)
                if source_id not in asset_map:
                    missing_asset_ids.add(source_id)
            if target_id:
                all_relation_asset_ids.add(target_id)
                if target_id not in asset_map:
                    missing_asset_ids.add(target_id)
        
        print(f"🔍 Total unique assets in manual relations: {len(all_relation_asset_ids)}")
        print(f"🔍 Missing assets to load: {len(missing_asset_ids)}")
        
        # Load missing assets from database (directly, bypassing connector filter)
        if missing_asset_ids:
            try:
                from database import Asset
                from sqlalchemy.orm import sessionmaker
                from sqlalchemy import create_engine
                from config import Config
                
                engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
                Session = sessionmaker(bind=engine)
                session = Session()
                
                try:
                    db_assets = session.query(Asset).filter(Asset.id.in_(missing_asset_ids)).all()
                    for db_asset in db_assets:
                        asset_id = db_asset.id
                        if asset_id not in asset_map:
                            # Convert database asset to dict format
                            asset_dict = {
                                'id': db_asset.id,
                                'name': db_asset.name,
                                'type': db_asset.type,
                                'catalog': db_asset.catalog,
                                'connector_id': db_asset.connector_id or '',
                                'schema': db_asset.schema_name,
                                'columns': db_asset.extra_data.get('columns', []) if db_asset.extra_data and isinstance(db_asset.extra_data, dict) else [],
                            }
                            asset_map[asset_id] = asset_dict
                            
                            # Create a node for this asset if it doesn't exist
                            node_exists = any(n.id == asset_id for n in nodes)
                            if not node_exists:
                                connector_id = asset_dict.get('connector_id', '')
                                source_system = 'Manual' if not connector_id else 'Unknown'
                                if connector_id.startswith('bq_'):
                                    source_system = 'BigQuery'
                                elif connector_id.startswith('starburst_'):
                                    source_system = 'Starburst Galaxy'
                                elif connector_id.startswith('s3_'):
                                    source_system = 'Amazon S3'
                                
                                node = LineageNode(
                                    id=asset_id,
                                    name=asset_dict.get('name', asset_id),
                                    type=asset_dict.get('type', 'Table'),
                                    catalog=asset_dict.get('catalog', ''),
                                    connector_id=connector_id or 'manual',
                                    source_system=source_system,
                                    columns=asset_dict.get('columns', []),
                                    dataset=asset_dict.get('dataset'),
                                    project_id=asset_dict.get('project_id'),
                                    schema=asset_dict.get('schema') or asset_dict.get('schema_name', ''),
                                    account_domain=asset_dict.get('account_domain')
                                )
                                nodes.append(node)
                                print(f"✅ Added missing asset node for manual lineage: {asset_id}")
                finally:
                    session.close()
            except Exception as e:
                print(f"WARN: Failed to load missing assets for manual relations: {e}")
                import traceback
                traceback.print_exc()
        
        # Initialize db_session if not already created
        db_session = None
        if missing_asset_ids and saved_relations:
            try:
                from database import Asset
                from sqlalchemy.orm import sessionmaker
                from sqlalchemy import create_engine
                from config import Config
                engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
                Session = sessionmaker(bind=engine)
                db_session = Session()
            except Exception as e:
                print(f"WARN: Could not create database session: {e}")
                db_session = None
        
        print(f"🔍 Processing {len(saved_relations)} saved relations. Asset map size: {len(asset_map)}")
        for relation in saved_relations:
            source_id = relation.get('source_id')
            target_id = relation.get('target_id')
            relation_type = relation.get('relation_type', 'derives_from')
            metadata = relation.get('metadata', {}) or relation.get('extra_data', {})
            
            print(f"🔍 Processing relation: {source_id} -> {target_id}")
            if (source_id, target_id) in existing_edge_keys:
                print(f"  ⏭️  Edge already exists, skipping")
                continue
            
            # Ensure both assets are in asset_map - load from DB if missing
            source_asset = None
            target_asset = None
            
            if source_id not in asset_map:
                # Try to load from database using db_helpers which handles session properly
                try:
                    all_assets = db_helpers.load_assets()
                    source_asset_data = next((a for a in all_assets if a.get('id') == source_id), None)
                    if source_asset_data:
                        asset_map[source_id] = source_asset_data
                        source_asset = source_asset_data
                        # Create node if doesn't exist
                        if not any(n.id == source_id for n in nodes):
                            connector_id = source_asset_data.get('connector_id', '')
                            source_system = 'Amazon S3' if connector_id and connector_id.startswith('s3_') else 'Unknown'
                            node = LineageNode(
                                id=source_id,
                                name=source_asset_data.get('name', source_id),
                                type=source_asset_data.get('type', 'Table'),
                                catalog=source_asset_data.get('catalog', ''),
                                connector_id=connector_id or 'manual',
                                source_system=source_system,
                                columns=source_asset_data.get('columns', []),
                                schema=source_asset_data.get('schema') or source_asset_data.get('schema_name', ''),
                            )
                            nodes.append(node)
                    else:
                        print(f"⚠️ Source asset not found in database: {source_id}")
                except Exception as e:
                    print(f"Error loading source asset {source_id}: {e}")
            else:
                source_asset = asset_map[source_id]
            
            if target_id not in asset_map:
                # Try to load from database using db_helpers
                try:
                    all_assets = db_helpers.load_assets()
                    target_asset_data = next((a for a in all_assets if a.get('id') == target_id), None)
                    if target_asset_data:
                        asset_map[target_id] = target_asset_data
                        target_asset = target_asset_data
                        # Create node if doesn't exist
                        if not any(n.id == target_id for n in nodes):
                            connector_id = target_asset_data.get('connector_id', '')
                            source_system = 'Amazon S3' if connector_id and connector_id.startswith('s3_') else 'Unknown'
                            node = LineageNode(
                                id=target_id,
                                name=target_asset_data.get('name', target_id),
                                type=target_asset_data.get('type', 'Table'),
                                catalog=target_asset_data.get('catalog', ''),
                                connector_id=connector_id or 'manual',
                                source_system=source_system,
                                columns=target_asset_data.get('columns', []),
                                schema=target_asset_data.get('schema') or target_asset_data.get('schema_name', ''),
                            )
                            nodes.append(node)
                    else:
                        print(f"⚠️ Target asset not found in database: {target_id}")
                except Exception as e:
                    print(f"Error loading target asset {target_id}: {e}")
            else:
                target_asset = asset_map[target_id]
            
            # Now create edge if both assets are available
            if source_asset and target_asset:
                print(f"✅ Creating edge for manual lineage: {source_id} -> {target_id}")
                
                column_lineage = []
                if metadata.get('column_lineage'):
                    for cl in metadata.get('column_lineage', []):
                        column_lineage.append(ColumnLineage(
                            source_table=source_id,
                            source_column=cl.get('source_column', ''),
                            target_table=target_id,
                            target_column=cl.get('target_column', ''),
                            relationship_type=cl.get('relationship_type', 'direct'),
                            contains_pii=cl.get('contains_pii', False),
                            data_quality_score=cl.get('data_quality_score', 95),
                            impact_score=cl.get('impact_score', 7)
                        ))
                
                if not column_lineage:
                    column_lineage = build_column_lineage_from_metadata(source_asset, target_asset)
                
                if not column_lineage:
                    source_cols = {c.get('name', '').lower(): c for c in (source_asset.get('columns', []) or [])}
                    target_cols = {c.get('name', '').lower(): c for c in (target_asset.get('columns', []) or [])}
                    for col_name in source_cols.keys():
                        if col_name in target_cols:
                            contains_pii_bool, _ = detect_pii_in_column(col_name, source_cols[col_name].get('description', ''))
                            column_lineage.append(ColumnLineage(
                                source_table=source_id,
                                source_column=source_cols[col_name].get('name', ''),
                                target_table=target_id,
                                target_column=target_cols[col_name].get('name', ''),
                                relationship_type="direct_mapping",
                                contains_pii=bool(contains_pii_bool),
                                data_quality_score=95,
                                impact_score=7
                            ))
                
                if column_lineage:
                    column_relationship_count += len(column_lineage)
                    pii_count = sum(1 for col in column_lineage if col.contains_pii)
                    avg_quality = sum(col.data_quality_score for col in column_lineage) / len(column_lineage) if column_lineage else 95.0
                else:
                    pii_count = 0
                    avg_quality = 95.0
                
                confidence, evidence = compute_edge_confidence(relationship=relation_type, column_lineage=column_lineage, transformations=[])
                now_iso = datetime.now().isoformat()
                edge = LineageEdge(
                    source=source_id,
                    target=target_id,
                    relationship=relation_type,
                    column_lineage=column_lineage,
                    total_pii_columns=pii_count,
                    avg_data_quality=round(avg_quality, 2),
                    last_validated=now_iso,
                    validation_status="valid",
                    confidence_score=round(confidence, 3),
                    evidence=evidence + ["saved_relation"],
                    sources=["database"],
                    created_at=now_iso,
                    updated_at=now_iso
                )
                edge.edge_signature = sign_edge(edge)
                edges.append(edge)
                existing_edge_keys.add((source_id, target_id))
                print(f"✅ Successfully added manual lineage edge: {source_id} -> {target_id}")
        
        total_pii = sum(e.total_pii_columns for e in edges)
        avg_quality_all = sum(e.avg_data_quality for e in edges) / len(edges) if edges else 0.0
        completeness = (len(nodes) / len(asset_map)) * 100 if asset_map else 0.0
        avg_confidence_all = sum((e.confidence_score or 0.0) for e in edges) / len(edges) if edges else 0.0
        try:
            for edge in edges:
                relation_data = {
                    'source_id': edge.source,
                    'target_id': edge.target,
                    'relation_type': edge.relationship,
                    'extra_data': edge.model_dump()
                }
                db_helpers.save_lineage_relation(relation_data)
        except Exception as e:
            print(f"WARN: Failed to save lineage relations to MySQL: {e}")
        page_size_int = int(page_size) if page_size else 1000
        if page_size_int > 0 and page_size_int < len(nodes):
            start_idx = int(page) * page_size_int
            end_idx = start_idx + page_size_int
            paginated_nodes = nodes[start_idx:end_idx]
            paginated_node_ids = {n.id for n in paginated_nodes}
            paginated_edges = [e for e in edges if e.source in paginated_node_ids and e.target in paginated_node_ids]
            avg_confidence_page = sum((e.confidence_score or 0.0) for e in paginated_edges) / len(paginated_edges) if paginated_edges else 0.0
            response = LineageResponse(
                nodes=paginated_nodes, 
                edges=paginated_edges,
                column_relationships=column_relationship_count,
                total_pii_columns=total_pii,
                avg_data_quality=round(avg_quality_all, 2),
                lineage_completeness=round(completeness, 2),
                avg_confidence=round(avg_confidence_page, 3)
            )
            if snapshot:
                save_lineage_snapshot(json.loads(response.model_dump_json()))
            return jsonify(response.model_dump())
        response = LineageResponse(
            nodes=nodes, 
            edges=edges,
            column_relationships=column_relationship_count,
            total_pii_columns=total_pii,
            avg_data_quality=round(avg_quality_all, 2),
            lineage_completeness=round(completeness, 2),
            avg_confidence=round(avg_confidence_all, 3)
        )
        print(f" FINAL RESPONSE: {len(nodes)} nodes, {len(edges)} edges")
        print(f" Node sample: {nodes[0].name if nodes else 'NONE'}")
        if snapshot:
            save_lineage_snapshot(json.loads(response.model_dump_json()))
        return jsonify(response.model_dump())
    except Exception as e:
        print(f"Error getting data lineage: {str(e)}")
        import traceback
        traceback.print_exc()
        abort(500, f"Failed to get data lineage: {str(e)}")
@lineage_bp.route("/lineage/impact/<path:asset_id>", methods=["GET"])
def get_impact_analysis(asset_id: str):
    try:
        from main import active_connectors, load_assets
        discovered_assets = load_assets()
        lineage_response = get_data_lineage()
        upstream_edges = [e for e in lineage_response.edges if e.target == asset_id]
        downstream_edges = [e for e in lineage_response.edges if e.source == asset_id]
        upstream_count = len(set(e.source for e in upstream_edges))
        downstream_count = len(set(e.target for e in downstream_edges))
        total_column_impacts = sum(len(e.column_lineage or []) for e in downstream_edges)
        return {
            "asset_id": asset_id,
            "impact_score": upstream_count * 10 + downstream_count * 20 + total_column_impacts * 5,
            "upstream_impact": {
                "dependencies": upstream_count,
                "tables": [e.source for e in upstream_edges]
            },
            "downstream_impact": {
                "dependent_tables": downstream_count,
                "tables": [e.target for e in downstream_edges],
                "column_relationships": total_column_impacts
            },
            "severity": "HIGH" if downstream_count > 5 or total_column_impacts > 20 else "MEDIUM" if downstream_count > 0 else "LOW"
        }
    except Exception as e:
        print(f"Error getting impact analysis: {str(e)}")
        abort(500, f"Failed to get impact analysis: {str(e)}")
@lineage_bp.route("/lineage/export", methods=["GET"])
def export_lineage():
    export_format = request.args.get("format", "json")
    asset_id = request.args.get("asset_id")
    try:
        from main import active_connectors, load_assets
        discovered_assets = load_assets()
        lineage_result = get_data_lineage()
        if asset_id:
            related_node_ids = {asset_id}
            for edge in lineage_result.edges:
                if asset_id in [edge.source, edge.target]:
                    related_node_ids.add(edge.source)
                    related_node_ids.add(edge.target)
            filtered_nodes = [n for n in lineage_result.nodes if n.id in related_node_ids]
            filtered_edges = [e for e in lineage_result.edges if asset_id in [e.source, e.target]]
            lineage_result = LineageResponse(
                nodes=filtered_nodes,
                edges=filtered_edges,
                column_relationships=sum(len(e.column_lineage or []) for e in filtered_edges)
            )
        if export_format == "csv":
            csv_lines = ["Source Table,Source Column,Target Table,Target Column,Relationship Type"]
            for edge in lineage_result.edges:
                source = next((n for n in lineage_result.nodes if n.id == edge.source), None)
                target = next((n for n in lineage_result.nodes if n.id == edge.target), None)
                source_name = source.name if source else edge.source
                target_name = target.name if target else edge.target
                if edge.column_lineage:
                    for col_lineage in edge.column_lineage:
                        csv_lines.append(
                            f"{source_name},{col_lineage.source_column},{target_name},{col_lineage.target_column},{col_lineage.relationship_type}"
                        )
                else:
                    csv_lines.append(f"{source_name},-,{target_name},-,{edge.relationship}")
            return {"format": "csv", "data": "\n".join(csv_lines)}
        else:  
            return {
                "format": "json",
                "export_date": datetime.now().isoformat(),
                "total_nodes": len(lineage_result.nodes),
                "total_edges": len(lineage_result.edges),
                "total_column_relationships": lineage_result.column_relationships,
                "nodes": [{"id": n.id, "name": n.name, "type": n.type, "catalog": n.catalog} for n in lineage_result.nodes],
                "edges": [{"source": e.source, "target": e.target, "relationship": e.relationship, "column_lineage": e.column_lineage} for e in lineage_result.edges]
            }
    except Exception as e:
        print(f"Error exporting lineage: {str(e)}")
        abort(500, f"Failed to export lineage: {str(e)}")
@lineage_bp.route("/lineage/search", methods=["GET"])
def search_lineage():
    query = request.args.get("query")
    search_type = request.args.get("search_type", "all")
    if not query:
        abort(400, "Search query is required")
    try:
        lineage_result = get_data_lineage()
        query_lower = query.lower()
        matching_nodes = []
        matching_edges = []
        for node in lineage_result.nodes:
            if search_type in ['all', 'table']:
                if query_lower in node.name.lower() or query_lower in node.id.lower():
                    matching_nodes.append(node)
            if search_type in ['all', 'column']:
                for col in node.columns:
                    if query_lower in col.get('name', '').lower():
                        matching_nodes.append(node)
                        break
        matching_node_ids = {n.id for n in matching_nodes}
        for edge in lineage_result.edges:
            if edge.source in matching_node_ids or edge.target in matching_node_ids:
                if edge not in matching_edges:
                    matching_edges.append(edge)
        for edge in lineage_result.edges:
            for col_lineage in edge.column_lineage:
                if query_lower in col_lineage.source_column.lower() or query_lower in col_lineage.target_column.lower():
                    if edge not in matching_edges:
                        matching_edges.append(edge)
        return {
            "query": query,
            "results": {
                "nodes": len(matching_nodes),
                "edges": len(matching_edges),
                "matching_nodes": matching_nodes[:20],  
                "matching_edges": matching_edges[:20]
            }
        }
    except Exception as e:
        print(f"Error searching lineage: {str(e)}")
        abort(500, f"Failed to search lineage: {str(e)}")
@lineage_bp.route("/lineage/health", methods=["GET"])
def check_lineage_health():
    try:
        from main import active_connectors, load_assets
        discovered_assets = load_assets()
        lineage_result = get_data_lineage()
        issues = []
        warnings = []
        node_ids_with_edges = set()
        for edge in lineage_result.edges:
            node_ids_with_edges.add(edge.source)
            node_ids_with_edges.add(edge.target)
        orphaned = [n for n in lineage_result.nodes if n.id not in node_ids_with_edges]
        if orphaned:
            warnings.append({
                "type": "orphaned_nodes",
                "count": len(orphaned),
                "nodes": [n.name for n in orphaned[:5]],
                "severity": "medium"
            })
        edges_without_columns = [e for e in lineage_result.edges if not e.column_lineage]
        if edges_without_columns:
            warnings.append({
                "type": "missing_column_lineage",
                "count": len(edges_without_columns),
                "severity": "low"
            })
        now = datetime.now()
        stale_edges = []
        for edge in lineage_result.edges:
            if edge.last_validated:
                last_validated = datetime.fromisoformat(edge.last_validated.replace('Z', '+00:00'))
                days_since = (now - last_validated.replace(tzinfo=None)).days
                if days_since > 30:
                    stale_edges.append(edge.relationship)
        if stale_edges:
            issues.append({
                "type": "stale_lineage",
                "count": len(stale_edges),
                "severity": "medium"
            })
        now_naive = datetime.now()
        freshness_days = []
        for edge in lineage_result.edges:
            if edge.last_validated:
                last_validated = datetime.fromisoformat(edge.last_validated.replace('Z', '+00:00')).replace(tzinfo=None)
                freshness_days.append(max(0, (now_naive - last_validated).days))
        avg_freshness_days = sum(freshness_days) / len(freshness_days) if freshness_days else 0.0
        avg_confidence = lineage_result.avg_confidence if hasattr(lineage_result, 'avg_confidence') else 0.0
        avg_quality = lineage_result.avg_data_quality or 0.0
        completeness = lineage_result.lineage_completeness or 0.0
        total_issues = len(issues) + len(warnings)
        health_score = 100
        health_score -= min(40, total_issues * 5)
        health_score -= min(20, avg_freshness_days)  
        health_score -= max(0, int((1.0 - (avg_confidence or 0.0)) * 20))
        health_score -= max(0, int((95.0 - (avg_quality or 95.0)) * 0.5))
        health_score = max(0, min(100, health_score))
        return {
            "health_score": health_score,
            "status": "healthy" if health_score >= 80 else "degraded" if health_score >= 50 else "critical",
            "issues": issues,
            "warnings": warnings,
            "statistics": {
                "total_nodes": len(lineage_result.nodes),
                "total_edges": len(lineage_result.edges),
                "orphaned_nodes": len(orphaned),
                "stale_edges": len(stale_edges),
                "completeness": completeness,
                "avg_confidence": round(avg_confidence, 3),
                "avg_data_quality": round(avg_quality, 2),
                "avg_freshness_days": round(avg_freshness_days, 1)
            }
        }
    except Exception as e:
        print(f"Error checking lineage health: {str(e)}")
        abort(500, f"Failed to check lineage health: {str(e)}")
@lineage_bp.route("/lineage/<path:asset_id>", methods=["GET"])
def get_asset_lineage(asset_id: str):
    try:
        from main import load_assets
        discovered_assets = load_assets()
        asset = next((a for a in discovered_assets if a['id'] == asset_id), None)
        if not asset:
            raise HTTPException(response=None, code=404, description="Asset not found")
        full_lineage = get_data_lineage()
        related_node_ids = {asset_id}
        upstream_edges = [e for e in full_lineage.edges if e.target == asset_id]
        for edge in upstream_edges:
            related_node_ids.add(edge.source)
        downstream_edges = [e for e in full_lineage.edges if e.source == asset_id]
        for edge in downstream_edges:
            related_node_ids.add(edge.target)
        filtered_nodes = [n for n in full_lineage.nodes if n.id in related_node_ids]
        filtered_edges = [e for e in full_lineage.edges if e.source in related_node_ids and e.target in related_node_ids]
        column_count = sum(len(e.column_lineage or []) for e in filtered_edges)
        return LineageResponse(
            nodes=filtered_nodes, 
            edges=filtered_edges,
            column_relationships=column_count,
            total_pii_columns=sum(e.total_pii_columns for e in filtered_edges),
            avg_data_quality=sum(e.avg_data_quality for e in filtered_edges) / len(filtered_edges) if filtered_edges else 0.0,
            lineage_completeness=100.0 if filtered_nodes else 0.0
        )
    except HTTPException as e:
        abort(e.code, description=e.description)
    except Exception as e:
        print(f"Error getting asset lineage: {str(e)}")
        abort(500, f"Failed to get asset lineage: {str(e)}")
@lineage_bp.route("/lineage-analysis/pipelines", methods=["GET"])
def get_pipeline_lineage():
    try:
        lineage_result = get_data_lineage()
        etl_pipelines = []
        elt_pipelines = []
        regular_relationships = []
        for edge in lineage_result.edges:
            if edge.relationship == 'etl_pipeline':
                etl_pipelines.append(edge)
            elif edge.relationship == 'elt_pipeline':
                elt_pipelines.append(edge)
            else:
                regular_relationships.append(edge)
        def find_pipeline_chains(pipeline_type):
            chains = []
            pipeline_edges = [e for e in (etl_pipelines if pipeline_type == 'etl' else elt_pipelines)]
            for edge in pipeline_edges:
                chain = {
                    'source': edge.source,
                    'target': edge.target,
                    'stage': edge.relationship,
                    'column_count': len(edge.column_lineage or []),
                    'pii_count': edge.total_pii_columns,
                    'quality': edge.avg_data_quality
                }
                chains.append(chain)
            return chains
        etl_chains = find_pipeline_chains('etl')
        elt_chains = find_pipeline_chains('elt')
        return {
            "pipeline_summary": {
                "total_etl_pipelines": len(etl_pipelines),
                "total_elt_pipelines": len(elt_pipelines),
                "total_direct_relationships": len(regular_relationships),
                "total_etl_steps": len(etl_chains),
                "total_elt_steps": len(elt_chains)
            },
            "etl_pipelines": etl_chains,
            "elt_pipelines": elt_chains,
            "visualization": {
                "has_etl": len(etl_pipelines) > 0,
                "has_elt": len(elt_pipelines) > 0,
                "pipeline_complexity": "simple" if len(etl_chains) + len(elt_chains) < 3 else "moderate" if len(etl_chains) + len(elt_chains) < 10 else "complex"
            }
        }
    except Exception as e:
        print(f"Error getting pipeline lineage: {str(e)}")
        abort(500, f"Failed to get pipeline lineage: {str(e)}")
@lineage_bp.route("/lineage/validate/keys", methods=["POST"])
@_require_role('admin')
def validate_keys():
    sample_size = int(request.args.get("sample_size", 0))
    try:
        lineage = get_data_lineage()
        findings = []
        for edge in lineage.edges:
            for cl in (edge.column_lineage or []):
                src = cl.source_column.lower()
                tgt = cl.target_column.lower()
                pk_like = src == 'id' or src.endswith('_id')
                fk_like = tgt == 'id' or tgt.endswith('_id')
                if pk_like or fk_like:
                    findings.append({
                        'source': edge.source,
                        'target': edge.target,
                        'source_column': cl.source_column,
                        'target_column': cl.target_column,
                        'type_match': True,
                        'name_pattern': 'pkfk_like',
                        'confidence_hint': 0.7
                    })
        if sample_size and sample_size > 0:
            from main import active_connectors
            bq_connectors = [c for c in active_connectors if c['id'].startswith('bq_')]
            try:
                from google.cloud import bigquery
                from google.oauth2 import service_account
                for bc in bq_connectors:
                    sa_json = bc.get('service_account_json')
                    if not sa_json:
                        continue
                    creds = service_account.Credentials.from_service_account_info(json.loads(sa_json))
                    client = bigquery.Client(credentials=creds, project=creds.project_id)
                    for f in findings[:5]:
                        table_id = f['source']
                        col = f['source_column']
                        q = f"SELECT COUNT(*) AS n, COUNT(DISTINCT {col}) AS nd FROM `{table_id}` LIMIT {sample_size}"
                        try:
                            res = list(client.query(q).result())
                            if res:
                                n = res[0].get('n') or 0
                                nd = res[0].get('nd') or 0
                                f['distinct_ratio_sample'] = float(nd) / float(n) if n else 0.0
                                f['confidence_hint'] = max(f['confidence_hint'], 0.85 if f['distinct_ratio_sample'] > 0.9 else 0.7)
                        except Exception:
                            continue
            except Exception:
                pass
        return jsonify({ 'findings': findings, 'count': len(findings) })
    except Exception as e:
        print(f"Error validating keys: {str(e)}")
        abort(500, f"Failed to validate keys: {str(e)}")
@lineage_bp.route("/lineage/curation/propose", methods=["POST"])
def propose_lineage_edit():
    payload = request.get_json()
    if not payload:
        abort(400, "No JSON payload provided")
    
    try:
        source = payload.get("source")
        target = payload.get("target")
        
        if not source or not target:
            abort(400, "Both source and target are required")
        
        # Directly create lineage relation (no approval needed)
        relation_type = payload.get("relationship", "manual")
        column_lineage = payload.get("column_lineage", [])
        notes = payload.get("notes", "")
        
        # Prepare extra_data with column lineage and notes if provided
        extra_data = {}
        if column_lineage:
            extra_data["column_lineage"] = column_lineage
        if notes:
            extra_data["notes"] = notes
        if not extra_data:
            extra_data = None
        
        relation_data = {
            "source_id": source,
            "target_id": target,
            "relation_type": relation_type,
            "extra_data": extra_data
        }
        
        if db_helpers.save_lineage_relation(relation_data):
            return jsonify({
                "status": "ok", 
                "message": "Lineage relation created successfully", 
                "relation": relation_data
            })
        else:
            abort(500, "Failed to save lineage relation to database")
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating lineage relation: {str(e)}")
        import traceback
        traceback.print_exc()
        abort(500, f"Failed to create lineage relation: {str(e)}")
@lineage_bp.route("/lineage/curation/approve", methods=["POST"])
@_require_role('admin')
def approve_lineage_edit():
    source = request.args.get("source")
    target = request.args.get("target")
    if not source or not target:
        abort(400, "Source and target are required")
    try:
        proposals = db_helpers.load_curation_proposals(status='proposed')
        match = None
        for p in proposals:
            if p.get("source") == source and p.get("target") == target:
                match = p
                break
        if not match:
            raise HTTPException(response=None, code=404, description="Proposal not found")
        now_iso = datetime.now().isoformat()
        edge = LineageEdge(
            source=source,
            target=target,
            relationship=match.get("relationship", "manual"),
            column_lineage=[ColumnLineage(**cl) for cl in match.get("column_lineage", [])] if match.get("column_lineage") else [],
            total_pii_columns=0,
            avg_data_quality=95.0,
            last_validated=now_iso,
            validation_status="valid",
            confidence_score=0.95,
            evidence=["manual_curation"],
            sources=["user"],
            created_at=now_iso,
            updated_at=now_iso
        )
        db_helpers.update_curation_proposal(source, target, "approved", datetime.now())
        return jsonify({"status": "ok", "edge": edge.model_dump()})
    except HTTPException as e:
        abort(e.code, description=e.description)
    except Exception as e:
        print(f"Error approving lineage edit: {str(e)}")
        abort(500, f"Failed to approve lineage edit: {str(e)}")
@lineage_bp.route("/lineage/ingest/querylog", methods=["POST"])
@_require_role('admin')
def ingest_query_log():
    payload = request.get_json()
    try:
        system = payload.get("system", "unknown")
        sql = payload.get("sql", "")
        timestamp = None
        if payload.get("timestamp"):
            try:
                timestamp = datetime.fromisoformat(payload.get("timestamp").replace('Z', '+00:00'))
            except:
                timestamp = datetime.now()
        db_helpers.save_query_log(system, sql, timestamp)
        all_logs = db_helpers.load_query_logs(limit=10000)
        return jsonify({"status": "ok", "stored": True, "count": len(all_logs)})
    except Exception as e:
        print(f"Error ingesting query log: {str(e)}")
        abort(500, f"Failed to ingest query log: {str(e)}")
@lineage_bp.route("/lineage/ingest/dbt", methods=["POST"])
@_require_role('admin')
def ingest_dbt():
    payload = request.get_json()
    try:
        data = {
            "nodes": payload.get("nodes", [])
        }
        db_helpers.save_integration_data('dbt', data)
        all_dbt = db_helpers.load_integration_data('dbt')
        return jsonify({"status": "ok", "stored": True, "dbt_batches": len(all_dbt)})
    except Exception as e:
        print(f"Error ingesting dbt: {str(e)}")
        abort(500, f"Failed to ingest dbt: {str(e)}")
@lineage_bp.route("/lineage/ingest/airflow", methods=["POST"])
@_require_role('admin')
def ingest_airflow():
    payload = request.get_json()
    try:
        data = {
            "dag_id": payload.get("dag_id"),
            "tasks": payload.get("tasks", [])
        }
        db_helpers.save_integration_data('airflow', data)
        all_airflow = db_helpers.load_integration_data('airflow')
        return jsonify({"status": "ok", "stored": True, "airflow_batches": len(all_airflow)})
    except Exception as e:
        print(f"Error ingesting Airflow: {str(e)}")
        abort(500, f"Failed to ingest Airflow: {str(e)}")
@lineage_bp.route("/lineage/ingest/openlineage", methods=["POST"])
@_require_role('admin')
def ingest_openlineage():
    payload = request.get_json()
    try:
        db_helpers.save_integration_data('openlineage', payload)
        all_openlineage = db_helpers.load_integration_data('openlineage')
        return jsonify({"status": "ok", "stored": True, "openlineage_events": len(all_openlineage)})
    except Exception as e:
        print(f"Error ingesting OpenLineage: {str(e)}")
        abort(500, f"Failed to ingest OpenLineage: {str(e)}")
@lineage_bp.route("/lineage/ingest/metadata", methods=["POST"])
@_require_role('admin')
def ingest_metadata():
    payload = request.get_json()
    try:
        data = {
            "payload": payload
        }
        db_helpers.save_integration_data('metadata', data)
        all_metadata = db_helpers.load_integration_data('metadata')
        return jsonify({"status": "ok", "stored": True, "metadata_batches": len(all_metadata)})
    except Exception as e:
        print(f"Error ingesting metadata: {str(e)}")
        abort(500, f"Failed to ingest metadata: {str(e)}")
@lineage_bp.route("/lineage/reconcile", methods=["POST"])
@_require_role('admin')
def reconcile_artifacts():
    try:
        new_edges: List[Dict[str, Any]] = []
        new_edges += _reconcile_openlineage_to_edges()
        new_edges += _reconcile_dbt_to_edges()
        new_edges += _reconcile_airflow_to_edges()
        new_edges += _reconcile_metadata_to_edges()
        for edge in new_edges:
            relation_data = {
                'source_id': edge.get('source'),
                'target_id': edge.get('target'),
                'relation_type': edge.get('relationship', 'unknown'),
                'extra_data': edge
            }
            db_helpers.save_lineage_relation(relation_data)
        return jsonify({"status": "ok", "created_edges": len(new_edges)})
    except HTTPException as e:
        abort(e.code, description=e.description)
    except Exception as e:
        print(f"Error reconciling artifacts: {str(e)}")
        abort(500, f"Failed to reconcile artifacts: {str(e)}")

@lineage_bp.route("/lineage/curation/upload", methods=["POST"])
@_require_role('admin')
def upload_manual_lineage():
    try:
        if 'file' not in request.files:
            abort(400, "No file provided")
        
        file = request.files['file']
        if file.filename == '':
            abort(400, "No file selected")
        
        file_ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else ''
        proposals_created = []
        
        if file_ext == 'csv':
            import csv
            import io
            stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
            csv_reader = csv.DictReader(stream)
            
            for row in csv_reader:
                source = row.get('source_table') or row.get('source')
                target = row.get('target_table') or row.get('target')
                relationship = row.get('relationship', 'manual')
                notes = row.get('notes', f'Uploaded from CSV: {file.filename}')
                
                if not source or not target:
                    continue
                
                column_lineage = []
                if row.get('source_column') and row.get('target_column'):
                    column_lineage.append({
                        'source_table': source,
                        'source_column': row['source_column'],
                        'target_table': target,
                        'target_column': row['target_column'],
                        'relationship_type': row.get('column_relationship', 'direct_match')
                    })
                
                proposal_data = {
                    'source': source,
                    'target': target,
                    'relationship': relationship,
                    'column_lineage': column_lineage if column_lineage else None,
                    'notes': notes,
                    'status': 'proposed'
                }
                
                if db_helpers.save_curation_proposal(proposal_data):
                    proposals_created.append(proposal_data)
        
        elif file_ext == 'json':
            data = json.loads(file.read().decode('utf-8'))
            
            entries = data if isinstance(data, list) else [data]
            
            for entry in entries:
                source = entry.get('source') or entry.get('source_table')
                target = entry.get('target') or entry.get('target_table')
                relationship = entry.get('relationship', 'manual')
                notes = entry.get('notes', f'Uploaded from JSON: {file.filename}')
                column_lineage = entry.get('column_lineage', [])
                
                if not source or not target:
                    continue
                
                proposal_data = {
                    'source': source,
                    'target': target,
                    'relationship': relationship,
                    'column_lineage': column_lineage if column_lineage else None,
                    'notes': notes,
                    'status': 'proposed'
                }
                
                if db_helpers.save_curation_proposal(proposal_data):
                    proposals_created.append(proposal_data)
        else:
            abort(400, "Unsupported file format. Use CSV or JSON")
        
        return jsonify({
            "status": "ok",
            "proposals_created": len(proposals_created),
            "proposals": proposals_created[:10]
        })
    except Exception as e:
        print(f"Error uploading manual lineage: {str(e)}")
        import traceback
        traceback.print_exc()
        abort(500, f"Failed to upload manual lineage: {str(e)}")

@lineage_bp.route("/lineage/curation/reject", methods=["POST"])
@_require_role('admin')
def reject_lineage_proposal():
    source = request.args.get("source")
    target = request.args.get("target")
    if not source or not target:
        abort(400, "Source and target are required")
    
    try:
        if db_helpers.update_curation_proposal(source, target, "rejected", None):
            return jsonify({"status": "ok", "message": "Proposal rejected"})
        else:
            abort(404, "Proposal not found")
    except Exception as e:
        print(f"Error rejecting proposal: {str(e)}")
        abort(500, f"Failed to reject proposal: {str(e)}")

@lineage_bp.route("/lineage/curation/list", methods=["GET"])
def list_curation_proposals():
    status = request.args.get("status", None)
    try:
        proposals = db_helpers.load_curation_proposals(status=status)
        return jsonify({
            "status": "ok",
            "count": len(proposals),
            "proposals": proposals
        })
    except Exception as e:
        print(f"Error listing proposals: {str(e)}")
        abort(500, f"Failed to list proposals: {str(e)}")
