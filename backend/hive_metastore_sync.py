from typing import Dict, Any, Optional, Tuple
from datetime import datetime
from database import HiveDB, HiveTable, HiveStorageDescriptor, HiveColumn
from flask import current_app
from main import db

def sync_asset_to_hive_metastore(asset: Dict[str, Any]) -> bool:
    try:
        with current_app.app_context():
            asset_type = asset.get('type', '').upper()
            table_types = ['TABLE', 'BASE TABLE', 'VIEW', 'DATA FILE']
            if asset_type not in table_types:
                return True
            
            catalog = asset.get('catalog', 'default')
            schema = asset.get('schema_name') or asset.get('schema', 'default')
            table_name = asset.get('name')
            asset_id = asset.get('id')
            
            if not table_name or not asset_id:
                print(f"  Skipping asset sync: missing name or id")
                return False
            
            hive_db = db.session.query(HiveDB).filter(HiveDB.name == catalog).first()
            if not hive_db:
                technical_meta = asset.get('technical_metadata', {})
                location = technical_meta.get('location', '')
                if not location and asset_id:
                    if asset_id.startswith(('s3://', 'gs://')):
                        location = asset_id.rsplit('/', 1)[0] if '/' in asset_id else asset_id
                    else:
                        location = f"s3://{catalog}/"
                
                hive_db = HiveDB(
                    name=catalog,
                    db_location_uri=location if location else f"s3://{catalog}/",
                    description=f"Database for {catalog}",
                    owner_name="torro",
                    owner_type="USER"
                )
                db.session.add(hive_db)
                db.session.flush()
                print(f"  Created Hive DB: {catalog}")
            
            technical_meta = asset.get('technical_metadata', {})
            location = technical_meta.get('location', asset_id)
            format_type = technical_meta.get('format', 'text/csv')
            
            input_format, output_format, serde_lib = _map_format_to_hive(format_type)
            serde_name = _get_serde_name(format_type)
            
            hive_sd = db.session.query(HiveStorageDescriptor).filter(
                HiveStorageDescriptor.location == location
            ).first()
            
            if not hive_sd:
                hive_sd = HiveStorageDescriptor(
                    input_format=input_format,
                    output_format=output_format,
                    location=location,
                    serde_lib=serde_lib,
                    serde_name=serde_name,
                    is_compressed=False,
                    is_stored_as_sub_directories=False
                )
                db.session.add(hive_sd)
                db.session.flush()
                print(f"  Created Hive Storage Descriptor: {location}")
            
            hive_table = db.session.query(HiveTable).filter(
                HiveTable.asset_id == asset_id
            ).first()
            
            current_timestamp = int(datetime.utcnow().timestamp())
            
            if not hive_table:
                hive_table = HiveTable(
                    db_id=hive_db.db_id,
                    tbl_name=table_name,
                    tbl_type='EXTERNAL_TABLE',
                    sd_id=hive_sd.sd_id,
                    owner='torro',
                    create_time=current_timestamp,
                    last_access_time=current_timestamp,
                    asset_id=asset_id
                )
                db.session.add(hive_table)
                db.session.flush()
                print(f"  Created Hive Table: {catalog}.{table_name}")
            else:
                hive_table.tbl_name = table_name
                hive_table.sd_id = hive_sd.sd_id
                hive_table.last_access_time = current_timestamp
                print(f"  Updated Hive Table: {catalog}.{table_name}")
            
            columns = asset.get('columns', [])
            if not columns and asset.get('extra_data'):
                if isinstance(asset.get('extra_data'), dict):
                    columns = asset.get('extra_data').get('columns', [])
            
            if columns:
                db.session.query(HiveColumn).filter(
                    HiveColumn.sd_id == hive_sd.sd_id
                ).delete()
                
                for idx, col in enumerate(columns):
                    col_type = _map_type_to_hive(col.get('type', 'string'))
                    hive_col = HiveColumn(
                        column_name=col.get('name', f'col_{idx}'),
                        type_name=col_type,
                        integer_idx=idx,
                        sd_id=hive_sd.sd_id,
                        comment=col.get('description', '') or col.get('comment', '')
                    )
                    db.session.add(hive_col)
                
                print(f"  Synced {len(columns)} columns for {table_name}")
            
            db.session.commit()
            return True
            
    except Exception as e:
        db.session.rollback()
        print(f"  Error syncing asset to Hive Metastore: {e}")
        import traceback
        traceback.print_exc()
        return False

def _map_format_to_hive(format_type: str) -> Tuple[str, str, str]:
    format_lower = format_type.lower()
    
    format_map = {
        'text/csv': (
            'org.apache.hadoop.mapred.TextInputFormat',
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        ),
        'csv': (
            'org.apache.hadoop.mapred.TextInputFormat',
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        ),
        'application/json': (
            'org.apache.hadoop.mapred.TextInputFormat',
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'org.apache.hadoop.hive.serde2.JsonSerDe'
        ),
        'json': (
            'org.apache.hadoop.mapred.TextInputFormat',
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'org.apache.hadoop.hive.serde2.JsonSerDe'
        ),
        'parquet': (
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        ),
        'application/x-parquet': (
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        ),
        'avro': (
            'org.apache.hadoop.mapred.TextInputFormat',
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
        ),
        'orc': (
            'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',
            'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',
            'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
        ),
    }
    
    if format_lower in format_map:
        return format_map[format_lower]
    
    for key, value in format_map.items():
        if key in format_lower or format_lower in key:
            return value
    
    return format_map['text/csv']

def _get_serde_name(format_type: str) -> str:
    format_lower = format_type.lower()
    
    if 'json' in format_lower:
        return 'JsonSerDe'
    elif 'parquet' in format_lower:
        return 'ParquetHiveSerDe'
    elif 'avro' in format_lower:
        return 'AvroSerDe'
    elif 'orc' in format_lower:
        return 'OrcSerde'
    else:
        return 'LazySimpleSerDe'

def _map_type_to_hive(type_str: str) -> str:
    if not type_str:
        return 'string'
    
    type_lower = type_str.lower().strip()
    
    type_map = {
        'string': 'string',
        'varchar': 'string',
        'text': 'string',
        'char': 'string',
        'int': 'int',
        'integer': 'int',
        'bigint': 'bigint',
        'long': 'bigint',
        'double': 'double',
        'float': 'float',
        'decimal': 'decimal(10,2)',
        'numeric': 'decimal(10,2)',
        'boolean': 'boolean',
        'bool': 'boolean',
        'timestamp': 'timestamp',
        'date': 'date',
        'datetime': 'timestamp',
        'array': 'array<string>',
        'map': 'map<string,string>',
        'struct': 'struct<>',
    }
    
    if type_lower in type_map:
        return type_map[type_lower]
    
    for key, value in type_map.items():
        if type_lower.startswith(key):
            return value
    
    return 'string'

def migrate_all_assets_to_hive() -> Dict[str, Any]:
    import db_helpers
    
    assets = db_helpers.load_assets()
    synced = 0
    failed = 0
    skipped = 0
    
    print(f"Starting migration of {len(assets)} assets to Hive Metastore...")
    
    for asset in assets:
        asset_type = asset.get('type', '')
        if asset_type not in ['Table', 'Data File']:
            skipped += 1
            continue
        
        if sync_asset_to_hive_metastore(asset):
            synced += 1
            if synced % 10 == 0:
                print(f"  Progress: {synced} synced, {failed} failed, {skipped} skipped")
        else:
            failed += 1
    
    result = {
        'total': len(assets),
        'synced': synced,
        'failed': failed,
        'skipped': skipped
    }
    
    print(f"Migration complete: {synced} synced, {failed} failed, {skipped} skipped")
    return result

