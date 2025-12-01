from database import (
    Connector, Asset, PublishedTag, LineageRelation,
    LineageSnapshot, CurationProposal, QueryLog, IntegrationData, User, PendingAsset,
)
from datetime import datetime
from typing import List, Dict, Any, Optional
import json
from flask import request, abort, current_app
from functools import wraps
from flask_login import current_user
from sqlalchemy import create_engine
from config import Config

engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
def save_connector(connector_data: Dict[str, Any]) -> bool:
    try:
        try:
            from flask import current_app
            from main import db
            with current_app.app_context():
                existing = db.session.query(Connector).filter(Connector.id == connector_data.get('id')).first()
                if existing:
                    for key, value in connector_data.items():
                        if key == 'last_run' and isinstance(value, str):
                            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        elif key == 'config':
                            existing_config = existing.config if existing.config else {}
                            if isinstance(existing_config, str):
                                import json
                                try:
                                    existing_config = json.loads(existing_config)
                                except (json.JSONDecodeError, ValueError):
                                    existing_config = {}
                            if isinstance(value, str):
                                import json
                                try:
                                    value = json.loads(value)
                                except (json.JSONDecodeError, ValueError):
                                    pass
                            if isinstance(value, dict) and isinstance(existing_config, dict):
                                merged_config = {**existing_config, **value}
                                setattr(existing, key, merged_config)
                            else:
                                setattr(existing, key, value)
                        else:
                            setattr(existing, key, value)
                    if 'enabled' not in connector_data:
                        existing.enabled = True
                    existing.updated_at = datetime.utcnow()
                else:
                    if 'last_run' in connector_data and isinstance(connector_data['last_run'], str):
                        connector_data['last_run'] = datetime.fromisoformat(connector_data['last_run'].replace('Z', '+00:00'))
                    if 'enabled' not in connector_data:
                        connector_data['enabled'] = True
                    connector = Connector(**connector_data)
                    db.session.add(connector)
                db.session.commit()
                return True
        except RuntimeError:
            from sqlalchemy.orm import sessionmaker
            Session = sessionmaker(bind=engine)
            session = Session()
            try:
                existing = session.query(Connector).filter(Connector.id == connector_data.get('id')).first()
                if existing:
                    for key, value in connector_data.items():
                        if key == 'last_run' and isinstance(value, str):
                            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        elif key == 'config':
                            existing_config = existing.config if existing.config else {}
                            if isinstance(existing_config, str):
                                import json
                                try:
                                    existing_config = json.loads(existing_config)
                                except (json.JSONDecodeError, ValueError):
                                    existing_config = {}
                            if isinstance(value, str):
                                import json
                                try:
                                    value = json.loads(value)
                                except (json.JSONDecodeError, ValueError):
                                    pass
                            if isinstance(value, dict) and isinstance(existing_config, dict):
                                merged_config = {**existing_config, **value}
                                setattr(existing, key, merged_config)
                            else:
                                setattr(existing, key, value)
                        else:
                            setattr(existing, key, value)
                    if 'enabled' not in connector_data:
                        existing.enabled = True
                    existing.updated_at = datetime.utcnow()
                else:
                    if 'last_run' in connector_data and isinstance(connector_data['last_run'], str):
                        connector_data['last_run'] = datetime.fromisoformat(connector_data['last_run'].replace('Z', '+00:00'))
                    if 'enabled' not in connector_data:
                        connector_data['enabled'] = True
                    connector = Connector(**connector_data)
                    session.add(connector)
                session.commit()
                return True
            except Exception as e:
                session.rollback()
                print(f"Error saving connector (no app context): {e}")
                import traceback
                traceback.print_exc()
                return False
            finally:
                session.close()
    except Exception as e:
        print(f"Error saving connector: {e}")
        import traceback
        traceback.print_exc()
        return False
def load_connectors() -> List[Dict[str, Any]]:
    try:
        try:
            from flask import current_app
            from main import db
            with current_app.app_context():
                connectors = db.session.query(Connector).all()
                result = []
                for conn in connectors:
                    config = conn.config
                    if config is None:
                        config = {}
                    elif isinstance(config, str):
                        try:
                            import json
                            config = json.loads(config)
                        except (json.JSONDecodeError, ValueError):
                            config = {}
                    
                    conn_dict = {
                        'id': conn.id,
                        'name': conn.name,
                        'type': conn.type,
                        'status': conn.status,
                        'enabled': conn.enabled if conn.enabled is not None else True,
                        'last_run': conn.last_run.isoformat() if conn.last_run else None,
                        'config': config,
                        'assets_count': conn.assets_count,
                    }
                    result.append(conn_dict)
                # Expunge all objects from session and remove session
                db.session.expunge_all()
                db.session.remove()
                return result
        except (RuntimeError, ImportError):
            from sqlalchemy.orm import sessionmaker
            Session = sessionmaker(bind=engine)
            session = Session()
            try:
                connectors = session.query(Connector).all()
                result = []
                for conn in connectors:
                    config = conn.config
                    if config is None:
                        config = {}
                    elif isinstance(config, str):
                        try:
                            import json
                            config = json.loads(config)
                        except (json.JSONDecodeError, ValueError):
                            config = {}
                    
                    conn_dict = {
                        'id': conn.id,
                        'name': conn.name,
                        'type': conn.type,
                        'status': conn.status,
                        'enabled': conn.enabled if conn.enabled is not None else True,
                        'last_run': conn.last_run.isoformat() if conn.last_run else None,
                        'config': config,
                        'assets_count': conn.assets_count,
                    }
                    result.append(conn_dict)
                return result
            finally:
                session.close()
    except Exception as e:
        print(f"Error loading connectors: {e}")
        import traceback
        traceback.print_exc()
        return []
def delete_connector(connector_id: str) -> bool:
    try:
        try:
            from flask import current_app
            from main import db
            with current_app.app_context():
                connector = db.session.query(Connector).filter(Connector.id == connector_id).first()
                if connector:
                    db.session.delete(connector)
                    db.session.commit()
                    return True
                return False
        except RuntimeError:
            from sqlalchemy.orm import sessionmaker
            Session = sessionmaker(bind=engine)
            session = Session()
            try:
                connector = session.query(Connector).filter(Connector.id == connector_id).first()
                if connector:
                    session.delete(connector)
                    session.commit()
                    return True
                return False
            except Exception as e:
                session.rollback()
                print(f"Error deleting connector (no app context): {e}")
                import traceback
                traceback.print_exc()
                return False
            finally:
                session.close()
    except Exception as e:
        print(f"Error deleting connector: {e}")
        import traceback
        traceback.print_exc()
        return False
def save_asset(asset_data: Dict[str, Any]) -> bool:
    try:
        if not asset_data.get('id'):
            print(f"  save_asset: Missing required field 'id'")
            return False
        if not asset_data.get('name'):
            print(f"  save_asset: Missing required field 'name' for asset {asset_data.get('id')}")
            return False
        if not asset_data.get('type'):
            print(f"  save_asset: Missing required field 'type' for asset {asset_data.get('id')}")
            return False
        
        try:
            from flask import current_app
            from main import db
            with current_app.app_context():
                existing = db.session.query(Asset).filter(Asset.id == asset_data.get('id')).first()
                if existing:
                    for key, value in asset_data.items():
                        if key == 'discovered_at' and isinstance(value, str):
                            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        if key == 'schema':
                            setattr(existing, 'schema_name', value)
                        elif key == 'metadata':
                            setattr(existing, 'extra_data', value)
                        elif key not in ['id']:
                            setattr(existing, key, value)
                    existing.updated_at = datetime.utcnow()
                else:
                    new_asset_data = {}
                    extra_data = asset_data.copy()
                    
                    if 'id' in asset_data:
                        new_asset_data['id'] = asset_data['id']
                    if 'name' in asset_data:
                        new_asset_data['name'] = asset_data['name']
                    if 'type' in asset_data:
                        new_asset_data['type'] = asset_data['type']
                    if 'catalog' in asset_data:
                        new_asset_data['catalog'] = asset_data['catalog']
                    if 'schema' in asset_data:
                        new_asset_data['schema_name'] = asset_data['schema']
                    elif 'schema_name' in asset_data:
                        new_asset_data['schema_name'] = asset_data['schema_name']
                    if 'connector_id' in asset_data:
                        new_asset_data['connector_id'] = asset_data['connector_id']
                    if 'discovered_at' in asset_data:
                        if isinstance(asset_data['discovered_at'], str):
                            new_asset_data['discovered_at'] = datetime.fromisoformat(asset_data['discovered_at'].replace('Z', '+00:00'))
                        else:
                            new_asset_data['discovered_at'] = asset_data['discovered_at']
                    if 'status' in asset_data:
                        new_asset_data['status'] = asset_data['status']
                    else:
                        new_asset_data['status'] = 'active'
                    if 'sort_order' in asset_data:
                        new_asset_data['sort_order'] = asset_data['sort_order']
                    
                    new_asset_data['extra_data'] = extra_data
                    
                    asset = Asset(**new_asset_data)
                    db.session.add(asset)
                try:
                    db.session.commit()
                    
                    try:
                        from hive_metastore_sync import sync_asset_to_hive_metastore
                        sync_asset_to_hive_metastore(asset_data)
                    except Exception as sync_err:
                        print(f"  Warning: Hive Metastore sync failed (asset still saved): {sync_err}")
                    
                    return True
                except Exception as commit_error:
                    db.session.rollback()
                    if 'Duplicate entry' in str(commit_error) or 'UNIQUE constraint' in str(commit_error):
                        try:
                            existing = db.session.query(Asset).filter(Asset.id == asset_data.get('id')).first()
                            if existing:
                                for key, value in asset_data.items():
                                    if key == 'discovered_at' and isinstance(value, str):
                                        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                    if key == 'schema':
                                        setattr(existing, 'schema_name', value)
                                    elif key == 'metadata':
                                        setattr(existing, 'extra_data', value)
                                    elif key not in ['id']:
                                        setattr(existing, key, value)
                                existing.updated_at = datetime.utcnow()
                                db.session.commit()
                                
                                try:
                                    from hive_metastore_sync import sync_asset_to_hive_metastore
                                    sync_asset_to_hive_metastore(asset_data)
                                except Exception as sync_err:
                                    print(f"  Warning: Hive Metastore sync failed (asset still saved): {sync_err}")
                                
                                return True
                        except Exception as update_error:
                            db.session.rollback()
                            print(f" Failed to update existing asset: {update_error}")
                    raise commit_error
        except RuntimeError:
            from sqlalchemy.orm import sessionmaker
            Session = sessionmaker(bind=engine)
            session = Session()
            try:
                existing = session.query(Asset).filter(Asset.id == asset_data.get('id')).first()
                if existing:
                    for key, value in asset_data.items():
                        if key == 'discovered_at' and isinstance(value, str):
                            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        if key == 'schema':
                            setattr(existing, 'schema_name', value)
                        elif key == 'metadata':
                            setattr(existing, 'extra_data', value)
                        elif key not in ['id']:
                            setattr(existing, key, value)
                    existing.updated_at = datetime.utcnow()
                else:
                    valid_asset_fields = ['id', 'name', 'type', 'catalog', 'schema_name', 'connector_id', 
                                        'discovered_at', 'status', 'sort_order', 'extra_data']
                    
                    new_asset_data = {}
                    extra_data = asset_data.copy()
                    
                    if 'id' in asset_data:
                        new_asset_data['id'] = asset_data['id']
                    if 'name' in asset_data:
                        new_asset_data['name'] = asset_data['name']
                    if 'type' in asset_data:
                        new_asset_data['type'] = asset_data['type']
                    if 'catalog' in asset_data:
                        new_asset_data['catalog'] = asset_data['catalog']
                    if 'schema' in asset_data:
                        new_asset_data['schema_name'] = asset_data['schema']
                    elif 'schema_name' in asset_data:
                        new_asset_data['schema_name'] = asset_data['schema_name']
                    if 'connector_id' in asset_data:
                        new_asset_data['connector_id'] = asset_data['connector_id']
                    if 'discovered_at' in asset_data:
                        if isinstance(asset_data['discovered_at'], str):
                            new_asset_data['discovered_at'] = datetime.fromisoformat(asset_data['discovered_at'].replace('Z', '+00:00'))
                        else:
                            new_asset_data['discovered_at'] = asset_data['discovered_at']
                    if 'status' in asset_data:
                        new_asset_data['status'] = asset_data['status']
                    else:
                        new_asset_data['status'] = 'active'
                    if 'sort_order' in asset_data:
                        new_asset_data['sort_order'] = asset_data['sort_order']
                    
                    new_asset_data['extra_data'] = extra_data
                    
                    asset = Asset(**new_asset_data)
                    session.add(asset)
                try:
                    session.commit()
                    return True
                except Exception as commit_error:
                    session.rollback()
                    if 'Duplicate entry' in str(commit_error) or 'UNIQUE constraint' in str(commit_error):
                        try:
                            existing = session.query(Asset).filter(Asset.id == asset_data.get('id')).first()
                            if existing:
                                for key, value in asset_data.items():
                                    if key == 'discovered_at' and isinstance(value, str):
                                        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                    if key == 'schema':
                                        setattr(existing, 'schema_name', value)
                                    elif key == 'metadata':
                                        setattr(existing, 'extra_data', value)
                                    elif key not in ['id']:
                                        setattr(existing, key, value)
                                existing.updated_at = datetime.utcnow()
                                session.commit()
                                return True
                        except Exception as update_error:
                            session.rollback()
                            print(f" Failed to update existing asset: {update_error}")
                    raise commit_error
            except Exception as e:
                session.rollback()
                error_msg = str(e)
                if 'Data too long' in error_msg or 'value too long' in error_msg.lower():
                    print(f" Asset ID or field too long: {asset_data.get('id', 'Unknown')[:100]}...")
                    print(f"   ID length: {len(asset_data.get('id', ''))}")
                elif 'Duplicate entry' in error_msg or 'UNIQUE constraint' in error_msg:
                    print(f"  Duplicate asset ID (updating existing): {asset_data.get('id', 'Unknown')[:100]}")
                    try:
                        existing = session.query(Asset).filter(Asset.id == asset_data.get('id')).first()
                        if existing:
                            for key, value in asset_data.items():
                                if key == 'discovered_at' and isinstance(value, str):
                                    value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                if key == 'schema':
                                    setattr(existing, 'schema_name', value)
                                elif key == 'metadata':
                                    setattr(existing, 'extra_data', value)
                                elif key not in ['id']:
                                    setattr(existing, key, value)
                            existing.updated_at = datetime.utcnow()
                            session.commit()
                            return True
                    except Exception as update_error:
                        print(f" Failed to update existing asset: {update_error}")
                else:
                    print(f" Error saving asset (no app context): {e}")
                    print(f"   Asset ID: {asset_data.get('id', 'Unknown')[:100]}")
                    print(f"   Asset Name: {asset_data.get('name', 'Unknown')[:100]}")
                    import traceback
                    traceback.print_exc()
                return False
            finally:
                session.close()
    except Exception as e:
        print(f"Error saving asset: {e}")
        import traceback
        traceback.print_exc()
        return False
def load_assets() -> List[Dict[str, Any]]:
    try:
        try:
            from flask import current_app
            from main import db
            with current_app.app_context():
                assets = db.session.query(Asset).all()
                result = []
                for asset in assets:
                    asset_dict = {
                        'id': asset.id,
                        'name': asset.name,
                        'type': asset.type,
                        'catalog': asset.catalog,
                        'schema': asset.schema_name,
                        'connector_id': asset.connector_id,
                        'discovered_at': asset.discovered_at.isoformat() if asset.discovered_at else None,
                        'status': asset.status,
                        'sort_order': asset.sort_order,
                    }
                    if asset.extra_data:
                        if isinstance(asset.extra_data, dict):
                            asset_dict.update(asset.extra_data)
                        elif isinstance(asset.extra_data, str):
                            try:
                                import json
                                extra_data_dict = json.loads(asset.extra_data)
                                asset_dict.update(extra_data_dict)
                            except:
                                pass
                        
                        if 'columns' not in asset_dict and isinstance(asset.extra_data, dict) and 'columns' in asset.extra_data:
                            asset_dict['columns'] = asset.extra_data['columns']
                    result.append(asset_dict)
                # Expunge all objects from session and remove session
                db.session.expunge_all()
                db.session.remove()
                return result
        except (RuntimeError, ImportError):
            from sqlalchemy.orm import sessionmaker
            Session = sessionmaker(bind=engine)
            session = Session()
            try:
                assets = session.query(Asset).all()
                result = []
                for asset in assets:
                    asset_dict = {
                        'id': asset.id,
                        'name': asset.name,
                        'type': asset.type,
                        'catalog': asset.catalog,
                        'schema': asset.schema_name,
                        'connector_id': asset.connector_id,
                        'discovered_at': asset.discovered_at.isoformat() if asset.discovered_at else None,
                        'status': asset.status,
                        'sort_order': asset.sort_order,
                    }
                    if asset.extra_data:
                        if isinstance(asset.extra_data, dict):
                            asset_dict.update(asset.extra_data)
                        elif isinstance(asset.extra_data, str):
                            try:
                                import json
                                extra_data_dict = json.loads(asset.extra_data)
                                asset_dict.update(extra_data_dict)
                            except:
                                pass
                        
                        if 'columns' not in asset_dict and isinstance(asset.extra_data, dict) and 'columns' in asset.extra_data:
                            asset_dict['columns'] = asset.extra_data['columns']
                    result.append(asset_dict)
                return result
            finally:
                session.close()
    except Exception as e:
        print(f"Error loading assets: {e}")
        import traceback
        traceback.print_exc()
        return []
def delete_assets_by_connector(connector_id: str) -> bool:
    try:
        try:
            from flask import current_app
            from main import db
            with current_app.app_context():
                db.session.query(Asset).filter(Asset.connector_id == connector_id).delete()
                db.session.commit()
                return True
        except RuntimeError:
            from sqlalchemy.orm import sessionmaker
            Session = sessionmaker(bind=engine)
            session = Session()
            try:
                session.query(Asset).filter(Asset.connector_id == connector_id).delete()
                session.commit()
                return True
            except Exception as e:
                session.rollback()
                print(f"Error deleting assets (no app context): {e}")
                import traceback
                traceback.print_exc()
                return False
            finally:
                session.close()
    except Exception as e:
        db.session.rollback()
        print(f"Error deleting assets: {e}")
        import traceback
        traceback.print_exc()
        return False
def save_published_tag(tag_data: Dict[str, Any]) -> bool:
    from main import db 
    with current_app.app_context():
        try:
            tag = PublishedTag(**tag_data)
            db.session.add(tag)
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            print(f"Error saving published tag: {e}")
            return False
def get_published_tags() -> List[Dict[str, Any]]:
    from main import db 
    with current_app.app_context():
        try:
            tags = db.session.query(PublishedTag).all()
            result = []
            for tag in tags:
                tag_dict = {
                    'id': tag.id,
                    'asset_id': tag.asset_id,
                    'tag_name': tag.tag_name,
                    'tag_type': tag.tag_type,
                    'target_column': tag.target_column,
                    'published_at': tag.published_at.isoformat() if tag.published_at else None,
                    'connector_type': tag.connector_type,
                    'metadata': tag.extra_data or {},
                }
                result.append(tag_dict)
            db.session.expunge_all()
            db.session.remove()
            return result
        except Exception as e:
            print(f"Error loading published tags: {e}")
            db.session.rollback()
            db.session.remove()
            return []
def save_lineage_relation(relation_data: Dict[str, Any]) -> bool:
    from main import db 
    with current_app.app_context():
        try:
            relation = LineageRelation(**relation_data)
            db.session.add(relation)
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            print(f"Error saving lineage relation: {e}")
            return False
def get_lineage_relations() -> List[Dict[str, Any]]:
    from main import db 
    with current_app.app_context():
        try:
            relations = db.session.query(LineageRelation).all()
            result = []
            for rel in relations:
                rel_dict = {
                    'id': rel.id,
                    'source_id': rel.source_id,
                    'target_id': rel.target_id,
                    'relation_type': rel.relation_type,
                    'metadata': rel.extra_data or {},
                }
                result.append(rel_dict)
            db.session.expunge_all()
            db.session.remove()
            return result
        except Exception as e:
            print(f"Error loading lineage relations: {e}")
            db.session.rollback()
            db.session.remove()
            return []
def save_lineage_snapshot(snapshot_data: Dict[str, Any], signature: Optional[str] = None, signature_alg: Optional[str] = None) -> bool:
    from main import db 
    with current_app.app_context():
        try:
            snapshot = LineageSnapshot(
                data=snapshot_data,
                signature=signature,
                signature_alg=signature_alg
            )
            db.session.add(snapshot)
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            print(f"Error saving lineage snapshot: {e}")
            return False
def load_lineage_snapshots(limit: int = 10) -> List[Dict[str, Any]]:
    from main import db 
    with current_app.app_context():
        try:
            snapshots = db.session.query(LineageSnapshot).order_by(LineageSnapshot.created_at.desc()).limit(limit).all()
            result = []
            for snap in snapshots:
                result.append({
                    'id': snap.id,
                    'data': snap.data,
                    'signature': snap.signature,
                    'signature_alg': snap.signature_alg,
                    'created_at': snap.created_at.isoformat() if snap.created_at else None
                })
            db.session.expunge_all()
            db.session.remove()
            return result
        except Exception as e:
            print(f"Error loading lineage snapshots: {e}")
            db.session.rollback()
            db.session.remove()
            return []
def save_curation_proposal(proposal_data: Dict[str, Any]) -> bool:
    from main import db 
    with current_app.app_context():
        try:
            proposal = CurationProposal(
                source=proposal_data.get('source'),
                target=proposal_data.get('target'),
                relationship=proposal_data.get('relationship', 'manual'),
                column_lineage=proposal_data.get('column_lineage'),
                notes=proposal_data.get('notes', ''),
                status=proposal_data.get('status', 'proposed')
            )
            db.session.add(proposal)
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            print(f"Error saving curation proposal: {e}")
            return False
def load_curation_proposals(status: Optional[str] = None) -> List[Dict[str, Any]]:
    from main import db 
    with current_app.app_context():
        try:
            query = db.session.query(CurationProposal)
            if status:
                query = query.filter(CurationProposal.status == status)
            proposals = query.all()
            result = []
            for prop in proposals:
                result.append({
                    'id': prop.id,
                    'source': prop.source,
                    'target': prop.target,
                    'relationship': prop.relationship,
                    'column_lineage': prop.column_lineage,
                    'notes': prop.notes,
                    'status': prop.status,
                    'proposed_at': prop.proposed_at.isoformat() if prop.proposed_at else None,
                    'approved_at': prop.approved_at.isoformat() if prop.approved_at else None
                })
            db.session.expunge_all()
            db.session.remove()
            return result
        except Exception as e:
            print(f"Error loading curation proposals: {e}")
            db.session.rollback()
            db.session.remove()
            return []
def update_curation_proposal(source: str, target: str, status: str, approved_at: Optional[datetime] = None) -> bool:
    from main import db 
    with current_app.app_context():
        try:
            proposal = db.session.query(CurationProposal).filter(
                CurationProposal.source == source,
                CurationProposal.target == target,
                CurationProposal.status == 'proposed'
            ).first()
            if proposal:
                proposal.status = status
                if approved_at:
                    proposal.approved_at = approved_at
                db.session.commit()
                return True
            return False
        except Exception as e:
            db.session.rollback()
            print(f"Error updating curation proposal: {e}")
            return False
def save_query_log(system: str, sql: str, timestamp: Optional[datetime] = None) -> bool:
    from main import db 
    with current_app.app_context():
        try:
            log = QueryLog(
                system=system,
                sql=sql,
                timestamp=timestamp or datetime.utcnow()
            )
            db.session.add(log)
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            print(f"Error saving query log: {e}")
            return False
def load_query_logs(limit: int = 1000) -> List[Dict[str, Any]]:
    from main import db 
    with current_app.app_context():
        try:
            logs = db.session.query(QueryLog).order_by(QueryLog.timestamp.desc()).limit(limit).all()
            result = []
            for log in logs:
                result.append({
                    'id': log.id,
                    'system': log.system,
                    'sql': log.sql,
                    'timestamp': log.timestamp.isoformat() if log.timestamp else None
                })
            db.session.expunge_all()
            db.session.remove()
            return result
        except Exception as e:
            print(f"Error loading query logs: {e}")
            db.session.rollback()
            db.session.remove()
            return []
def save_integration_data(source_type: str, data: Dict[str, Any]) -> bool:
    from main import db 
    with current_app.app_context():
        try:
            integration = IntegrationData(
                source_type=source_type,
                data=data
            )
            db.session.add(integration)
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            print(f"Error saving integration data: {e}")
            return False
def load_integration_data(source_type: Optional[str] = None) -> List[Dict[str, Any]]:
    from main import db 
    with current_app.app_context():
        try:
            query = db.session.query(IntegrationData)
            if source_type:
                query = query.filter(IntegrationData.source_type == source_type)
            integrations = query.order_by(IntegrationData.received_at.desc()).all()
            result = []
            for integration in integrations:
                result.append({
                    'id': integration.id,
                    'source_type': integration.source_type,
                    'data': integration.data,
                    'received_at': integration.received_at.isoformat() if integration.received_at else None
                })
            db.session.expunge_all()
            db.session.remove()
            return result
        except Exception as e:
            print(f"Error loading integration data: {e}")
            db.session.rollback()
            db.session.remove()
            return []

def _require_role(role: str):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                abort(401, description="Authentication required.")
            if current_user.role.lower() != role.lower() and current_user.role.lower() != 'admin':
                abort(403, description=f"Role '{role}' is required for this action.")
            return f(*args, **kwargs)
        return decorated_function
    return decorator
