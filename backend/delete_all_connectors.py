#!/usr/bin/env python3
"""
Script to delete all connectors from the database
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import Connector, Asset, PendingAsset, HiveTable, HiveColumn, HiveStorageDescriptor, HiveDB
from sqlalchemy.orm import sessionmaker
from config import Config
from sqlalchemy import create_engine

def delete_all_connectors():
    """Delete all connectors and their associated assets from the database"""
    engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Count connectors before deletion
        connector_count = session.query(Connector).count()
        print(f"Found {connector_count} connector(s) in database")
        
        if connector_count == 0:
            print("No connectors to delete.")
            return
        
        # Get all connector IDs
        connectors = session.query(Connector).all()
        connector_ids = [conn.id for conn in connectors]
        
        # Get all asset IDs for these connectors
        assets = session.query(Asset).filter(Asset.connector_id.in_(connector_ids)).all()
        asset_ids = [asset.id for asset in assets]
        
        # Delete in correct order to handle foreign key constraints
        # 1. Delete HiveColumns (references HiveStorageDescriptor)
        hive_columns_deleted = 0
        if asset_ids:
            hive_tables = session.query(HiveTable).filter(HiveTable.asset_id.in_(asset_ids)).all()
            hive_sd_ids = [ht.sd_id for ht in hive_tables if ht.sd_id]
            if hive_sd_ids:
                hive_columns_deleted = session.query(HiveColumn).filter(HiveColumn.sd_id.in_(hive_sd_ids)).delete()
        
        # 2. Delete HiveTables (references assets)
        hive_tables_deleted = 0
        if asset_ids:
            hive_tables_deleted = session.query(HiveTable).filter(HiveTable.asset_id.in_(asset_ids)).delete()
        
        # 3. Delete HiveStorageDescriptors
        hive_sds_deleted = 0
        if asset_ids:
            hive_tables = session.query(HiveTable).filter(HiveTable.asset_id.in_(asset_ids)).all()
            hive_sd_ids = [ht.sd_id for ht in hive_tables if ht.sd_id]
            if hive_sd_ids:
                hive_sds_deleted = session.query(HiveStorageDescriptor).filter(HiveStorageDescriptor.sd_id.in_(hive_sd_ids)).delete()
        
        # 4. Delete assets associated with these connectors
        assets_deleted = 0
        if connector_ids:
            assets_deleted = session.query(Asset).filter(Asset.connector_id.in_(connector_ids)).delete()
        
        # 5. Delete pending assets
        pending_deleted = 0
        if connector_ids:
            pending_deleted = session.query(PendingAsset).filter(PendingAsset.connector_id.in_(connector_ids)).delete()
        
        # 6. Delete all connectors
        connectors_deleted = session.query(Connector).delete()
        
        # Commit all deletions
        session.commit()
        
        print(f"✅ Successfully deleted:")
        print(f"   - {connectors_deleted} connector(s)")
        print(f"   - {assets_deleted} asset(s)")
        print(f"   - {pending_deleted} pending asset(s)")
        print(f"   - {hive_tables_deleted} hive table(s)")
        print(f"   - {hive_columns_deleted} hive column(s)")
        print(f"   - {hive_sds_deleted} hive storage descriptor(s)")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error deleting connectors: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        session.close()
    
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("Deleting ALL connectors from database...")
    print("=" * 60)
    
    # Skip confirmation if --yes flag is provided
    if len(sys.argv) > 1 and sys.argv[1] == '--yes':
        print("Skipping confirmation (--yes flag provided)")
    else:
        confirm = input("Are you sure you want to delete ALL connectors? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Cancelled.")
            sys.exit(0)
    
    success = delete_all_connectors()
    
    if success:
        print("\n" + "=" * 60)
        print("✅ All connectors deleted successfully!")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("❌ Failed to delete connectors")
        print("=" * 60)
        sys.exit(1)

