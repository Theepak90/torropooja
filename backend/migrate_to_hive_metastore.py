import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask
from config import Config
from database import init_db
from hive_metastore_sync import migrate_all_assets_to_hive

def main():
    print("=" * 80)
    print("Hive Metastore Migration Script")
    print("=" * 80)
    print("This script will sync all existing assets to Hive Metastore format")
    print("for Starburst Enterprise compatibility.")
    print("=" * 80)
    
    app = Flask(__name__)
    app.config.from_object(Config)
    
    with app.app_context():
        from main import db
        db.init_app(app)
        
        print("\n Creating database tables (including Hive Metastore tables)...")
        db.create_all()
        print(" Database tables created")
        
        print("\n Starting migration...")
        result = migrate_all_assets_to_hive()
        
        print("\n" + "=" * 80)
        print("Migration Summary")
        print("=" * 80)
        print(f"Total assets: {result['total']}")
        print(f" Synced to Hive Metastore: {result['synced']}")
        print(f" Failed: {result['failed']}")
        print(f"  Skipped (non-table assets): {result['skipped']}")
        print("=" * 80)
        
        if result['synced'] > 0:
            print("\n Migration completed successfully!")
            print("Your assets are now available in Hive Metastore format.")
            print("Starburst Enterprise can now query these assets.")
        else:
            print("\n  No assets were synced. Check if you have any Table or Data File assets.")

if __name__ == '__main__':
    main()

