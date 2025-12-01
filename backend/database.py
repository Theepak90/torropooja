from sqlalchemy import create_engine, Column, String, Integer, DateTime, Boolean, Text, JSON, text, LargeBinary, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import os
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import UserMixin
from config import Config 
DATABASE_URL = Config.SQLALCHEMY_DATABASE_URI
Base = declarative_base()
class Connector(Base):
    __tablename__ = "connectors"
    id = Column(String(255), primary_key=True)
    name = Column(String(255), nullable=False)
    type = Column(String(100), nullable=False)
    status = Column(String(50), default="active")
    enabled = Column(Boolean, default=True)
    last_run = Column(DateTime, nullable=True)
    config = Column(JSON, nullable=True)
    assets_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
class Asset(Base):
    __tablename__ = "assets"
    id = Column(String(255), primary_key=True)
    name = Column(String(255), nullable=False)
    type = Column(String(100), nullable=False)
    catalog = Column(String(255), nullable=True)
    schema_name = Column(String(255), nullable=True)
    connector_id = Column(String(255), nullable=True)
    discovered_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(50), default="active")
    sort_order = Column(Integer, nullable=True)
    extra_data = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
class PublishedTag(Base):
    __tablename__ = "published_tags"
    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_id = Column(String(255), nullable=False)
    tag_name = Column(String(255), nullable=False)
    tag_type = Column(String(50), nullable=True)  
    target_column = Column(String(255), nullable=True)
    published_at = Column(DateTime, default=datetime.utcnow)
    connector_type = Column(String(100), nullable=True)
    extra_data = Column(JSON, nullable=True)
class LineageRelation(Base):
    __tablename__ = "lineage_relations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(String(255), nullable=False)
    target_id = Column(String(255), nullable=False)
    relation_type = Column(String(100), default="derives_from")
    extra_data = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
class LineageSnapshot(Base):
    __tablename__ = "lineage_snapshots"
    id = Column(Integer, primary_key=True, autoincrement=True)
    data = Column(JSON, nullable=False)
    signature = Column(String(255), nullable=True)
    signature_alg = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
class CurationProposal(Base):
    __tablename__ = "curation_proposals"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(255), nullable=False)
    target = Column(String(255), nullable=False)
    relationship = Column(String(100), nullable=False)
    column_lineage = Column(JSON, nullable=True)
    notes = Column(Text, nullable=True)
    status = Column(String(50), default="proposed")
    proposed_at = Column(DateTime, default=datetime.utcnow)
    approved_at = Column(DateTime, nullable=True)
class QueryLog(Base):
    __tablename__ = "query_logs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    system = Column(String(100), nullable=False)
    sql = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
class IntegrationData(Base):
    __tablename__ = "integration_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_type = Column(String(50), nullable=False)  
    data = Column(JSON, nullable=False)
    received_at = Column(DateTime, default=datetime.utcnow)
class PendingAsset(Base):
    __tablename__ = "pending_assets"
    id = Column(String(255), primary_key=True)
    name = Column(String(255), nullable=False)
    type = Column(String(100), nullable=False)
    catalog = Column(String(255), nullable=True)
    connector_id = Column(String(255), nullable=False)
    change_type = Column(String(50), nullable=False)
    s3_event_type = Column(String(100), nullable=False)
    asset_id = Column(String(255), nullable=False)
    asset_data = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(50), default='pending')
    processed_at = Column(DateTime, nullable=True)

class HiveDB(Base):
    __tablename__ = "hive_dbs"
    db_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), unique=True, nullable=False, index=True)
    db_location_uri = Column(String(4000))
    description = Column(String(4000))
    owner_name = Column(String(128))
    owner_type = Column(String(10))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class HiveStorageDescriptor(Base):
    __tablename__ = "hive_sds"
    sd_id = Column(Integer, primary_key=True, autoincrement=True)
    cd_id = Column(Integer)
    input_format = Column(String(4000))
    output_format = Column(String(4000))
    location = Column(String(4000))
    is_compressed = Column(Boolean, default=False)
    is_stored_as_sub_directories = Column(Boolean, default=False)
    serde_lib = Column(String(4000))
    serde_name = Column(String(128))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class HiveTable(Base):
    __tablename__ = "hive_tables"
    tbl_id = Column(Integer, primary_key=True, autoincrement=True)
    db_id = Column(Integer, ForeignKey('hive_dbs.db_id'), nullable=False, index=True)
    tbl_name = Column(String(256), nullable=False, index=True)
    tbl_type = Column(String(128))
    sd_id = Column(Integer, ForeignKey('hive_sds.sd_id'), nullable=False, index=True)
    owner = Column(String(767))
    create_time = Column(Integer)
    last_access_time = Column(Integer)
    retention = Column(Integer)
    asset_id = Column(String(255), ForeignKey('assets.id'), unique=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class HiveColumn(Base):
    __tablename__ = "hive_columns"
    cd_id = Column(Integer, primary_key=True, autoincrement=True)
    comment = Column(String(4000))
    column_name = Column(String(767), nullable=False, index=True)
    type_name = Column(String(4000), nullable=False)
    integer_idx = Column(Integer, nullable=False)
    sd_id = Column(Integer, ForeignKey('hive_sds.sd_id'), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class User(UserMixin, Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    email = Column(String(120), unique=True, nullable=False, index=True)
    password_hash = Column(LargeBinary) 
    role = Column(String(50), default='user') 
    def set_password(self, password):
        self.password_hash = generate_password_hash(password).encode('utf-8')
    def check_password(self, password):
        return check_password_hash(self.password_hash.decode('utf-8'), password)
    def get_id(self):
        return str(self.id)
    def __repr__(self):
        return f'<User {self.username}>'
def get_db():
    from flask import current_app
    return current_app.extensions['sqlalchemy'].db.session
def init_db():
    try:
        from flask import current_app
        from main import db
        
        with current_app.app_context():
            db.create_all()
            print(" Database tables created successfully (including Hive Metastore tables)")
        return True
    except RuntimeError:
        print(" Database initialization deferred (will create tables on app startup)")
        return True
    except Exception as e:
        print(f" Error initializing database: {e}")
        import traceback
        traceback.print_exc()
        return False
