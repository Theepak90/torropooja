#!/usr/bin/env python3
"""
Test script to verify Azure Blob Storage rediscovery timer logic
"""
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_helpers import load_connectors

def test_rediscovery_logic():
    """Test the rediscovery interval logic"""
    print("=" * 60)
    print("Testing Azure Blob Storage Rediscovery Timer Logic")
    print("=" * 60)
    
    connectors = load_connectors()
    azure_connectors = [c for c in connectors if c.get('type', '').lower() == 'azure blob storage']
    
    if not azure_connectors:
        print("❌ No Azure Blob Storage connectors found in database")
        print("   Please create a connector first via the UI")
        return False
    
    print(f"\nFound {len(azure_connectors)} Azure Blob Storage connector(s):\n")
    
    for connector in azure_connectors:
        name = connector.get('name', 'Unknown')
        enabled = connector.get('enabled', False)
        last_run = connector.get('last_run')
        config = connector.get('config', {})
        rediscovery_interval = config.get('rediscovery_interval_minutes', 5)
        
        print(f"Connector: {name}")
        print(f"  Enabled: {enabled}")
        print(f"  Rediscovery Interval: {rediscovery_interval} minutes")
        print(f"  Last Run: {last_run}")
        
        if not enabled:
            print(f"  ⚠️  Status: DISABLED - will not be checked by scheduler")
            print()
            continue
        
        if not last_run:
            print(f"  ✅ Status: Ready for initial rediscovery (no last_run timestamp)")
            print()
            continue
        
        try:
            # Parse last_run timestamp
            last_run_dt = datetime.fromisoformat(last_run.replace('Z', '+00:00'))
            now = datetime.now()
            time_since_last_run = (now - last_run_dt.replace(tzinfo=None)).total_seconds() / 60
            
            print(f"  Time since last run: {time_since_last_run:.2f} minutes")
            
            if time_since_last_run < rediscovery_interval:
                minutes_remaining = rediscovery_interval - time_since_last_run
                print(f"  ⏸️  Status: WAITING - {minutes_remaining:.2f} minutes until next rediscovery")
            else:
                print(f"  ✅ Status: READY - Interval met! Should rediscover now")
                print(f"     (Time since last run: {time_since_last_run:.2f} >= Interval: {rediscovery_interval} min)")
            
        except Exception as e:
            print(f"  ❌ Error parsing last_run: {e}")
        
        print()
    
    print("=" * 60)
    print("Scheduler Status Check:")
    print("=" * 60)
    print("The scheduler runs every 1 second and checks all enabled connectors.")
    print("If a connector's time interval has passed, it will trigger rediscovery.")
    print("\nTo see real-time logs, check the backend terminal output.")
    
    return True

if __name__ == "__main__":
    test_rediscovery_logic()

