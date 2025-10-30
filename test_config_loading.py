#!/usr/bin/env python3
# Test script to verify TOML configuration loading

import sys
import os

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server import load_rules_config

def test_config_loading():
    """Test that the TOML configuration is loaded correctly"""
    print("Testing TOML configuration loading...")

    # Load the configuration
    config = load_rules_config()

    # Check that the configuration has the expected structure
    assert "rules" in config, "Configuration should have 'rules' section"
    assert "select_star" in config["rules"], "Configuration should have 'select_star' rule"
    assert "partition_filter" in config["rules"], "Configuration should have 'partition_filter' rule"
    assert "table_alias" in config["rules"], "Configuration should have 'table_alias' rule"
    assert "sensitive_columns" in config["rules"], "Configuration should have 'sensitive_columns' rule"
    assert "field_alias_naming" in config["rules"], "Configuration should have 'field_alias_naming' rule"

    # Check specific rule configurations
    select_star_config = config["rules"]["select_star"]
    assert select_star_config["enabled"] == True, "select_star rule should be enabled"
    assert select_star_config["level"] == "error", "select_star rule should have 'error' level"
    assert "COUNT" in select_star_config["exclude_functions"], "select_star rule should exclude COUNT function"

    partition_filter_config = config["rules"]["partition_filter"]
    assert partition_filter_config["enabled"] == True, "partition_filter rule should be enabled"
    assert partition_filter_config["level"] == "error", "partition_filter rule should have 'error' level"
    assert "dt" in partition_filter_config["partition_fields"], "partition_filter rule should include 'dt' field"

    print("✅ TOML configuration loading test PASSED")
    print("Configuration loaded successfully:")
    for rule_name, rule_config in config["rules"].items():
        print(f"  - {rule_name}: {'enabled' if rule_config.get('enabled', False) else 'disabled'}")

if __name__ == "__main__":
    try:
        test_config_loading()
    except Exception as e:
        print(f"❌ Error during test: {e}")
        sys.exit(1)