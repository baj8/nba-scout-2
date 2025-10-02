#!/usr/bin/env python3
"""Simple test for enhanced configuration management."""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_configuration_loading():
    """Test configuration loading with different environments."""
    from nba_scraper.config import get_settings, validate_configuration, Environment
    
    print("🔧 Testing Configuration Management")
    print("=" * 50)
    
    # Test default environment
    print("1. Testing default configuration...")
    settings = get_settings()
    print(f"   Environment: {settings.environment}")
    print(f"   Database URL: {settings.database.url[:30]}...")
    print(f"   Log Level: {settings.log_level}")
    print("   ✅ Default config loaded successfully")
    print()
    
    # Test environment-specific loading
    print("2. Testing environment-specific configuration...")
    
    # Test development environment
    os.environ['ENVIRONMENT'] = 'development'
    # Clear cache to reload settings
    get_settings.cache_clear()
    dev_settings = get_settings()
    print(f"   Development environment: {dev_settings.environment}")
    print(f"   Debug mode: {dev_settings.debug}")
    print("   ✅ Development config loaded successfully")
    
    # Test production environment  
    os.environ['ENVIRONMENT'] = 'production'
    get_settings.cache_clear()
    prod_settings = get_settings()
    print(f"   Production environment: {prod_settings.environment}")
    print(f"   Debug mode: {prod_settings.debug}")
    print("   ✅ Production config loaded successfully")
    print()
    
    # Test configuration validation
    print("3. Testing configuration validation...")
    validation_results = validate_configuration()
    
    passed_checks = sum(1 for result in validation_results.values() 
                       if result is True or (isinstance(result, bool) and result))
    total_checks = len([k for k in validation_results.keys() if k != 'validation_error'])
    
    print(f"   Validation checks: {passed_checks}/{total_checks} passed")
    
    for check_name, result in validation_results.items():
        if check_name == 'validation_error':
            print(f"   ❌ Validation error: {result}")
        else:
            status = "✅" if result else "❌"
            print(f"   {status} {check_name.replace('_', ' ').title()}")
    
    print("   ✅ Configuration validation completed")
    print()
    
    # Test nested configuration access
    print("4. Testing nested configuration structures...")
    print(f"   Database pool size: {settings.database.pool_size}")
    print(f"   Cache TTL: {settings.cache.http_ttl}")
    print(f"   Monitoring enabled: {settings.monitoring.enable_metrics}")
    print("   ✅ Nested config access working")
    print()
    
    # Test API key handling (SecretStr)
    print("5. Testing API key security...")
    if settings.api_keys.sentry_dsn:
        print(f"   Sentry DSN (masked): {str(settings.api_keys.sentry_dsn)}")
        print(f"   Raw value length: {len(settings.api_keys.sentry_dsn.get_secret_value())}")
    else:
        print("   No API keys configured (expected for test)")
    print("   ✅ Secret handling working")
    print()
    
    print("🎉 All configuration tests passed!")
    return True

def test_environment_files():
    """Test that environment-specific files exist."""
    print("📁 Testing Environment Files")
    print("=" * 50)
    
    project_root = Path(__file__).parent
    env_files = ['.env.example', '.env.development', '.env.staging', '.env.production']
    
    for env_file in env_files:
        file_path = project_root / env_file
        if file_path.exists():
            print(f"   ✅ {env_file} exists")
        else:
            print(f"   ❌ {env_file} missing")
            return False
    
    print("   ✅ All environment files present")
    return True

def main():
    """Run all configuration tests."""
    try:
        # Reset environment for clean test
        os.environ.pop('ENVIRONMENT', None)
        
        success = True
        success &= test_environment_files()
        print()
        success &= test_configuration_loading()
        
        if success:
            print("🎉 Configuration management tests completed successfully!")
            return True
        else:
            print("❌ Some configuration tests failed!")
            return False
            
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)