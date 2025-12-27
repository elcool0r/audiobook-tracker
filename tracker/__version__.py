import os

def get_version():
    # First, try environment variable (set by Docker)
    version = os.getenv('VERSION')
    if version and version not in ['dev', 'latest']:
        version = version.lstrip('v')  # Remove 'v' prefix if present
        return version
    
    # Try to read from VERSION file
    try:
        version_file = os.path.join(os.path.dirname(__file__), '..', 'VERSION')
        if os.path.isfile(version_file):
            with open(version_file, 'r') as f:
                version = f.read().strip()
                version = version.lstrip('v')  # Remove 'v' prefix if present
                return version
    except Exception:
        pass
    
    # Fallback to static version
    return "1.0.0"

__version__ = get_version()