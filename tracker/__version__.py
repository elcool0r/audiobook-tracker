import os

def get_version():
    # First, check environment variable (set by Docker)
    env_version = os.getenv('VERSION')

    version_file = os.path.join(os.path.dirname(__file__), '..', 'VERSION')

    # If explicit env var provided
    if env_version:
        # Use explicit tag except when it's a special tag like 'dev' or 'latest'
        if env_version not in ('dev', 'latest'):
            return env_version.lstrip('v')

        # For 'dev' or 'latest', try to read base version from file and append suffix
        try:
            if os.path.isfile(version_file):
                with open(version_file, 'r') as f:
                    base = f.read().strip().lstrip('v')
                    return f"{base}-{env_version}"
        except Exception:
            pass
        # Fallback to returning the env tag
        return env_version

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