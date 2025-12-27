import subprocess
import os

def get_version():
    # First, try environment variable (set by Docker)
    version = os.getenv('VERSION')
    if version and version != 'dev':
        version = version.lstrip('v')  # Remove 'v' prefix if present
        return version
    
    # Try to get version from git tag
    try:
        git_cwd = '/app' if os.path.isdir('/app') else os.getcwd()
        result = subprocess.run(['git', 'describe', '--tags', '--abbrev=0'],
                              capture_output=True, text=True,
                              cwd=git_cwd)
        if result.returncode == 0:
            version = result.stdout.strip()
            version = version.lstrip('v')  # Remove 'v' prefix if present
            return version
    except Exception:
        # If git is unavailable or the command fails, fall back to the static version below.
        pass
    
    # Fallback to static version
    return "1.0.0"

__version__ = get_version()