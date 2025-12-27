import subprocess
import os

def get_version():
    try:
        # Try to get version from git tag
        result = subprocess.run(['git', 'describe', '--tags', '--abbrev=0'], capture_output=True, text=True, cwd=os.path.dirname(__file__))
        if result.returncode == 0:
            return result.stdout.strip()
    except:
        pass
    
    # Fallback to static version
    return "1.0.0"

__version__ = get_version()