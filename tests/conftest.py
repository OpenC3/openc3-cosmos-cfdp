import os
import sys

# Make the lib directory importable and ensure authentication has a password.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
os.environ.setdefault("OPENC3_SCOPE", "DEFAULT")
# Leave OPENC3_API_PASSWORD unset so no authentication network call is attempted.
