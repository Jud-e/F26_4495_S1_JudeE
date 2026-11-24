"""Print the bearer token for one device:   python provision_token.py infusion-pump-001   (reads GATEWAY_API_TOKEN)."""
import sys

from auth import device_token

if len(sys.argv) != 2:
    sys.exit("usage: python provision_token.py <device-id>")
print(device_token(sys.argv[1]))