#!/bin/bash
# xochitl TLS close_notify fix
# Patches Qt's abort path to use clean close instead
# For xochitl 3.3.2.1666 (reMarkable 2)

set -e

XOCHITL="${1:-/usr/bin/xochitl}"
BACKUP="${XOCHITL}.backup.$(date +%Y%m%d)"

# Verify we're patching the right binary
if ! grep -q "Notifications socket" "$XOCHITL" 2>/dev/null; then
    echo "ERROR: This doesn't look like xochitl"
    exit 1
fi

# Check if already patched (NOP at the patch location)
CURRENT=$(xxd -s 0x1ea854 -l 4 "$XOCHITL" | awk '{print $2$3}')
if [ "$CURRENT" = "0000a0e1" ]; then
    echo "Already patched!"
    exit 0
fi

if [ "$CURRENT" != "6600001a" ]; then
    echo "ERROR: Unexpected bytes at patch location: $CURRENT"
    echo "Expected: 6600001a (BNE)"
    echo "This may be a different xochitl version"
    exit 1
fi

echo "Creating backup: $BACKUP"
cp "$XOCHITL" "$BACKUP"

echo "Patching BNE -> NOP at offset 0x1ea854..."
printf '\x00\x00\xa0\xe1' | dd of="$XOCHITL" bs=1 seek=$((0x1ea854)) conv=notrunc 2>/dev/null

# Verify patch
PATCHED=$(xxd -s 0x1ea854 -l 4 "$XOCHITL" | awk '{print $2$3}')
if [ "$PATCHED" = "0000a0e1" ]; then
    echo "SUCCESS: Patched!"
    echo "Restart xochitl with: systemctl restart xochitl"
else
    echo "ERROR: Verification failed!"
    echo "Restoring backup..."
    cp "$BACKUP" "$XOCHITL"
    exit 1
fi
