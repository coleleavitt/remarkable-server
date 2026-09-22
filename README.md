# remarkable-server

A local sync server for reMarkable tablets, implementing the sync v3 API.

## Features

- Full sync v3 API compatibility
- Hash-based content-addressable storage
- CRC32C checksum validation
- rm-filename header enforcement
- Token refresh mocking

## Usage

```bash
# Build
cargo build --release

# Run (default: localhost:8080)
./target/release/remarkable-server

# Custom options
./target/release/remarkable-server --bind 0.0.0.0:8080 --storage ./data
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/sync/v3/root` | GET | Current root hash |
| `/sync/v3/files/{hash}` | GET | Download file |
| `/sync/v3/files/{hash}` | PUT | Upload file |
| `/token/json/2/user/new` | POST | Token refresh |
| `/discovery/v1/endpoints` | GET | Service discovery |
| `/health` | GET | Health check |

## Device Configuration

To point your device at this server:

1. Generate SSL certificates (see tools/local-sync/ssl_certs.py)
2. Install CA cert on device
3. Redirect traffic via /etc/hosts or mitmproxy
4. Start server with generated certificates

See [remarkable-research](https://github.com/coleleavitt/remarkable-research) for device configuration tools.

## License

MIT
