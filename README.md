# Calyprium CLI

Command-line tool for [Calyprium](https://calyprium.com) — autonomous web scraping from natural language.

## Install

```bash
pip install calyprium-cli
```

## Authentication

The CLI requires authentication. Choose one method:

### API Key (recommended for automation)

```bash
export CALYPRIUM_API_KEY=clp_your_key_here
calyprium spider list
```

### Browser Login (interactive)

```bash
calyprium login
```

This opens your browser for Keycloak SSO. Tokens are cached in `~/.calyprium/tokens.json`.

### Client Credentials (service accounts)

```bash
export KEYCLOAK_CLIENT_SECRET=your_secret
calyprium spider list
```

## Usage

```bash
# Scrape a website
calyprium scrape https://example.com

# Manage spiders
calyprium spider list
calyprium spider run my-spider

# Domain intelligence
calyprium intel analyze example.com

# Fetch a page with stealth browser
calyprium fetch https://example.com --stealth

# See all commands
calyprium --help
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `CALYPRIUM_API_KEY` | API key for authentication (preferred) |
| `CALYPRIUM_URL` | Agent API URL |
| `FORGE_URL` | Backend API URL |
| `MIMIC_URL` | Browser service URL |
| `PRISM_URL` | Domain analysis URL |
| `KEYCLOAK_URL` | Keycloak server URL |
| `KEYCLOAK_CLIENT_SECRET` | Client credentials auth |

## License

MIT
