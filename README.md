# AEGIS Runner

**Fault-tolerant, fair-share asynchronous job processing engine**

AEGIS Runner is a Go-based job orchestration prototype backed by PostgreSQL. It demonstrates durable job storage, idempotent submissions, tenant-aware scheduling, worker heartbeats, crash recovery, and a web dashboard.

## Features

- REST API for submitting and inspecting background jobs
- PostgreSQL-backed task persistence
- Idempotency keys to prevent duplicate job submissions
- Deficit Round-Robin (DRR) scheduling for tenant fairness
- Worker leases and heartbeat-based recovery
- Live dashboard and Server-Sent Events (SSE)
- Chaos-testing endpoints for resilience demonstrations
- Docker configuration and Terraform files for cloud deployment experiments

> This is a hackathon prototype. Review the implementation and security configuration before production use. Features described here are intended capabilities; not every production guarantee has been independently verified.

## Technology stack

- Go
- PostgreSQL
- HTML, CSS and JavaScript
- Docker and Docker Compose
- Terraform and Azure infrastructure configuration
- Python batch client

## Prerequisites

- Git
- Go version compatible with `go.mod`
- Docker Engine and Docker Compose plugin, if using the containerized database
- Python 3, if running the batch client

## Clone the repository

```bash
git clone https://github.com/Pirai7/aegis-runner.git
cd aegis-runner
```

## Run locally on Fedora or Linux

### 1. Start PostgreSQL

Inspect `docker-compose.yml` and confirm the database settings before starting it.

```bash
docker compose up -d
docker compose ps
```

### 2. Configure the database

Inspect `cmd/aegis/main.go` and `internal/db/db.go` to confirm how the application reads its database connection settings. Configure the required environment variables for your local PostgreSQL instance.

Do not use development credentials in a public or production deployment.

### 3. Build and test

```bash
go mod download
go test ./...
make build
```

### 4. Start the application

```bash
./aegis
```

If the application starts successfully, open:

http://localhost:8080/

Keep the terminal open while using the dashboard.

## Available Make targets

| Command | Purpose |
|---|---|
| `make build` | Build the Go application |
| `make run` | Build and run the application |
| `make test` | Run Go tests |
| `make test-race` | Run tests with Go's race detector |
| `make clean` | Remove generated binaries |
| `make demo` | Run the Python batch client |

Run these commands from the repository root.

## Batch client

The Python example client is located at `scripts/batch_client.py`.

Install its dependency if needed:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install requests
```

Start the application before running the client:

```bash
python3 scripts/batch_client.py
```

Confirm the API URL and tenant configuration in the script match your local setup.

## API overview

The documented API design includes:

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/api/v1/jobs` | Submit a job |
| `POST` | `/api/v1/jobs/batch` | Submit multiple jobs |
| `GET` | `/api/v1/jobs/{id}` | Inspect job status |
| `GET` | `/api/v1/stream/events` | Stream execution events |
| `POST` | `/api/v1/chaos/kill-worker` | Test worker recovery |
| `POST` | `/api/v1/chaos/flood` | Test queue fairness |

Check `internal/api/routes.go` for the actual registered routes and required request headers.

## Project structure

```text
.
├── cmd/aegis/       # Application entry point
├── internal/api/    # API handlers and routes
├── internal/chaos/  # Chaos testing
├── internal/db/     # Database access and schema
├── internal/idempotency/
├── internal/reaper/ # Stalled worker recovery
├── internal/scheduler/
├── internal/worker/
├── scripts/         # Python demo client
├── terraform/       # Azure infrastructure configuration
├── web/             # Dashboard assets
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── go.mod
└── go.sum
```

## Troubleshooting

- **Database connection errors:** Confirm PostgreSQL is running and the connection settings match.
- **Port 8080 is occupied:** Stop the conflicting process or use a supported application port configuration.
- **Go build errors:** Check your Go version and run `go mod download`.
- **Dashboard does not load:** Check the application terminal for startup errors and confirm the configured port.
- **Batch client errors:** Confirm the API is running and the URL, tenant ID and required headers are correct.

## Contributing

1. Fork the repository.
2. Create a feature branch.
3. Make a focused change.
4. Run `go test ./...`.
5. Open a pull request describing your change.

## License

No license has been selected yet. Until a license is added, do not assume others have permission to redistribute or reuse this code.
