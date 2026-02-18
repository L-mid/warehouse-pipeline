# warehouse-pipeline

## Dependencies
- Docker + Docker Compose
- Python 3.11+


## Installation: 
```bash
python -m venv .venv
# Windows Powershell:
.venv\Scripts\Activate.ps1
# macOS/Linux
# source .venv/bin/activate

python -m pip install -U pip
pip install -e ".[dev]"

# run tests
make demo
make test
```


## Commands: 
Availible:
```bash
make demo

# Inspect with psql:
docker compose exec db psql -U postgres -d warehouse
\dt


# clean reset of db:
make down
make demo
```


