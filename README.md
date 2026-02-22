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

# initalize the DB:
pipeline db init        # restarts the DB from scratch every time

# run tests
make demo
```


## Commands: 
Availible:
```bash
# start 
make demo


# Inspect the DB with psql anytime:
docker compose exec db psql -U postgres -d warehouse
\dt


# clean reset of DB:
make down           
pipeline db init    
make demo


# run tests only
make test
```


