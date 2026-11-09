PY ?= .venv/bin/python
DBT ?= .venv/bin/dbt

.PHONY: setup data sample init run rerun daily report test coverage dbt dashboard reset all

setup:            ## create the virtualenv and install everything
	python3 -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt -r requirements-analytics.txt

data:             ## generate the full synthetic dataset (40 stores x 60 days, ~2.1M rows)
	$(PY) generator/generate_data.py

sample:           ## small dataset for quick experiments (written to data_sample/)
	$(PY) generator/generate_data.py --stores 8 --days 14 --out data_sample

init:             ## create warehouse tables and load dimensions
	$(PY) -m etl init-db

run:              ## process all new landing files
	$(PY) -m etl run

rerun:            ## full rerun from scratch, timed
	$(PY) -m etl reset --yes
	time $(PY) -m etl run --quiet

daily:            ## simulate one run per arrival folder (incremental)
	$(PY) -m etl reset --yes
	for d in $$(ls data/landing); do $(PY) -m etl run --until $$d --quiet --no-report || exit 1; done
	$(PY) -m etl report

report:           ## rebuild reports/ from the warehouse
	$(PY) -m etl report

test:             ## unit + end-to-end tests
	$(PY) -m pytest -v

coverage:
	$(PY) -m pytest --cov=etl --cov-report=term-missing

dbt:              ## build + test the analytics models
	$(DBT) build --project-dir analytics --profiles-dir analytics

dashboard:
	.venv/bin/streamlit run dashboard/app.py

reset:
	$(PY) -m etl reset --yes

all: data rerun dbt test
