# stations-pipelines

## Table of contents
* [General info](#general-info)
* [Technologies](#technologies)
* [Setup](#setup)
* [Testing](#testing)

## General info
This project is a refactoring project of da-charging.
	
## Technologies
Project is created with:
* Python 3.9
* Alembic
* Sqlalchemy 
* pandas
* etc...

# Coding Style & Formatting
Please take advantage of the following tooling:
```bash
pip install isort autoflake black
```

Black reformats the code, isort orders the imports and flake8 checks for remaining issues.
Example usage:
```bash
isort -rc -sl .
autoflake --remove-all-unused-imports -i -r --exclude alembic .
isort -rc -m 3 .
```
	
## Setup

### Install Pyenv if not already existing

Linux:

    curl https://pyenv.run | bash

Mac:

    brew install pyenv

Install and activate Python 3.9.x , e.g.

    pyenv install 3.9.10
    pyenv local 3.9.10

Check local version

    python --version

### Create enviroment and activate:
```
python -m venv venv
source venv/bin/activate  
```

and install the requirements file:
```
pip install -r requirements.txt  
```

### Set environment variables

Add

    DB_NAME=gis
    DB_HOST=localhost
    DB_PORT=54322
    DB_USER=docker
    DB_PASSWORD=docker

to `.env` file in the root directory

### Start docker container
```bash
docker compose up  
```

### Initialize DB
```bash
alembic upgrade head
```


## Alembic
If you change the data model, you can create new revision by
```bash
alembic revision --autogenerate -m "your revision comment"
```

## Testing

### Execute tests
Before you run the tests you need to install test dependencies by 
```bash
pip install -r test/requirements.txt
```
You can run all tests under `/test` by running the following command:
```bash
python -m unittest discover test
```

### Testdata import
For running the script `testdata_import.py` you need to be added as user to the API in Google Cloud 
and the Spreadsheet needs to be shared with the same user.
