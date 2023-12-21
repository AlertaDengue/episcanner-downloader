# Episcanner Downloader

Episcanner Downloader is a data downloader for the Episcanner application. It retrieves data related to diseases like dengue, zika and chikungunya and saves it in a specified directory with the formats csv, parquet or duckdb.

## Features

- Fetches data related to diseases from the Episcanner application
- Supports downloading data for specific diseases
- Saves downloaded data to a designated directory

## Installation

To install Episcanner Downloader, follow these steps:

1. Clone the repository:
```shell
   git clone https://github.com/AlertaDengue/episcanner-downloader.git
``` 
2. Navigate to the cloned directory:
```shell
   cd episcanner-downloader
``` 
### Using Conda

1. Create a Conda environment using the provided YAML file:

```shell
   conda env create -f conda/env-base.yaml
``` 
2. conda activate episcanner-downloader
```shell
   conda activate episcanner-downloader
``` 
3. Install the dependencies using Poetry:
```shell
   poetry install
``` 
### Using a Virtual Environment (venv)
1. Create a virtual environment:
```shell
   python -m venv env
```
2. Activate the virtual environment:
```shell
   source env/bin/activate
```
3. Install the dependencies using Poetry:
```shell
   poetry install
``` 
## Setting Environment Variables
Before running Episcanner Downloader, make sure to set the required environment variables for connecting to the PSQL database. You can use the provided Makefile to create a .env file with the exported variables:
1. Set the required environment variables for connecting to the PSQL database:
```shell
   export EPISCANNER_PSQL_URI="postgresql://user:password@host:port/database"
```

2. Create a .env file in the project root directory with the exported variables.
```shell
   make dotenv
```
## Usage
To use Episcanner Downloader, follow these steps:

1. Execute the binary (examples for Linux):
```shell
   cd dist/
   ./episcanner -h
   ./episcanner -y 2023 -s RJ -d dengue -o /tmp -f duckdb
   ./episcanner -y 2021 2022 2023 --all -d zika -f csv parquet --verbose
``` 
*Replace <source> with the desired source (e.g., 'MG', or '--all' to download all states) and <diseases> with the specific diseases you want to download (e.g., 'dengue chikungunya'). Specify the <output_directory> where the data should be saved.*

## License
Episcanner Downloader is licensed under the [MIT License](https://github.com/AlertaDengue/episcanner-downloader/blob/main/LICENSE). See the LICENSE file for more details.
