# options: dev, prod
ENV:=$(ENV)
HOST_UID:=$(HOST_UID)
HOST_GID:=$(HOST_GID)


.PHONY:create-dotenv
create-dotenv:
	touch .env
	echo -n "HOST_UID=`id -u`\nHOST_GID=`id -g`" > .env

# Export data for all states and diseases to Parquet files in the default output directory
.PHONY:fetch-data
fetch-data:
	python epi_scanner/downloader/export_data.py -s all -d dengue chikungunya

#  Run pytest for all tests in epi_scanner/tests inside the container
.PHONY:test-fetch-data
test-fetch-data:
	pytest -vv epi_scanner/tests

.PHONY: clean
clean: ## clean all artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	rm -fr .idea/
	rm -fr */.eggs
	rm -fr db
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -fr {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	find . -name '*.ipynb_checkpoints' -exec rm -rf {} +
	find . -name '*.pytest_cache' -exec rm -rf {} +
