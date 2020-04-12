# The binary to build (just the basename).
MODULE := db

# Where to push the docker image.
REGISTRY ?= docker.pkg.github.com/martinheinz/python-project-blueprint

IMAGE := $(REGISTRY)/$(MODULE)

# This version-strategy uses git tags to set the version string
TAG := $(shell git describe --tags --always --dirty)

BLUE='\033[0;34m'
NC='\033[0m' # No Color

#PYTHON='/usr/local/bin/python3'

run:
	@python -m $(MODULE)

init:
	pip install -r requirements.txt

test:
	nosetests tests

cdk-update:
	pip install --upgrade aws-cdk.core

lint:
	@echo "\n${BLUE}Running Pylint against source files...${NC}\n"
	@pylint --rcfile=setup.cfg db/*.py

	@echo "\n${BLUE}Running Bandit against source files...${NC}\n"
	@bandit -r --ini setup.cfg
	
	@echo "\n${BLUE}Running Pylint against test files...${NC}\n"
	@pylint --rcfile=setup.cfg tests/*.py
	
	@echo "\n${BLUE}Running Flake8 against source and test files...${NC}\n"
	@flake8

clean:
	rm -rf .pytest_cache .coverage .pytest_cache coverage.xml
