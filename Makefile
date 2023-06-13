.PHONY: build install test

VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
PYTHON=python3

default: build install

build:
	pip install -r requirements.txt 
	$(PYTHON) -m build --wheel

install:
	pip install ./dist/*.whl --force-reinstall

test:
	$(PYTHON) -m unittest discover 

release:
	pip install -r requirements.txt
	$(PYTHON) -m build --wheel -o ./build/	

docker:
	docker build -t "python-featurebase:$(VERSION)" .
	@echo "docker build complete python-featurebase:$(VERSION)"

docker-test:
	make docker
	docker-compose up --abort-on-container-exit

docker-release:
	make docker
	docker create --name python-featurebase-build python-featurebase:$(VERSION)
	docker cp python-featurebase-build:/python-featurebase/dist/ ./build/
	docker rm python-featurebase-build

