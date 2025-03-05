VERSION := $(shell git describe --tags --abbrev=0 | sed 's/^v//')

version:
	echo "Version: $(VERSION)"

build:
	python3 -m pip install --upgrade build
	python3 -m build

install: build
	python3 -m pip install --upgrade dist/timeplus_connect-$(VERSION).tar.gz

lint:
	pylint --rcfile=./pylintrc  timeplus_connect