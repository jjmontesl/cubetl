# CubETL Makefile

CLOC=cloc

BASE_DIR=.
BUILD_DIR=$(BASE_DIR)/build

#VERSION=$(shell cat VERSION.txt)
VERSION=$(shell grep '^APP_VERSION = ' $(BASE_DIR)/cubetl/__init__.py | sed -E 's/^APP_VERSION = \"(.*)\"/\1/' )


.PHONY: doc
doc:
	. $(VIRTUALENV_ACTIVATE) && $(MAKE) -C doc html
	
doc-pdf:	
	. $(VIRTUALENV_ACTIVATE) && $(MAKE) -C doc latexpdf

doc-package:	
	cd $(BUILD_DIR)/doc-public && tar cvzf ../cubetl-doc-$(VERSION).tar.gz --transform 's/^html/cubetl-doc/' html
	cp $(BUILD_DIR)/doc-public/latex/cubetl-doc.pdf $(BUILD_DIR)/cubetl-doc-$(VERSION).pdf

.PHONY: loc
loc:
	@echo LOC: $(VERSION)
	cloc cubetl/ library/ examples/ tests/ --exclude-dir=examples/sdmx/data

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)
	rm -rf cubetl.egg-info
	$(MAKE) -C doc clean

.PHONY: test
test:
	@echo Testing: $(VERSION)
	. $(VIRTUALENV_ACTIVATE) && pytest --cov=cubetl tests

.PHONY: build
build:
	echo Version: $(VERSION)
	mkdir -p $(BUILD_DIR)/cubetl
	rsync -av \
		  --exclude='*.sqlite3' --exclude='*.log' \
		  --exclude='*.pyc' \
		  bin cubetl examples library README.md CHANGES.txt Makefile requirements.txt \
		  $(BUILD_DIR)/cubetl/
	echo $(VERSION) > $(BUILD_DIR)/cubetl/VERSION.txt

.PHONY: dist
dist: build
	cd $(BUILD_DIR) && tar cvzf cubetl-$(VERSION).tar.gz cubetl

.PHONY: virtualenv
virtualenv:
	#sudo apt-get --yes --force-yes install python-virtualenv
	[ ! -d env ] && virtualenv env || true
	#. $(VIRTUALENV_ACTIVATE) && pip install --upgrade setuptools
	. $(VIRTUALENV_ACTIVATE) && pip install -r requirements.txt --upgrade

.PHONY: all
all: doc dist 

#release: clean
#	python setup.py sdist upload

#sdist: clean
#	python setup.py sdist
#	ls -l dist
    


