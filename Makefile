all: ps_scan

VER := $(shell grep -E "^__version__" ps_scan.py | sed -E -e 's/[^"]+//' -e 's/"//g')
OUTFILE = ps_scan-${VER}.zip

ps_scan: clean
	mkdir -p build
	mkdir -p releases
	zip -r build/${OUTFILE} LICENSE README.md example_es_credentials.*
	zip -r build/${OUTFILE} ps_scan.py ps_cmd.py helpers/* libs/*.py libs/hydra/* visualizations/*
	cp build/${OUTFILE} releases

.PHONY: clean
clean: clean_compiled
	-rm -rf build
	-rm -rf releases
	-rm -rf *.egg-info
	-rm -f tests/.coverage

.PHONY: clean_compiled
clean_compiled:
	find . -name __pycache__ -type d -exec rm -rf {} +
	find . -name *.pyc -delete

unittest:
	python -m unittest discover tests
