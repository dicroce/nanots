.PHONY: all clean

# Default target - build wheel
all:
	python setup.py bdist_wheel

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -f nanots*.so
	rm -f nanots.c
	rm -f nanots.cpp
	rm -rf amalgamated_src
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete
