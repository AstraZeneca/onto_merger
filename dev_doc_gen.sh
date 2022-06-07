#!/bin/bash
clear
echo generating Sphinx docs
rm -rf build
cd docs
make html
cd ..
open -a "Google Chrome" build/html/index.html