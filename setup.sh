#!/bin/bash

# Ensure Python 3 and pip are installed
if ! command -v python3 &> /dev/null
then
    echo "Python 3 could not be found. Please install Python 3."
    exit
fi

if ! command -v pip3 &> /dev/null
then
    echo "pip3 could not be found. Please install pip3."
    exit
fi

# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
# pip install -r requirements.txt
