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


# Get the absolute path to the directory containing this script
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Export the PYTHONPATH dynamically based on the project directory
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

echo "PYTHONPATH set to: $PYTHONPATH"

# Install dependencies
pip3 install -r requirements.txt
