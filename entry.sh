#!/bin/bash

# Exit immediately if a command fails
set -e

# Navigate to the src directory
cd src

# Execute commands in order
sh period0.sh
uv run period0.py
uv run period2.py
uv run period3.py

echo "All commands executed successfully!"

