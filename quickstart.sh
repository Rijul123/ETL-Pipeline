#!/bin/bash
# =====================================================
# Quick Start Script for ETL Data Warehouse Pipeline
# =====================================================

set -e  # Exit on error

echo "======================================================"
echo "ETL Data Warehouse Pipeline - Quick Start"
echo "======================================================"
echo ""

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -q -r requirements.txt

echo ""
echo "======================================================"
echo "SETUP OPTIONS"
echo "======================================================"
echo ""
echo "1. Full Setup (Create DB + Run Pipeline + Generate Reports)"
echo "2. Create Database Only"
echo "3. Run Pipeline Only"
echo "4. Generate Reports Only"
echo "5. Exit"
echo ""
read -p "Select option (1-5): " option

case $option in
    1)
        echo ""
        echo "Step 1: Creating database..."
        python warehouse/setup_database.py
        
        echo ""
        echo "Step 2: Running ETL pipeline..."
        python main_pipeline.py
        
        echo ""
        echo "Step 3: Generating reports..."
        python queries/analytics.py
        ;;
    2)
        python warehouse/setup_database.py
        ;;
    3)
        python main_pipeline.py
        ;;
    4)
        python queries/analytics.py
        ;;
    5)
        echo "Exiting..."
        exit 0
        ;;
    *)
        echo "Invalid option"
        exit 1
        ;;
esac

echo ""
echo "======================================================"
echo "Done!"
echo "======================================================"
