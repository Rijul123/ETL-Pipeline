#!/bin/bash
# =====================================================
# Cron Job Setup Script for ETL Pipeline
# =====================================================
# This script sets up a cron job to run the ETL pipeline
# daily at 2:00 AM

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Log file
LOG_FILE="$PROJECT_DIR/logs/cron_pipeline.log"

# Python executable (adjust if needed)
PYTHON_EXEC="$(which python3)"

# Pipeline script
PIPELINE_SCRIPT="$PROJECT_DIR/main_pipeline.py"

# Create cron job entry
# Format: minute hour day month day_of_week command
# Runs daily at 2:00 AM
CRON_JOB="0 2 * * * cd $PROJECT_DIR && $PYTHON_EXEC $PIPELINE_SCRIPT >> $LOG_FILE 2>&1"

echo "======================================================"
echo "ETL Pipeline Cron Job Setup"
echo "======================================================"
echo ""
echo "Project Directory: $PROJECT_DIR"
echo "Pipeline Script: $PIPELINE_SCRIPT"
echo "Log File: $LOG_FILE"
echo "Schedule: Daily at 2:00 AM"
echo ""
echo "Cron job command:"
echo "$CRON_JOB"
echo ""

# Function to add cron job
add_cron_job() {
    # Check if cron job already exists
    if crontab -l 2>/dev/null | grep -q "$PIPELINE_SCRIPT"; then
        echo "⚠️  Cron job already exists for this pipeline"
        echo "Current cron jobs:"
        crontab -l | grep "$PIPELINE_SCRIPT"
        read -p "Do you want to replace it? (y/n): " replace
        if [[ $replace == "y" || $replace == "Y" ]]; then
            # Remove existing job
            crontab -l 2>/dev/null | grep -v "$PIPELINE_SCRIPT" | crontab -
        else
            echo "Setup cancelled"
            exit 0
        fi
    fi
    
    # Add new cron job
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
    
    echo "✅ Cron job added successfully!"
    echo ""
    echo "Current cron jobs:"
    crontab -l | grep "$PIPELINE_SCRIPT"
}

# Function to remove cron job
remove_cron_job() {
    if crontab -l 2>/dev/null | grep -q "$PIPELINE_SCRIPT"; then
        crontab -l 2>/dev/null | grep -v "$PIPELINE_SCRIPT" | crontab -
        echo "✅ Cron job removed successfully!"
    else
        echo "⚠️  No cron job found for this pipeline"
    fi
}

# Function to list cron jobs
list_cron_jobs() {
    echo "Current cron jobs for ETL pipeline:"
    echo "------------------------------------"
    crontab -l 2>/dev/null | grep "$PIPELINE_SCRIPT" || echo "No jobs found"
}

# Main menu
case "${1:-}" in
    add)
        add_cron_job
        ;;
    remove)
        remove_cron_job
        ;;
    list)
        list_cron_jobs
        ;;
    *)
        echo "Usage: $0 {add|remove|list}"
        echo ""
        echo "Commands:"
        echo "  add     - Add the ETL pipeline cron job"
        echo "  remove  - Remove the ETL pipeline cron job"
        echo "  list    - List current ETL pipeline cron jobs"
        echo ""
        echo "Examples:"
        echo "  $0 add      # Schedule the pipeline"
        echo "  $0 remove   # Unschedule the pipeline"
        echo "  $0 list     # View scheduled jobs"
        exit 1
        ;;
esac
