#!/bin/bash

# NYC Taxi Data Downloader - Bash Version
# This script downloads NYC taxi data and organizes it by month

# Default values
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
SERVICE_TYPE="yellow"
YEAR=$(date +"%Y")
START_MONTH=1
END_MONTH=7
FILE_FORMAT=""
OUTPUT_DIR=""
WORKERS=4

# Setup logging
log_info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') - $1" >&2
}

log_warning() {
    echo "[WARNING] $(date '+%Y-%m-%d %H:%M:%S') - $1" >&2
}

# Function to download a file
download_file() {
    local url="$1"
    local local_path="$2"
    
    log_info "Downloading from $url to $local_path"
    
    # Create directory if it doesn't exist
    mkdir -p "$(dirname "$local_path")"
    
    # Download file with curl and follow redirects
    if curl -L -f -o "$local_path" "$url" 2>/dev/null; then
        # Check if the download succeeded and file is not empty
        if [ -s "$local_path" ]; then
            log_info "Successfully downloaded to $local_path"
            return 0
        else
            log_error "Downloaded file is empty: $local_path"
            rm -f "$local_path"
            return 1
        fi
    else
        log_error "Failed to download from $url"
        return 1
    fi
}

# Process a month based on format
process_month() {
    local year="$1"
    local month="$2"
    local service_type="$3"
    local file_format="$4"
    
    # Format month with leading zero
    formatted_month=$(printf "%02d" "$month")
    
    case "$file_format" in
        csv)
            file_name="${service_type}_tripdata_${year}-${formatted_month}.csv"
            ;;
        parquet)
            file_name="${service_type}_tripdata_${year}-${formatted_month}.parquet"
            ;;
        csv.gz)
            file_name="${service_type}_tripdata_${year}-${formatted_month}.csv.gz"
            ;;
        *)
            log_error "Unsupported file format: $file_format"
            return 1
            ;;
    esac
    
    url="${BASE_URL}/${file_name}"
    month_dir="${OUTPUT_DIR}/${service_type}/${year}/${formatted_month}"
    local_path="${month_dir}/${file_name}"
    
    log_info "Processing $file_format file: $file_name"
    
    if download_file "$url" "$local_path"; then
        echo "$month" # Output successful month
        return 0
    else
        return 1
    fi
}

# Process months using parallel execution
process_months() {
    local year="$1"
    local start_month="$2"
    local end_month="$3"
    local service_type="$4"
    local file_format="$5"
    local workers="$6"
    
    # Temporary file to store results
    local temp_file=$(mktemp)
    
    # Use GNU parallel if available, otherwise fallback to sequential processing
    if command -v parallel &>/dev/null; then
        log_info "Using GNU parallel with $workers workers"
        
        # Generate sequence of months
        seq "$start_month" "$end_month" | \
        parallel -j "$workers" "bash -c 'source \"$0\"; process_month $year {} $service_type $file_format' >> \"$temp_file\" 2>/dev/null" "$0"
    else
        log_warning "GNU parallel not found, processing sequentially"
        
        for month in $(seq "$start_month" "$end_month"); do
            process_month "$year" "$month" "$service_type" "$file_format" >> "$temp_file" 2>/dev/null
        done
    fi
    
    # Read results from temp file
    successful_months=$(cat "$temp_file" | sort -n | tr '\n' ' ')
    rm -f "$temp_file"
    
    echo "$successful_months"
}

# Display usage information
show_usage() {
    echo "Usage: $0 [options]"
    echo "Downloads NYC Taxi data to local folders organized by month"
    echo ""
    echo "Options:"
    echo "  --output-dir DIR     Base directory to store downloaded data (required)"
    echo "  --base-url URL       Base URL for NYC taxi data"
    echo "                       (default: $BASE_URL)"
    echo "  --service-type TYPE  Type of taxi service data to download"
    echo "                       [yellow|green|fhv] (default: yellow)"
    echo "  --file-format FORMAT File format to download [csv|parquet|csv.gz]"
    echo "                       (required)"
    echo "  --year YEAR          Year of data to download (default: current year)"
    echo "  --start-month MONTH  Starting month (1-12, default: 1)"
    echo "  --end-month MONTH    Ending month (1-12, default: 7)"
    echo "  --workers NUM        Number of parallel workers (default: 4)"
    echo "  --help               Display this help message"
    echo ""
    echo "Example:"
    echo "  $0 --output-dir ./taxi_data --file-format parquet"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --base-url)
            BASE_URL="$2"
            shift 2
            ;;
        --service-type)
            case "$2" in
                yellow|green|fhv)
                    SERVICE_TYPE="$2"
                    shift 2
                    ;;
                *)
                    log_error "Invalid service type: $2. Must be yellow, green, or fhv."
                    show_usage
                    exit 1
                    ;;
            esac
            ;;
        --file-format)
            case "$2" in
                csv|parquet|csv.gz)
                    FILE_FORMAT="$2"
                    shift 2
                    ;;
                *)
                    log_error "Invalid file format: $2. Must be csv, parquet, or csv.gz."
                    show_usage
                    exit 1
                    ;;
            esac
            ;;
        --year)
            if [[ "$2" =~ ^[0-9]+$ ]]; then
                YEAR="$2"
                shift 2
            else
                log_error "Year must be a number."
                show_usage
                exit 1
            fi
            ;;
        --start-month)
            if [[ "$2" =~ ^[0-9]+$ ]] && [ "$2" -ge 1 ] && [ "$2" -le 12 ]; then
                START_MONTH="$2"
                shift 2
            else
                log_error "Start month must be between 1 and 12."
                show_usage
                exit 1
            fi
            ;;
        --end-month)
            if [[ "$2" =~ ^[0-9]+$ ]] && [ "$2" -ge 1 ] && [ "$2" -le 12 ]; then
                END_MONTH="$2"
                shift 2
            else
                log_error "End month must be between 1 and 12."
                show_usage
                exit 1
            fi
            ;;
        --workers)
            if [[ "$2" =~ ^[0-9]+$ ]]; then
                WORKERS="$2"
                shift 2
            else
                log_error "Number of workers must be a number."
                show_usage
                exit 1
            fi
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$OUTPUT_DIR" ]; then
    log_error "Output directory is required."
    show_usage
    exit 1
fi

if [ -z "$FILE_FORMAT" ]; then
    log_error "File format is required."
    show_usage
    exit 1
fi

if [ "$START_MONTH" -gt "$END_MONTH" ]; then
    log_error "Start month must be less than or equal to end month."
    show_usage
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Main execution
log_info "Starting NYC Taxi Data Downloader"
log_info "Service type: $SERVICE_TYPE"
log_info "Year: $YEAR"
log_info "Months: $START_MONTH-$END_MONTH"
log_info "File format: $FILE_FORMAT"
log_info "Output directory: $OUTPUT_DIR"
log_info "Workers: $WORKERS"

# Download data
successful_months=$(process_months "$YEAR" "$START_MONTH" "$END_MONTH" "$SERVICE_TYPE" "$FILE_FORMAT" "$WORKERS")
successful_count=$(echo "$successful_months" | wc -w)
failed_count=$((END_MONTH - START_MONTH + 1 - successful_count))

# Print summary
echo ""
echo "Download Summary:"
echo "Successfully downloaded months: $successful_months"
echo "Total successful: $successful_count"
echo "Total failed: $failed_count"
echo "Files saved to: $OUTPUT_DIR"

# Examples in comments:
# Basic usage:
# ./nyc_taxi_downloader.sh --output-dir ./taxi_data --file-format parquet
#
# Specify different base URL and service type:
# ./nyc_taxi_downloader.sh --output-dir ./taxi_data --base-url "https://d37ci6vzurychx.cloudfront.net/trip-data" --service-type green --file-format parquet
#
# Full example with all parameters:
# ./nyc_taxi_downloader.sh \
#     --output-dir data/raw \
#     --base-url "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow" \
#     --service-type yellow \
#     --year 2019 \
#     --start-month 1 \
#     --end-month 12 \
#     --file-format csv.gz \
#     --workers 4