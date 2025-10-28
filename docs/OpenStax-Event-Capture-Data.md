# Introduction
OpenStax's Event Capture data houses specific data on when users interact with OpenStax's online reading experience (REX) and OpenStax Assignable beta. Assignable beta allows instructors to build customized assignments directly in their Learning Management Systems. Assignable beta has enrichment and support materials, documents, assessments, and other assets for users.

## What can be found in the Event Capture data?
`¯\_(ツ)_/¯ ` Do we need a summary here? Or will the schema suffice?

## How to query Event Capture data
`¯\_(ツ)_/¯ ` Are there other instructions we want to put here?

# References

## Known Working Code Examples
Connects to the Event Capture S3 bucket, reads the parquet file and sends a dummy CSV file (1, 2, 3, 4, 5) as the results.

<details>

<summary>
Click me to see the sample R snippet
</summary>


```r
library(arrow)
library(paws)
library(furrr)
library(dplyr)
library(future)
library(jsonlite)
library(httr)  # For sending files to an API
library(readr) # For writing CSV files

# Set Trusted Output App API Endpoint
trusted_output_endpoint <- Sys.getenv("TRUSTED_OUTPUT_ENDPOINT")

# AWS S3 bucket and folder (prefix) details
bucket_name <- "quasar-sandbox-events"
s3_folder <- "rjr-parquet/created_highlight/year=2021/"  # S3 folder (prefix) containing the Parquet files
# s3_folder <- "" # All files in the S3 bucket (used for testing smaller buckets)

# Assume Machine IAM permissions.
# If testing locally, make sure to export AWS Access key, secret and session for access

# Region for S3 bucket is needed to be set. us-east-2 is for Event capture Sandbox
Sys.setenv("AWS_DEFAULT_REGION" = "us-east-2")

# Initialize the S3 client
s3 <- paws::s3()

# Step 1: List all Parquet files in the S3 folder
# List all objects in the given S3 bucket and folder (prefix)
list_objects <- s3$list_objects_v2(Bucket = bucket_name, Prefix = s3_folder)

# Extract the file names (keys) from the result
s3_files <- list_objects$Contents

# Filter for only .parquet files and print them
if (length(s3_files) > 0) {
  file_names <- sapply(s3_files, function(x) x$Key)
  parquet_files <- file_names[grepl("\\.parquet$", file_names)]

  if (length(parquet_files) > 0) {
    print(parquet_files)
  } else {
    print("No .parquet files found in the specified folder.")
  }
} else {
  stop("No Parquet files found in the specified S3 folder.")
}

# Initialize parallel processing using furrr and future
plan(multisession, workers = 2)  # hard coded to 2 CPU cores

# Function to read a Parquet file either directly from S3 or by downloading it first
read_parquet_file <- function(s3_key) {
  message("Reading Parquet file: ", s3_key)

  # Construct the full S3 URI
  s3_uri <- paste0("s3://", bucket_name, "/", s3_key)

  # Method 1: Directly read from S3 using Arrow's s3 support (if configured)
  tryCatch({
    s3fs <- s3_bucket(bucket_name, anonymous = FALSE)
    parquet_data <- read_parquet(s3fs$path(s3_key))
    return(parquet_data)
  }, error = function(e) {
    message("Direct S3 read failed for: ", s3_key, " Error: ", e$message)

    # Method 2: Download the file locally using aws.s3, then read it
    # temp_file <- tempfile(fileext = ".parquet")
    # save_object(s3_key, bucket = bucket_name, file = temp_file)
    # parquet_data <- read_parquet(temp_file)
    # unlink(temp_file)  # Clean up the temp file
    # return(parquet_data)
  })
}

# Step 2: Process the Parquet files in parallel using furrr
# We will split the files into chunks based on batch size and run the reading process in parallel
batch_size <- 1000  # Adjust the batch size based on available memory and number of files

# Read all files in parallel
parquet_data_list <- future_map(parquet_files, read_parquet_file, .progress = TRUE)

# Step 3: Optionally combine all data into a single data frame
all_parquet_data <- bind_rows(parquet_data_list)

# Print the combined data preview
print(head(all_parquet_data))

# Save the combined data to a new parquet file locally (if needed)
# write_parquet(all_parquet_data, "combined_data.parquet")

# Clean up the parallel workers
plan(sequential)  # Reset the plan back to sequential processing

# Do some researcher with all_parquet_data
# Stubbed until we figure out what we should here, but for now
# just create a dummy CSV file. The parquet data is not used because it has
# NULL columns which cause and error int he data frame
numbers <- c("1", "2", "3", "4", "5")
results <- data.frame(numbers)

# Write the parquet results to a CSV file
output_csv <- "aggregate_results.csv"
write_csv(results, output_csv)

# write_csv_arrow(results, output_csv)

# Send aggregate results to Trusted Output App
response <- POST(
  url = trusted_output_endpoint,
  body = list(file = upload_file(output_csv)),  # Attach the CSV file
  encode = "multipart"  # Multipart form data encoding
)

# DEBUG: Print the response content
# response_content <- content(response, as = "parsed", type = "application/json")
# print(response_content)

# Check the API response
if (response$status_code == 200) {
  print("File uploaded successfully.")
} else {
  print(paste("File upload failed. Status code:", response$status_code))
}

# Performance Notes
# When loading 468 parquet files from the sandbox bucket, each ~7KB, this
# script took ~3mins to read and combine the files into the single
# parquet_data_list when limited to 2CPU cores on a powerful Mac.
#
# In production Event Capture, there are ~1500 files per day, each ~7KB to
# ~10KB. At 2 cores, we can extrapolate that this script will take 10-15mins to
# read a single days worth of parquet files. To view data over a semester, that
# may be 5-6 hours, just to read the data. Then the research will start.
```

</details>

## Technical Data Docs Index

* [Event Capture API Repository](https://github.com/openstax/event-capture-api)

# Schema
## Common attributes (all events)

The fields below are added to event by api server at event storage time.

Field | Type | Description
:--- | :--- | :---
device_uuid | UUID | Unique device identifier (browser+device fingerprint)
user_uuid | UUID | Unique user identifier (OpenStax authenticated user)
session_uuid | UUID | Unique session identifier
session_order | integer (ordinal) | Order of this event within this session (not in started_session)
scheme | string | HTTP scheme (i.e. https)
host | string | HTTP hostname (i.e. openstax.org )
path | string | HTTP path (e.g. /books/biology-2e/pages/6-5-enzymes)
query | string | HTTP query string (after ?)

The fields below are an artifact of S3 connector storage and use Athena to query this data.

Field | Type | Description
:--- | :--- | :---
occurred_at | timestamp | UTC time that event occurred (corrected)
year | string | Partitioning value (date of storage to S3)
month | string | Partitioning value (date of storage to S3)
day | string | Partitioning value (date of storage to S3)

## started_session

Field | Type | Description
:--- | :--- | :---
ip_address | string | IP address of client
referrer | string | Referrer for HTTP access
user_agent | string | User agent string of browser
release_id | string | Code version of the REX software app
service_worker | String (enumerated) | Service worker state, one of: unsupported, inactive, active

## interacted_element

Field | Type | Description
:--- | :--- | :---
target_id | string | HTML ID of the interacted element
target_type | string | HTML type interacted with (A, BUTTON, etc.)
target_attributes | map | HTML attributes of target
context_id | string | HTML ID of the first interesting parent of the target
context_type | string | HTML type of the context element
context_attributes | map | HTML attributes of target
context_region | string | Area of page context belongs to: (e.g. body, TOC, nav, etc.)

## created_highlight

Field | Type | Description
:--- | :--- | :---
highlight_id | UUID | ID of highlight in highlight server
source_id | string | The highlight source id (e.g., page uuid).
source_type | string | The highlight source type (e.g., openstax_page).
source_metadata | map | The highlight source metadata. This contains the source version and pipeline version, needed to fetch the actual content highlighted.
annotation | string | The highlight annotation.
anchor | string | The highlight anchor.
color | string | The highlight color.
location_strategies | string | The highlight location strategies (e.g., a text position selector).
scope_id | UUID | The highlight location scope (container for the source, like a book uuid).

## accessed_study_guide

Field | Type | Description
:--- | :--- | :---
page_id | UUID | Unique id for page
book_id | string | Unique id for book

## nudged

Field | Type | Description
:--- | :--- | :---
app | string | The app sourcing the nudge (e.g., tutor)
context | uuid | The nudge context (e.g., a book uuid).
medium | text | The nudge medium (e.g., email).
target | string | The target of the nudge (e.g., study_guides).
flavor | string | The nudge flavor (e.g., full screen v2).

