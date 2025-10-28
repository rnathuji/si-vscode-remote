# Introduction
OpenStax Notes and Highlighting is a feature in the OpenStax reading experience (REX) where a user can log-in and make highlights and take notes for any of OpenStax's textbooks on the website.

## What can be found in the Notes and Highlights data?
`¯\_(ツ)_/¯ ` Do we need further description here or will schema suffice?

## How to query Notes and Highlights data
`¯\_(ツ)_/¯ ` Larissa has no idea. 

# References

## Known Working Code Examples
Connect to the Highlights RDS instance and return simple known clean information (number of rows, etc.).

<details>

<summary>
Click me to see the sample R snippet
</summary>


```
# Load the RPostgres library
library(RPostgres)
library(jsonlite)
library(httr)  # For sending files to an API
library(readr) # For writing CSV files

# Get DB connection info from Environment
db_host <- Sys.getenv("DB_HOST")
db_port <- Sys.getenv("DB_PORT")
db_name <- Sys.getenv("DB_NAME")
db_user <- Sys.getenv("DB_USER")
db_password <- Sys.getenv("DB_PASSWORD")

# Set Trusted Output App API Endpoint
trusted_output_endpoint <- Sys.getenv("TRUSTED_OUTPUT_ENDPOINT")

# Establish a connection to the PostgreSQL database
con <- dbConnect(
  drv = Postgres(),
  host = db_host,
  port = db_port,
  dbname = db_name,
  user = db_user,
  password = db_password
)

# Check the connection
if (!dbIsValid(con)) {
  stop("Failed to connect to the database.")
} else {
  print("Successfully connected to the database!")
}

# Example query: Get the first 10 rows from a table
query_result <- dbGetQuery(con, "SELECT COUNT(*) FROM highlights")

# DEBUG: Query to list all tables in the database
# query_result <- dbGetQuery(con,"
# SELECT table_schema, table_name
# FROM information_schema.tables
# WHERE table_type = 'BASE TABLE'
#   AND table_schema NOT IN ('information_schema', 'pg_catalog');
# ")

# Print the query result
print(query_result)

# Always disconnect when you're done
dbDisconnect(con)

# Do some research with the results
# Stubbed until we figure out what we should here, but for now
# just create a CSV file from the select table

# Write the query results to a CSV file
csv_file_path <- "query_result.csv"
write_csv(query_result, csv_file_path)

# Send aggregate results to Trusted Output App
response <- POST(
  url = trusted_output_endpoint,
  body = list(file = upload_file(csv_file_path)),  # Attach the CSV file
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
```

</details>

## Technical Data Docs Index

* [Notes and Highlights API Repository](https://github.com/openstax/highlights-api/blob/master/db/schema.rb)

# Schema

## curator_scopes
Field | Type | Defaults | Description
:--- | :--- | :--- | :---
id | uuid | -> { "gen_random_uuid()" } |
curator_id | uuid | |
scope_id | uuid | |

## highlights
Field | Type | Defaults | Description
:--- | :--- | :--- | :---
id | uuid | -> { "gen_random_uuid()" } | 
user_id | uuid | |   
source_type | integer | 0 |
source_id | string | |   
source_metadata | jsonb | | 
anchor | text | | 
highlighted_content | text | | 
annotation | text | | 
color | string | | 
location_strategies | jsonb | | 
created_at | datetime | | 
updated_at | datetime | | 
scope_id | string | | 
order_in_source | float | | 
prev_highlight_id | uuid | | 
next_highlight_id | uuid | | 
content_path | integer | | 

## precalculusteds
Field | Type | Defaults | Description
:--- | :--- | :--- | :---
data_type | string | info | 
data | json | | 

## user_sources
Field | Type | Defaults | Description
:--- | :--- | :--- | :---
id | uuid | -> { "gen_random_uuid()" } | 
user_id | uuid | | 
source_id | string | |
source_type | string | |
num_highlights | integer | 0 |

## users
Field | Type | Defaults | Description
:--- | :--- | :--- | :---
id | uuid | -> { "gen_random_uuid()" } | 
num_annotation_characters | integer | 0 |
num_highlights | integer | 0 |