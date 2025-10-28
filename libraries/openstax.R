# OpenStax Research Data Access Functions
#
# This file provides simple, user-facing functions for querying OpenStax
# educational datasets. For implementation details, see openstax_helper.R

# Load required libraries and helper functions
source("libraries/openstax_libraries.R")
source("libraries/openstax_helper.R")


#' Query OpenStax Tutor dataset from S3 parquet files
#'
#' Executes SQL queries against OpenStax Tutor data stored as parquet files
#' in S3 using DuckDB. This function provides access to Tutor learning
#' analytics data including student interactions, exercise completions, and
#' performance metrics. Files are organized by year following the naming
#' pattern: openstax_tutor_YYYY-01-01__YYYY-12-31.parquet
#'
#' @param sql_query A character string containing the SQL query to execute
#'   against the Tutor dataset. The query should reference the 'tutor_data'
#'   table which is automatically created as a view over the parquet files.
#'   Example: "SELECT * FROM tutor_data WHERE course_id = 123 LIMIT 1000"
#'
#' @param start_date Optional. A Date object or character string (YYYY-MM-DD)
#'   specifying the start date for file selection. If not provided, the
#'   function will attempt to extract date ranges from the SQL query's WHERE
#'   clause for columns like created_at, updated_at, etc. If no dates are
#'   found in the query, defaults to 2024-01-01.
#'
#' @param end_date Optional. A Date object or character string (YYYY-MM-DD)
#'   specifying the end date for file selection. If not provided, the
#'   function will attempt to extract from SQL or default to 2024-12-31.
#'
#' @return A data.frame containing the query results. Large result sets are
#'   automatically processed in chunks to prevent memory issues.
#'
#' @details
#' This function uses DuckDB with the httpfs extension to query parquet files
#' directly from S3. It follows the same pattern as query_notes_and_highlights:
#' - Automatically determines which year files to load based on date range
#' - Can extract date ranges from SQL WHERE clauses (created_at, updated_at)
#' - Creates a unified view 'tutor_data' across all relevant files
#' - Uses memory-efficient chunked processing for large queries
#' - Provides detailed logging of file loading and query execution
#'
#' S3 bucket structure: s3://openstax-enclave-data/tutor/v1/
#'
#' File naming convention:
#' - openstax_tutor_2018-01-01__2018-12-31.parquet
#' - openstax_tutor_2019-01-01__2019-12-31.parquet
#' - openstax_tutor_2024-01-01__2024-12-31.parquet
#'
#' @examples
#' \dontrun{
#' # Query with explicit date range
#' data_2024 <- query_tutor(
#'   "SELECT * FROM tutor_data WHERE course_id = 123",
#'   start_date = "2024-01-01",
#'   end_date = "2024-12-31"
#' )
#'
#' # Query with dates extracted from SQL
#' result <- query_tutor(
#'   "SELECT * FROM tutor_data
#'    WHERE created_at >= '2023-02-01' AND created_at <= '2024-11-30'"
#' )
#' }
#'
#' @seealso \code{\link{query_notes_and_highlights}} for similar S3 access,
#'   \code{\link{query_tutor_with_duckdb}} for implementation details
query_tutor <- function(sql_query, start_date = NULL, end_date = NULL) {
  message("Starting memory-efficient Tutor query using DuckDB")
  message("SQL Query: ", sql_query)

  # If dates not provided, try to extract from SQL query
  if (is.null(start_date) || is.null(end_date)) {
    message("Extracting date range from SQL query...")
    extracted_dates <- extract_date_range_from_sql(sql_query)

    if (is.null(start_date)) {
      start_date <- extracted_dates$start_date
      message("Using extracted/default start_date: ", start_date)
    }
    if (is.null(end_date)) {
      end_date <- extracted_dates$end_date
      message("Using extracted/default end_date: ", end_date)
    }
  }

  message("Date range for file selection: ", start_date, " to ", end_date)

  return(query_tutor_with_duckdb(sql_query, start_date, end_date))
}

#' Query OpenStax Tutor Exercises dataset from S3 parquet file
#'
#' Executes SQL queries against OpenStax Tutor Exercises data stored as a
#' single parquet file in S3 using DuckDB. This dataset contains exercise
#' interaction data including user responses, timing, and performance metrics.
#'
#' @param sql_query A character string containing the SQL query to execute
#'   against the Tutor Exercises dataset. The query should reference the
#'   'exercises_data' table which is automatically created as a view over
#'   the S3 parquet file.
#'   Example: "SELECT * FROM exercises_data WHERE user_id = 123 LIMIT 1000"
#'
#' @return A data.frame containing the query results. Large result sets are
#'   automatically processed in chunks to prevent memory issues.
#'
#' @details
#' This function uses DuckDB with the httpfs extension to query the parquet
#' file directly from S3. Unlike the Tutor data which is split by year, the
#' Exercises data is contained in a single parquet file covering all dates.
#'
#' Features:
#' - Queries a single unified parquet file containing all exercise data
#' - Uses memory-efficient chunked processing for large queries
#' - Provides detailed logging of query execution
#' - Automatically handles AWS credential authentication
#'
#' S3 bucket structure: s3://openstax-enclave-data/tutor/v1/
#' File: openstax_tutor_exercises.parquet
#'
#' @examples
#' \dontrun{
#' # Count unique users in a date range
#' user_count <- query_tutor_exercises(
#'   "SELECT COUNT(DISTINCT user_id) as unique_users
#'    FROM exercises_data
#'    WHERE event_time >= '2023-01-01' AND event_time <= '2023-12-31'"
#' )
#'
#' # Get exercise responses with aggregation
#' response_stats <- query_tutor_exercises(
#'   "SELECT exercise_id, COUNT(*) as attempts, AVG(score) as avg_score
#'    FROM exercises_data
#'    WHERE event_time >= '2024-01-01'
#'    GROUP BY exercise_id"
#' )
#' }
#'
#' @seealso \code{\link{query_tutor}} for the main Tutor dataset
query_tutor_exercises <- function(sql_query) {
  message("Starting memory-efficient Tutor Exercises query using DuckDB")
  message("SQL Query: ", sql_query)

  return(query_tutor_exercises_with_duckdb(sql_query))
}

#' Query OpenStax Tutor Notes and Highlights dataset from S3 parquet file
#'
#' Executes SQL queries against OpenStax Tutor Notes and Highlights data
#' stored as a single parquet file in S3 using DuckDB. This dataset contains
#' student notes and highlights from the Tutor platform.
#'
#' @param sql_query A character string containing the SQL query to execute
#'   against the Tutor Notes and Highlights dataset. The query should
#'   reference the 'tutor_notes_highlights' table which is automatically
#'   created as a view over the S3 parquet file.
#'   Example: "SELECT * FROM tutor_notes_highlights WHERE user_id = 123"
#'
#' @return A data.frame containing the query results. Large result sets are
#'   automatically processed in chunks to prevent memory issues.
#'
#' @details
#' This function uses DuckDB with the httpfs extension to query the parquet
#' file directly from S3. The Tutor Notes and Highlights data is contained
#' in a single parquet file covering all dates.
#'
#' Features:
#' - Queries a single unified parquet file containing all notes/highlights
#' - Uses memory-efficient chunked processing for large queries
#' - Provides detailed logging of query execution
#' - Automatically handles AWS credential authentication
#'
#' S3 bucket structure: s3://openstax-enclave-data/tutor/v1/
#' File: openstax_tutor_notes_and_highlights.parquet
#'
#' @examples
#' \dontrun{
#' # Get highlights for a specific date range
#' highlights <- query_tutor_notes_and_highlights(
#'   "SELECT * FROM tutor_notes_highlights
#'    WHERE created_at >= '2023-01-01' AND created_at <= '2023-12-31'"
#' )
#'
#' # Count notes by user
#' note_counts <- query_tutor_notes_and_highlights(
#'   "SELECT user_id, COUNT(*) as note_count
#'    FROM tutor_notes_highlights
#'    WHERE type = 'note'
#'    GROUP BY user_id"
#' )
#' }
#'
#' @seealso \code{\link{query_tutor}} for the main Tutor dataset,
#'   \code{\link{query_tutor_exercises}} for Tutor Exercises data
query_tutor_notes_and_highlights <- function(sql_query) {
  message("Starting Tutor Notes and Highlights query using DuckDB")
  message("SQL Query: ", sql_query)

  return(query_tutor_notes_and_highlights_with_duckdb(sql_query))
}

#' Query OpenStax Notes and Highlights dataset via PostgreSQL database
#'
#' Connects to the OpenStax Notes and Highlights PostgreSQL database and executes
#' a SQL query. This function provides direct database access for real-time queries
#' and is suitable for smaller result sets or when you need the most current data.
#'
#' @param sql_query A character string containing the SQL query to execute.
#'   The query should reference tables available in the Notes and Highlights database.
#'   Example: "SELECT * FROM highlights WHERE created_at > '2023-01-01' LIMIT 1000"
#'
#' @return A data.frame containing the query results. Returns an empty data.frame
#'   if no rows match the query criteria.
#'
#' @details
#' Required environment variables:
#' - HIGHLIGHTS_DB_HOST: Database hostname or IP address
#' - HIGHLIGHTS_DB_PORT: Database port (typically 5432 for PostgreSQL)
#' - HIGHLIGHTS_DB_USER: Database username with read access
#' - HIGHLIGHTS_DB_PASSWORD: Database password
#' - HIGHLIGHTS_DB_NAME: Database name
#'
#' The function automatically handles database connection establishment and cleanup.
#' Connection validation is performed before query execution.
#'
#' @examples
#' \dontrun{
#' # Get recent highlights
#' recent_highlights <- query_notes_and_highlights_db(
#'   "SELECT * FROM highlights WHERE created_at > CURRENT_DATE - INTERVAL '7 days'"
#' )
#'
#' # Count highlights by book
#' book_counts <- query_notes_and_highlights_db(
#'   "SELECT book_id, COUNT(*) as highlight_count FROM highlights GROUP BY book_id"
#' )
#' }
#'
#' @seealso \code{\link{query_notes_and_highlights}} for S3 parquet access
query_notes_and_highlights_db <- function(sql_query) {
  # Load environment variables for database connection
  db_host <- Sys.getenv("HIGHLIGHTS_DB_HOST")
  db_port <- Sys.getenv("HIGHLIGHTS_DB_PORT")
  db_user <- Sys.getenv("HIGHLIGHTS_DB_USER")
  db_password <- Sys.getenv("HIGHLIGHTS_DB_PASSWORD")
  db_name <- Sys.getenv("HIGHLIGHTS_DB_NAME")

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

  # Run the SQL Query
  query_result <- dbGetQuery(con, sql_query)

  return(query_result)
}


#' Query OpenStax Notes and Highlights dataset via S3 parquet files
#'
#' Executes SQL queries against OpenStax Notes and Highlights data stored as
#' parquet files in S3 using DuckDB. This is the recommended method for large
#' queries and analytics workloads as it provides better performance and
#' memory efficiency compared to direct database access.
#'
#' @param sql_query A character string containing the SQL query to execute.
#'   The query should reference the 'highlights' table which is automatically
#'   created as a view over the S3 parquet files.
#'   Example: "SELECT * FROM highlights WHERE created_at > '2023-01-01'"
#'
#' @return A data.frame containing the query results. Large result sets are
#'   automatically chunked to prevent memory issues. Returns an empty data.frame
#'   if no rows match the query criteria.
#'
#' @details
#' This function uses DuckDB with the httpfs extension to query parquet files
#' directly from S3 without downloading them locally. It automatically:
#' - Configures memory limits (5GB for DuckDB, 3GB reserved for system)
#' - Detects large queries and uses chunked processing
#' - Monitors memory usage throughout execution
#' - Handles AWS credential authentication
#'
#' Required environment variables (optional, uses AWS credential chain if not set):
#' - AWS_ACCESS_KEY_ID: AWS access key
#' - AWS_SECRET_ACCESS_KEY: AWS secret key
#' - AWS_DEFAULT_REGION: AWS region (defaults to us-east-2)
#'
#' S3 data location: s3://openstax-enclave-data/notes_and_highlights/
#'
#' @examples
#' \dontrun{
#' # Get highlights with aggregation (efficient)
#' summary_data <- query_notes_and_highlights(
#'   "SELECT book_id, COUNT(*) as count, AVG(LENGTH(content)) as avg_length
#'    FROM highlights
#'    WHERE created_at > '2023-01-01'
#'    GROUP BY book_id"
#' )
#'
#' # Get specific highlights (use LIMIT for large datasets)
#' sample_highlights <- query_notes_and_highlights(
#'   "SELECT * FROM highlights WHERE book_id = 'biology-2e' LIMIT 10000"
#' )
#' }
#'
#' @seealso \code{\link{query_notes_and_highlights_db}} for direct database access
query_notes_and_highlights <- function(sql_query) {
  result <- query_large_parquet_files("highlights", sql_query)
  return(result)
}


#' Query OpenStax Event Capture dataset from S3 with date partitioning
#'
#' Queries OpenStax Event Capture data stored in S3 with date-based partitioning.
#' This function constructs S3 paths for the specified date range and event type,
#' then executes the provided SQL query against the parquet files.
#'
#' @param event A character string specifying the event type to query.
#'   This corresponds to the top-level directory in the S3 bucket structure.
#'   Examples: "page_view", "highlight_created", "search_performed"
#'
#' @param start_date A Date object or character string (YYYY-MM-DD format)
#'   specifying the start date for the query range (inclusive).
#'   Will be automatically converted to Date if provided as character.
#'
#' @param end_date A Date object or character string (YYYY-MM-DD format)
#'   specifying the end date for the query range (inclusive).
#'   Will be automatically converted to Date if provided as character.
#'
#' @param sql_query A character string containing the SQL query to execute
#'   against the event data. The query can reference event-specific columns
#'   and should be optimized for the expected data structure.
#'
#' @return Currently returns NULL as implementation is incomplete.
#'   When completed, will return a data.frame containing query results.
#'
#' @details
#' S3 bucket structure: s3://openstax-event-capture/{event}/year={YYYY}/month={MM}/day={DD}/*.parquet
#'
#' The function:
#' - Validates and converts date parameters to Date objects
#' - Generates a sequence of dates from start_date to end_date
#' - Constructs S3 file paths for each date using Hive-style partitioning
#' - Provides detailed logging of path construction and processing steps
#'
#' Date range considerations:
#' - Large date ranges may result in many S3 paths and longer query times
#' - Consider using aggregated queries for large date ranges
#' - Each day is stored as separate parquet files in date-partitioned directories
#'
#' @examples
#' \dontrun{
#' # Query page views for a specific week
#' page_views <- query_event_capture(
#'   event = "page_view",
#'   start_date = "2023-01-01",
#'   end_date = "2023-01-07",
#'   sql_query = "SELECT * FROM events WHERE book_id = 'biology-2e'"
#' )
#'
#' # Query highlight creation events for a month with aggregation
#' highlight_stats <- query_event_capture(
#'   event = "highlight_created",
#'   start_date = as.Date("2023-01-01"),
#'   end_date = as.Date("2023-01-31"),
#'   sql_query = "SELECT DATE(timestamp) as day, COUNT(*) as highlights_created
#'                FROM events GROUP BY DATE(timestamp) ORDER BY day"
#' )
#' }
#'
#' @note This function is currently under development. The query execution
#'   portion is not yet implemented and will return NULL.
query_event_capture <-
  function(event, start_date, end_date, sql_query) {

    message("Starting query_event_capture function.")
    # Set S3 Bucket for Event Capture
    bucket <- "openstax-event-capture"

    # Ensure start_date and end_date are Date objects
    if (!inherits(start_date, "Date")) {
      start_date <- as.Date(start_date)
      message("Converted start_date to Date: ", start_date)
    }
    if (!inherits(end_date, "Date")) {
      end_date <- as.Date(end_date)
      message("Converted end_date to Date: ", end_date)
    }
    # Generate a sequence of dates from start_date to end_date
    message("Generating date sequence from ", start_date, " to ", end_date, ".")
    dates <- seq.Date(start_date, end_date, by = "day")

    # Construct S3 file paths for each day using the partitioned directory
    # structure
    message("Constructing S3 file paths for each date.")
    file_paths <- sapply(dates, function(d) {
      year  <- format(d, "%Y")
      month <- format(d, "%m")
      day   <- format(d, "%d")
      path <-
        paste0("s3://",
          bucket, "/",
          event,
          "/year=", year,
          "/month=", month,
          "/day=", day,
          "/*.parquet"
        )
      message("Constructed path: ", path)
      return(path)
    })

    result <- NULL

    # Read Parquet files from S3 and execute SQL query

    message("query_event_capture function completed.")
    return(result)
}
