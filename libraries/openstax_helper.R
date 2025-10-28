# OpenStax Research Helper Functions
# This file contains internal helper functions used by OpenStax query functions

#' Extract date range from SQL query WHERE clause
#'
#' Parses a SQL query to extract date ranges from WHERE clauses for date
#' columns like created_at, updated_at, first_completed_at, etc.
#'
#' @param sql_query A character string containing the SQL query
#'
#' @return A list with start_date and end_date. If dates cannot be extracted,
#'   returns defaults: start_date = "2024-01-01", end_date = "2024-12-31"
#'
#' @details
#' This function looks for date comparisons in the WHERE clause for columns:
#' - created_at
#' - updated_at
#' - first_completed_at
#' - last_completed_at
#' - first_published_at
#' - last_published_at
#'
#' It searches for patterns like:
#' - column >= 'YYYY-MM-DD'
#' - column > 'YYYY-MM-DD'
#' - column <= 'YYYY-MM-DD'
#' - column < 'YYYY-MM-DD'
#' - column BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
extract_date_range_from_sql <- function(sql_query) {
  # Default date range (2024 only)
  default_start <- "2024-01-01"
  default_end <- "2024-12-31"

  start_date <- NULL
  end_date <- NULL

  # Convert to lowercase for easier parsing
  sql_lower <- tolower(sql_query)

  # Date columns to look for
  date_columns <- c(
    "created_at", "updated_at", "first_completed_at",
    "last_completed_at", "first_published_at", "last_published_at",
    "dropped_at", "withdrawn_at", "first_paid_at",
    "publish_last_requested_at", "last_graded_at",
    "updated_by_instructor_at"
  )

  # Try to extract dates for each date column
  for (col in date_columns) {
    # Pattern for >= or > (start date)
    start_pattern <- paste0(col, "\\s*>=?\\s*'([0-9]{4}-[0-9]{2}-[0-9]{2})'")
    start_match <- regmatches(sql_lower,
                             regexec(start_pattern, sql_lower))
    if (length(start_match[[1]]) > 1) {
      candidate_start <- start_match[[1]][2]
      if (is.null(start_date) ||
          as.Date(candidate_start) < as.Date(start_date)) {
        start_date <- candidate_start
        message("Found start date from ", col, ": ", start_date)
      }
    }

    # Pattern for <= or < (end date)
    end_pattern <- paste0(col, "\\s*<=?\\s*'([0-9]{4}-[0-9]{2}-[0-9]{2})'")
    end_match <- regmatches(sql_lower,
                           regexec(end_pattern, sql_lower))
    if (length(end_match[[1]]) > 1) {
      candidate_end <- end_match[[1]][2]
      if (is.null(end_date) ||
          as.Date(candidate_end) > as.Date(end_date)) {
        end_date <- candidate_end
        message("Found end date from ", col, ": ", end_date)
      }
    }

    # Pattern for BETWEEN
    between_pattern <- paste0(col,
      "\\s+between\\s+'([0-9]{4}-[0-9]{2}-[0-9]{2})'\\s+and\\s+'([0-9]{4}-[0-9]{2}-[0-9]{2})'"
    )
    between_match <- regmatches(sql_lower,
                               regexec(between_pattern, sql_lower))
    if (length(between_match[[1]]) > 2) {
      candidate_start <- between_match[[1]][2]
      candidate_end <- between_match[[1]][3]
      if (is.null(start_date) ||
          as.Date(candidate_start) < as.Date(start_date)) {
        start_date <- candidate_start
        message("Found start date from ", col, " BETWEEN: ", start_date)
      }
      if (is.null(end_date) ||
          as.Date(candidate_end) > as.Date(end_date)) {
        end_date <- candidate_end
        message("Found end date from ", col, " BETWEEN: ", end_date)
      }
    }
  }

  # Use defaults if not found
  if (is.null(start_date)) {
    start_date <- default_start
    message("No start date found in SQL, using default: ", start_date)
  }
  if (is.null(end_date)) {
    end_date <- default_end
    message("No end date found in SQL, using default: ", end_date)
  }

  return(list(start_date = start_date, end_date = end_date))
}
#' Execute DuckDB query against Tutor S3 parquet files with date range
#'
#' Core implementation for querying Tutor parquet files in S3 using DuckDB.
#' This function handles all aspects of S3 parquet access including connection
#' setup, memory configuration, and query execution with automatic optimization.
#'
#' @param sql_query A character string containing the SQL query to execute
#'   against the Tutor dataset.
#'
#' @param start_date A Date object or character string (YYYY-MM-DD format)
#'   for the start of the query range.
#'
#' @param end_date A Date object or character string (YYYY-MM-DD format)
#'   for the end of the query range.
#'
#' @return A data.frame containing the query results.
#'
#' @details
#' This function follows the same pattern as query_with_duckdb but constructs
#' S3 paths based on the date range provided. It:
#' - Extracts years from the date range
#' - Constructs S3 paths for each year's parquet file
#' - Uses the same DuckDB configuration and memory optimization
#' - Creates a unified 'tutor_data' view across all year files
#'
#' S3 Configuration:
#' - Bucket: openstax-tutor-data
#' - Path pattern: openstax_tutor_YYYY-01-01__YYYY-12-31.parquet
#' - Region: us-east-2 (configurable via AWS_DEFAULT_REGION)
#'
#' @examples
#' \dontrun{
#' # Direct usage (typically called via query_tutor)
#' tutor_data <- query_tutor_with_duckdb(
#'   "SELECT course_id, COUNT(*) FROM tutor_data GROUP BY course_id",
#'   "2024-01-01",
#'   "2024-12-31"
#' )
#' }
#'
#' @seealso \code{\link{query_tutor}} for high-level interface
query_tutor_with_duckdb <- function(sql_query, start_date, end_date) {
  message("Using DuckDB approach for memory-efficient querying")

  # Ensure start_date and end_date are Date objects
  if (!inherits(start_date, "Date")) {
    start_date <- as.Date(start_date)
    message("Converted start_date to Date: ", start_date)
  }
  if (!inherits(end_date, "Date")) {
    end_date <- as.Date(end_date)
    message("Converted end_date to Date: ", end_date)
  }

  # Extract years from date range
  start_year <- as.integer(format(start_date, "%Y"))
  end_year <- as.integer(format(end_date, "%Y"))
  years <- start_year:end_year
  message("Years to query: ", paste(years, collapse = ", "))

  # Construct S3 file paths for each year
  bucket <- "openstax-enclave-data"
  s3_paths <- sapply(years, function(year) {
    filename <- sprintf("openstax_tutor_%d-01-01__%d-12-31.parquet",
                       year, year)
    path <- paste0("s3://", bucket, "/tutor/v1/", filename)
    message("S3 path: ", path)
    return(path)
  })

  con <- NULL
  result <- NULL

  tryCatch({
    # Connect to DuckDB
    message("Connecting to DuckDB...")
    con <- dbConnect(duckdb::duckdb())

    # Install and load S3 extension
    message("Setting up S3 access...")
    dbExecute(con, "INSTALL httpfs;")
    dbExecute(con, "LOAD httpfs;")

    # Configure S3 settings for DuckDB
    aws_region <- "us-west-2"
    dbExecute(con, paste0("SET s3_region='", aws_region, "';"))
    dbExecute(con, paste0("SET s3_endpoint='s3.", aws_region, ".amazonaws.com';"))
    dbExecute(con, "SET s3_url_style=\'path\';")
    dbExecute(con, paste0("CREATE OR REPLACE SECRET s3_creds (",
      "TYPE s3,",
      "PROVIDER credential_chain,",
      "REGION '", aws_region, "'",
    ");"))
    message('Created DuckDB secret for S3 using default credential chain')
    dbExecute(con, "SET s3_use_ssl=true;")

    # Configure memory settings for 8GB container
    message("Configuring memory limits...")
    dbExecute(con, "SET memory_limit='5GB';")
    dbExecute(con, "SET max_memory='5GB';")
    dbExecute(con, "SET temp_directory='/tmp';")
    dbExecute(con, "SET threads=4;")
    dbExecute(con, "SET preserve_insertion_order=false;")
    dbExecute(con, "SET enable_progress_bar=false;")

    # Create a view that references the S3 parquet files
    message("Creating view for S3 parquet files...")
    file_list_str <- paste0("'", s3_paths, "'", collapse = ", ")
    create_view_sql <- paste0(
      "CREATE VIEW tutor_data AS ",
      "SELECT * FROM read_parquet([", file_list_str, "])"
    )
    dbExecute(con, create_view_sql)

    # Get row count for monitoring
    row_count <- dbGetQuery(con,
                           "SELECT COUNT(*) as total_rows FROM tutor_data")
    message("Total rows available: ", row_count$total_rows)

    # Check if query might return large result set
    message("Analyzing query for memory optimization...")
    is_large_query <- check_if_large_query(sql_query)

    start_time <- Sys.time()
    if (is_large_query) {
      message("Large query detected - using chunked result collection...")
      result <- execute_chunked_query(con, sql_query, "tutor_data")
    } else {
      message("Executing query against parquet files...")
      monitor_r_memory("before query execution")
      result <- dbGetQuery(con, sql_query)
      monitor_r_memory("after query execution")
    }
    end_time <- Sys.time()

    query_time <- round(difftime(end_time, start_time, units = "secs"), 2)
    message("Query completed in ", query_time, " seconds")
    message("Result rows: ", nrow(result))
    message("Result columns: ", ncol(result))

  }, error = function(e) {
    message("Error in query_tutor_with_duckdb: ", e$message)
    message("S3 paths attempted: ", paste(s3_paths, collapse = ", "))
    stop(e)
  }, finally = {
    # Always clean up connection
    if (!is.null(con)) {
      dbDisconnect(con)
      message("DuckDB connection closed")
    }
  })

  return(result)
}

#' Execute DuckDB query against Tutor Exercises S3 parquet file
#'
#' Core implementation for querying Tutor Exercises parquet file in S3 using
#' DuckDB. This function handles all aspects of S3 parquet access including
#' connection setup, memory configuration, and query execution with automatic
#' optimization.
#'
#' @param sql_query A character string containing the SQL query to execute
#'   against the Tutor Exercises dataset.
#'
#' @return A data.frame containing the query results.
#'
#' @details
#' This function queries a single unified parquet file containing all Tutor
#' Exercises data. Unlike query_tutor_with_duckdb which handles multiple
#' year-based files, this function works with one file covering all dates.
#'
#' Features:
#' - Connects to DuckDB with httpfs extension for S3 access
#' - Configures memory limits (5GB for DuckDB, 3GB reserved for system)
#' - Creates 'exercises_data' view over the S3 parquet file
#' - Uses chunked processing for large result sets
#' - Monitors memory usage throughout execution
#'
#' S3 Configuration:
#' - Bucket: openstax-enclave-data
#' - Path: tutor/v1/openstax_tutor_exercises.parquet
#' - Region: us-west-2
#'
#' @examples
#' \dontrun{
#' # Direct usage (typically called via query_tutor_exercises)
#' exercises_data <- query_tutor_exercises_with_duckdb(
#'   "SELECT user_id, COUNT(*) FROM exercises_data GROUP BY user_id"
#' )
#' }
#'
#' @seealso \code{\link{query_tutor_exercises}} for high-level interface
query_tutor_exercises_with_duckdb <- function(sql_query) {
  message("Using DuckDB approach for memory-efficient querying")

  # Construct S3 file path for the single exercises parquet file
  bucket <- "openstax-enclave-data"
  filename <- "openstax_tutor_assessment_content_exercises.parquet"
  s3_path <- paste0("s3://", bucket, "/tutor/v1/", filename)
  message("S3 path: ", s3_path)

  con <- NULL
  result <- NULL

  tryCatch({
    # Connect to DuckDB
    message("Connecting to DuckDB...")
    con <- dbConnect(duckdb::duckdb())

    # Install and load S3 extension
    message("Setting up S3 access...")
    dbExecute(con, "INSTALL httpfs;")
    dbExecute(con, "LOAD httpfs;")

    # Configure S3 settings for DuckDB
    aws_region <- "us-west-2"
    dbExecute(con, paste0("SET s3_region='", aws_region, "';"))
    dbExecute(con, paste0("SET s3_endpoint='s3.", aws_region, ".amazonaws.com';"))
    dbExecute(con, "SET s3_url_style=\'path\';")
    dbExecute(con, paste0("CREATE OR REPLACE SECRET s3_creds (",
      "TYPE s3,",
      "PROVIDER credential_chain,",
      "REGION '", aws_region, "'",
    ");"))
    message('Created DuckDB secret for S3 using default credential chain')
    dbExecute(con, "SET s3_use_ssl=true;")

    # Configure memory settings for 8GB container
    message("Configuring memory limits...")
    dbExecute(con, "SET memory_limit='5GB';")
    dbExecute(con, "SET max_memory='5GB';")
    dbExecute(con, "SET temp_directory='/tmp';")
    dbExecute(con, "SET threads=4;")
    dbExecute(con, "SET preserve_insertion_order=false;")
    dbExecute(con, "SET enable_progress_bar=false;")

    # Create a view that references the S3 parquet file
    message("Creating view for S3 parquet file...")
    create_view_sql <- paste0(
      "CREATE VIEW exercises_data AS ",
      "SELECT * FROM read_parquet('", s3_path, "')"
    )
    dbExecute(con, create_view_sql)

    # Get row count for monitoring
    row_count <- dbGetQuery(con,
                           "SELECT COUNT(*) as total_rows FROM exercises_data")
    message("Total rows available: ", row_count$total_rows)

    # Check if query might return large result set
    message("Analyzing query for memory optimization...")
    is_large_query <- check_if_large_query(sql_query)

    start_time <- Sys.time()
    if (is_large_query) {
      message("Large query detected - using chunked result collection...")
      result <- execute_chunked_query(con, sql_query, "exercises_data")
    } else {
      message("Executing query against parquet file...")
      monitor_r_memory("before query execution")
      result <- dbGetQuery(con, sql_query)
      monitor_r_memory("after query execution")
    }
    end_time <- Sys.time()

    query_time <- round(difftime(end_time, start_time, units = "secs"), 2)
    message("Query completed in ", query_time, " seconds")
    message("Result rows: ", nrow(result))
    message("Result columns: ", ncol(result))

  }, error = function(e) {
    message("Error in query_tutor_exercises_with_duckdb: ", e$message)
    message("S3 path attempted: ", s3_path)
    stop(e)
  }, finally = {
    # Always clean up connection
    if (!is.null(con)) {
      dbDisconnect(con)
      message("DuckDB connection closed")
    }
  })

  return(result)
}

#' Execute DuckDB query against Tutor Notes and Highlights S3 parquet file
#'
#' Core implementation for querying Tutor Notes and Highlights parquet file
#' in S3 using DuckDB. This function handles all aspects of S3 parquet access
#' including connection setup, memory configuration, and query execution.
#'
#' @param sql_query A character string containing the SQL query to execute
#'   against the Tutor Notes and Highlights dataset.
#'
#' @return A data.frame containing the query results.
#'
#' @details
#' This function queries a single unified parquet file containing all Tutor
#' Notes and Highlights data from the Tutor platform.
#'
#' Features:
#' - Connects to DuckDB with httpfs extension for S3 access
#' - Configures memory limits (5GB for DuckDB, 3GB reserved for system)
#' - Creates 'tutor_notes_highlights' view over the S3 parquet file
#' - Uses chunked processing for large result sets
#' - Monitors memory usage throughout execution
#'
#' S3 Configuration:
#' - Bucket: openstax-enclave-data
#' - Path: tutor/v1/openstax_tutor_notes_and_highlights.parquet
#' - Region: us-west-2
#'
#' @examples
#' \dontrun{
#' # Direct usage (typically called via query_tutor_notes_and_highlights)
#' notes_data <- query_tutor_notes_and_highlights_with_duckdb(
#'   "SELECT COUNT(*) FROM tutor_notes_highlights WHERE type = 'note'"
#' )
#' }
#'
#' @seealso \code{\link{query_tutor_notes_and_highlights}} for interface
query_tutor_notes_and_highlights_with_duckdb <- function(sql_query) {
  message("Using DuckDB approach for memory-efficient querying")

  # Construct S3 file path for the notes and highlights parquet file
  bucket <- "openstax-enclave-data"
  filename <- "openstax_tutor_highlights_notes.parquet"
  s3_path <- paste0("s3://", bucket, "/tutor/v1/", filename)
  message("S3 path: ", s3_path)

  con <- NULL
  result <- NULL

  tryCatch({
    # Connect to DuckDB
    message("Connecting to DuckDB...")
    con <- dbConnect(duckdb::duckdb())

    # Install and load S3 extension
    message("Setting up S3 access...")
    dbExecute(con, "INSTALL httpfs;")
    dbExecute(con, "LOAD httpfs;")

    # Configure S3 settings for DuckDB
    aws_region <- "us-west-2"
    dbExecute(con, paste0("SET s3_region='", aws_region, "';"))
    dbExecute(con, paste0("SET s3_endpoint='s3.", aws_region, ".amazonaws.com';"))
    dbExecute(con, "SET s3_url_style=\'path\';")
    dbExecute(con, paste0("CREATE OR REPLACE SECRET s3_creds (",
      "TYPE s3,",
      "PROVIDER credential_chain,",
      "REGION '", aws_region, "'",
    ");"))
    message('Created DuckDB secret for S3 using default credential chain')
    dbExecute(con, "SET s3_use_ssl=true;")

    # Configure memory settings for 8GB container
    message("Configuring memory limits...")
    dbExecute(con, "SET memory_limit='5GB';")
    dbExecute(con, "SET max_memory='5GB';")
    dbExecute(con, "SET temp_directory='/tmp';")
    dbExecute(con, "SET threads=4;")
    dbExecute(con, "SET preserve_insertion_order=false;")
    dbExecute(con, "SET enable_progress_bar=false;")

    # Create a view that references the S3 parquet file
    message("Creating view for S3 parquet file...")
    create_view_sql <- paste0(
      "CREATE VIEW tutor_notes_highlights AS ",
      "SELECT * FROM read_parquet('", s3_path, "')"
    )
    dbExecute(con, create_view_sql)

    # Get row count for monitoring
    row_count <- dbGetQuery(con,
                           "SELECT COUNT(*) as total_rows FROM tutor_notes_highlights")
    message("Total rows available: ", row_count$total_rows)

    # Check if query might return large result set
    message("Analyzing query for memory optimization...")
    is_large_query <- check_if_large_query(sql_query)

    start_time <- Sys.time()
    if (is_large_query) {
      message("Large query detected - using chunked result collection...")
      result <- execute_chunked_query(con, sql_query, "tutor_notes_highlights")
    } else {
      message("Executing query against parquet file...")
      monitor_r_memory("before query execution")
      result <- dbGetQuery(con, sql_query)
      monitor_r_memory("after query execution")
    }
    end_time <- Sys.time()

    query_time <- round(difftime(end_time, start_time, units = "secs"), 2)
    message("Query completed in ", query_time, " seconds")
    message("Result rows: ", nrow(result))
    message("Result columns: ", ncol(result))

  }, error = function(e) {
    message("Error in query_tutor_notes_and_highlights_with_duckdb: ", e$message)
    message("S3 path attempted: ", s3_path)
    stop(e)
  }, finally = {
    # Always clean up connection
    if (!is.null(con)) {
      dbDisconnect(con)
      message("DuckDB connection closed")
    }
  })

  return(result)
}

#' Monitor R process memory usage with warnings for high usage
#'
#' Monitors the current R process memory usage and provides warnings when
#' memory consumption becomes high. This is essential for container environments
#' with limited memory (8GB) to prevent out-of-memory errors during large
#' data processing operations.
#'
#' @param stage A character string describing the current processing stage
#'   for logging purposes. This helps identify which operations are consuming
#'   the most memory. Default is an empty string.
#'   Examples: "before query", "after chunk 5", "data processing complete"
#'
#' @return On Linux systems, returns the memory usage in GB as a numeric value.
#'   On non-Linux systems, returns NULL but still logs memory estimates.
#'
#' @details
#' Memory monitoring approach:
#' - Linux: Reads /proc/self/status for accurate RSS (Resident Set Size) memory
#' - Non-Linux: Uses R's gc() function for rough memory estimates
#' - Threshold: Issues warnings when memory usage exceeds 6GB
#' - Container limit: Designed for 8GB containers with 3GB reserved for system
#'
#' The function provides detailed logging of memory usage and automatically
#' issues warnings when memory consumption approaches dangerous levels.
#' This is crucial for preventing container crashes during large data operations.
#'
#' @examples
#' \dontrun{
#' # Monitor memory before starting a large operation
#' initial_memory <- monitor_r_memory("before large query")
#'
#' # Check memory usage after processing chunks
#' for (i in 1:10) {
#'   # ... process data chunk ...
#'   current_memory <- monitor_r_memory(paste("after chunk", i))
#'   if (!is.null(current_memory) && current_memory > 7) {
#'     warning("Memory usage too high, stopping processing")
#'     break
#'   }
#' }
#' }
#'
#' @seealso \code{\link{execute_chunked_query}} for memory-safe query processing
monitor_r_memory <- function(stage = "") {
  # Get R process memory usage
  if (Sys.info()["sysname"] == "Linux") {
    tryCatch({
      status_file <- "/proc/self/status"
      if (file.exists(status_file)) {
        status_lines <- readLines(status_file)
        vm_rss <- grep("VmRSS:", status_lines, value = TRUE)
        if (length(vm_rss) > 0) {
          memory_kb <- as.numeric(gsub(".*?([0-9]+).*", "\\1", vm_rss))
          memory_gb <- round(memory_kb / 1024 / 1024, 2)
          message("R process memory usage ", stage, ": ", memory_gb, " GB")

          # Warning if memory usage is high
          if (memory_gb > 6) {
            warning("High memory usage detected: ", memory_gb, " GB")
          }
          return(memory_gb)
        }
      }
    }, error = function(e) {
      message("Could not read memory usage: ", e$message)
    })
  } else {
    # Fallback for non-Linux systems
    gc_info <- gc()
    used_mb <- sum(gc_info[, "used"]) * 8 / 1024  # Rough estimate
    message("R memory estimate ", stage, ": ", round(used_mb, 1), " MB")
  }
}

#' Analyze SQL query to determine if it will return a large result set
#'
#' Uses heuristics to analyze a SQL query and predict whether it will return
#' a large result set that might cause memory issues. This analysis is used
#' to automatically switch to chunked processing for memory safety.
#'
#' @param sql_query A character string containing the SQL query to analyze.
#'   The query is converted to lowercase for case-insensitive analysis.
#'
#' @return A logical value:
#'   \code{TRUE} if the query is predicted to return a large result set
#'   \code{FALSE} if the query is predicted to return a manageable result set
#'
#' @details
#' Query analysis heuristics:
#' - Queries with LIMIT <= 1,000,000 rows are considered small
#' - Queries with aggregation functions (COUNT, SUM, AVG, MAX, MIN, GROUP BY)
#'   are considered small as they typically reduce data volume
#' - Queries without LIMIT and without aggregation are considered potentially large
#' - The 1M row threshold balances memory safety with processing efficiency
#'
#' This function helps the query engine automatically choose the appropriate
#' processing strategy:
#' - Small queries: Direct execution with dbGetQuery()
#' - Large queries: Chunked execution with execute_chunked_query()
#'
#' @examples
#' \dontrun{
#' # These would be classified as small queries (FALSE)
#' check_if_large_query("SELECT COUNT(*) FROM highlights")
#' check_if_large_query("SELECT * FROM highlights LIMIT 1000")
#' check_if_large_query("SELECT book_id, AVG(length) FROM highlights GROUP BY book_id")
#'
#' # These would be classified as large queries (TRUE)
#' check_if_large_query("SELECT * FROM highlights")
#' check_if_large_query("SELECT * FROM highlights WHERE created_at > '2020-01-01'")
#' check_if_large_query("SELECT * FROM highlights LIMIT 5000000")
#' }
#'
#' @seealso \code{\link{execute_chunked_query}} for handling large query results
check_if_large_query <- function(sql_query) {
  # Simple heuristics to detect potentially large queries
  sql_lower <- tolower(sql_query)

  # If query has LIMIT with small number, it's not large
  if (grepl("limit\\s+[0-9]+", sql_lower)) {
    limit_match <- regmatches(sql_lower, regexpr("limit\\s+([0-9]+)", sql_lower))
    if (length(limit_match) > 0) {
      limit_num <- as.numeric(gsub("limit\\s+", "", limit_match))
      if (limit_num <= 1000000) {  # Less than 1M rows is considered small
        return(FALSE)
      }
    }
  }

  # If no LIMIT clause and no aggregation, assume potentially large
  has_aggregation <- grepl("(count|sum|avg|max|min|group\\s+by)", sql_lower)
  has_limit <- grepl("limit", sql_lower)

  return(!has_limit && !has_aggregation)
}

#' Execute SQL query with chunked result collection for memory safety
#'
#' Executes a SQL query and collects results in chunks to prevent memory
#' exhaustion when dealing with large result sets. This function is automatically
#' used for queries predicted to return large amounts of data.
#'
#' @param con A database connection object (DBI connection)
#'   from dbConnect(), typically a DuckDB connection for S3 parquet access.
#'
#' @param sql_query A character string containing the SQL query to execute.
#'   The query should be valid for the connected database system.
#'
#' @param table_name A character string with the table name for logging purposes.
#'   Used in log messages to identify which table is being processed.
#'
#' @return A data.frame containing all query results, combined from chunks.
#'   Returns an empty data.frame if no results are found.
#'
#' @details
#' Chunked processing strategy:
#' - Chunk size: 100,000 rows per chunk (balance of memory and efficiency)
#' - Memory monitoring: Checks memory usage after each chunk
#' - Safety threshold: Stops processing if memory usage exceeds 7GB
#' - Garbage collection: Forces gc() between chunks to free memory
#' - Error handling: Automatically cleans up database resources on failure
#'
#' Memory management:
#' - Monitors memory before, during, and after chunk processing
#' - Automatically stops if memory usage becomes dangerous (>7GB)
#' - Combines chunks efficiently using rbind()
#' - Clears intermediate chunk data to free memory
#'
#' The function provides detailed logging of progress including:
#' - Chunk number and row counts
#' - Memory usage after each chunk
#' - Total processing time and final result dimensions
#'
#' @examples
#' \dontrun{
#' # Typically called automatically by query functions, but can be used directly:
#' con <- dbConnect(duckdb::duckdb())
#' # ... setup connection and views ...
#' large_result <- execute_chunked_query(
#'   con,
#'   "SELECT * FROM large_table WHERE condition = 'value'",
#'   "large_table"
#' )
#' dbDisconnect(con)
#' }
#'
#' @seealso \code{\link{check_if_large_query}} for query size prediction,
#'   \code{\link{monitor_r_memory}} for memory monitoring
execute_chunked_query <- function(con, sql_query, table_name) {
  message("Executing query with chunked result collection...")
  monitor_r_memory("before chunked execution")

  tryCatch({
    # Send query without fetching results yet
    result_set <- dbSendQuery(con, sql_query)

    chunks <- list()
    chunk_size <- 100000  # 100K rows per chunk
    chunk_num <- 1
    total_rows <- 0

    repeat {
      message("Fetching chunk ", chunk_num, "...")
      chunk <- dbFetch(result_set, n = chunk_size)

      if (nrow(chunk) == 0) {
        break  # No more data
      }

      chunks[[chunk_num]] <- chunk
      total_rows <- total_rows + nrow(chunk)
      chunk_num <- chunk_num + 1

      # Monitor memory after each chunk
      current_memory <- monitor_r_memory(paste("after chunk", chunk_num - 1))

      # Stop if memory usage gets too high
      if (!is.null(current_memory) && current_memory > 7) {
        warning("Memory usage too high (", current_memory, " GB), stopping chunked collection")
        break
      }

      # Force garbage collection between chunks
      gc()
    }

    # Clear the result set
    dbClearResult(result_set)

    # Combine chunks efficiently
    message("Combining ", length(chunks), " chunks with total ", total_rows, " rows...")
    if (length(chunks) == 0) {
      result <- data.frame()
    } else if (length(chunks) == 1) {
      result <- chunks[[1]]
    } else {
      result <- do.call(rbind, chunks)
    }

    # Clear chunks from memory
    chunks <- NULL
    gc()

    monitor_r_memory("after chunked execution complete")

    return(result)

  }, error = function(e) {
    # Clean up on error
    if (exists("result_set")) {
      try(dbClearResult(result_set), silent = TRUE)
    }
    stop("Chunked query execution failed: ", e$message)
  })
}

#' Main function to query large parquet files efficiently using DuckDB
#'
#' High-level interface for querying large parquet files in S3 using DuckDB.
#' This function serves as the primary entry point for S3-based data access,
#' providing memory-efficient querying with automatic optimization.
#'
#' @param table A character string specifying the table name to query.
#'   This corresponds to the dataset name and is used to construct the
#'   S3 path pattern. Example: "highlights" for Notes & Highlights data.
#'
#' @param sql_query A character string containing the SQL query to execute.
#'   The query should reference the table name specified in the table parameter.
#'   Example: "SELECT * FROM highlights WHERE created_at > '2023-01-01'"
#'
#' @return A data.frame containing the query results. Large result sets are
#'   automatically processed in chunks to prevent memory issues.
#'
#' @details
#' This function orchestrates the complete parquet querying process:
#' - Delegates to query_with_duckdb() for the actual implementation
#' - Provides consistent logging and error handling
#' - Serves as the main entry point for all parquet-based queries
#'
#' The function automatically handles:
#' - DuckDB connection management
#' - S3 authentication and configuration
#' - Memory optimization and chunked processing
#' - Performance monitoring and logging
#'
#' @examples
#' \dontrun{
#' # Query highlights data with basic filtering
#' recent_highlights <- query_large_parquet_files(
#'   "highlights",
#'   "SELECT * FROM highlights WHERE created_at > '2023-01-01' LIMIT 10000"
#' )
#'
#' # Query with aggregation for better performance
#' book_stats <- query_large_parquet_files(
#'   "highlights",
#'   "SELECT book_id, COUNT(*) as count FROM highlights GROUP BY book_id"
#' )
#' }
#'
#' @seealso \code{\link{query_with_duckdb}} for implementation details
query_large_parquet_files <- function(table, sql_query) {
  message("Starting memory-efficient parquet query using DuckDB")
  message("Table: ", table)
  message("SQL Query: ", sql_query)

  return(query_with_duckdb(table, sql_query))
}

#' Execute DuckDB query against S3 parquet files with memory optimization
#'
#' Core implementation for querying parquet files in S3 using DuckDB.
#' This function handles all aspects of S3 parquet access including connection
#' setup, memory configuration, and query execution with automatic optimization.
#'
#' @param table A character string specifying the table/dataset name.
#'   Used to construct the S3 path pattern for the parquet files.
#'   Must correspond to a valid dataset in the openstax-enclave-data-prod bucket.
#'
#' @param sql_query A character string containing the SQL query to execute.
#'   The query will be executed against a view created from the S3 parquet files.
#'
#' @return A data.frame containing the query results. Returns an empty
#'   data.frame if no rows match the query criteria.
#'
#' @details
#' Complete DuckDB setup and configuration:
#'
#' Connection and Extensions:
#' - Creates new DuckDB connection for each query (ensures clean state)
#' - Installs and loads httpfs extension for S3 access
#' - Configures SSL and regional settings for optimal S3 performance
#'
#' Memory Configuration (optimized for 8GB containers):
#' - memory_limit: 5GB (leaves 3GB for system and R)
#' - max_memory: 5GB (hard limit)
#' - temp_directory: /tmp (allows disk spilling for large operations)
#' - threads: 4 (prevents CPU oversubscription)
#' - preserve_insertion_order: false (memory optimization)
#' - enable_progress_bar: false (reduces overhead)
#'
#' S3 Configuration:
#' - Supports AWS credentials from environment variables or credential chain
#' - Uses us-east-2 as default region
#' - Enables SSL for secure S3 access
#' - Constructs paths for combined*.parquet files in prod bucket
#'
#' Query Optimization:
#' - Creates a view over S3 parquet files for natural SQL syntax
#' - Analyzes query complexity to choose processing strategy
#' - Uses chunked processing for large result sets
#' - Monitors memory usage throughout execution
#' - Provides detailed timing and result set information
#'
#' Error Handling:
#' - Comprehensive try-catch with cleanup
#' - Automatic connection cleanup in finally block
#' - Detailed error messages with S3 path information
#'
#' @examples
#' \dontrun{
#' # Direct usage (typically called via query_large_parquet_files)
#' highlights_data <- query_with_duckdb(
#'   "highlights",
#'   "SELECT book_id, COUNT(*) FROM highlights GROUP BY book_id"
#' )
#' }
#'
#' @seealso \code{\link{query_large_parquet_files}} for high-level interface,
#'   \code{\link{execute_chunked_query}} for large result processing
query_with_duckdb <- function(table, sql_query) {
  message("Using DuckDB approach for memory-efficient querying")

  con <- NULL
  result <- NULL

  tryCatch({
    # Connect to DuckDB
    message("Connecting to DuckDB...")
    con <- dbConnect(duckdb::duckdb())

    # Install and load S3 extension
    message("Setting up S3 access...")
    dbExecute(con, "INSTALL httpfs;")
    dbExecute(con, "LOAD httpfs;")

    # Configure S3 settings for DuckDB
    # Try to use AWS credentials from environment or default credential chain
    aws_access_key <- Sys.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret_key <- Sys.getenv("AWS_SECRET_ACCESS_KEY", "")
    aws_region <- Sys.getenv("AWS_DEFAULT_REGION", "us-east-2")

    if (aws_access_key != "" && aws_secret_key != "") {
      message("Using AWS credentials from environment variables")
      dbExecute(con, paste0("SET s3_access_key_id='", aws_access_key, "';"))
      dbExecute(con, paste0("SET s3_secret_access_key='", aws_secret_key, "';"))
    } else {
      message("No AWS credentials found in environment - using default credential chain")
      # DuckDB will try to use default AWS credential chain
    }

    dbExecute(con, paste0("SET s3_region='", aws_region, "';"))
    dbExecute(con, "SET s3_use_ssl=true;")

    # Configure aggressive memory settings for 8GB container (leave 3GB for system)
    message("Configuring memory limits...")
    dbExecute(con, "SET memory_limit='5GB';")
    dbExecute(con, "SET max_memory='5GB';")
    dbExecute(con, "SET temp_directory='/tmp';")  # Use disk for spilling
    dbExecute(con, "SET threads=4;")  # Limit CPU usage
    dbExecute(con, "SET preserve_insertion_order=false;")  # Memory optimization
    dbExecute(con, "SET enable_progress_bar=false;")  # Reduce overhead

    # Construct S3 path pattern for combined*.parquet files
    bucket <- "openstax-enclave-data"
    key_pattern <- paste0("notes_and_highlights/highlights-prod-s3-export/highlights/public.",
                         table, "/1/combined*.parquet")
    s3_pattern <- paste0("s3://", bucket, "/", key_pattern)
    message("S3 pattern: ", s3_pattern)

    # Create a view that references the S3 parquet files directly
    # Use the table name as the view name so queries can reference it naturally
    message("Creating view for S3 parquet files...")
    create_view_sql <- paste0(
      "CREATE VIEW ", table, " AS ",
      "SELECT * FROM read_parquet('", s3_pattern, "')"
    )
    dbExecute(con, create_view_sql)

    # Get row count for monitoring
    row_count <- dbGetQuery(con, paste0("SELECT COUNT(*) as total_rows FROM ", table))
    message("Total rows available: ", row_count$total_rows)

    # Check if query might return large result set and use chunked collection
    message("Analyzing query for memory optimization...")
    is_large_query <- check_if_large_query(sql_query)

    start_time <- Sys.time()
    if (is_large_query) {
      message("Large query detected - using chunked result collection...")
      result <- execute_chunked_query(con, sql_query, table)
    } else {
      message("Executing query against parquet files...")
      monitor_r_memory("before query execution")
      result <- dbGetQuery(con, sql_query)
      monitor_r_memory("after query execution")
    }
    end_time <- Sys.time()

    query_time <- round(difftime(end_time, start_time, units = "secs"), 2)
    message("Query completed in ", query_time, " seconds")
    message("Result rows: ", nrow(result))
    message("Result columns: ", ncol(result))

  }, error = function(e) {
    message("Error in DuckDB approach: ", e$message)
    message("S3 Pattern attempted: ", s3_pattern)

    stop(e)
  }, finally = {
    # Always clean up connection
    if (!is.null(con)) {
      dbDisconnect(con)
      message("DuckDB connection closed")
    }
  })

  return(result)
}

#' System-wide memory monitoring utility
#'
#' Monitors overall system memory availability, complementing the R-specific
#' memory monitoring provided by monitor_r_memory(). This function provides
#' system-level memory information useful for understanding container resource usage.
#'
#' @return No return value. Outputs memory status information via message().
#'
#' @details
#' Memory monitoring approach:
#' - Linux: Reads /proc/meminfo for system-wide memory availability
#' - Non-Linux: Provides fallback message suggesting system tools
#'
#' On Linux systems, this function reads the MemAvailable field from /proc/meminfo,
#' which represents the amount of memory available for starting new applications
#' without swapping. This is different from free memory as it includes reclaimable
#' memory like caches.
#'
#' This function is useful for:
#' - Understanding overall container memory pressure
#' - Monitoring system memory alongside R process memory
#' - Debugging memory-related performance issues
#' - Validating container resource allocation
#'
#' @examples
#' \dontrun{
#' # Check system memory status
#' monitor_memory()
#'
#' # Use in combination with R memory monitoring
#' monitor_r_memory("before operation")
#' monitor_memory()
#' # ... perform memory-intensive operation ...
#' monitor_r_memory("after operation")
#' monitor_memory()
#' }
#'
#' @seealso \code{\link{monitor_r_memory}} for R-specific memory monitoring
monitor_memory <- function() {
  if (Sys.info()["sysname"] == "Linux") {
    # Linux memory info
    mem_info <- readLines("/proc/meminfo")
    available <- grep("MemAvailable", mem_info, value = TRUE)
    message("Memory status: ", available)
  } else {
    # Fallback for other systems
    message("Memory monitoring: Use system tools to monitor memory usage")
  }
}