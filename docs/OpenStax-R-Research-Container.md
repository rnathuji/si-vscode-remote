# OpenStax R Research Image

https://github.com/safeinsights/openstax-research-image/r-4.5.1

**Version**: 4.5.1

## OpenStax Dataset Connection Functions

This section explains how to use the R functions in `openstax.R` to access OpenStax educational datasets for your research.

### 3. Tutor Data

```r
# Get something from Tutor
tutor_data <- query_optimized_tutor("SELECT * FROM tutor_table")
```

### Notes & Highlights Data

```r
# Get highlights with basic filtering
highlights <- query_notes_and_highlights(
  "SELECT * FROM highlights WHERE book_id = 'biology-2e' LIMIT 10000"
)

# Get summary statistics (recommended for large datasets)
summary_stats <- query_notes_and_highlights(
  "SELECT book_id, COUNT(*) as total_highlights, AVG(LENGTH(content)) as avg_length
   FROM highlights
   WHERE created_at > '2023-01-01'
   GROUP BY book_id"
)
```

### 2. Event Capture Data

```r
# Get page view events for a specific week
page_views <- query_event_capture(
  event = "page_view",
  start_date = "2023-01-01",
  end_date = "2023-01-07",
  sql_query = "SELECT * FROM events WHERE book_id = 'biology-2e'"
)

# Get daily highlight creation counts for a month
daily_highlights <- query_event_capture(
  event = "highlight_created",
  start_date = "2023-01-01",
  end_date = "2023-01-31",
  sql_query = "SELECT DATE(timestamp) as day, COUNT(*) as count
               FROM events
               GROUP BY DATE(timestamp)
               ORDER BY day"
)
```

## Example Research Workflow

```r
source("openstax.R")
source("safeinsights_common.R")

###############################################################################
# Initialize Research Container
toa_job_start()

###############################################################################
# Insert query code here

# OpenStax Tutor Example:
dataframe <- query_openstax_tutor("SELECT * FROM student_activity LIMIT 10")

###############################################################################
# Insert manipulate data and analysis code here

write.csv(dataframe, "results.csv", row.names = FALSE)

###############################################################################
# Upload results
toa_results_upload("results.csv")
```

## Installed R Packages and Versions

ToDo: Verify versions of packages

### Core packages
- httr 1.4.7
- RPostgres 1.4.7
- paws 0.7.0
- sqldf 0.4-11
- duckdb 1.1.1
- DBI 1.2.3
- here 1.0.1
- qualtRics 1.4.0

### Visualization and manipulation packages
- ggplot2 3.4.3
- dplyr 1.1.4
- tidyr 1.3.1
- readr 2.1.5
- purrr 1.0.1
- tibble 3.2.1
- forcats 1.0.0
- lubridate 1.9.2
- jsonlite 1.8.8
- XML 3.99-0.12
- stringr 1.5.0
- stringi 1.8.4

### Additional visualization packages
- ggvis 0.4.8
- rgl 1.2.8
- patchwork 1.2.0
- janitor 2.2.0
- magrittr 2.0.3
- vcd 1.4-12
- maps 3.4.2
- ggmap 4.0.0
- sp 2.1-4
- maptools 1.1-8

### Modeling packages
- tidymodels 1.0.0
- future 1.33.2
- furrr 0.3.1
- lme4 1.1-35.5
- nlme 3.1-164
- caret 6.0-94
- survival 3.5-8
- car 3.1-2
- psych 2.4.6.26
- mice 3.16.0
- miceadds 3.17-44
- glmnet 4.1-8

### Reporting packages
- rmarkdown 2.26
- shiny 1.10.0
- xtable 1.8-4
- knitr 1.47
- kableExtra 1.4.0
- excluder 0.5.1
- careless 1.2.2

### Time series packages
- zoo 1.8-12
- xts 0.13-1
- quantmod 0.4-20
- fable 0.3.4
- tsibble 1.1.4
- urca 1.3-4
- feasts 0.3.2

### High performance and development packages
- Rcpp 1.0.11
- data.table 1.14.8
- testthat 3.2.1.1
- devtools 2.4.5
- roxygen2 7.3.2
- lintr 3.1.2
- languageserver 0.3.16
