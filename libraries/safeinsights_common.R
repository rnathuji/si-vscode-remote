# safeinsights_common.R
# Functions to make a members' and researchers' life easier when creating their
# base researcher container and analysis code in R (respectively).

# Load required libraries
library(jsonlite)  # For JSON handling
library(httr)  # For sending files to an API

# Function to set Trusted Output App Endpoint and Credentials from the env
# Args: none
toa_set_endpoint_and_creds <- function() {
  # Set Trusted Output App API Endpoint
  trusted_output_endpoint <- Sys.getenv("TRUSTED_OUTPUT_ENDPOINT")

  # Retrieve the basic auth credentials from the environment variable
  auth_credentials <- Sys.getenv("TRUSTED_OUTPUT_BASIC_AUTH")

  # Check if environment variables are defined
  if (trusted_output_endpoint == "" || auth_credentials == "") {
    return(NULL)
  }

  # Split the credentials into username and password
  auth_parts <- strsplit(auth_credentials, ":", fixed = TRUE)[[1]]

  # Check if credentials were properly formatted
  if (length(auth_parts) < 2) {
    return(NULL)
  }

  username <- auth_parts[1]
  password <- auth_parts[2]

  return(list(endpoint =
    trusted_output_endpoint,
    username = username,
    password = password))
}

# Function to Tell the TOA The Research Analysis as started
# Args: none
initialize <- function() {

  # Set Trusted Output App API Endpoint from Function
  toa <- toa_set_endpoint_and_creds()

  # Check if credentials are available
  if (is.null(toa)) {
    message("Trusted Output App credentials not configured. Skipping initialization.")
    return(invisible(NULL))
  }

  response <- PUT(
    url = toa$endpoint,
    body = toJSON(list(status = "JOB-RUNNING"), auto_unbox = TRUE),
    encode = "json",
    authenticate(toa$username, toa$password),
    content_type_json()
  )

  response_content <- content(
    response,
    as = "parsed",
    type = "application/json")
  print(response_content)
}

# Function to upload results to Trusted Output App
# Args:
#   results_file: The file containing the results of the analysis
toa_results_upload <- function(results_file) {

  # Set Trusted Output App API Endpoint from Function
  toa <- toa_set_endpoint_and_creds()

  # Check if credentials are available
  if (is.null(toa)) {
    message("Trusted Output App credentials not configured. Skipping upload.")
    return(invisible(NULL))
  }

  response <- POST(
    url = paste0(toa$endpoint, "/upload"),
    body = list(file = upload_file(results_file)),  # Attach the results file
    encode = "multipart",  # Multipart form data encoding
    authenticate(toa$username, toa$password)
  )

  # DEBUG: Print the response content
  response_content <- content(response, as = "parsed", type = "application/json")
  print(response_content)

  # Check the API response
  if (response$status_code == 200) {
    print("File uploaded successfully.")
  } else {
    print(paste("File upload failed. Status code:", response$status_code))
  }
}
