# OpenStax Research Libraries
# This file contains all library imports used by OpenStax research functions

# Core packages
library(httr)  # For sending files to an API
library(RPostgres) # For connecting to Postgres databases
library(paws)
library(sqldf)
library(duckdb)
library(DBI)
library(here)
# library(qualtRics)

# Visualization and manipulation packages
library(ggplot2)
library(dplyr) # For %>% operator and collect function
library(tidyr)
library(readr) # For writing CSV files
library(purrr)
library(tibble)
library(forcats)
library(lubridate)
library(jsonlite)
library(XML)
library(stringr)
library(stringi)

# Additional visualization packages
library(ggvis)
library(rgl)
library(patchwork)
library(janitor)
library(magrittr)
library(vcd)
library(maps)
library(ggmap)
library(sp)
# library(maptools)

# Modeling packages
library(tidymodels)
library(future)
library(furrr)
# library(lme4)
library(nlme)
library(caret)
library(survival)
# library(car)
library(psych)
# library(mice)
# library(miceadds)
library(glmnet)

# Reporting packages
library(rmarkdown)
library(shiny)
library(xtable)
library(knitr)
# library(kableExtra)
library(excluder)
library(careless)

# Time series packages
library(zoo)
library(xts)
library(quantmod)
library(fable)
library(tsibble)
library(urca)
library(feasts)

# High performance and development packages
library(Rcpp)
library(data.table)
library(testthat)
# library(devtools)
library(roxygen2)
library(lintr)
library(languageserver)
