library(EMLaide)
library(dplyr)
library(readxl)
library(readr)
library(EML)
library(httr)
library(lubridate)

# Update tables
# Get tables from blob storage - work with Inigo on developing pipeline and posting new tables in updated_tables_march

updated_catch <- read_csv("data-raw/updated_tables/rbdd_catch.csv") |>
  mutate(start_date = as_date(start_date, format = "%m/%d/%Y"),
         dead = ifelse(tolower(dead) == "yes", TRUE, FALSE),
         run = case_when(run == "F" ~ "fall run",
                         run == "L" ~ "late fall run",
                         run == "S" ~ "spring run",
                         run == "W" ~ "winter run",
                         run == "n/p" ~ "not recorded"),
         ad_pelvic = ifelse(mark_code == "Ad_pelvic", TRUE, FALSE),
         adipose_clipped = case_when(mark_code == "Adclipped" ~ TRUE,
                                     ad_pelvic ~ TRUE,
                                     T ~ FALSE),
         weight = ifelse(weight == 0, NA, weight),
         fork_length = ifelse(fork_length == 0, NA, fork_length),
         station_code = tolower(station_code)) |>
  select(-mark_code) |> glimpse()

updated_trap <- read_csv("data-raw/updated_tables/rbdd_trap.csv") |>
  mutate(start_date = as_date(start_date, format = "%m/%d/%Y"),
         weather = case_when(weather_code == "CLD" ~ "cloudy",
                             weather_code == "CLR" ~ "clear",
                             weather_code == "FOG" ~ "foggy",
                             weather_code == "RAN" ~ "rainy",
                             weather_code == "W" ~ "windy"),
         gear_condition = case_when(tolower(gear_condition) == "n" ~ "normal",
                                   tolower(gear_condition) == "pb" ~ "partial block",
                                   tolower(gear_condition) == "tb" ~ "total block",
                                   tolower(gear_condition) == "nr" ~ "not rotating",
                                   tolower(gear_condition) %in% c("n/p") ~ "not recorded"),
        river_depth = ifelse(river_depth > 9, 99, river_depth),
        station_code = tolower(station_code),
        temperature = ifelse(temperature > 1000, NA, temperature)) |>
  select(-weather_code) |> glimpse()

# TODO will want to update any that have a new table posted in blob

min_date_updated_catch <- min(updated_catch$start_date, na.rm = T)
min_date_updated_trap <- min(updated_trap$start_date, na.rm = T)

version <- 1
vl <- readr::read_csv("data-raw/version_log.csv", col_types = c('c', "D"))
previous_edi_number <- tail(vl['edi_version'], n=1)
identifier <- unlist(strsplit(previous_edi_number$edi_version, "\\."))[2]
version <- as.numeric(stringr::str_extract(previous_edi_number, "[^.]*$"))

# View existing tables
httr::GET(url = paste0("https://pasta.lternet.edu/package/name/eml/edi/", identifier, "/", version), handle = httr::handle(""))
# join existing tables with updated tables
existing_catch <- httr::GET(
  url = paste0("https://pasta.lternet.edu/package/data/eml/edi/", identifier, "/", version, "/58540ac4ed34ce05f3309510f4be91e5"),
  handle = httr::handle("")) |>
  httr::content() |>
  as_tibble() |>
  filter(start_date > min_date_updated_catch) |> glimpse()

existing_trap <- httr::GET(
  url = paste0("https://pasta.lternet.edu/package/data/eml/edi/", identifier, "/", version, "/eed3b61b7eb6030dafc9e4765f07a106"),
  handle = httr::handle("")) |>
  httr::content() |>
  as_tibble() |>
  filter(start_date > min_date_updated_trap) |> glimpse()

# append updated tables to existing data and save to data/tables
updated_catch <- bind_rows(existing_catch, updated_catch) |>  glimpse()
updated_trap <- bind_rows(existing_trap, updated_trap) |> glimpse()

#write csv
write_csv(updated_catch, "data/catch.csv")
write_csv(updated_trap, "data/trap.csv")

# Updated mark recap datasets
updated_release <- read_csv("data-raw/updated_tables/rbdd_release.csv") |>
  mutate(mark_date = as_date(mark_date, format = "%m/%d/%Y"),
         release_date = as_date(release_date, format = "%m/%d/%Y")) |> glimpse()

updated_release_fish <- read_csv("data-raw/updated_tables/rbdd_release_fish.csv") |> glimpse()

updated_recapture_sum <- read_csv("data-raw/updated_tables/rbdd_recapture.csv") |>
  mutate(station_code = tolower(station_code)) |> glimpse()

updated_recapture_fish <- read_csv("data-raw/updated_tables/rbdd_recapture_fish.csv")|>
  mutate(dead = ifelse(tolower(dead) == "yes", TRUE, FALSE))|> glimpse()

updated_recapture <- left_join(updated_recapture_fish, updated_recapture_sum,
                             by = c("mark_recap_row_id" = "recapture_row_id" )) |>
  select(mark_recap_id = recap_row_id, trial_id, sample_date, sample_time, station_code, flows, mark_code, fork_length, count, dead) |>
  filter(!is.na(sample_date)) |>
  glimpse()

min_date_release <- min(updated_release$release_date, na.rm = T)
min_date_recapture <- min(updated_recapture$sample_date, na.rm = T)

existing_release <- httr::GET(
  url = paste0("https://pasta.lternet.edu/package/data/eml/edi/", identifier, "/", version, "/414dd61cd26985641875fb194328f8a6"),
  handle = httr::handle("")) |>
  httr::content() |>
  as_tibble() |>
  filter(release_date > min_date_release) |> glimpse()

exisitng_release_fish <-  httr::GET(
  url = paste0("https://pasta.lternet.edu/package/data/eml/edi/", identifier, "/", version, "/f1649215c4114b74d964b825d6371b66"),
  handle = httr::handle("")) |>
  httr::content() |>
  as_tibble() |> glimpse()

existing_recapture <- httr::GET(
  url = paste0("https://pasta.lternet.edu/package/data/eml/edi/", identifier, "/", version, "/460853b8a4a0a2308c2bfb4d3dc2793c"),
  handle = httr::handle("")) |>
  httr::content() |>
  as_tibble() |>
  filter(sample_date > min_date_recapture) |> glimpse()

# TODO bind rows shows inconsistent col names from existing to updated = go in and
# update updated table naming to match existing after reading in updated tables (lines 82 - 100)
# append updated tables to existing data and save to data/tables
updated_release <- bind_rows(existing_release, updated_release) |>  glimpse()
updated_release_fish <- bind_rows(existing_release_fish, updated_release_fish) |> distinct() |> glimpse()
updated_recapture <- bind_rows(existing_recapture, updated_recapture) |> glimpse()

#write csv
write_csv(updated_release, "data/release.csv")
write_csv(updated_release_fish, "data/release_fish.csv")
write_csv(updated_recapture, "data/recapture.csv")

# TODO need to save and push to github before running make metadata script (so it
# can pull metadata of updated tables from github)

#run make xml script & update data package
source("data-raw/make_metadata_xml.R")

