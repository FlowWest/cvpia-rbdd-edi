library(EMLaide)
library(dplyr)
library(readxl)
library(EML)
library(httr)

# Pull EDI user credentials from system environment ----------------------------
user_id <- Sys.getenv("user_id")
password <- Sys.getenv("password")

# DEFINE ALL DATA PACKAGE ELEMENTS ---------------------------------------------
datatable_metadata <-
  dplyr::tibble(filepath = c("data/catch.csv",
                             "data/trap.csv",
                             "data/recapture.csv",
                             "data/release.csv",
                             "data/release_fish.csv"),
                attribute_info = c("data-raw/metadata_generated_by_flowwest/catch-metadata.xlsx",
                                   "data-raw/metadata_generated_by_flowwest/trap-metadata.xlsx",
                                   "data-raw/metadata_generated_by_flowwest/recaptures-metadata.xlsx",
                                   "data-raw/metadata_generated_by_flowwest/releases-metadata.xlsx",
                                   "data-raw/metadata_generated_by_flowwest/releases-fish-metadata.xlsx"),
                datatable_description = c("Daily catch data",
                                          "Daily trap operations and environmental data",
                                          "Recaptured catch from efficiency trials",
                                          "Release trial overview data",
                                          "Individual data on released fish"),
                datatable_url = paste0("https://raw.githubusercontent.com/FlowWest/rbdd-rst-edi/main/data/",
                                       c("catch.csv",
                                         "trap.csv",
                                         "recapture.csv",
                                         "release.csv",
                                         "data/release_fish.csv"))
                )
# save cleaned data to `data/`
excel_path <- "data-raw/RBDD_RST_DRAFT_Metadata_form_022823.xlsx"
sheets <- readxl::excel_sheets(excel_path)
metadata <- lapply(sheets, function(x) readxl::read_excel(excel_path, sheet = x))
names(metadata) <- sheets

abstract_docx <- "data-raw/RBDD_RST_Abstract_022823.docx"
methods_docx <- "data-raw/methods_link.md"

# GET EDI NUMBER
# TODO first version always intiated manually,
# add into log or figure out way to do this manually
vl <- readr::read_csv("data-raw/version_log.csv", col_types = c('c', "D"))
if (nrow(vl) == 0) {
  # reserve ID, and define below
  current_edi_number <- reserve_edi_id(user_id = user_id,
                                       password = password,
                                       environment = "staging")
  new_row <- data.frame(
    edi_version = current_edi_number,
    date = as.character(Sys.Date())
  )
} else {
  previous_edi_number <- tail(vl['edi_version'], n=1)
  identifier <- unlist(strsplit(previous_edi_number$edi_version, "\\."))[2]
  previous_edi_ver <- as.numeric(stringr::str_extract(previous_edi_number, "[^.]*$"))
  current_edi_ver <- as.character(previous_edi_ver + 1)
  current_edi_number <- paste0("edi.", identifier, ".", current_edi_ver)

  new_row <- data.frame(
    edi_version = current_edi_number,
    date = as.character(Sys.Date())
  )
}

vl <- bind_rows(vl, new_row)
readr::write_csv(vl, "data-raw/version_log.csv")

dataset <- list() %>%
  add_pub_date() %>%
  add_title(metadata$title) %>%
  add_personnel(metadata$personnel) %>%
  add_keyword_set(metadata$keyword_set) %>%
  add_abstract(abstract_docx) %>%
  add_license(metadata$license) %>%
  add_method(methods_docx) %>%
  add_maintenance(metadata$maintenance) %>%
  add_project(metadata$funding) %>%
  add_coverage(metadata$coverage, metadata$taxonomic_coverage) %>%
  add_datatable(datatable_metadata)

# GO through and check on all units
custom_units <- data.frame(id = c("number of rotations", "NTU", "revolutions", "number of fish", "number of traps", "tubs", "unitless", "cubicFeet"),
                           unitType = c("dimensionless", "dimensionless", "dimensionless", "dimensionless", "dimensionless", "dimensionless", "dimensionless", "dimensionless"),
                           parentSI = c(NA, NA, NA, NA, NA, NA, NA, NA),
                           multiplierToSI = c(NA, NA, NA, NA, NA, NA, NA, NA),
                           description = c("number of rotations",
                                           "nephelometric turbidity units, common unit for measuring turbidity",
                                           "number of trap revolutions per minute",
                                           "number of fish counted",
                                           "number of traps fishing",
                                           "number of tubs of debris collected in trap",
                                           "no units associated with this numeric measure",
                                           "cubic feet of"))
#TODO check on
# cubic feet, cubic feet per seccond, unitless, feet, feet per second, celcius, cubic feet per second, grams

unitList <- EML::set_unitList(custom_units)

eml <- list(packageId = current_edi_number,
            system = "EDI",
            access = add_access(),
            dataset = dataset,
            additionalMetadata = list(metadata = list(unitList = unitList))
)

# Write eml document using eml list --------------------------------------------
EML::write_eml(eml, paste0(current_edi_number, ".xml"))

# Check for errors in EML document ---------------------------------------------
EML::eml_validate(paste0(current_edi_number, ".xml"))

# Call evaluation or update to EDI ---------------------------------------------
old_id <- previous_edi_number$edi_version
# Update on edi - call
# TODO update after fixing EMLaide - evaluate to remove view statement
# report_df <- EMLaide::evaluate_edi_package(user_id,
#                               password,
#                               eml_file_path = paste0(current_edi_number, ".xml"))
#
# if (any(report_df |> pull(Status) == "error")) {
#   stop("Your XML did not pass the EDI congruency checker, please check XML and try again")
# }
# EMLaide::upload_edi_package(user_id = Sys.getenv("user_id"),
#                             password = Sys.getenv("password"),
#                             eml_file_path = "edi.1026.1.xml",
#                             environment = "staging")
#
EMLaide::update_edi_package(user_id,
                            password,
                            existing_package_identifier = old_id,
                            eml_file_path = paste0(current_edi_number, ".xml"),
                            environment = "production")

