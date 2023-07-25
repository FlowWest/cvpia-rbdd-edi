library(EMLaide)
library(dplyr)
library(readxl)
library(EML)
library(httr)

user_id = "srjpe"
password = "Srjpe2023!"

# token <- login(userId = "ipeng", userPass = "flow1234")

# identifier <- create_reservation(scope = "edi", env = "staging")
max_timeout <- 60

base_url <- "https://pasta-s.lternet.edu/package/reservations/eml/edi"

response <- httr::POST(url = base_url,
                       config = httr::authenticate(
                         paste0("uid=", user_id, ",o=EDI", ",dc=edirepository,dc=org"),
                         password
                       ), timeout = httr::timeout(max_timeout))
print(response)


# base_url <- "https://pasta-s.lternet.edu/package/"
# existing_package_identifier <- "edi.1365.2"
# eml_file_path <- paste0("edi",1365.3, ".xml")
# environment <- "staging"
# scope <- unlist(strsplit(eml_file_path, "\\."))[1]
# identifier <- unlist(strsplit(eml_file_path, "\\."))[2]
# revision <- unlist(strsplit(eml_file_path, "\\."))[3]
# response <- httr::POST(
#   url = paste0(base_url, "eml/"),
#   config = httr::authenticate(paste('uid=', user_id, ",o=EDI", ',dc=edirepository,dc=org'), password),
#   body = httr::upload_file(eml_file_path),
#   timeout = httr::timeout(max_timeout)
# )
