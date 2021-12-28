library("dplyr")
# Downloading the cransays repository branch history
download.file("https://github.com/lockedata/cransays/archive/history.zip",
              destfile = "output/cransays-history.zip")
path_zip <- here::here("output", "cransays-history.zip")
# We unzip the files 
if (dir.exists("static")) {
  dat <- list.files("static/cransays-history/", full.names = TRUE)
} else {
  dat <- unzip(path_zip, exdir = "static")
}
setwd("static/cransays-history/")

# First two heading systems:
h <- system2("head", 
             "-1 cran-incoming_-*.csv | grep -v '==>'| grep -v '^$'",
             stdout = TRUE)
h <- strsplit(h, ",", fixed = TRUE)
weird_headers <- system2("ls", "cran-incoming_-*-*.csv", 
                         stdout = TRUE)
first_header <- weird_headers[lengths(h) == 11]
first_header <- paste0(first_header, collapse = " ")
# Create a single file with all the same heading buffer shouldn't go above 
# getconf ARG_MAX => 2097152 (both are now below 3000 and shouldn't increase)

system2("tail", paste0("-n +2 ", first_header, 
                       " | grep -v '==>'| grep -v '^$'> ../cran-incoming_-first.csv",
                       collapse = ""))
incoming_first <- read.csv("../cran-incoming_-first.csv", header = FALSE)
colnames(incoming_first) <- gsub('\\"', "", h[lengths(h) == 11][[1]])
incoming_first <- incoming_first[, -1]

second_header <- weird_headers[lengths(h) == 10]
second_header <- paste0(second_header, collapse = " ")

system2("tail", paste0("-n +2 ", second_header, 
                       " | grep -v '==>'| grep -v '^$'> ../cran-incoming_-second.csv",
                        collapse = ""))
incoming_second <- read.csv("../cran-incoming_-second.csv", header = FALSE)
colnames(incoming_second) <- gsub('\\"', "", h[lengths(h) == 10][[1]])
incoming_second <- incoming_second[, -1]

incoming_weird <- merge(incoming_first, incoming_second, sort = FALSE, all = TRUE)

# Stable heading system
system2("tail",  
        "-n +2 cran-incoming-*.csv | grep -v '==>'| grep -v '^$'> ../cran-incoming-.csv")

h <- system2("head", "-1 cran-incoming-*.csv | grep -v '==>'| grep -v '^$' | uniq",
             stdout = TRUE)
h <- strsplit(h, ",", fixed = TRUE)[[1]]
incoming <- read.csv("../cran-incoming-.csv", header = FALSE)
colnames(incoming) <- h
setwd("../..") # Back to project directory
cran_submissions <- rbind(incoming_weird[, colnames(incoming)], incoming)

cran_submissions <- cran_submissions |> 
  arrange(package, snapshot_time, folder) |> 
  group_by(package, snapshot_time) |> 
  mutate(n = 1:n()) |> 
  filter(n == n()) |> 
  ungroup() |> 
  select(-n)


## ----submissions_cleanup--------------------------------------------------------------------------
diff0 <- structure(0, class = "difftime", units = "hours")
cran_submissions <- cran_submissions |> 
  arrange(package, version, snapshot_time) |> 
  group_by(package) |> 
  # Packages last seen in queue less than 24 ago are considered same submission
  mutate(diff_time = difftime(snapshot_time,  lag(snapshot_time), units = "hour"),
         diff_time = if_else(is.na(diff_time), diff0, diff_time), # Fill NAs
         diff_v = version != lag(version),
         diff_v = ifelse(is.na(diff_v), TRUE, diff_v), # Fill NAs
         near_t = abs(diff_time) <= 24,
         resubmission = !near_t | diff_v, 
         resubmission = if_else(resubmission == FALSE & diff_time == 0, 
                                TRUE, resubmission),
         resubmission_n = cumsum(as.numeric(resubmission)),
         new_version = !near_t & diff_v, 
         new_version = if_else(new_version == FALSE & diff_time == 0, 
                               TRUE, new_version),
         submission_n = cumsum(as.numeric(new_version))) |>
  ungroup() |> 
  select(-diff_time, -diff_v, -new_version, -resubmission)

saveRDS(cran_submissions, file = "output/cran_till_now.RDS")

l <- lapply(unique(cran_submissions$package),
            function(x){
              y <- tryCatch(pkgsearch::cran_package_history(x), 
                            error = function(e){FALSE})
              if (!isFALSE(y)) {
                z <- y$`Date/Publication`
                a <- z[!is.na(z)]
                return(lubridate::as_datetime(z))
              } else {
                return(NA)
              }
            })
names(l) <- unique(cran_submissions$package)
saveRDS(l, "output/CRAN_archival_dates.RDS")


# Clean up
unlink("static/", recursive = TRUE)
unlink("output/cransays-history/", recursive = TRUE)
unlink("output/cransays-history.zip", recursive = TRUE)
