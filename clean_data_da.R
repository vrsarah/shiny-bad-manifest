library("dplyr")
library(tidyr)
library(stringr)
library(readr)
library(DBI)
library(RSQLite)

base_dir <- getwd()
raw_path <- file.path(base_dir, "archive", "netflix_titles.csv")
out_path <- file.path(base_dir, "archive", "netflix_titles_clean.csv")
db_path  <- file.path(base_dir, "archive", "netflix.sqlite")

parse_duration_minutes <- function(x) {
  mins    <- as.numeric(str_match(x, "^(\\d+)\\s*min")[,2])
  seasons <- as.numeric(str_match(x, "^(\\d+)\\s*Season")[,2])
  ifelse(!is.na(mins), mins,
         ifelse(!is.na(seasons), seasons * 450, NA))
}

df <- read_csv(raw_path)
names(df) <- tolower(names(df))

df <- df %>% mutate(across(where(is.character), ~trimws(.)))

df <- df %>%
  mutate(
    date_added_parsed = readr::parse_date(date_added, format = "%B %d, %Y"),
    release_year = as.integer(release_year),
    type = case_when(
      tolower(type) %in% c("movie","movies") ~ "Movie",
      tolower(type) %in% c("tv show","tv shows","tv") ~ "TV Show",
      TRUE ~ type
    ),

    duration_min = parse_duration_minutes(duration),
    country_list_str = sapply(str_split(replace_na(country, "Unknown"), ",\\s*"),
                              function(v) paste(unique(v[nzchar(v)]), collapse = "|")),
    
    genre_list_str = sapply(str_split(replace_na(listed_in, "Unknown"), ",\\s*"),
                            function(v) paste(unique(v[nzchar(v)]), collapse = "|")),
    rating = replace_na(rating, "Unknown")
  )

clean <- df %>%
  select(
    show_id, title, type, country, country_list_str, date_added, date_added_parsed,
    release_year, rating, duration, duration_min, listed_in, genre_list_str,
    director, cast, description
  )

write_csv(clean, out_path)

cat("Saved cleaned dataset to:\n", out_path, "\n")

if (!dir.exists(file.path(base_dir, "archive"))) dir.create(file.path(base_dir, "archive"), recursive = TRUE)

con <- dbConnect(SQLite(), db_path)
on.exit(try(dbDisconnect(con), silent = TRUE), add = TRUE)

if (dbExistsTable(con, "titles")) dbRemoveTable(con, "titles")
dbWriteTable(con, "titles", clean, overwrite = TRUE)

try(dbExecute(con, "CREATE UNIQUE INDEX IF NOT EXISTS idx_titles_show_id ON titles(show_id)"), silent = TRUE)
try(dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_titles_type ON titles(type)"), silent = TRUE)
try(dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_titles_release_year ON titles(release_year)"), silent = TRUE)
try(dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_titles_rating ON titles(rating)"), silent = TRUE)

cat("Wrote cleaned data to SQLite DB:\n", db_path, "\n")
