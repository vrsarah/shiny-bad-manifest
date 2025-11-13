library(shiny)
library(DT)
library(DBI)
library(RSQLite)
library(pool)
library(dplyr)
library(dbplyr)
library(ggplot2)
library(plotly)
library(stringr)

base_dir <- getwd()
db_path  <- file.path(base_dir, "archive", "netflix.sqlite")
csv_clean <- file.path(base_dir, "archive", "netflix_titles_clean.csv")

pool <- dbPool(RSQLite::SQLite(), dbname = db_path)
onStop(function() { poolClose(pool) })

split_pipe_vec <- function(x) {
  x <- ifelse(is.na(x), "", x)
  trimws(unlist(strsplit(paste(x, collapse = "|"), "\\|"))) -> v
  v[nzchar(v)]
}

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0 && !is.na(a)) a else b

collapse_pipe <- function(x) {
  if (is.null(x) || all(is.na(x))) return(NA)
  s <- trimws(x[1])
  if (!nzchar(s)) return(NA)
  vals <- trimws(unlist(strsplit(s, ",")))
  vals <- vals[nzchar(vals)]
  if (!length(vals)) NA else paste(unique(vals), collapse = "|")
}

next_show_id <- function(pool) {
  ids <- dbGetQuery(pool, "SELECT show_id FROM titles")$show_id
  nums <- suppressWarnings(as.integer(sub("^s", "", ids)))
  max_num <- max(nums[is.finite(nums)], na.rm = TRUE)
  paste0("s", ifelse(is.finite(max_num), max_num + 1L, 1L))
}

ui <- fluidPage(
  titlePanel("Netflix Titles - DB Dashboard"),
  sidebarLayout(
    sidebarPanel(
      selectInput("type", "Type", choices = c("All","Movie","TV Show"), selected = "All"),
      uiOutput("country_ui"),
      sliderInput("year_range", "Release year range", min = 1900, max = 2030, value = c(2000, 2025), sep = ""),
      uiOutput("rating_ui"),
      actionButton("reset", "Reset filters"),
      hr()
    ),
    mainPanel(
      tabsetPanel(
        tabPanel(
          "Dashboard",
          fluidRow(
            column(6, plotlyOutput("p_scatter", height = 380)),
            column(6, plotlyOutput("p_heatmap", height = 380))
          ),
          fluidRow(
            column(6, plotlyOutput("p_movies_year", height = 360)),
            column(6, plotlyOutput("p_top_genres", height = 360))
          ),
          hr(),
          h4("Data (filtered)"),
          DTOutput("tbl_dashboard")
        ),
        tabPanel(
          "Create",
          h4("Create New Record"),
          fluidRow(
            column(6,
              textInput("f_title", "Title"),
              selectInput("f_type", "Type", c("Movie","TV Show")),
              numericInput("f_release_year", "Release Year", value = 2020, min = 1900, max = 2100),
              textInput("f_rating", "Rating", value = "Unknown"),
              textInput("f_duration", "Duration (e.g., '90 min' or '2 Seasons')"),
              numericInput("f_duration_min", "Duration (minutes)", value = NA, min = 1, step = 1)
            ),
            column(6,
              textInput("f_country", "Country (comma separated)"),
              textInput("f_listed_in", "Genres (comma separated)"),
              textInput("f_director", "Director"),
              textInput("f_cast", "Cast"),
              textAreaInput("f_description", "Description", rows = 3),
              dateInput("f_date_added", "Date Added", value = Sys.Date())
            )
          ),
          actionButton("btn_add", "Add New", class = "btn-primary"),
          br(), textOutput("create_msg")
        ),
        tabPanel(
          "Update",
          h4("Update by Show ID"),
          textInput("u_show_id", "Show ID (e.g., s123)"),
          fluidRow(
            column(6,
              textInput("u_title", "Title"),
              selectInput("u_type", "Type", c("","Movie","TV Show"), selected = ""),
              numericInput("u_release_year", "Release Year", value = NA, min = 1900, max = 2100),
              textInput("u_rating", "Rating"),
              textInput("u_duration", "Duration (e.g., '90 min' or '2 Seasons')"),
              numericInput("u_duration_min", "Duration (minutes)", value = NA, min = 1, step = 1)
            ),
            column(6,
              textInput("u_country", "Country (comma separated)"),
              textInput("u_listed_in", "Genres (comma separated)"),
              textInput("u_director", "Director"),
              textInput("u_cast", "Cast"),
              textAreaInput("u_description", "Description", rows = 3)
            )
          ),
          actionButton("btn_update", "Update", class = "btn-warning"),
          br(), textOutput("update_msg")
        ),
        tabPanel(
          "Delete",
          h4("Delete by Show ID"),
          textInput("d_show_id", "Show ID to delete"),
          actionButton("btn_delete", "Delete", class = "btn-danger"),
          br(), textOutput("delete_msg")
        )
      )
    )
  )
)

server <- function(input, output, session) {
  tbl_titles <- tbl(pool, "titles")
  data_version <- reactiveVal(0L)

  output$rating_ui <- renderUI({
    data_version()
    vals <- tbl_titles %>% distinct(rating) %>% collect() %>% pull(rating) %>% sort()
    selectInput("rating", "Rating", choices = c("All", vals), selected = "All")
  })

  output$country_ui <- renderUI({
    data_version()
    cvec <- tbl_titles %>% select(country_list_str) %>% collect() %>% pull(country_list_str)
    top_c <- names(sort(table(split_pipe_vec(cvec)), decreasing = TRUE))
    selectizeInput("country", "Country (top)", choices = head(top_c, 40), multiple = TRUE)
  })

  sql_escape_like <- function(x) {
    x <- gsub("'", "''", x)
    x <- gsub("%", "\\%", x)
    x <- gsub("_", "\\_", x)
    x
  }
  sql_token_match_any <- function(col, values) {
    if (length(values) == 0) return(NULL)
    vals <- sql_escape_like(values)
    pats <- paste0("('|' || IFNULL(", col, ", '') || '|') LIKE '%|", vals, "|%' ESCAPE '\\'")
    paste(pats, collapse = " OR ")
  }

  filtered_tbl <- reactive({
    data_version()
    d <- tbl_titles
    if (input$type != "All") d <- d %>% filter(type == !!input$type)
    if (!is.null(input$rating) && input$rating != "All") d <- d %>% filter(rating == !!input$rating)
    if (!is.null(input$year_range)) {
      d <- d %>% filter(is.na(release_year) | (release_year >= !!input$year_range[1] & release_year <= !!input$year_range[2]))
    }
    if (!is.null(input$country) && length(input$country) > 0) {
      where <- sql_token_match_any("country_list_str", input$country)
      if (!is.null(where)) d <- d %>% filter(sql(where))
    }
    d
  })

  filtered_df <- reactive({ filtered_tbl() %>% collect() })

  observeEvent(input$reset, {
    updateSelectInput(session, "type", selected = "All")
    updateSelectInput(session, "rating", selected = "All")
    updateSelectizeInput(session, "country", selected = character(0))
    updateSliderInput(session, "year_range", value = c(2000, 2025))
    updateTextInput(session, "q", value = "")
  })

  output$p_scatter <- renderPlotly({
    d <- filtered_tbl() %>% select(release_year, duration_min) %>% collect()
    if (!all(c("release_year","duration_min") %in% names(d))) return(NULL)
    p <- ggplot(d, aes(release_year, duration_min)) +
      geom_point(alpha = 0.6, color = "#2C7FB8") +
      labs(x = "Release Year", y = "Duration (min)", title = "Release Year vs Duration") +
      theme_minimal()
    ggplotly(p)
  })

  output$p_heatmap <- renderPlotly({
    ab <- filtered_tbl() %>%
      mutate(type = coalesce(type, "Unknown"), rating = coalesce(rating, "Unknown")) %>%
      count(type, rating, name = "n") %>% collect()
    if (!nrow(ab)) return(NULL)
    p <- ggplot(ab, aes(rating, type, fill = n)) +
      geom_tile() +
      labs(x = "Rating", y = "Type", title = "Type vs Rating (Counts)") +
      theme_minimal() + theme(axis.text.x = element_text(angle = 45, hjust = 1))
    ggplotly(p)
  })

  output$p_movies_year <- renderPlotly({
    dd <- filtered_tbl() %>% filter(type == "Movie") %>%
      mutate(release_year = as.integer(release_year)) %>%
      count(release_year, name = "n") %>% arrange(release_year) %>% collect()
    if (!nrow(dd)) return(NULL)
    p <- ggplot(dd, aes(release_year, n)) + geom_col(fill = "#41AB5D") +
      labs(x = "Release Year (Movies)", y = "Count", title = "Movies Released per Year") + theme_minimal()
    ggplotly(p)
  })

  output$p_top_genres <- renderPlotly({
    dd <- filtered_tbl() %>% select(genre_list_str) %>% collect()
    g <- split_pipe_vec(dd$genre_list_str)
    if (!length(g)) return(NULL)
    tb <- as.data.frame(sort(table(g), decreasing = TRUE))
    names(tb) <- c("genre","n")
    topk <- tb[seq_len(min(10, nrow(tb))), ]
    p <- ggplot(topk, aes(x = reorder(genre, n), y = n)) +
      geom_col(fill = "#FB6A4A") + coord_flip() +
      labs(x = "Genre", y = "Titles", title = "Top Genres (Filtered)") + theme_minimal()
    ggplotly(p)
  })

  output$tbl_dashboard <- DT::renderDT({
    dd <- filtered_tbl() %>% collect()
    DT::datatable(dd, options = list(pageLength = 10, scrollX = TRUE))
  })

  observeEvent(input$btn_add, {
    title_val <- trimws(input$f_title)
    if (!nzchar(title_val)) {
      output$create_msg <- renderText("Title is required.")
      return()
    }
    show_id <- next_show_id(pool)
    country_raw <- if (!is.null(input$f_country) && nzchar(trimws(input$f_country))) trimws(input$f_country) else NA
    genre_raw <- if (!is.null(input$f_listed_in) && nzchar(trimws(input$f_listed_in))) trimws(input$f_listed_in) else NA
    date_val <- input$f_date_added
    date_display <- if (!is.null(date_val) && !is.na(date_val)) format(date_val, "%B %d, %Y") else NA
    date_parsed <- if (!is.null(date_val) && !is.na(date_val)) as.character(date_val) else NA
    sql <- paste(
      "INSERT INTO titles",
      "(show_id, title, type, country, country_list_str, date_added, date_added_parsed, release_year,",
      " rating, duration, duration_min, listed_in, genre_list_str, director, cast, description)",
      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    )
    params <- list(
      show_id,
      title_val,
      input$f_type,
      country_raw,
      collapse_pipe(country_raw),
      date_display,
      date_parsed,
      if (!is.na(input$f_release_year)) as.integer(input$f_release_year) else NA,
      trimws(input$f_rating),
      trimws(input$f_duration),
      if (!is.na(input$f_duration_min)) as.integer(input$f_duration_min) else NA,
      genre_raw,
      collapse_pipe(genre_raw),
      trimws(input$f_director),
      trimws(input$f_cast),
      trimws(input$f_description)
    )
    dbExecute(pool, sql, params = params)
    data_version(isolate(data_version()) + 1L)
    output$create_msg <- renderText(sprintf("Added record %s", show_id))
  })

  observeEvent(input$btn_update, {
    sid <- trimws(input$u_show_id)
    if (!nzchar(sid)) { output$update_msg <- renderText("Enter a Show ID to update."); return() }
    exists <- dbGetQuery(pool, "SELECT 1 FROM titles WHERE show_id = ? LIMIT 1", params = list(sid))
    if (nrow(exists) == 0) { output$update_msg <- renderText(sprintf("Show ID %s not found.", sid)); return() }

    sets <- c(); params <- list()
    add_set <- function(expr, val) { sets <<- c(sets, expr); params <<- c(params, list(val)) }

    if (nzchar(trimws(input$u_title))) add_set("title = ?", trimws(input$u_title))
    if (nzchar(trimws(input$u_type))) add_set("type = ?", input$u_type)
    if (!is.na(input$u_release_year)) add_set("release_year = ?", as.integer(input$u_release_year))
    if (nzchar(trimws(input$u_rating))) add_set("rating = ?", trimws(input$u_rating))
    if (nzchar(trimws(input$u_duration))) add_set("duration = ?", trimws(input$u_duration))
    if (!is.na(input$u_duration_min)) add_set("duration_min = ?", as.integer(input$u_duration_min))
    if (!is.null(input$u_country) && nzchar(trimws(input$u_country))) {
      cval <- trimws(input$u_country)
      add_set("country = ?", cval)
      add_set("country_list_str = ?", collapse_pipe(cval))
    }
    if (!is.null(input$u_listed_in) && nzchar(trimws(input$u_listed_in))) {
      gval <- trimws(input$u_listed_in)
      add_set("listed_in = ?", gval)
      add_set("genre_list_str = ?", collapse_pipe(gval))
    }
    if (nzchar(trimws(input$u_director))) add_set("director = ?", trimws(input$u_director))
    if (nzchar(trimws(input$u_cast))) add_set("cast = ?", trimws(input$u_cast))
    if (nzchar(trimws(input$u_description))) add_set("description = ?", trimws(input$u_description))

    if (!length(sets)) { output$update_msg <- renderText("No fields provided to update."); return() }
    sql <- paste0("UPDATE titles SET ", paste(sets, collapse = ", "), " WHERE show_id = ?")
    params <- c(params, list(sid))
    dbExecute(pool, sql, params = params)
    data_version(isolate(data_version()) + 1L)
    output$update_msg <- renderText(sprintf("Updated record %s", sid))
  })

  observeEvent(input$btn_delete, {
    sid <- trimws(input$d_show_id)
    if (!nzchar(sid)) { output$delete_msg <- renderText("Enter a Show ID to delete."); return() }
    dbExecute(pool, "DELETE FROM titles WHERE show_id = ?", params = list(sid))
    data_version(isolate(data_version()) + 1L)
    output$delete_msg <- renderText(sprintf("Deleted record %s (if existed)", sid))
  })
}

shinyApp(ui, server)
