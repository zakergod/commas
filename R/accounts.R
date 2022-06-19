# libs ----
library(httr2)
library(dplyr)
library(tidyr)
library(tibble)
library(openssl)
library(purrr)
library(glue)
library(googlesheets4)
library(lubridate)

# credentals ----
credentals <- jsonlite::fromJSON(Sys.getenv("credentals"))
auth_google <- Sys.getenv("GKEY")
link <- credentals[["LINK_ACC"]]
secret_key <- credentals[["SECRET"]]
api_key <- credentals[["API_KEY"]]
name_google <- credentals[["GNAME"]]

# paths ----
accounts <- "/ver1/accounts"
sum_accounts <- "/ver1/accounts/summary"
pie_chart <- "pie_chart_data"

# utils ----
time_local <- "Europe/Moscow"
delay <- 3

# functions ----
# get json responce ----
get_json <- \(.query, .method) {
  host <- "https://api.3commas.io"
  base_path <- "/public/api"
  endpoint <- paste0(base_path, .query)

  signature <- sha256(endpoint, key = secret_key)
  path <- paste0(host, endpoint)

  req <- request(path) |>
    req_method(.method) |>
    req_headers(
      "APIKEY" = api_key,
      "Signature" = signature
    ) |>
    req_perform()

  resp <- req |>
    resp_body_json()

  resp
}

# unnest wider all columns ----
to_wider <- \(.data){
  map_dfc(names(.data), ~
  .data |>
    select(all_of(.x)) |>
    unnest_wider(all_of(.x),
      names_sep = "_"
    )) |>
    rename_with(.fn = \(x) gsub("_[0-9]$", "", x)) |>
    mutate(across(.fns = as.character))
}

# resp_json to data.frame ----
to_df <- \(resp, is_nested = TRUE){
  data <- if (is_nested) {
    json <- list(targets = resp)
    result <- tibble(result = json[["targets"]]) |>
      unnest_wider(col = .data[["result"]])
    result
  } else {
    json <- list(targets = list(resp))
    result <- tibble(result = json[["targets"]]) |>
      unnest_wider(col = .data[["result"]])
    result
  }
}

# get accounts items ----
get_account_items <- \(.id, .rel_path, .method){
  path <- glue("/ver1/accounts/{.id}/{.rel_path}")
  resp <- get_json(path, .method)
  result <- to_df(resp)

  Sys.sleep(delay)
  result
}

# get accounts ----
resp_acc <- get_json(accounts, "GET")
accounts <- to_df(resp_acc) |>
  to_wider()

# get summary accounts ----
resp_sum <- get_json(sum_accounts, "GET")
summary_acc <- to_df(resp_sum, FALSE) |>
  to_wider()


acc_sum <- summary_acc |>
  add_row(accounts, .before = 2) |>
  mutate(dt_load = as.character(now(tzone = time_local)))

# get pie charts ----
id_accounts <- unique(accounts$id)

all_pies <- map_df(id_accounts, \(x) get_account_items(x, pie_chart, "POST"))
pie_chart_data <- to_wider(all_pies)

acc <- accounts |>
  select(id, name) |>
  rename(account_name = name)

pie_acc <- acc |>
  left_join(pie_chart_data, by = c("id" = "account_id")) |>
  mutate(dt_load = as.character(now(tzone = time_local)))

### auth ----
file_con <- file(name_google)
writeLines(auth_google, file_con)
close(file_con)
gs4_auth(path = name_google)

# write to table ----
write_sheet(
  pie_acc,
  link,
  "Pie charts"
)

write_sheet(
  acc_sum,
  link,
  "Accounts"
)
