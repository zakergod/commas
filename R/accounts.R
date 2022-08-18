# libs ----
library(httr2)
library(dplyr)
library(tidyr)
library(tibble)
library(openssl)
library(purrr)
library(glue)
library(lubridate)
library(jsonlite)
library(DBI)

# credentals ----
credentals <- fromJSON(Sys.getenv("credentals"))
name_db <- credentals[["NAME_DB"]]
host_db <- credentals[["HOST_DB"]]
user_db <- credentals[["USER_DB"]]
password_db <- credentals[["PASSWORD_DB"]]
secret_key <- credentals[["SECRET"]]
api_key <- credentals[["API_KEY"]]

# paths ----
accounts <- "/ver1/accounts"
sum_accounts <- "/ver1/accounts/summary"
trade_entities <- "active_trading_entities"
deals <- "/ver1/deals?limit=1000"

# utils ----
time_local <- "Europe/Moscow"
delay <- 2
now_date <- now(tzone = time_local)

# functions ----
# get json responce ----
get_json <- \(.query, .method) {
  host <- "https://api.3commas.io"
  base_path <- "/public/api"
  endpoint <- paste0(base_path, .query)

  signature <- sha256(endpoint, key = secret_key)
  path <- paste0(host, endpoint)
  req <- tryCatch(
    expr = {
      rs <- request(path) |>
        req_method(.method) |>
        req_headers(
          "APIKEY" = api_key,
          "Signature" = signature
        ) |>
        req_perform()
      rs
    },
    error = function(e) {
      message(paste("Error:", path))
    }
  )
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
  result <- tryCatch(
    expr = {
      path <- glue("/ver1/accounts/{.id}/{.rel_path}")
      resp <- get_json(path, .method)
      rs <- to_df(resp) |>
        mutate(id = .id)

      rs
    },
    error = function(e) {
      data.frame()
    }
  )
  Sys.sleep(delay)

  result
}

# get bot items ----
get_bot_items <- \(.id, .method){
  path <- glue("/ver1/bots/stats?account_id={.id}")
  resp <- get_json(path, .method)
  result <- to_df(resp$profits_in_usd, FALSE) |>
    mutate(id = .id)

  Sys.sleep(delay)
  result
}

# get accounts ----
resp_acc <- get_json(accounts, "GET")
accounts <- to_df(resp_acc) |>
  to_wider() |>
  select(id, name, usd_amount, btc_amount)

# get summary accounts ----
resp_sum <- get_json(sum_accounts, "GET")
summary_acc <- to_df(resp_sum, FALSE) |>
  to_wider() |>
  select(id, name, usd_amount, btc_amount)


# get trade entities ----
# unique ids
id_accounts <- unique(accounts$id)

all_trade_entities <- map_df(id_accounts, \(x) get_account_items(x, trade_entities, "GET"))
trade_entities_data <- all_trade_entities |>
  to_wider() |>
  select(id, active_deals_count)

Sys.sleep(delay)
# get bots stats ----
all_bots_stats <- map_df(id_accounts, \(x) get_bot_items(x, "GET"))
bots_stats_data <- all_bots_stats |>
  select(id, overall_usd_profit, active_deals_usd_profit, today_usd_profit) |>
  rename_with(.cols = -id, .fn = \(x) paste0(x, "_bots"))

Sys.sleep(delay)
# get deals stat----
resp_deals <- get_json(deals, "GET")
deals_data <- resp_deals |>
  to_df() |>
  select(account_id, closed_at, usd_final_profit) |>
  mutate(
    closed_at = as_datetime(closed_at),
    usd_final_profit = as.numeric(usd_final_profit),
    account_id = as.character(account_id)
  ) |>
  rename(id = account_id) |>
  filter(!is.na(closed_at)) |>
  arrange(id, desc(closed_at))

last_7d_profit <- deals_data |>
  mutate(week = floor_date(closed_at, "week", week_start = 1)) |>
  filter(week == max(.data$week)) |>
  filter(closed_at >= week) |>
  group_by(id, week) |>
  summarise(this_week_usd_profit = sum(usd_final_profit)) |>
  ungroup() |>
  select(-week)

last_30d_profit <- deals_data |>
  group_by(id) |>
  filter(between(closed_at, now_date - days(30), now_date)) |>
  summarise(last_30d_usd_profit = sum(usd_final_profit)) 

this_month_profit <- deals_data |>
  mutate(month = floor_date(closed_at, "month")) |>
  group_by(id, month) |>
  summarise(this_month_usd_profit = sum(usd_final_profit)) |>
  ungroup() |>
  filter(month == max(.data$month)) |>
  select(-month)

#deals
all_deals <- resp_deals |>
  to_df() |>
  rename(finished = 'finished?') |>
  rename(cancellable = 'cancellable?') |>
  rename(panic_sellable = 'panic_sellable?') |>
  filter(!is.na(closed_at)) 

# bots
deals_data_bots <- resp_deals |>
  to_df() |>
  select(account_id,bot_name, closed_at, usd_final_profit) |>
  mutate(
    closed_at = as_datetime(closed_at),
    usd_final_profit = as.numeric(usd_final_profit),
    account_id = as.character(account_id)
  ) |>
  rename(id = account_id) |>
  rename(name = bot_name) |>
  filter(!is.na(closed_at)) |>
  arrange(id, desc(closed_at))


accounts_usd_btc <- accounts |>
  select(id,usd_amount,btc_amount)

accounts_bots <- deals_data_bots |>
  select(id,name) |>
  distinct() |>
  left_join(accounts_usd_btc, by = "id")
  
bots_stats_data_bots <- bots_stats_data |>
  select(id,overall_usd_profit_bots,active_deals_usd_profit_bots)

today_usd_profit_bots <-deals_data_bots |>
  mutate(day = floor_date(closed_at, "day")) |>
  filter(day == Sys.Date()) |>
  filter(closed_at >= day) |>
  group_by(name,day) |>
  summarise(today_usd_profit_bots = sum(usd_final_profit)) |>
  ungroup() |>
  select(name,today_usd_profit_bots)
  

last_7d_profit_bots <- deals_data_bots |>
  mutate(week = floor_date(closed_at, "week", week_start = 1)) |>
  filter(week == max(.data$week)) |>
  filter(closed_at >= week) |>
  group_by(name, week) |>
  summarise(this_week_usd_profit = sum(usd_final_profit)) |>
  ungroup() |>
  select(name,this_week_usd_profit)

last_30d_profit_bots <- deals_data_bots |>
  group_by(name) |>
  filter(between(closed_at, now_date - days(30), now_date)) |>
  summarise(last_30d_usd_profit = sum(usd_final_profit)) |>
  select(name,last_30d_usd_profit)

this_month_profit_bots <- deals_data_bots |>
  mutate(month = floor_date(closed_at, "month")) |>
  group_by(name, month) |>
  summarise(this_month_usd_profit = sum(usd_final_profit)) |>
  ungroup() |>
  filter(month == max(.data$month)) |>
  select(name,this_month_usd_profit)


### join all ----

stats_acc_bots <- accounts_bots |>
  left_join(trade_entities_data, by = "id") |>
  left_join(bots_stats_data_bots, by = "id") |>
  left_join(today_usd_profit_bots, by = "name") |>
  left_join(last_7d_profit_bots, by = "name") |>
  left_join(last_30d_profit_bots, by = "name") |>
  left_join(this_month_profit_bots, by = "name") |>
  mutate(
    active_deals_count = as.numeric(active_deals_count),
    overall_usd_profit_bots = as.numeric(overall_usd_profit_bots),
    active_deals_usd_profit_bots = as.numeric(active_deals_usd_profit_bots),
    today_usd_profit_bots = ifelse(is.na(today_usd_profit_bots), 0, today_usd_profit_bots),
    this_week_usd_profit = ifelse(is.na(this_week_usd_profit), 0, this_week_usd_profit),
    last_30d_usd_profit = ifelse(is.na(last_30d_usd_profit), 0, last_30d_usd_profit),
    this_month_usd_profit = ifelse(is.na(this_month_usd_profit), 0, this_month_usd_profit)
  ) |>
  filter(last_30d_usd_profit != 0) |>
  mutate(across(.cols = where(is.numeric), .fns = \(x) ifelse(is.na(x), 0, x))) 


stats_acc <- accounts |>
  left_join(trade_entities_data, by = "id") |>
  left_join(bots_stats_data, by = "id") |>
  left_join(last_7d_profit, by = "id") |>
  left_join(last_30d_profit, by = "id") |>
  left_join(this_month_profit, by = "id") |>
  mutate(
    active_deals_count = as.numeric(active_deals_count),
    overall_usd_profit_bots = as.numeric(overall_usd_profit_bots),
    active_deals_usd_profit_bots = as.numeric(active_deals_usd_profit_bots),
    today_usd_profit_bots = as.numeric(today_usd_profit_bots),
    this_week_usd_profit = ifelse(is.na(this_week_usd_profit), 0, this_week_usd_profit),
    last_30d_usd_profit = ifelse(is.na(last_30d_usd_profit), 0, last_30d_usd_profit),
    this_month_usd_profit = ifelse(is.na(this_month_usd_profit), 0, this_month_usd_profit)
  ) |>
  filter(last_30d_usd_profit != 0) |>
  mutate(across(.cols = where(is.numeric), .fns = \(x) ifelse(is.na(x), 0, x))) 

# summary stats
summary_stat <- stats_acc |>
  select(where(is.numeric)) |>
  summarise(across(.fns = sum))

all_results <- summary_acc |>
  bind_cols(summary_stat) |>
  add_row(stats_acc_bots, .before = 2) |>
  add_row(stats_acc, .before = 2) |>
  mutate(
    usd_amount = round(as.numeric(usd_amount), 4),
    btc_amount = round(as.numeric(btc_amount), 8),
    dt_load = as_datetime(as.character(now_date))
  ) |>
  filter(!grepl("Paper", name)) 

# write to table ----

  con <- dbConnect(RPostgres::Postgres(),
    dbname = name_db,
    host = host_db,
    user = user_db,
    password = password_db
  )

  dealsdf <- dbGetQuery(con, "SELECT * FROM deals")
  finaldf <- all_deals[!(all_deals$id %in% dealsdf$id),]

if (nrow(all_results) != 0) {
  dbWriteTable(con, "accounts", all_results, append = TRUE)
}
if (nrow(finaldf) != 0) {  
  dbWriteTable(con, "deals", finaldf, append = TRUE)
}

  dbDisconnect(con)

