# ==============================================================================
# extract_achmea_2025.R
#
# Extracts IFRS 17 financial data from Achmea 2025 Annual Report PDF using
# the Claude API, replicating the structure of DRAFT_Databook.xlsx (2024 data).
#
# Output: Achmea_2025_Databook.xlsx
#
# Usage: Rscript extract_achmea_2025.R
#
# Required packages: pdftools, httr2, jsonlite, openxlsx
# API key: set ANTHROPIC_API_KEY in .Renviron in this directory or parent
# ==============================================================================

library(pdftools)
library(httr2)
library(jsonlite)
library(openxlsx)

# ---- Config ------------------------------------------------------------------

PDF_PATH   <- "Annual report Achmea BV.pdf"
OUT_PATH   <- "Achmea_2025_Databook.xlsx"
MODEL      <- "claude-opus-4-6"
MAX_TOKENS <- 4096

# Load API key
if (file.exists(".Renviron")) readRenviron(".Renviron")
api_key <- Sys.getenv("ANTHROPIC_API_KEY")
if (!nzchar(api_key)) stop("ANTHROPIC_API_KEY not set. Add it to .Renviron.")

# ---- Page map for 2025 Achmea report -----------------------------------------
# Determined by scanning the PDF table of contents and keyword search
PAGE_MAP <- list(
  balance_sheet         = 198,
  income_statement      = 199,
  results_overview      = c(30, 38, 39, 40, 43),
  portfolio_overview    = 253,             # Note 7 summary table
  csm_maturity          = 254,             # CSM maturity buckets
  nonlife_movements     = c(257, 258),     # Note 7.1 Non-Life movements
  health_movements      = c(262, 263),     # Note 7.2 Health movements
  life_movements        = c(268, 269, 270),# Note 7.3 Life movements
  csm_rollforward       = c(259, 260, 269, 270),
  loss_component        = c(258, 263, 267, 265),
  insurance_svc_result  = c(296, 297),     # Note 10
  net_financial_result  = c(298, 299),     # Note 11
  investment_result     = 300,             # Note 12
  discount_rates        = c(273, 274, 275),
  solvency              = c(34, 35),
  investments_note      = c(248, 249, 250, 251, 252),
  gross_premium         = c(30, 33, 39),
  financed_emissions    = c(157, 158)
)

# ---- Helpers -----------------------------------------------------------------

#' Extract text from a vector of page numbers as a single string
extract_pages <- function(pages_text, page_nums) {
  paste(pages_text[page_nums], collapse = "\n\n---PAGE BREAK---\n\n")
}

#' Call Claude API with a structured extraction prompt, return parsed JSON
call_claude <- function(prompt_text, section_name) {
  message(glue::glue("  Calling Claude for: {section_name} ..."))

  resp <- request("https://api.anthropic.com/v1/messages") |>
    req_headers(
      "x-api-key"         = api_key,
      "anthropic-version" = "2023-06-01",
      "content-type"      = "application/json"
    ) |>
    req_body_json(list(
      model      = MODEL,
      max_tokens = MAX_TOKENS,
      messages   = list(list(role = "user", content = prompt_text)),
      system     = paste(
        "You are a specialist in IFRS 17 insurance financial statement analysis.",
        "Extract ONLY the numeric values explicitly stated in the provided text.",
        "Return ONLY valid JSON — no markdown fences, no explanation.",
        "Use null for any value not found. All monetary values in EUR millions.",
        "Percentages as decimals (e.g. 4.5% = 0.045).",
        "Return integers where possible (no decimal for whole numbers)."
      )
    )) |>
    req_timeout(120) |>
    req_retry(max_tries = 3, backoff = ~30) |>
    req_perform()

  body <- resp_body_json(resp)
  raw  <- body$content[[1]]$text

  # Strip markdown code fences if Claude wraps the JSON
  clean <- gsub("^```(?:json)?\\s*|\\s*```$", "", trimws(raw), perl = TRUE)

  tryCatch(
    fromJSON(clean, simplifyVector = FALSE),
    error = function(e) {
      warning(glue::glue("JSON parse failed for {section_name}: {conditionMessage(e)}\nRaw: {raw}"))
      NULL
    }
  )
}

# ---- Load PDF ----------------------------------------------------------------

message("Loading PDF...")
pages_text <- pdf_text(PDF_PATH)
message(glue::glue("  {length(pages_text)} pages loaded."))

# ==============================================================================
# SECTION 1: Portfolio Overview (IFRS 17 contract liabilities by model)
# ==============================================================================

message("\n[1/7] Portfolio overview...")

s1_text <- extract_pages(pages_text, PAGE_MAP$portfolio_overview)
s1 <- call_claude(paste0(
  "From this Achmea 2025 annual report extract (Note 7 — Assets and Liabilities Related to Insurance Contracts), ",
  "extract the following 2025 year-end values (EUR millions, insurance liabilities net of assets).\n\n",
  "Return JSON with exactly these keys:\n",
  '{"life_gmm": <number>, "life_vfa": <number>, "life_paa": <number>,',
  ' "life_total": <number>,',
  ' "nonlife_gmm": <number>, "nonlife_paa": <number>, "nonlife_total": <number>,',
  ' "health_gmm": <number>, "health_paa": <number>, "health_total": <number>,',
  ' "total_gmm": <number>, "total_vfa": <number>, "total_paa": <number>, "total_direct": <number>,',
  ' "reins_life_gmm": <number>, "reins_life_paa": <number>,',
  ' "reins_nonlife_gmm": <number>, "reins_nonlife_paa": <number>,',
  ' "reins_health_gmm": <number>, "reins_health_paa": <number>,',
  ' "reins_total_gmm": <number>, "reins_total_paa": <number>, "reins_total": <number>}\n\n',
  "Text:\n", s1_text
), "section_1_portfolio")

# ==============================================================================
# SECTION 2: Financial Performance
# ==============================================================================

message("\n[2/7] Financial performance...")

# 2a: Top-level P&L items (income statement + insurance service breakdown)
s2a_text <- extract_pages(pages_text, c(PAGE_MAP$income_statement, PAGE_MAP$insurance_svc_result))
s2a <- call_claude(paste0(
  "From this Achmea 2025 annual report extract (income statement and Note 10 Insurance Service Result), ",
  "extract 2025 values (EUR millions).\n\n",
  "Return JSON:\n",
  '{"net_insurance_result": <total insurance service result 2025>,',
  ' "csm_release_total": <CSM recognised for services provided total GMM+VFA>,',
  ' "ra_release_total": <Change Risk Adjustment total GMM+VFA>,',
  ' "paa_insurance_revenue": <PAA insurance revenue total>,',
  ' "paa_incurred_claims": <PAA incurred claims total — negative sign>,',
  ' "gmm_incurred_claims": <GMM incurred claims total — negative sign>,',
  ' "net_reinsurance_result": <Net result from reinsurance contracts>,',
  ' "profit_before_tax": <Profit before tax>,',
  ' "insurance_revenue_total": <Insurance revenue total>,',
  ' "insurance_service_expenses_total": <Insurance service expenses total>}\n\n',
  "Text:\n", s2a_text
), "section_2a_pnl")

# 2b: Net financial result (Note 11)
s2b_text <- extract_pages(pages_text, PAGE_MAP$net_financial_result)
s2b <- call_claude(paste0(
  "From this Achmea 2025 annual report extract (Note 11 Net Financial Result), ",
  "extract 2025 totals (EUR millions).\n\n",
  "Return JSON:\n",
  '{"investment_result_reinsurance": <Total investment result from (re)insurance activities 2025>,',
  ' "finance_result_ic_policyholders": <Changes in fair value of financial investments policyholder risk 2025 — negative if outflow>,',
  ' "finance_result_ic_general": <Interest accretion and changes in financial assumptions 2025>,',
  ' "finance_result_reinsurance": <Total finance result from reinsurance contracts 2025>,',
  ' "net_financial_result_total": <Net financial result from (re)insurance activities 2025>}\n\n',
  "Text:\n", s2b_text
), "section_2b_financial_result")

# 2c: Investment result from non-insurance (Note 12) + other income/expenses
s2c_text <- extract_pages(pages_text, c(PAGE_MAP$investment_result, PAGE_MAP$income_statement))
s2c <- call_claude(paste0(
  "From this Achmea 2025 annual report extract (Note 12 and income statement), ",
  "extract 2025 values (EUR millions).\n\n",
  "Return JSON:\n",
  '{"investment_income_other": <Investment income from other activities 2025>,',
  ' "income_service_contracts": <Income from service contracts 2025>,',
  ' "other_income": <Other income 2025>,',
  ' "other_operating_expenses": <Other operating expenses 2025>,',
  ' "interest_similar_expenses": <Interest and similar expenses 2025>,',
  ' "other_expenses": <Other expenses 2025>,',
  ' "income_associates": <Income from associates and joint ventures 2025>}\n\n',
  "Text:\n", s2c_text
), "section_2c_other_income")

# 2d: Discount rates
s2d_text <- extract_pages(pages_text, PAGE_MAP$discount_rates)
s2d <- call_claude(paste0(
  "From this Achmea 2025 annual report extract, extract the discount rate curves at 31 December 2025 ",
  "for both liquid and illiquid yield curves (as annual rates, expressed as decimals e.g. 2.5% = 0.025). ",
  "Also extract cost of capital rate and confidence levels for RA if present.\n\n",
  "Return JSON:\n",
  '{"liquid_1y": <number>, "liquid_5y": <number>, "liquid_10y": <number>,',
  ' "liquid_15y": <number>, "liquid_20y": <number>, "liquid_30y": <number>, "liquid_50y": <number>,',
  ' "illiquid_1y": <number>, "illiquid_5y": <number>, "illiquid_10y": <number>,',
  ' "illiquid_15y": <number>, "illiquid_20y": <number>, "illiquid_30y": <number>, "illiquid_50y": <number>,',
  ' "last_liquid_point": <integer years>, "ltfr": <long term forward rate as decimal>,',
  ' "cost_of_capital_rate": <decimal>, "confidence_life": <decimal>, "confidence_nonlife": <decimal>, "confidence_health": <decimal>}\n\n',
  "Text:\n", s2d_text
), "section_2d_discount_rates")

# ==============================================================================
# SECTION 3: CSM, RA and Loss Component Rollforwards
# ==============================================================================

message("\n[3/7] CSM, RA and loss component rollforwards...")

# 3a: CSM rollforward (Life + Non-Life + Health combined, then split)
s3a_text <- extract_pages(pages_text, PAGE_MAP$csm_rollforward)
s3a <- call_claude(paste0(
  "From this Achmea 2025 annual report extract, find the Contractual Service Margin (CSM) ",
  "rollforward tables for 2025 (opening -> closing). Extract total and per-segment values (EUR millions).\n\n",
  "Return JSON:\n",
  '{"csm_opening_total": <number>, "csm_new_business": <number>, "csm_acquisitions": <number>,',
  ' "csm_finance_result": <number>, "csm_future_service_changes": <number>,',
  ' "csm_release": <number>, "csm_fx": <number>, "csm_other": <number>, "csm_closing_total": <number>,',
  ' "csm_opening_nonlife": <number>, "csm_closing_nonlife": <number>,',
  ' "csm_opening_life": <number>, "csm_closing_life": <number>,',
  ' "csm_opening_health": <number>, "csm_closing_health": <number>,',
  ' "csm_new_business_nonlife": <number>, "csm_new_business_life": <number>,',
  ' "csm_finance_nonlife": <number>, "csm_finance_life": <number>,',
  ' "csm_future_changes_nonlife": <number>, "csm_future_changes_life": <number>,',
  ' "csm_release_nonlife": <number>, "csm_release_life": <number>, "csm_release_health": <number>,',
  ' "csm_other_nonlife": <number>, "csm_other_life": <number>,',
  ' "csm_maturity_lt1y": <number>, "csm_maturity_1to5y": <number>}\n\n',
  "Text:\n", s3a_text
), "section_3a_csm")

# 3b: Risk Adjustment rollforward
s3b_text <- extract_pages(pages_text, c(257, 258, 263, 268, 269))
s3b <- call_claude(paste0(
  "From this Achmea 2025 annual report extract, find the Risk Adjustment (RA) rollforward ",
  "for 2025. Extract total and per-segment values (EUR millions).\n\n",
  "Return JSON:\n",
  '{"ra_opening_total": <number>, "ra_new_business": <number>, "ra_acquisitions": <number>,',
  ' "ra_finance_result": <number>, "ra_future_service_adj_csm": <number>, "ra_future_service_no_csm": <number>,',
  ' "ra_past_service": <number>, "ra_release": <number>, "ra_fx": <number>,',
  ' "ra_other": <number>, "ra_closing_total": <number>,',
  ' "ra_opening_nonlife": <number>, "ra_closing_nonlife": <number>,',
  ' "ra_opening_life": <number>, "ra_closing_life": <number>,',
  ' "ra_finance_nonlife": <number>, "ra_finance_life": <number>,',
  ' "ra_release_nonlife": <number>, "ra_release_life": <number>,',
  ' "disaggregate_ifie": "Y",',
  ' "ra_new_business_nonlife": <number>, "ra_new_business_life": <number>}\n\n',
  "Text:\n", s3b_text
), "section_3b_ra")

# 3c: Loss component rollforward (GMM and PAA)
s3c_text <- extract_pages(pages_text, PAGE_MAP$loss_component)
s3c <- call_claude(paste0(
  "From this Achmea 2025 annual report extract, find the Loss Component rollforward tables ",
  "for 2025 — both for GMM/VFA contracts and PAA contracts. Extract values (EUR millions).\n\n",
  "Return JSON:\n",
  '{"lc_gmm_opening": <number>, "lc_gmm_losses_recognised": <number>,',
  ' "lc_gmm_new_business": <number>, "lc_gmm_systematic_alloc": <number>,',
  ' "lc_gmm_future_service_changes": <number>, "lc_gmm_finance": <number>,',
  ' "lc_gmm_other": <number>, "lc_gmm_closing": <number>,',
  ' "lc_paa_opening": <number>, "lc_paa_losses_recognised": <number>,',
  ' "lc_paa_future_service_changes": <number>, "lc_paa_finance": <number>,',
  ' "lc_paa_closing": <number>,',
  ' "lc_life_opening": <number>, "lc_life_losses": <number>,',
  ' "lc_life_new_business": <number>, "lc_life_systematic": <number>,',
  ' "lc_life_future_changes": <number>, "lc_life_finance": <number>,',
  ' "lc_life_other": <number>, "lc_life_closing": <number>,',
  ' "lc_nonlife_opening": <number>, "lc_nonlife_gmm_losses": <number>,',
  ' "lc_nonlife_finance": <number>, "lc_nonlife_other": <number>, "lc_nonlife_closing": <number>,',
  ' "lc_health_opening": <number>, "lc_health_losses": <number>,',
  ' "lc_health_future_changes": <number>, "lc_health_closing": <number>}\n\n',
  "Text:\n", s3c_text
), "section_3c_loss_component")

# ==============================================================================
# SECTION 5: Capital & Financial Positions
# ==============================================================================

message("\n[5/7] Capital positions...")

s5_text <- extract_pages(pages_text, PAGE_MAP$solvency)
s5 <- call_claude(paste0(
  "From this Achmea 2025 annual report extract, find the Solvency II ratios and capital at 31 December 2025.\n\n",
  "Return JSON:\n",
  '{"solvency2_ratio": <decimal e.g. 1.82 for 182%>, "solvency2_target": <decimal>,',
  ' "scr": <EUR millions>, "eligible_own_funds": <EUR millions>,',
  ' "capital_generated": <EUR millions if disclosed>}\n\n',
  "Text:\n", s5_text
), "section_5_solvency")

# ==============================================================================
# SECTION 6: Investment Asset Mix
# ==============================================================================

message("\n[6/7] Investment asset mix...")

s6_text <- extract_pages(pages_text, PAGE_MAP$investments_note)
s6 <- call_claude(paste0(
  "From this Achmea 2025 annual report extract (Note 6 Investments), ",
  "extract the investment asset mix at 31 December 2025 (EUR millions), ",
  "classifying insurance/other activities and banking credit portfolio separately.\n\n",
  "Return JSON:\n",
  '{"equities": <number>, "govt_bonds": <number>, "securitised_bonds": <number>,',
  ' "corporate_bonds": <number>, "convertible_bonds": <number>,',
  ' "mortgages_insurance": <number>, "loans_deposits_insurance": <number>,',
  ' "other_fis_insurance": <number>, "total_fis_insurance": <number>,',
  ' "derivatives_insurance": <number>, "other_financial_insurance": <number>,',
  ' "total_investments_insurance": <number>,',
  ' "mortgages_banking": <number>, "loans_deposits_banking": <number>,',
  ' "other_fis_banking": <number>, "total_fis_banking": <number>,',
  ' "derivatives_banking": <number>, "other_financial_banking": <number>,',
  ' "total_banking": <number>,',
  ' "investment_property": <number>, "total_investments": <number>,',
  ' "fvoci": <number>, "fvtpl": <number>, "amortised_cost": <number>}\n\n',
  "Text:\n", s6_text
), "section_6_investments")

# ==============================================================================
# SECTION 7: Financed Emissions
# ==============================================================================

message("\n[7/7] Financed emissions...")

s7_text <- extract_pages(pages_text, PAGE_MAP$financed_emissions)
s7 <- call_claude(paste0(
  "From this Achmea 2025 annual report extract on financed GHG emissions, ",
  "extract the investment breakdown at 31 December 2025 (EUR millions).\n\n",
  "Return JSON:\n",
  '{"own_listed_equity": <number>, "own_corporate_bonds": <number>,',
  ' "own_govt_bonds": <number>, "own_mortgages_insurance": <number>,',
  ' "own_mortgages_banking": <number>, "own_investment_property": <number>,',
  ' "own_investment_loans": <number>, "own_other": <number>, "own_total": <number>,',
  ' "ph_equities": <number>, "ph_corporate_bonds": <number>,',
  ' "ph_eu_govt_bonds": <number>, "ph_other": <number>, "ph_total": <number>}\n\n',
  "Text:\n", s7_text
), "section_7_emissions")

# ==============================================================================
# SECTION 4 + Gross Written Premium
# ==============================================================================

message("\nExtracting gross written premium and solvency capital...")

s_gwp_text <- extract_pages(pages_text, PAGE_MAP$gross_premium)
s_gwp <- call_claude(paste0(
  "From this Achmea 2025 annual report extract, find the gross written premium or insurance revenue ",
  "split by Life/Health/Pensions, Non-Life, and international activities for 2025 (EUR millions).\n\n",
  "Return JSON:\n",
  '{"gwp_life_health_pensions": <number>, "gwp_pensions_life": <number>,',
  ' "gwp_health": <number>, "gwp_nonlife": <number>,',
  ' "gwp_intl_nonlife": <number>, "gwp_intl_health": <number>,',
  ' "gwp_total": <number>}\n\n',
  "Text:\n", s_gwp_text
), "gwp")

# ==============================================================================
# ASSEMBLE EXCEL
# ==============================================================================

message("\nAssembling Excel workbook...")

wb <- createWorkbook()
addWorksheet(wb, "Achmea")

# Helper: write a section header
row_ptr <- 1L
ws <- "Achmea"

write_header <- function(text, bold = TRUE, size = 11) {
  writeData(wb, ws, text, startRow = row_ptr, startCol = 1)
  if (bold) addStyle(wb, ws, createStyle(textDecoration = "bold", fontSize = size),
                     rows = row_ptr, cols = 1)
  row_ptr <<- row_ptr + 1L
}

write_row <- function(label, v2025, v2024 = NA, v2023 = NA, comment = "") {
  writeData(wb, ws, label, startRow = row_ptr, startCol = 1)
  writeData(wb, ws, v2025,  startRow = row_ptr, startCol = 2)
  writeData(wb, ws, v2024,  startRow = row_ptr, startCol = 3)
  writeData(wb, ws, v2023,  startRow = row_ptr, startCol = 4)
  if (nzchar(comment))
    writeData(wb, ws, comment, startRow = row_ptr, startCol = 5)
  row_ptr <<- row_ptr + 1L
}

blank <- function(n = 1) { row_ptr <<- row_ptr + n }

safe_neg <- function(x) if (is.null(x) || length(x) == 0 || is.na(x)) NA_real_ else -abs(x)

# -- Title & legend
writeData(wb, ws, "Mapping Financial Statements - Achmea Dataset", startRow = 1, startCol = 1)
addStyle(wb, ws, createStyle(textDecoration = "bold", fontSize = 14), rows = 1, cols = 1)
writeData(wb, ws, "SOTI  FY2025", startRow = 2, startCol = 1)

# Column headers
writeData(wb, ws, data.frame(
  A = "Insurer", B = "ACHMEA", C = "Comments 2025", D = "Comments 2024"
), startRow = 3, startCol = 1, colNames = FALSE)
writeData(wb, ws, data.frame(
  A = "Year", B = 2025, C = 2024, D = 2023
), startRow = 4, startCol = 1, colNames = FALSE)
addStyle(wb, ws, createStyle(textDecoration = "bold"), rows = 3:4, cols = 1:4, gridExpand = TRUE)
row_ptr <- 5L

# ---- (1) OVERVIEW OF PORTFOLIO -----------------------------------------------

write_header("(1) OVERVIEW OF PORTFOLIO")
blank()
write_header("(i) LIFE PORTFOLIO [DIRECT & ASSUMED]:")
write_row("General Measurement Model",
          s1$life_gmm, NA, NA, "Note 7 [Pg.253]")
write_row("Variable Fee Approach",
          s1$life_vfa, NA, NA, "Note 7 [Pg.253]")
write_row("Premium Allocation Approach",
          s1$life_paa, NA, NA, "Note 7 [Pg.253]")
write_row("Subtotal Life", s1$life_total, NA, NA, "Subtotal")
blank()
write_header("(ii) NON-LIFE PORTFOLIO [DIRECT & ASSUMED]:")
write_row("General Measurement Model",
          s1$nonlife_gmm, NA, NA, "Note 7 [Pg.253]")
write_row("Premium Allocation Approach",
          s1$nonlife_paa, NA, NA, "Note 7 [Pg.253]")
write_row("Subtotal Non-Life", s1$nonlife_total, NA, NA, "Subtotal")
blank()
write_header("(iii) HEALTH PORTFOLIO [DIRECT & ASSUMED]:")
write_row("General Measurement Model",
          s1$health_gmm, NA, NA, "Note 7 [Pg.253]")
write_row("Premium Allocation Approach",
          s1$health_paa, NA, NA, "Note 7 [Pg.253]")
write_row("Subtotal Health", s1$health_total, NA, NA, "Subtotal")
blank()
write_header("(iv) TOTAL PORTFOLIO [DIRECT & ASSUMED]:")
write_row("General Measurement Model", s1$total_gmm, NA, NA, "Subtotal")
write_row("Variable Fee Approach",     s1$total_vfa, NA, NA, "Subtotal")
write_row("Premium Allocation Approach", s1$total_paa, NA, NA, "Subtotal")
write_row("TOTAL", s1$total_direct, NA, NA, "Subtotal")
blank()
write_header("(v) REINSURANCE CEDED (Life / Non-Life / Health):")
write_row("Life - GMM",       s1$reins_life_gmm,    NA, NA, "Note 7 [Pg.253]")
write_row("Life - PAA",       s1$reins_life_paa,    NA, NA, "Note 7 [Pg.253]")
write_row("Non-Life - GMM",   s1$reins_nonlife_gmm, NA, NA, "Note 7 [Pg.253]")
write_row("Non-Life - PAA",   s1$reins_nonlife_paa, NA, NA, "Note 7 [Pg.253]")
write_row("Health - GMM",     s1$reins_health_gmm,  NA, NA, "Note 7 [Pg.253]")
write_row("Health - PAA",     s1$reins_health_paa,  NA, NA, "Note 7 [Pg.253]")
write_row("Total Reinsurance Ceded", s1$reins_total, NA, NA, "Subtotal")
blank(2)

# ---- (2) FINANCIAL PERFORMANCE -----------------------------------------------

write_header("(2) FINANCIAL PERFORMANCE")
blank()
write_header("(i) OVERVIEW OF RESULTS BEFORE TAX:")
write_row("a) Net insurance result",     s2a$net_insurance_result,  NA, NA, "Note 10 [Pg.296]")
write_row("1) CSM release",              s2a$csm_release_total,     NA, NA, "Note 10 [Pg.297]")
write_row("2) RA release",               s2a$ra_release_total,      NA, NA, "Note 10 [Pg.297]")
write_row("5) PAA Insurance revenue",    s2a$paa_insurance_revenue, NA, NA, "Note 10 [Pg.297]")
write_row("   PAA Incurred claims",      s2a$paa_incurred_claims,   NA, NA, "Note 7 [Pg.257-270]")
write_row("   GMM Incurred claims",      s2a$gmm_incurred_claims,   NA, NA, "Note 7 [Pg.257-270]")
blank()
write_row("b) Net reinsurance result",   s2a$net_reinsurance_result, NA, NA, "Pg.199")
write_row("c) Net investment result",    s2b$net_financial_result_total, NA, NA, "Note 11 [Pg.298]")
write_row("  Investment result (re)ins", s2b$investment_result_reinsurance, NA, NA, "Note 11 [Pg.298]")
write_row("  Finance IC - policyholders", s2b$finance_result_ic_policyholders, NA, NA, "Note 11 [Pg.298]")
write_row("  Finance IC - general acct", s2b$finance_result_ic_general, NA, NA, "Note 11 [Pg.298]")
write_row("  Finance result reinsurance", s2b$finance_result_reinsurance, NA, NA, "Note 11 [Pg.298]")
blank()
write_row("d) Other results",            s2c$income_service_contracts, NA, NA, "")
write_row("  Fee and commission results", s2c$income_service_contracts, NA, NA, "Note 22 [Pg.317]")
write_row("  Non-attributable opex",     safe_neg(s2c$other_operating_expenses), NA, NA, "Pg.199")
write_row("  Associates & JVs",          s2c$income_associates,        NA, NA, "Pg.199")
write_row("  Other",                     s2c$other_income,             NA, NA, "Note 22/25")
blank()
write_row("TOTAL RESULT BEFORE TAX (P&L)", s2a$profit_before_tax, NA, NA, "Pg.199")
blank(2)

# 4) Discount rates
write_header("(4) OVERVIEW OF DISCOUNT RATE/CURVE:")
write_header("a) Liquid curve / GMM & VFA")
write_row("1 year",    s2d$liquid_1y,  NA, NA, "Pg.273")
write_row("5 years",   s2d$liquid_5y,  NA, NA, "Pg.273")
write_row("10 years",  s2d$liquid_10y, NA, NA, "Pg.273")
write_row("15 years",  s2d$liquid_15y, NA, NA, "Pg.273")
write_row("20 years",  s2d$liquid_20y, NA, NA, "Pg.273")
write_row("30 years",  s2d$liquid_30y, NA, NA, "Pg.273")
write_row("50 years",  s2d$liquid_50y, NA, NA, "Pg.273")
blank()
write_header("b) Illiquid curve / GMM & VFA")
write_row("1 year",    s2d$illiquid_1y,  NA, NA, "Pg.275")
write_row("5 years",   s2d$illiquid_5y,  NA, NA, "Pg.275")
write_row("10 years",  s2d$illiquid_10y, NA, NA, "Pg.275")
write_row("15 years",  s2d$illiquid_15y, NA, NA, "Pg.275")
write_row("20 years",  s2d$illiquid_20y, NA, NA, "Pg.275")
write_row("30 years",  s2d$illiquid_30y, NA, NA, "Pg.275")
write_row("50 years",  s2d$illiquid_50y, NA, NA, "Pg.275")
blank()
write_header("c) Supplementary information")
write_row("i) Last Liquid point (years)",  s2d$last_liquid_point,    NA, NA, "Pg.273")
write_row("ii) Long Term Forward Rate",    s2d$ltfr,                 NA, NA, "Pg.273")
write_row("Cost of capital rate",          s2d$cost_of_capital_rate, NA, NA, "Pg.273")
write_row("Confidence level - Life",       s2d$confidence_life,      NA, NA, "Pg.275")
write_row("Confidence level - Non-Life",   s2d$confidence_nonlife,   NA, NA, "Pg.275")
write_row("Confidence level - Health",     s2d$confidence_health,    NA, NA, "Pg.275")
blank(2)

# ---- (3) LRC AND LIC DEEP-DIVE -----------------------------------------------

write_header("(3) LRC AND LIC DEEP-DIVE")
blank()
write_header("(i.a) OVERVIEW OF CSM DEVELOPMENT:")
write_row("a) Opening balance",              s3a$csm_opening_total,          NA, NA, "Note 7")
write_row("b.1) New business",               s3a$csm_new_business,           NA, NA, "Note 7")
write_row("b.2) Acquisitions",               s3a$csm_acquisitions,           NA, NA, "Note 7")
write_row("c) Insurance finance results",    s3a$csm_finance_result,         NA, NA, "Note 7")
write_row("d) Future service changes",       s3a$csm_future_service_changes, NA, NA, "Note 7")
write_row("e) CSM release",                  s3a$csm_release,                NA, NA, "Note 7")
write_row("f) Foreign currency",             s3a$csm_fx,                     NA, NA, "Note 7")
write_row("g) Other",                        s3a$csm_other,                  NA, NA, "Note 7")
write_row("h) Closing balance",              s3a$csm_closing_total,          NA, NA, "Note 7")
blank()
write_header("CSM DEVELOPMENT WITH SPLIT:")
write_row("A) Opening balance (Non-Life GMM)", s3a$csm_opening_nonlife, NA, NA, "Note 7 [Pg.259-260]")
write_row("B) Opening balance (Life GMM & VFA)", s3a$csm_opening_life,  NA, NA, "Note 7 [Pg.269-270]")
write_row("C) Opening balance (Health)",       s3a$csm_opening_health,  NA, NA, "Note 7")
write_row("A.1) New business (Non-Life)",      s3a$csm_new_business_nonlife, NA, NA, "Note 7 [Pg.259-260]")
write_row("B.1) New business (Life)",          s3a$csm_new_business_life,    NA, NA, "Note 7 [Pg.269-270]")
write_row("A.3) Finance (Non-Life)",           s3a$csm_finance_nonlife,      NA, NA, "Note 7 [Pg.259-260]")
write_row("B.3) Finance (Life)",               s3a$csm_finance_life,         NA, NA, "Note 7 [Pg.269-270]")
write_row("A.4) Future service (Non-Life)",    s3a$csm_future_changes_nonlife, NA, NA, "Note 7 [Pg.259-260]")
write_row("B.4) Future service (Life)",        s3a$csm_future_changes_life,  NA, NA, "Note 7 [Pg.269-270]")
write_row("A.6) CSM release (Non-Life)",       s3a$csm_release_nonlife,      NA, NA, "Note 7 [Pg.259-260]")
write_row("B.6) CSM release (Life)",           s3a$csm_release_life,         NA, NA, "Note 7 [Pg.269-270]")
write_row("C.6) CSM release (Health)",         s3a$csm_release_health,       NA, NA, "Note 7")
write_row("A.8) Other (Non-Life)",             s3a$csm_other_nonlife,        NA, NA, "Note 7 [Pg.259-260]")
write_row("B.8) Other (Life)",                 s3a$csm_other_life,           NA, NA, "Note 7 [Pg.269-270]")
write_row("A) Closing balance (Non-Life)",     s3a$csm_closing_nonlife,      NA, NA, "Note 7 [Pg.259-260]")
write_row("B) Closing balance (Life)",         s3a$csm_closing_life,         NA, NA, "Note 7 [Pg.269-270]")
write_row("C) Closing balance (Health)",       s3a$csm_closing_health,       NA, NA, "Note 7")
blank()
write_header("(i.b) CSM RELEASE MATURITY:")
write_row("a) 0-1 year",   s3a$csm_maturity_lt1y, NA, NA, "Note 7 [Pg.254]")
write_row("b) 1-5 years",  s3a$csm_maturity_1to5y, NA, NA, "Note 7 [Pg.254]")
blank(2)

write_header("(ii.a) OVERVIEW OF RISK ADJUSTMENT DEVELOPMENT:")
write_row("a) Opening balance",       s3b$ra_opening_total,            NA, NA, "Note 7")
write_row("b.1) New business",        s3b$ra_new_business,             NA, NA, "Note 7")
write_row("b.2) Acquisitions",        s3b$ra_acquisitions,             NA, NA, "Note 7")
write_row("c) Insurance finance",     s3b$ra_finance_result,           NA, NA, "Note 7")
write_row("d.i) Future svc (adj CSM)", s3b$ra_future_service_adj_csm, NA, NA, "Note 7")
write_row("d.ii) Future svc (no CSM)", s3b$ra_future_service_no_csm,  NA, NA, "Note 7")
write_row("e) Past service",          s3b$ra_past_service,             NA, NA, "Note 7")
write_row("f) RA release",            s3b$ra_release,                  NA, NA, "Note 7")
write_row("g) Foreign currency",      s3b$ra_fx,                       NA, NA, "Note 7")
write_row("h) Other",                 s3b$ra_other,                    NA, NA, "Note 7")
write_row("i) Closing balance",       s3b$ra_closing_total,            NA, NA, "Note 7")
blank()
write_row("Policy choice to disaggregate IFIE?", s3b$disaggregate_ifie, NA, NA, "")
write_header("RA SPLIT BY SEGMENT:")
write_row("A) Opening (Non-Life GMM)", s3b$ra_opening_nonlife, NA, NA, "Note 7 [Pg.258]")
write_row("B) Opening (Life GMM&VFA)", s3b$ra_opening_life,    NA, NA, "Note 7 [Pg.269]")
write_row("A.1) New business (Non-Life)", s3b$ra_new_business_nonlife, NA, NA, "Note 7 [Pg.258]")
write_row("B.1) New business (Life)",    s3b$ra_new_business_life,    NA, NA, "Note 7 [Pg.269]")
write_row("A.3) Finance (Non-Life)",     s3b$ra_finance_nonlife,      NA, NA, "Note 7 [Pg.258]")
write_row("B.3) Finance (Life)",         s3b$ra_finance_life,         NA, NA, "Note 7 [Pg.269]")
write_row("A.7) Release (Non-Life)",     s3b$ra_release_nonlife,      NA, NA, "Note 7 [Pg.258]")
write_row("B.7) Release (Life)",         s3b$ra_release_life,         NA, NA, "Note 7 [Pg.269]")
write_row("A) Closing (Non-Life GMM)",   s3b$ra_closing_nonlife,      NA, NA, "Note 7 [Pg.258]")
write_row("B) Closing (Life GMM&VFA)",   s3b$ra_closing_life,         NA, NA, "Note 7 [Pg.269]")
blank(2)

write_header("(iii.a) LOSS COMPONENT — GMM:")
write_row("a) Opening balance",              s3c$lc_gmm_opening,           NA, NA, "Note 7")
write_row("b) Losses on onerous contracts",  s3c$lc_gmm_losses_recognised, NA, NA, "Note 7")
write_row("  i) New business",               s3c$lc_gmm_new_business,      NA, NA, "Note 7")
write_row("  ii) Systematic allocation",     s3c$lc_gmm_systematic_alloc,  NA, NA, "Note 7")
write_row("  iii) Future service changes",   s3c$lc_gmm_future_service_changes, NA, NA, "Note 7")
write_row("c) Insurance finance",            s3c$lc_gmm_finance,           NA, NA, "Note 7")
write_row("d) Other",                        s3c$lc_gmm_other,             NA, NA, "Note 7")
write_row("e) Closing balance",              s3c$lc_gmm_closing,           NA, NA, "Note 7")
blank()
write_header("(iii.b) LOSS COMPONENT — PAA:")
write_row("a) Opening balance",              s3c$lc_paa_opening,              NA, NA, "Note 7")
write_row("b) Losses on onerous contracts",  s3c$lc_paa_losses_recognised,    NA, NA, "Note 7")
write_row("  iii) Future service changes",   s3c$lc_paa_future_service_changes, NA, NA, "Note 7")
write_row("c) Finance",                      s3c$lc_paa_finance,              NA, NA, "Note 7")
write_row("e) Closing balance",              s3c$lc_paa_closing,              NA, NA, "Note 7")
blank()
write_header("LOSS COMPONENT — LIFE:")
write_row("a) Opening",           s3c$lc_life_opening,        NA, NA, "Note 7 [Pg.268]")
write_row("b) Losses recognised", s3c$lc_life_losses,         NA, NA, "Note 7 [Pg.268]")
write_row("  New business",       s3c$lc_life_new_business,   NA, NA, "Note 7 [Pg.268]")
write_row("  Systematic alloc",   s3c$lc_life_systematic,     NA, NA, "Note 7 [Pg.268]")
write_row("  Future changes",     s3c$lc_life_future_changes, NA, NA, "Note 7 [Pg.268]")
write_row("c) Finance",           s3c$lc_life_finance,        NA, NA, "Note 7 [Pg.268]")
write_row("d) Other",             s3c$lc_life_other,          NA, NA, "Note 7 [Pg.268]")
write_row("e) Closing",           s3c$lc_life_closing,        NA, NA, "Note 7 [Pg.268]")
blank()
write_header("LOSS COMPONENT — NON-LIFE:")
write_row("a) Opening",           s3c$lc_nonlife_opening,     NA, NA, "Note 7 [Pg.258]")
write_row("b) GMM losses",        s3c$lc_nonlife_gmm_losses,  NA, NA, "Note 7 [Pg.258]")
write_row("c) Finance",           s3c$lc_nonlife_finance,     NA, NA, "Note 7 [Pg.258]")
write_row("d) Other",             s3c$lc_nonlife_other,       NA, NA, "Note 7 [Pg.258]")
write_row("e) Closing",           s3c$lc_nonlife_closing,     NA, NA, "Note 7 [Pg.258]")
blank()
write_header("LOSS COMPONENT — HEALTH:")
write_row("a) Opening",           s3c$lc_health_opening,         NA, NA, "Note 7 [Pg.263]")
write_row("b) Losses recognised", s3c$lc_health_losses,          NA, NA, "Note 7 [Pg.263]")
write_row("  Future changes",     s3c$lc_health_future_changes,  NA, NA, "Note 7 [Pg.263]")
write_row("e) Closing",           s3c$lc_health_closing,         NA, NA, "Note 7 [Pg.263]")
blank(2)

# ---- (5) CAPITAL & FINANCIAL POSITIONS ----------------------------------------

write_header("(5) CAPITAL & FINANCIAL POSITIONS")
blank()
write_header("(i) Solvency ratios")
write_row("a) Solvency II ratio",           s5$solvency2_ratio,    NA, NA, "Pg.34")
write_row("b) Solvency II target ratio",    s5$solvency2_target,   NA, NA, "Pg.34")
write_row("c) Solvency Capital Req (SCR)",  s5$scr,                NA, NA, "Pg.34")
write_row("d) Eligible own funds",          s5$eligible_own_funds, NA, NA, "Pg.34")
write_row("e) Free capital (EOF - target*SCR)",
          if (!is.null(s5$eligible_own_funds) && !is.null(s5$solvency2_target) && !is.null(s5$scr))
            s5$eligible_own_funds - s5$solvency2_target * s5$scr else NA,
          NA, NA, "Formula driven")
write_row("f) Free capital (EOF - SCR)",
          if (!is.null(s5$eligible_own_funds) && !is.null(s5$scr))
            s5$eligible_own_funds - s5$scr else NA,
          NA, NA, "Formula driven")
write_row("g) Capital generated",           s5$capital_generated,  NA, NA, "Pg.34")
blank(2)

# ---- (6) INVESTMENT ASSET MIX -------------------------------------------------

write_header("(6) INVESTMENT ASSET MIX")
blank()
write_header("Insurance and other activities")
write_row("Equities & similar",            s6$equities,              NA, NA, "Note 6 [Pg.248]")
write_row("Bonds - Government",            s6$govt_bonds,            NA, NA, "Note 6 [Pg.248]")
write_row("Securitised bonds",             s6$securitised_bonds,     NA, NA, "Note 6 [Pg.248]")
write_row("Corporate bonds",               s6$corporate_bonds,       NA, NA, "Note 6 [Pg.248]")
write_row("Convertible bonds",             s6$convertible_bonds,     NA, NA, "Note 6 [Pg.248]")
write_row("Mortgages (insurance)",         s6$mortgages_insurance,   NA, NA, "Note 6 [Pg.248]")
write_row("Loans & deposits (insurance)",  s6$loans_deposits_insurance, NA, NA, "Note 6 [Pg.248]")
write_row("Other FIS (insurance)",         s6$other_fis_insurance,   NA, NA, "Note 6 [Pg.248]")
write_row("Total FIS (insurance)",         s6$total_fis_insurance,   NA, NA, "Subtotal")
write_row("Derivatives (insurance)",       s6$derivatives_insurance, NA, NA, "Note 6 [Pg.248]")
write_row("Other financial (insurance)",   s6$other_financial_insurance, NA, NA, "Note 6 [Pg.248]")
write_row("Total investments (insurance)", s6$total_investments_insurance, NA, NA, "Subtotal")
blank()
write_header("Banking credit portfolio")
write_row("Mortgages (banking)",           s6$mortgages_banking,        NA, NA, "Note 6 [Pg.248]")
write_row("Loans & deposits (banking)",    s6$loans_deposits_banking,   NA, NA, "Note 6 [Pg.248]")
write_row("Other FIS (banking)",           s6$other_fis_banking,        NA, NA, "Note 6 [Pg.248]")
write_row("Total FIS (banking)",           s6$total_fis_banking,        NA, NA, "Subtotal")
write_row("Derivatives (banking)",         s6$derivatives_banking,      NA, NA, "Note 6 [Pg.248]")
write_row("Other financial (banking)",     s6$other_financial_banking,  NA, NA, "Note 6 [Pg.248]")
write_row("Total banking portfolio",       s6$total_banking,            NA, NA, "Subtotal")
blank()
write_row("Investment property",           s6$investment_property,      NA, NA, "Note 5 [Pg.247]")
write_row("Total investments",             s6$total_investments,        NA, NA, "Subtotal")
blank()
write_header("Measurement category:")
write_row("FVOCI",          s6$fvoci,          NA, NA, "Note 6 [Pg.248]")
write_row("FVTPL",          s6$fvtpl,          NA, NA, "Note 6 [Pg.248]")
write_row("Amortised cost", s6$amortised_cost, NA, NA, "Note 6 [Pg.248]")
blank(2)

# ---- (7) FINANCED EMISSIONS ---------------------------------------------------

write_header("(7) INVESTMENT ASSET MIX (financed emissions)")
blank()
write_header("1. Own risk")
write_row("Listed equity",           s7$own_listed_equity,     NA, NA, "Supplement [Pg.157]")
write_row("Corporate bonds",         s7$own_corporate_bonds,   NA, NA, "Supplement [Pg.157]")
write_row("Government bonds",        s7$own_govt_bonds,        NA, NA, "Supplement [Pg.157]")
write_row("Mortgages (insurance)",   s7$own_mortgages_insurance, NA, NA, "Supplement [Pg.157]")
write_row("Mortgages (banking)",     s7$own_mortgages_banking,  NA, NA, "Supplement [Pg.157]")
write_row("Investment property",     s7$own_investment_property, NA, NA, "Supplement [Pg.157]")
write_row("Investment loans",        s7$own_investment_loans,  NA, NA, "Supplement [Pg.157]")
write_row("Other categories",        s7$own_other,             NA, NA, "Supplement [Pg.157]")
write_row("Total own risk",          s7$own_total,             NA, NA, "Subtotal")
blank()
write_header("2. Account and risk policyholders")
write_row("Equities",                s7$ph_equities,      NA, NA, "Supplement [Pg.157]")
write_row("Corporate bonds",         s7$ph_corporate_bonds, NA, NA, "Supplement [Pg.157]")
write_row("EU government bonds",     s7$ph_eu_govt_bonds,  NA, NA, "Supplement [Pg.157]")
write_row("Other categories",        s7$ph_other,          NA, NA, "Supplement [Pg.157]")
write_row("Total policyholders",     s7$ph_total,          NA, NA, "Subtotal")
blank(2)

# ---- Non-GAAP premium --------------------------------------------------------

write_header("(5) PREMIUM AS NON-GAAP MEASURE")
write_row("a) GWP - Life, Health & Pensions", s_gwp$gwp_life_health_pensions, NA, NA, "Pg.30")
write_row("  i) Pensions & Life",             s_gwp$gwp_pensions_life,        NA, NA, "Pg.30")
write_row("  ii) Health",                     s_gwp$gwp_health,               NA, NA, "Pg.30")
write_row("b) GWP - Non-Life",                s_gwp$gwp_nonlife,              NA, NA, "Pg.30")
write_row("c) GWP - International Non-Life",  s_gwp$gwp_intl_nonlife,         NA, NA, "Pg.30")
write_row("d) GWP - International Health",    s_gwp$gwp_intl_health,          NA, NA, "Pg.30")
write_row("Total GWP",                        s_gwp$gwp_total,                NA, NA, "Subtotal")

# ---- Column widths & freeze --------------------------------------------------

setColWidths(wb, ws, cols = 1,   widths = 65)
setColWidths(wb, ws, cols = 2:4, widths = 14)
setColWidths(wb, ws, cols = 5,   widths = 45)
freezePane(wb, ws, firstActiveRow = 5, firstActiveCol = 2)

# ---- Save --------------------------------------------------------------------

saveWorkbook(wb, OUT_PATH, overwrite = TRUE)
message(glue::glue("\nDone. Output written to: {OUT_PATH}"))
