# ==============================================================================
# extract_all_2025.R
#
# Extracts IFRS 17 financial data from 2025 annual reports for:
#   - Achmea BV
#   - a.s.r. Nederland N.V.
#   - NN Group N.V.
#   - Athora Netherlands N.V.
#
# Output: All_Insurers_2025_Databook.xlsx  (one tab per insurer)
#
# Usage: Rscript extract_all_2025.R
# Required packages: pdftools, httr2, jsonlite, openxlsx, glue
# API key: ANTHROPIC_API_KEY in .Renviron
# ==============================================================================

library(pdftools)
library(httr2)
library(jsonlite)
library(openxlsx)

# ---- Config ------------------------------------------------------------------

OUT_PATH   <- "All_Insurers_2025_Databook.xlsx"
MODEL      <- "claude-opus-4-6"
MAX_TOKENS <- 4096

if (file.exists(".Renviron")) readRenviron(".Renviron")
api_key <- Sys.getenv("ANTHROPIC_API_KEY")
if (!nzchar(api_key)) stop("ANTHROPIC_API_KEY not set. Add it to .Renviron.")

# ---- Page maps ---------------------------------------------------------------
# Each list entry is a named vector of page numbers.
# Comments document what is on each page.

PAGE_MAPS <- list(

  achmea = list(
    pdf               = "Annual report Achmea BV.pdf",
    short_name        = "Achmea",
    balance_sheet     = 198,
    income_statement  = 199,
    portfolio_overview = 253,           # Note 7 summary (GMM/PAA/VFA by segment)
    nonlife_movements = c(257, 259),    # p257=total NL 2025, p259=GMM NL 2025
    health_movements  = c(263),         # p263=total Health 2025
    life_movements    = c(267, 269, 270),
    csm_rollforward   = c(259, 269, 270), # Non-Life GMM + Life GMM/VFA (2025 only)
    ra_rollforward    = c(257, 259, 263, 267, 269),
    loss_component    = c(257, 259, 263, 267, 269),
    csm_maturity      = 254,
    insurance_svc_result = c(296, 297),
    net_financial_result = c(298, 299),
    discount_rates    = c(275, 276, 277), # p275=UFR text, p276=rate table, p277=confidence
    solvency          = c(34, 35),
    investments_note  = c(248, 249, 250, 251, 252),
    gross_premium     = c(30, 33, 39),
    # Notes on discount curve structure (used in prompt below):
    # - Table on p276: three EUR curves (PAA Euro, GMM Euro, Life/VFA Euro) as min-max ranges
    # - GMM Euro  -> liquid_*;  Life Netherlands GMM & VFA -> illiquid_*
    # - Dutch comma decimals (2,17 = 2.17); return midpoint of range
    # - UFR = 2.3% (stated in text p275); CoC = 4.5% (p276); confidence on p277
    disc_liquid_label    = "GMM Euro",
    disc_illiquid_label  = "Life Netherlands GMM and VFA"
  ),

  asr = list(
    pdf               = "2025-annual-report-asr.pdf",
    short_name        = "a.s.r.",
    balance_sheet     = c(262),         # Consolidated financial statements start
    income_statement  = c(263),         # Consolidated income statement
    portfolio_overview = c(315, 316),   # Note 7.5.13: Insurance contract liabilities
    nonlife_movements = c(316, 317, 319), # Non-life: total + GMM component tables 2025
    life_movements    = c(321, 322, 323), # Life: total + GMM component tables 2025
    csm_rollforward   = c(319, 323),    # Non-life GMM (319) + Life GMM/VFA (323) 2025
    ra_rollforward    = c(316, 319, 321, 323),
    loss_component    = c(316, 317, 321, 322),
    csm_maturity      = c(332, 339),     # p332=GMM maturity table (section 7.5.13.6); p339=VFA maturity table (section 7.5.14.3)
    insurance_svc_result = c(347, 348),  # p347=7.6.1 insurance contract revenue (CSM/RA release by segment)
                                         # p348=insurance contract revenue reconciliation
    net_financial_result = c(350),       # p350=section 7.6.7 investment and finance result
    discount_rates    = c(329, 330),     # p329=actuarial assumptions (UFR=3.20%, FSP=20y, CoC=6%, confidence 95-98%)
                                         # p330=discount curve table: 0%(min)=liquid, 100%(max)=illiquid, maturities 1-50y
    solvency          = c(406),          # p406: EOF=12,618 (excl FI), SCR=5,743, ratio=220%
    investments_note  = c(306, 307, 308),
    gross_premium     = c(286, 287, 288, 295, 296, 297), # segment P&L + insurance service result
    # Notes on discount curve:
    # - UFR = 3.20% (2025), FSP = 20 years, based on 6-month EURIBOR swap
    # - CoC = 6%; confidence 1-year 95-98%, ultimate 66-76%
    # - Table on p330: rows labeled "0% (min)" = liquid, "100% (max)" = illiquid
    disc_liquid_label    = "0% (min)",
    disc_illiquid_label  = "100% (max)"
  ),

  nn = list(
    pdf               = "nn-group-annual-report-2025.pdf",
    short_name        = "NN Group",
    balance_sheet     = c(161, 162),
    income_statement  = c(163),
    portfolio_overview = c(192, 221),   # p192: insurance contracts by measurement model (Life GMM/VFA, Non-life GMM/PAA); p221: by segment
    nonlife_movements = c(200, 201),    # PAA movements 2025
    life_movements    = c(193, 194),    # GMM/VFA movements 2025
    csm_rollforward   = c(193, 195, 196), # GMM/VFA with CSM disaggregation
    ra_rollforward    = c(193, 197, 200),
    loss_component    = c(197, 198, 200, 201),
    csm_maturity      = c(195, 196),
    insurance_svc_result = c(163, 164),
    net_financial_result = c(163, 164),
    discount_rates    = c(191, 192),    # Discount curve methodology + confidence levels
    solvency          = c(37, 38, 45),
    investments_note  = c(173, 174, 175, 176), # p173-176: investment breakdown + FVTPL split (policyholders=47,925 + company=8,645)
    gross_premium     = c(163, 221),    # p163: income statement, p221: GWP Life=8,787 Non-life=4,469 Total=13,256
    # p191 table columns are "General Model" (liquid) and "Variable Fee Approach" (illiquid).
    # Maturities in table: 1y, 5y, 10y, 20y, 30y, 40y. No 15y or 50y rows.
    # LTFR = 3.20% (maps to ufr). LLP = 30 years (maps to fsp_years). CoC = 4%.
    # p192 confidence: Life 1y=86% / ult=67%, Non-life 1y=67% / ult=62%.
    disc_liquid_label    = "General Model",
    disc_illiquid_label  = "Variable Fee Approach"
  ),

  athora = list(
    pdf               = "annual-report-athora-netherlands-nv-2025.pdf",
    short_name        = "Athora NL",
    balance_sheet     = c(80),           # p80=consolidated statement of financial position
    income_statement  = c(81),           # p81=P&L: insurance revenue=2,110, service result=262, PBT=-237
    # Athora is 100% Life (GMM+VFA). No Non-life, no Health, no PAA.
    # p119 table: GMM=29,255, VFA=14,132, Total=43,387
    # p80 balance sheet: insurance contract liabilities=43,387, reinsurance liabilities=104, reinsurance assets=3
    portfolio_overview = c(80, 119),
    nonlife_movements  = c(120),         # (none — all Life, kept for schema compat)
    life_movements     = c(120, 122),    # p120=LRC table 2025; p122=measurement component 2025
    # S3 pages: only 2 unique pages needed (was 11 page-slots across 3 old calls)
    # p122: CSM columns (opening=1,994/closing=2,645) + RA column (opening=942/closing=746)
    # p120: Loss component column (opening=98/closing=85)
    csm_rollforward   = c(122),
    ra_rollforward    = c(122),
    loss_component    = c(120),
    # p131=coverage units + acquisition cash flow note; p132=cash flow maturity by due date
    # (Athora does not publish a CSM-release-by-time-bucket table; maturity fields may be null)
    csm_maturity      = c(131, 132),
    # p81=income statement (CSM release=-197, RA release=-36 implicit in service result)
    # p120=LRC table (explicit CSM/RA release lines); p122=measurement component (CSM/RA columns)
    insurance_svc_result = c(81, 120, 122),
    net_financial_result = c(81),
    # p129=rate table: Liquid/Illiquid for 1y/5y/10y/15y/20y/30y/50y (exact column headers "Liquid"/"Illiquid")
    # p130=methodology: UFR=3.3%, Last Liquid Point=20y, CoC implied, confidence=68.7%
    discount_rates    = c(129, 130),
    # p177=SII ratio=197%, eligible own funds=3,532, SCR=1,790; p178=breakdown detail
    solvency          = c(177, 178),
    investments_note  = c(99, 100),      # p99: bonds=16,313, mortgages=3,282, total=66,502 (all FVTPL)
    # p14: Gross Written Premium=3,147, Gross Inflows=3,960, Net inflow PPI=813
    # p81: insurance revenue=2,110 (IFRS 17 line, best proxy if GWP not available)
    gross_premium     = c(14, 81),
    disc_liquid_label    = "Liquid",    # exact column header on p129
    disc_illiquid_label  = "Illiquid"  # exact column header on p129
  )
)

# ---- Helpers -----------------------------------------------------------------

# Strip navigation chrome that appears on every page of Dutch insurer reports:
# header band ("... Annual accounts ... Other information ... Appendix") and
# page footer ("[Company] Annual Report 20xx | NNN").
# Also collapses runs of 3+ spaces to 2 — preserves table column alignment
# while meaningfully reducing token count (~15-20% per page).
clean_page_text <- function(text) {
  lines <- strsplit(text, "\n", fixed = TRUE)[[1]]
  lines <- lines[!grepl("Annual accounts.{1,60}Other information.{1,60}Appendix", lines)]
  lines <- lines[!grepl("Annual Report 20\\d{2}\\s*\\|\\s*\\d{1,4}\\s*$", lines)]
  lines <- gsub(" {3,}", "  ", lines)
  lines <- lines[nzchar(trimws(lines))]
  paste(lines, collapse = "\n")
}

extract_pages <- function(pages_text, page_nums) {
  cleaned <- vapply(pages_text[page_nums], clean_page_text, character(1L),
                    USE.NAMES = FALSE)
  paste(cleaned, collapse = "\n\n---PAGE BREAK---\n\n")
}

call_claude <- function(prompt_text, section_name, insurer_name) {
  label <- glue::glue("{insurer_name} / {section_name}")
  message(glue::glue("  Calling Claude for: {label} ..."))

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
        "Return integers where possible (no decimal for whole numbers).",
        "Numbers may use Dutch/European comma decimal notation (2,17 means 2.17) — convert to dot decimals.",
        "Min-max ranges (e.g. 2.17-2.41) should be returned as their midpoint unless instructed otherwise."
      )
    )) |>
    req_timeout(180) |>
    req_retry(max_tries = 3, backoff = ~15,
              is_transient = \(resp) httr2::resp_status(resp) %in% c(429, 500, 502, 503, 529)) |>
    req_perform()

  body <- resp_body_json(resp)
  raw  <- body$content[[1]]$text
  # Extract the first complete flat JSON object.
  # [^{}]* correctly ignores any } characters in prose that follows the JSON,
  # and handles prose-before-fence, prose-after-fence, and bare JSON equally.
  # All our prompts request flat scalar JSON with no nested {}, so this is safe.
  clean <- trimws(raw)
  m <- regmatches(clean, regexpr("\\{[^{}]*\\}", clean, perl = TRUE))
  clean <- if (length(m) == 1L) m else ""

  tryCatch(
    fromJSON(clean, simplifyVector = FALSE),
    error = function(e) {
      warning(glue::glue("JSON parse failed for {label}: {conditionMessage(e)}\nRaw: {raw}"))
      NULL
    }
  )
}

safe_val <- function(x) if (is.null(x) || length(x) == 0) NA_real_ else x

# ==============================================================================
# EXTRACTION FUNCTION — runs all Claude calls for one insurer
# ==============================================================================

extract_insurer <- function(pm) {
  insurer <- pm$short_name
  message(paste0("\n", strrep("=", 60)))
  message(paste0("Loading PDF: ", pm$pdf))
  pages_text <- pdf_text(pm$pdf)
  message(paste0("  ", length(pages_text), " pages loaded."))

  res <- list()

  # ---- S1: Portfolio overview -------------------------------------------------
  message(glue::glue("\n[S1] Portfolio overview ({insurer})..."))
  s1_text <- extract_pages(pages_text, pm$portfolio_overview)
  res$s1 <- call_claude(paste0(
    "From this ", insurer, " 2025 annual report extract (IFRS 17 insurance contract overview), ",
    "extract 31 December 2025 net balance sheet values (EUR millions, net = liabilities minus assets). ",
    "Look for a summary table showing insurance contract liabilities by measurement model. ",
    "The table may have rows for Life insurance contracts and Non-life insurance contracts, ",
    "with columns for General Model (GMM), Variable Fee Approach (VFA), Premium Allocation Approach (PAA) and Total. ",
    "Use the net total row (liabilities minus assets) for each segment.\n\n",
    "Return JSON:\n",
    '{"life_gmm": <Life GMM net>, "life_vfa": <Life VFA net>, "life_paa": <Life PAA net>,',
    ' "life_total": <Life total net>,',
    ' "nonlife_gmm": <Non-Life GMM net>, "nonlife_paa": <Non-Life PAA net>, "nonlife_total": <Non-Life total net>,',
    ' "health_gmm": <Health GMM net or null>, "health_paa": <Health PAA net or null>, "health_total": <Health total net or null>,',
    ' "total_gmm": <total GMM>, "total_vfa": <total VFA>, "total_paa": <total PAA>,',
    ' "total_direct": <grand total direct insurance contracts net>,',
    ' "reins_total": <total outward reinsurance contracts held net or null>}\n\n',
    "Text:\n", s1_text
  ), "portfolio_overview", insurer)

  # ---- S2a: Insurance service result ------------------------------------------
  message(glue::glue("\n[S2a] Insurance service result ({insurer})..."))
  s2a_text <- extract_pages(pages_text, c(pm$income_statement, pm$insurance_svc_result))
  res$s2a <- call_claude(paste0(
    "From this ", insurer, " 2025 annual report extract (income statement and insurance service result note), ",
    "extract 2025 values (EUR millions).\n\n",
    "Return JSON:\n",
    '{"net_insurance_result": <total insurance service result 2025>,',
    ' "csm_release_total": <CSM recognised for services provided>,',
    ' "ra_release_total": <Change in risk adjustment for non-financial risk released>,',
    ' "paa_insurance_revenue": <PAA insurance revenue total>,',
    ' "paa_incurred_claims": <PAA incurred claims total>,',
    ' "gmm_incurred_claims": <GMM incurred claims/benefits total>,',
    ' "net_reinsurance_result": <Net result from reinsurance contracts>,',
    ' "net_financial_result": <Net financial result from insurance activities>,',
    ' "profit_before_tax": <Profit before tax>,',
    ' "insurance_revenue_total": <Insurance revenue total>}\n\n',
    "Text:\n", s2a_text
  ), "insurance_service_result", insurer)

  # ---- S2b: Discount rates ----------------------------------------------------
  message(glue::glue("\n[S2b] Discount rates ({insurer})..."))
  s2d_text <- extract_pages(pages_text, pm$discount_rates)
  res$s2d <- call_claude(paste0(
    "From this ", insurer, " 2025 annual report extract (discount curve section), ",
    "extract discount rate parameters at 31 December 2025 for EUR insurance contracts.\n\n",
    "RULES:\n",
    "- Convert Dutch comma decimals (2,17 -> 2.17).\n",
    "- Convert percentages to decimals (3.20%% -> 0.032).\n",
    "- For min-max ranges (e.g. 2,17-2,41) return the midpoint.\n",
    "- '", pm$disc_liquid_label, "' maps to liquid_* keys.\n",
    "- '", pm$disc_illiquid_label, "' maps to illiquid_* keys.\n",
    "- UFR / Ultimate Forward Rate / LTFR (Long-Term Forward Rate): convert to decimal and return as ufr.\n",
    "- FSP / First Smoothing Point / Last Liquid Point (LLP): return as integer years and return as fsp_years.\n",
    "- Cost of capital rate: convert to decimal.\n",
    "- Confidence levels: look for % figures linked to Non-Life, Life, Health.\n\n",
    "Return JSON:\n",
    '{"liquid_1y": <number or null>, "liquid_5y": <number or null>, "liquid_10y": <number or null>,',
    ' "liquid_15y": <number or null>, "liquid_20y": <number or null>,',
    ' "liquid_30y": <number or null>, "liquid_40y": <number or null>, "liquid_50y": <number or null>,',
    ' "illiquid_1y": <number or null>, "illiquid_5y": <number or null>, "illiquid_10y": <number or null>,',
    ' "illiquid_15y": <number or null>, "illiquid_20y": <number or null>,',
    ' "illiquid_30y": <number or null>, "illiquid_40y": <number or null>, "illiquid_50y": <number or null>,',
    ' "ufr": <decimal or null>, "fsp_years": <integer or null>,',
    ' "cost_of_capital_rate": <decimal or null>,',
    ' "confidence_nonlife": <decimal or null>, "confidence_life": <decimal or null>,',
    ' "confidence_health": <decimal or null>}\n\n',
    "Text:\n", s2d_text
  ), "discount_rates", insurer)

  # ---- S3: LRC deep-dive (CSM + RA + Loss component) — single merged call -----
  # Deduplicate pages across the three sections; send each page only once.
  # Achmea: 13 page-slots → 6 unique | ASR: 10 → 6 | NN: 10 → 7 | Athora: 11 → 9
  message(glue::glue("\n[S3] LRC deep-dive ({insurer})..."))
  s3_pages <- sort(unique(c(pm$csm_rollforward, pm$ra_rollforward, pm$loss_component)))
  s3_text  <- extract_pages(pages_text, s3_pages)
  res$s3 <- call_claude(paste0(
    "From this ", insurer, " 2025 annual report extract (LRC/LIC movement tables), ",
    "extract ALL THREE of the following for 2025 (EUR millions). ",
    "Extract 2025 figures only — ignore 2024 comparative columns/tables.\n\n",
    "(A) CSM ROLLFORWARD: CSM may appear as dedicated columns or embedded in GMM tables.\n",
    "  csm_opening_total = opening balance; csm_new_business = contracts initially recognised;\n",
    "  csm_future_service_changes = changes in estimates adjusting CSM;\n",
    "  csm_finance_result = finance result on CSM; csm_release = CSM for services provided (negative);\n",
    "  csm_other = FX/acquisitions/other; csm_closing_total = closing balance.\n",
    "  Also split by segment where available: csm_*_nonlife and csm_*_life.\n\n",
    "(B) RISK ADJUSTMENT (RA) ROLLFORWARD: RA as standalone table or column.\n",
    "  ra_opening_total, ra_new_business, ra_future_service_adj_csm, ra_future_service_no_csm,\n",
    "  ra_past_service, ra_finance_result, ra_release, ra_other, ra_closing_total.\n",
    "  Segment splits: ra_opening_nonlife, ra_closing_nonlife, ra_opening_life, ra_closing_life.\n\n",
    "(C) LOSS COMPONENT: column in LRC movement tables.\n",
    "  lc_opening_total, lc_losses_recognised, lc_systematic_alloc, lc_future_service_changes,\n",
    "  lc_finance, lc_other, lc_closing_total.\n",
    "  Segment splits: lc_opening_nonlife, lc_closing_nonlife, lc_opening_life, lc_closing_life,\n",
    "  lc_opening_health, lc_closing_health.\n\n",
    "Return ONE JSON with all fields (use null for any value not found):\n",
    '{"csm_opening_total": <n>, "csm_new_business": <n>, "csm_future_service_changes": <n>,',
    ' "csm_finance_result": <n>, "csm_release": <n>, "csm_other": <n>, "csm_closing_total": <n>,',
    ' "csm_opening_nonlife": <n>, "csm_closing_nonlife": <n>,',
    ' "csm_release_nonlife": <n>, "csm_new_business_nonlife": <n>,',
    ' "csm_opening_life": <n>, "csm_closing_life": <n>,',
    ' "csm_release_life": <n>, "csm_new_business_life": <n>,',
    ' "ra_opening_total": <n>, "ra_new_business": <n>,',
    ' "ra_future_service_adj_csm": <n>, "ra_future_service_no_csm": <n>,',
    ' "ra_past_service": <n>, "ra_finance_result": <n>,',
    ' "ra_release": <n>, "ra_other": <n>, "ra_closing_total": <n>,',
    ' "ra_opening_nonlife": <n>, "ra_closing_nonlife": <n>,',
    ' "ra_opening_life": <n>, "ra_closing_life": <n>,',
    ' "lc_opening_total": <n>, "lc_losses_recognised": <n>,',
    ' "lc_systematic_alloc": <n>, "lc_future_service_changes": <n>,',
    ' "lc_finance": <n>, "lc_other": <n>, "lc_closing_total": <n>,',
    ' "lc_opening_nonlife": <n>, "lc_closing_nonlife": <n>,',
    ' "lc_opening_life": <n>, "lc_closing_life": <n>,',
    ' "lc_opening_health": <n>, "lc_closing_health": <n>}\n\n',
    "Text:\n", s3_text
  ), "lrc_deepdive", insurer)

  # ---- S3d: CSM maturity ------------------------------------------------------
  message(glue::glue("\n[S3d] CSM maturity ({insurer})..."))
  s3d_text <- extract_pages(pages_text, pm$csm_maturity)
  res$s3d <- call_claude(paste0(
    "From this ", insurer, " 2025 annual report extract, find the CSM maturity or coverage period breakdown ",
    "showing how much CSM is expected to be released per time bucket (EUR millions, 2025 year-end).\n\n",
    "Return JSON:\n",
    '{"csm_maturity_lt1y": <0-1 year>, "csm_maturity_1to5y": <1-5 years>,',
    ' "csm_maturity_5to10y": <5-10 years or null>, "csm_maturity_gt10y": <over 10 years or null>,',
    ' "csm_maturity_total": <total>}\n\n',
    "Text:\n", s3d_text
  ), "csm_maturity", insurer)

  # ---- S4: Solvency -----------------------------------------------------------
  message(glue::glue("\n[S4] Solvency ({insurer})..."))
  s4_text <- extract_pages(pages_text, pm$solvency)
  res$s4 <- call_claude(paste0(
    "From this ", insurer, " 2025 annual report extract, find the Solvency II ratio and capital at 31 December 2025.\n\n",
    "Return JSON:\n",
    '{"solvency2_ratio": <decimal e.g. 1.82 for 182%%>, "solvency2_target": <decimal or null>,',
    ' "scr": <EUR millions or null>, "eligible_own_funds": <EUR millions or null>,',
    ' "capital_generated": <EUR millions or null>}\n\n',
    "Text:\n", s4_text
  ), "solvency", insurer)

  # ---- S5: Investments --------------------------------------------------------
  message(glue::glue("\n[S5] Investments ({insurer})..."))
  s5_text <- extract_pages(pages_text, pm$investments_note)
  res$s5 <- call_claude(paste0(
    "From this ", insurer, " 2025 annual report extract, extract the investment asset mix at 31 December 2025 ",
    "(EUR millions). Classify by asset type. Separate own-risk (insurance) from policyholder / unit-linked if possible.\n\n",
    "Return JSON:\n",
    '{"equities": <number or null>, "govt_bonds": <number or null>, "corporate_bonds": <number or null>,',
    ' "mortgages": <number or null>, "other_fixed_income": <number or null>,',
    ' "derivatives_net": <number or null>, "other_investments": <number or null>,',
    ' "total_investments": <total>, "fvoci": <number or null>, "fvtpl": <number or null>,',
    ' "amortised_cost": <number or null>}\n\n',
    "Text:\n", s5_text
  ), "investments", insurer)

  # ---- S6: Gross written premium ----------------------------------------------
  message(glue::glue("\n[S6] Gross written premium ({insurer})..."))
  s6_text <- extract_pages(pages_text, pm$gross_premium)
  res$s6 <- call_claude(paste0(
    "From this ", insurer, " 2025 annual report extract, find gross written premium (GWP) or insurance revenue ",
    "split by Life, Non-Life, Health/Disability and total for 2025 (EUR millions). ",
    "If GWP is not available, use insurance revenue totals by segment as the best proxy. ",
    "Look for segment tables, notes with premium income, or consolidated income statement lines ",
    "that break out Life / Non-Life / Health / Pensions / International.\n\n",
    "Return JSON:\n",
    '{"gwp_life": <number or null>, "gwp_health": <number or null>,',
    ' "gwp_nonlife": <number or null>, "gwp_pensions": <number or null>,',
    ' "gwp_intl": <number or null>, "gwp_total": <number>}\n\n',
    "Text:\n", s6_text
  ), "gross_premium", insurer)

  res
}

# ==============================================================================
# EXCEL ASSEMBLY HELPERS
# ==============================================================================

write_insurer_tab <- function(wb, ws, insurer_name, res, pm) {

  row_ptr <- 1L

  write_header <- function(text, bold = TRUE, size = 11) {
    writeData(wb, ws, text, startRow = row_ptr, startCol = 1)
    if (bold) addStyle(wb, ws, createStyle(textDecoration = "bold", fontSize = size),
                       rows = row_ptr, cols = 1, stack = TRUE)
    row_ptr <<- row_ptr + 1L
  }

  write_row <- function(label, v2025, comment = "") {
    writeData(wb, ws, label,  startRow = row_ptr, startCol = 1)
    writeData(wb, ws, v2025,  startRow = row_ptr, startCol = 2)
    if (nzchar(comment))
      writeData(wb, ws, comment, startRow = row_ptr, startCol = 3)
    row_ptr <<- row_ptr + 1L
  }

  blank <- function(n = 1) { row_ptr <<- row_ptr + n }
  sv    <- function(x) safe_val(x)

  s1  <- res$s1;  s2a <- res$s2a; s2d <- res$s2d
  s3  <- res$s3;  s3d <- res$s3d   # s3 holds merged CSM + RA + LC rollforward
  s4  <- res$s4;  s5  <- res$s5;  s6  <- res$s6

  # Title
  writeData(wb, ws, paste("Mapping Financial Statements —", insurer_name), startRow = 1, startCol = 1)
  addStyle(wb, ws, createStyle(textDecoration = "bold", fontSize = 14), rows = 1, cols = 1, stack = TRUE)
  writeData(wb, ws, "SOTI  FY2025", startRow = 2, startCol = 1)
  writeData(wb, ws, data.frame(A = insurer_name, B = "FY2025", C = "Source"), startRow = 3, startCol = 1, colNames = FALSE)
  addStyle(wb, ws, createStyle(textDecoration = "bold"), rows = 3, cols = 1:3, gridExpand = TRUE, stack = TRUE)
  row_ptr <- 4L

  # ---- (1) PORTFOLIO OVERVIEW -------------------------------------------------
  write_header("(1) OVERVIEW OF PORTFOLIO")
  blank()
  write_header("(i) LIFE")
  write_row("General Measurement Model",       sv(s1$life_gmm),    "Note 7")
  write_row("Variable Fee Approach",           sv(s1$life_vfa),    "Note 7")
  write_row("Premium Allocation Approach",     sv(s1$life_paa),    "Note 7")
  write_row("Subtotal Life",                   sv(s1$life_total),  "Subtotal")
  blank()
  write_header("(ii) NON-LIFE")
  write_row("General Measurement Model",       sv(s1$nonlife_gmm),    "Note 7")
  write_row("Premium Allocation Approach",     sv(s1$nonlife_paa),    "Note 7")
  write_row("Subtotal Non-Life",               sv(s1$nonlife_total),  "Subtotal")
  blank()
  write_header("(iii) HEALTH")
  write_row("General Measurement Model",       sv(s1$health_gmm),    "Note 7")
  write_row("Premium Allocation Approach",     sv(s1$health_paa),    "Note 7")
  write_row("Subtotal Health",                 sv(s1$health_total),  "Subtotal")
  blank()
  write_header("(iv) TOTAL DIRECT")
  write_row("General Measurement Model",       sv(s1$total_gmm),    "Subtotal")
  write_row("Variable Fee Approach",           sv(s1$total_vfa),    "Subtotal")
  write_row("Premium Allocation Approach",     sv(s1$total_paa),    "Subtotal")
  write_row("TOTAL",                           sv(s1$total_direct), "Subtotal")
  blank()
  write_row("Total reinsurance ceded (net)",   sv(s1$reins_total),  "Note 7")
  blank(2)

  # ---- (2) FINANCIAL PERFORMANCE ----------------------------------------------
  write_header("(2) FINANCIAL PERFORMANCE")
  blank()
  write_row("a) Net insurance service result",   sv(s2a$net_insurance_result),  "P&L / Note 10")
  write_row("  1) CSM release",                  sv(s2a$csm_release_total),     "Note 10")
  write_row("  2) RA release",                   sv(s2a$ra_release_total),      "Note 10")
  write_row("  3) PAA insurance revenue",        sv(s2a$paa_insurance_revenue), "Note 10")
  write_row("  4) PAA incurred claims",          sv(s2a$paa_incurred_claims),   "Note 10")
  write_row("  5) GMM incurred claims",          sv(s2a$gmm_incurred_claims),   "Note 10")
  blank()
  write_row("b) Net reinsurance result",         sv(s2a$net_reinsurance_result), "P&L")
  write_row("c) Net financial result (ins act)", sv(s2a$net_financial_result),   "P&L")
  blank()
  write_row("PROFIT BEFORE TAX",                 sv(s2a$profit_before_tax),     "P&L")
  blank()
  write_row("Insurance revenue (total)",         sv(s2a$insurance_revenue_total), "P&L")
  blank(2)

  # ---- (3) DISCOUNT RATES -----------------------------------------------------
  write_header("(3) OVERVIEW OF DISCOUNT RATES / CURVES")
  blank()
  write_header("a) Liquid curve (GMM)")
  write_row("1 year",   sv(s2d$liquid_1y),  "Discount note")
  write_row("5 years",  sv(s2d$liquid_5y),  "Discount note")
  write_row("10 years", sv(s2d$liquid_10y), "Discount note")
  write_row("15 years", sv(s2d$liquid_15y), "Discount note")
  write_row("20 years", sv(s2d$liquid_20y), "Discount note")
  write_row("30 years", sv(s2d$liquid_30y), "Discount note")
  write_row("40 years", sv(s2d$liquid_40y), "Discount note")
  write_row("50 years", sv(s2d$liquid_50y), "Discount note")
  blank()
  write_header("b) Illiquid curve (Life / VFA)")
  write_row("1 year",   sv(s2d$illiquid_1y),  "Discount note")
  write_row("5 years",  sv(s2d$illiquid_5y),  "Discount note")
  write_row("10 years", sv(s2d$illiquid_10y), "Discount note")
  write_row("15 years", sv(s2d$illiquid_15y), "Discount note")
  write_row("20 years", sv(s2d$illiquid_20y), "Discount note")
  write_row("30 years", sv(s2d$illiquid_30y), "Discount note")
  write_row("40 years", sv(s2d$illiquid_40y), "Discount note")
  write_row("50 years", sv(s2d$illiquid_50y), "Discount note")
  blank()
  write_header("c) Supplementary")
  write_row("Ultimate Forward Rate (UFR)",   sv(s2d$ufr),                  "Discount note")
  write_row("First Smoothing Point (years)", sv(s2d$fsp_years),            "Discount note")
  write_row("Cost of capital rate",          sv(s2d$cost_of_capital_rate), "RA section")
  write_row("Confidence — Non-Life",         sv(s2d$confidence_nonlife),   "RA section")
  write_row("Confidence — Life",             sv(s2d$confidence_life),      "RA section")
  write_row("Confidence — Health",           sv(s2d$confidence_health),    "RA section")
  blank(2)

  # ---- (4) CSM AND RA ROLLFORWARDS --------------------------------------------
  write_header("(4) LRC / LIC DEEP-DIVE")
  blank()
  write_header("(i.a) CSM DEVELOPMENT — TOTAL")
  write_row("a) Opening balance",          sv(s3$csm_opening_total),          "Note 7")
  write_row("b) New business",             sv(s3$csm_new_business),           "Note 7")
  write_row("c) Finance result",           sv(s3$csm_finance_result),         "Note 7")
  write_row("d) Future service changes",   sv(s3$csm_future_service_changes), "Note 7")
  write_row("e) CSM release",              sv(s3$csm_release),                "Note 7")
  write_row("f) Other / FX",              sv(s3$csm_other),                  "Note 7")
  write_row("g) Closing balance",          sv(s3$csm_closing_total),          "Note 7")
  blank()
  write_header("(i.b) CSM — NON-LIFE SPLIT")
  write_row("Opening",      sv(s3$csm_opening_nonlife),     "Note 7")
  write_row("New business", sv(s3$csm_new_business_nonlife),"Note 7")
  write_row("Release",      sv(s3$csm_release_nonlife),     "Note 7")
  write_row("Closing",      sv(s3$csm_closing_nonlife),     "Note 7")
  blank()
  write_header("(i.c) CSM — LIFE SPLIT")
  write_row("Opening",      sv(s3$csm_opening_life),     "Note 7")
  write_row("New business", sv(s3$csm_new_business_life),"Note 7")
  write_row("Release",      sv(s3$csm_release_life),     "Note 7")
  write_row("Closing",      sv(s3$csm_closing_life),     "Note 7")
  blank()
  write_header("(i.d) CSM MATURITY")
  write_row("0-1 year",    sv(s3d$csm_maturity_lt1y),   "Note 7")
  write_row("1-5 years",   sv(s3d$csm_maturity_1to5y),  "Note 7")
  write_row("5-10 years",  sv(s3d$csm_maturity_5to10y), "Note 7")
  write_row(">10 years",   sv(s3d$csm_maturity_gt10y),  "Note 7")
  write_row("Total",       sv(s3d$csm_maturity_total),  "Subtotal")
  blank(2)

  write_header("(ii.a) RISK ADJUSTMENT DEVELOPMENT — TOTAL")
  write_row("a) Opening balance",              sv(s3$ra_opening_total),          "Note 7")
  write_row("b) New business",                 sv(s3$ra_new_business),           "Note 7")
  write_row("c) Finance result",               sv(s3$ra_finance_result),         "Note 7")
  write_row("d.i) Future svc (adj CSM)",       sv(s3$ra_future_service_adj_csm), "Note 7")
  write_row("d.ii) Future svc (no CSM)",       sv(s3$ra_future_service_no_csm),  "Note 7")
  write_row("e) Past service",                 sv(s3$ra_past_service),           "Note 7")
  write_row("f) RA release",                   sv(s3$ra_release),                "Note 7")
  write_row("g) Other / FX",                  sv(s3$ra_other),                  "Note 7")
  write_row("h) Closing balance",              sv(s3$ra_closing_total),          "Note 7")
  blank()
  write_row("RA Non-Life — opening", sv(s3$ra_opening_nonlife), "Note 7")
  write_row("RA Non-Life — closing", sv(s3$ra_closing_nonlife), "Note 7")
  write_row("RA Life — opening",     sv(s3$ra_opening_life),    "Note 7")
  write_row("RA Life — closing",     sv(s3$ra_closing_life),    "Note 7")
  blank(2)

  write_header("(iii) LOSS COMPONENT")
  write_row("Opening total",          sv(s3$lc_opening_total),          "Note 7")
  write_row("Losses recognised",      sv(s3$lc_losses_recognised),      "Note 7")
  write_row("Systematic allocation",  sv(s3$lc_systematic_alloc),       "Note 7")
  write_row("Future service changes", sv(s3$lc_future_service_changes), "Note 7")
  write_row("Finance",                sv(s3$lc_finance),                "Note 7")
  write_row("Other",                  sv(s3$lc_other),                  "Note 7")
  write_row("Closing total",          sv(s3$lc_closing_total),          "Note 7")
  blank()
  write_row("Non-Life LC opening",    sv(s3$lc_opening_nonlife), "Note 7")
  write_row("Non-Life LC closing",    sv(s3$lc_closing_nonlife), "Note 7")
  write_row("Life LC opening",        sv(s3$lc_opening_life),    "Note 7")
  write_row("Life LC closing",        sv(s3$lc_closing_life),    "Note 7")
  write_row("Health LC opening",      sv(s3$lc_opening_health),  "Note 7")
  write_row("Health LC closing",      sv(s3$lc_closing_health),  "Note 7")
  blank(2)

  # ---- (5) SOLVENCY -----------------------------------------------------------
  write_header("(5) CAPITAL POSITIONS")
  blank()
  write_row("Solvency II ratio",           sv(s4$solvency2_ratio),    "SII section")
  write_row("Solvency II target ratio",    sv(s4$solvency2_target),   "SII section")
  write_row("SCR (EUR m)",                 sv(s4$scr),                "SII section")
  write_row("Eligible own funds (EUR m)",  sv(s4$eligible_own_funds), "SII section")
  write_row("Capital generated (EUR m)",   sv(s4$capital_generated),  "SII section")
  blank(2)

  # ---- (6) INVESTMENTS --------------------------------------------------------
  write_header("(6) INVESTMENT ASSET MIX")
  blank()
  write_row("Equities",          sv(s5$equities),          "Investments note")
  write_row("Government bonds",  sv(s5$govt_bonds),        "Investments note")
  write_row("Corporate bonds",   sv(s5$corporate_bonds),   "Investments note")
  write_row("Mortgages",         sv(s5$mortgages),         "Investments note")
  write_row("Other fixed income",sv(s5$other_fixed_income),"Investments note")
  write_row("Derivatives (net)", sv(s5$derivatives_net),   "Investments note")
  write_row("Other",             sv(s5$other_investments), "Investments note")
  write_row("TOTAL",             sv(s5$total_investments), "Subtotal")
  blank()
  write_row("FVOCI",         sv(s5$fvoci),         "Investments note")
  write_row("FVTPL",         sv(s5$fvtpl),         "Investments note")
  write_row("Amortised cost",sv(s5$amortised_cost),"Investments note")
  blank(2)

  # ---- (7) PREMIUM ------------------------------------------------------------
  write_header("(7) GROSS WRITTEN PREMIUM (NON-GAAP)")
  blank()
  write_row("Life",              sv(s6$gwp_life),    "Results section")
  write_row("Health",            sv(s6$gwp_health),  "Results section")
  write_row("Non-Life",          sv(s6$gwp_nonlife), "Results section")
  write_row("Pensions",          sv(s6$gwp_pensions),"Results section")
  write_row("International",     sv(s6$gwp_intl),    "Results section")
  write_row("TOTAL",             sv(s6$gwp_total),   "Subtotal")

  # Column formatting
  setColWidths(wb, ws, cols = 1,   widths = 55)
  setColWidths(wb, ws, cols = 2,   widths = 14)
  setColWidths(wb, ws, cols = 3,   widths = 35)
  freezePane(wb, ws, firstActiveRow = 4, firstActiveCol = 2)
}

# ==============================================================================
# MAIN — extract all four insurers and write workbook
# ==============================================================================

message("\n", strrep("=", 60))
message("IFRS 17 Extraction — All Insurers 2025")
message(strrep("=", 60))

# Note: mclapply / fork-based parallelism is unsafe here because both pdftools
# (poppler C lib) and httr2 (curl/OpenSSL) have shared library state that does
# not survive fork on macOS. Sequential lapply is correct and reliable.
message(glue::glue("Extracting {length(PAGE_MAPS)} insurers sequentially..."))

t0 <- proc.time()

results <- lapply(PAGE_MAPS, extract_insurer)

elapsed <- round((proc.time() - t0)[["elapsed"]])
message(glue::glue("\nAll extractions done in {elapsed}s."))

wb <- createWorkbook()

for (key in names(PAGE_MAPS)) {
  pm  <- PAGE_MAPS[[key]]
  ws  <- pm$short_name
  res <- results[[key]]
  if (is.null(res) || inherits(res, "try-error")) {
    message(glue::glue("WARNING: {ws} extraction failed — skipping tab."))
    next
  }
  message(glue::glue("Assembling tab: {ws}"))
  addWorksheet(wb, ws)
  write_insurer_tab(wb, ws, pm$short_name, res, pm)
}

message("\nSaving workbook...")
saveWorkbook(wb, OUT_PATH, overwrite = TRUE)
message(glue::glue("Done. Output written to: {OUT_PATH}"))
