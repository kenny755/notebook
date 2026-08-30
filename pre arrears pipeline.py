r"""
pre_arrears_pipeline.py
================================================================================
Pre-Arrears Credit Management Strategy -- full Python conversion.

This single script assembles all 11 independently-converted, independently-
tested components (326 unit tests total across the source notebooks) into
one end-to-end runnable pipeline, in the same execution order as the
original SAS program.

HOW TO RUN
----------
    python pre_arrears_pipeline.py

WHAT YOU MUST CHANGE BEFORE RUNNING THIS FOR REAL
--------------------------------------------------
Everything you need to edit is collected in ONE place: the
"USER CONFIGURATION" section below (search for "# >>> EDIT").  Nothing
else in this file should need to change for a normal monthly run.

1. RUNDATE -- the first day of the PRIOR month, e.g. if running in
   August 2026, set RUNDATE = date(2026, 7, 1).

2. DATABASE CONNECTION -- DB_SERVER / DB_DATABASE / DB_DRIVER. Defaults
   match the values found in the SAS source; confirm they're still
   correct for your environment. Uses trusted (Windows-integrated)
   authentication, matching the original SAS libname -- no password is
   stored here.

3. FIFTEEN THRESHOLD VALUES WITH NO DEFAULT IN THE SAS SOURCE.
   The original SAS program referenced these as macro variables
   (&lagp, &RG1BS, etc.) that are set somewhere in the wider production
   job but were never shown in the supplied source file. This script
   will raise a clear error on startup if any of them are left as
   `None` -- you MUST fill in real values before this will run:
       LAGP                                  (Step 5  -- contact "cooldown" cutoff)
       RESI_G1_BEHAVIOURAL_SCORE_CUTOFF       (Step 6)
       RESI_G2_BEHAVIOURAL_SCORE_CUTOFF       (Step 6)
       GENERAL_BUREAU_RISK_SCORE_CUTOFF       (Step 6 -- this is also &GBRS
                                                referenced, disabled, in Step 4)
       BTL_G1_BEHAVIOURAL_SCORE_CUTOFF        (Step 6)
       BTL_G2_BEHAVIOURAL_SCORE_CUTOFF        (Step 6)
       SECONDARY_BUREAU_RISK_SCORE_CUTOFF     (Step 6)
       BTL_GEO_DELPHI_CUTOFF                  (Step 6)
       RG1_MAX / RG2_MAX / RG3_MAX            (Resi volume caps)
       BG1_MAX / BG2_MAX / BG3_MAX            (BTL volume caps)

4. TWO UNRESOLVED EXTERNAL DATA SOURCES. The SAS source reads from and
   writes to SAS libraries (`ddr`, `out`) that are never `libname`-
   defined in the supplied excerpt -- their physical location (SQL
   table? SAS dataset on a shared drive? something else?) could not be
   confirmed. This script leaves these as clearly-marked functions
   that currently raise NotImplementedError -- search for
   "# >>> IMPLEMENT" and wire each one up to the real source:
       read_ddr_target_extract(table_name)      -- Step 3 input
       read_resi_contact_history()              -- Step 5 input
       read_btl_contact_history()                -- Step 5 input
       write_resi_contact_history(df)            -- Step 9 output
       write_btl_contact_history(df)              -- Step 9 output
       write_monthly_snapshot(table_name, df)     -- Step 9 output

5. OUTPUT FILE PATHS (Step 10). Defaults mirror the SAS source's
   Windows UNC paths exactly (folder/file naming structure preserved)
   but will need updating to wherever THIS script has write access.

6. TWO SCOPE DECISIONS TO CONFIRM WITH THE BUSINESS (no code change
   needed unless you decide these are wrong):
     - Steps 7 and 8 are scoped to AccountPool='SBS00001' only (SBS),
       NOT the AHL/NYM company scope used in Steps 1/2/4/6. If AHL/NYM
       can hold BTL accounts, they will never be flagged as portfolio
       landlords (Step 7) and can never be marked contactable (Step 8).
     - The residual-arrears export (Step 10) writes to a path under a
       "Collections Strategies 2023" folder, while every other export
       in this script writes under a "Collections Strategies 2019"
       folder. This mismatch is preserved exactly from the source, not
       "fixed".

WHAT THIS SCRIPT DELIBERATELY DOES NOT INCLUDE
------------------------------------------------
Unit tests and reconciliation-query helpers were intentionally left out
of this consolidated script to keep it focused on production execution.
They are available in the original per-component Jupyter notebooks this
script was assembled from (326 tests total, all passing) -- use those
notebooks for UAT / source-to-target reconciliation before go-live, and
whenever this script is modified.
================================================================================
"""

import logging
import math
import numpy as np
import os
import pandas as pd
import re
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import List, Optional, Callable

logger = logging.getLogger("pre_arrears")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


####################################################################################################
# STEP 1: PRE-ARREARS QUALIFIERS
####################################################################################################
"""
pre_arrears_qualifiers.py
Production module: Pre-Arrears Strategy — Step 1: Pre-Arrears Qualifiers.

Converted 1:1 from Pre-Arrears.txt lines 49-214 (SAS macro %ss).
Behaviour-preserving conversion: no selection, threshold, or join logic
has been altered. See markdown cells above for the full risk log.
"""


if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class QualifierExtractionError(Exception):
    """Raised when the Step 1 extraction cannot be trusted to proceed."""


class QualifierValidationError(QualifierExtractionError):
    """Raised when the extracted qualifier population fails validation."""


@dataclass(frozen=True)
class PreArrearsQualifierConfig:
    """
    All tunable values for the Pre-Arrears Qualifiers component.

    Every default below is IDENTICAL to the literal hardcoded in the SAS
    source. Parameterising does not change behaviour unless a caller
    deliberately overrides a value.
    """

    # --- Run parameter -----------------------------------------------------
    rundate: date  # must be the 1st day of a calendar month (SAS &rundate contract)

    # --- Connection (Windows-integrated / trusted auth, matches SAS libname) -
    db_server: str = "SBSPSQLVS101\\MISPSQL01"
    db_database: str = "P_DW"
    db_driver: str = "ODBC Driver 17 for SQL Server"
    connect_timeout_seconds: int = 30
    query_timeout_seconds: int = 600
    max_retries: int = 3
    retry_backoff_seconds: float = 5.0

    # --- Business thresholds (defaults == SAS literals) ---------------------
    capital_balance_threshold: float = 1000.0          # fma.CapitalBalanceLive > 1000
    min_months_since_completion: int = 3                # DATEDIFF(...) > 3
    min_remaining_term_months: int = 12                 # fma.RemainingTerm > 12
    bh_score_missing_default: int = 999                  # CASE WHEN NULL THEN 999

    company_scope: List[str] = field(default_factory=lambda: [
        "Skipton Building Society", "Amber Homeloans Ltd", "North Yorkshire Mortgages",
    ])
    excluded_account_pool: str = "DRC00001"
    excluded_product_codes: List[str] = field(default_factory=lambda: [
        "MIN01", "HOM10", "HM120", "HM121", "HM122", "HM123", "HM124", "HM125",
    ])
    deceased_check_pool: str = "SBS00001"
    deceased_relationship_type_code: str = "19"
    lpa_check_pool: str = "SBS00001"
    lpa_classifications: List[str] = field(default_factory=lambda: [
        "Residential", "BTL Commercial", "Pure Commercial",
    ])
    rs_letter_codes: List[str] = field(default_factory=lambda: [
        "RED001", "RED01E", "RED01F",
    ])
    # TODO: VERIFY this ad-hoc exclusion is still required by Credit Management.
    manual_excluded_accounts: List[int] = field(default_factory=lambda: [132876009])

    def __post_init__(self):
        if self.rundate.day != 1:
            raise ValueError(
                f"rundate must be the first day of a month (SAS &rundate contract); "
                f"got {self.rundate.isoformat()}"
            )


def get_reporting_period(rundate: date) -> int:
    """Equivalent of SAS: intnx(month,&rundate,0) formatted yymmn6. -> int YYYYMM."""
    return rundate.year * 100 + rundate.month


def get_month_bounds(rundate: date) -> "tuple[int, int]":
    """
    Equivalent of SAS &RSFrom / &RSTo:
        intnx(month,&rundate,0,'b') -> first day of rundate's month
        intnx(month,&rundate,0,'e') -> last day of rundate's month
    Returned as YYYYMMDD integers, matching yymmddn8. formatting.
    """
    import calendar

    first_day = date(rundate.year, rundate.month, 1)
    last_day_num = calendar.monthrange(rundate.year, rundate.month)[1]
    last_day = date(rundate.year, rundate.month, last_day_num)
    to_yyyymmdd = lambda d: d.year * 10000 + d.month * 100 + d.day
    return to_yyyymmdd(first_day), to_yyyymmdd(last_day)


def floor_or_default(score: Optional[float], default: int) -> int:
    """Equivalent of: CASE WHEN bs.S3Score IS NULL THEN 999 ELSE FLOOR(bs.S3Score) END."""
    if score is None or (isinstance(score, float) and math.isnan(score)):
        return default
    return math.floor(score)


def _in_clause(n: int) -> str:
    """Build a parameterised `(?, ?, ...)` clause of length n. n=0 -> impossible-match clause."""
    if n == 0:
        return "(SELECT NULL WHERE 1 = 0)"
    return "(" + ", ".join(["?"] * n) + ")"


def build_qualifiers_query(cfg: PreArrearsQualifierConfig) -> "tuple[str, list]":
    """
    Rebuilds the Step 1 T-SQL pass-through query with bound parameters in
    place of SAS macro-variable text substitution and inline literals.
    Business logic, joins, and filter conditions are unchanged from the
    SAS source (Pre-Arrears.txt lines 64-208).
    """
    yyyymm = get_reporting_period(cfg.rundate)
    rs_from, rs_to = get_month_bounds(cfg.rundate)

    params: list = []

    company_in = _in_clause(len(cfg.company_scope))
    params += list(cfg.company_scope)

    product_ex = _in_clause(len(cfg.excluded_product_codes))
    params += list(cfg.excluded_product_codes)

    lpa_class_in = _in_clause(len(cfg.lpa_classifications))
    params += list(cfg.lpa_classifications)

    rs_codes_in = _in_clause(len(cfg.rs_letter_codes))
    params += list(cfg.rs_letter_codes)

    manual_excl_in = _in_clause(len(cfg.manual_excluded_accounts))
    params += list(cfg.manual_excluded_accounts)

    query = f"""
WITH CTE_DRS AS
(
SELECT  m99.primacno_o AS PrimaryAccount
        , m98.accnbr_o AS AccountNumber
        , da.AccountOrigin
        , CONVERT(DATE, drs._timestamp) AS DRS_DateRecorded
        , CONVERT(DATE, drs.startDate) AS DRS_StartDate
        , CONVERT(DATE, drs.endDate) AS DRS_EndDate
        , drs.arrearsAmount AS DRS_Arrears
FROM    P_ODS.SKIPCORE.DatDebtRespiteSchemeDetails drs
INNER JOIN  P_ODS.SKIPCORE.MOR98 m98
    ON  drs.myAccount = m98.oid
    AND m98.pool_id_o = '{cfg.lpa_check_pool}'
INNER JOIN  P_ODS.SKIPCORE.MOR99 m99
    ON  m98.myMOR99 = m99.oid
    AND m98.pool_id_o = m99.pool_id_o
    AND m98.primacno_o = m99.primacno_o
INNER JOIN  P_DW.dbo.dimAccount da
    ON  m98.oid = da.BK_Account
    AND m98.pool_id_o = da.AccountPool
    AND m98.accnbr_o = da.AccountNumber
    AND da.AccountType = 10
WHERE   CONVERT(DATE, drs.endDate) >= CONVERT(DATE, GETDATE())
AND     drs.ceased = 0
),
CTE_LPA AS
(
SELECT  rcv.primacno_o AS PrimaryAccount
FROM    P_ODS.SKIPCORE.RCV98 rcv
INNER JOIN  P_DW.dbo.dimAccount da
    ON  rcv.pool_id_o = da.AccountPool
    AND rcv.primacno_o = da.AccountNumber
WHERE   rcv.pool_id_o = '{cfg.lpa_check_pool}'
AND     (da.CurrentAccountStatus IN ('Live', 'Possession')
         OR da.AccountEndDate >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0))
AND     da.CurrentAccountClassification IN {lpa_class_in}
AND     rcv.dc_recvr_o_dt IS NOT NULL
AND     rcv._deleted = 0
)
SELECT  CASE WHEN lu.Allocated_AccountNumber IS NOT NULL THEN lu.Allocated_AccountNumber ELSE da.AccountNumber END AS AccountNumber
        , CASE WHEN dc.CompanyName = 'Amber Homeloans Ltd' OR da.AccountOrigin = 'Amber Homeloans Ltd' THEN 'AHL'
               WHEN dc.CompanyName = 'North Yorkshire Mortgages' OR da.AccountOrigin = 'North Yorkshire Mortgages Ltd' THEN 'NYM'
               ELSE 'SBS' END AS Lender
        , dm.CalendarYear * 100 + dm.CalendarMonthNumber AS Snapshot
        , CASE WHEN dsp.SubPopulationCode = 'BTL' THEN 'BTL' ELSE 'Resi' END AS Segment
        , fma.CapitalBalanceLive AS BalSS
        , CASE WHEN dc.CompanyName IN ('Amber Homeloans Ltd', 'North Yorkshire Mortgages') AND fma.StandardArrearsInMonths > 0 THEN fma.StandardArrearsInMonths
               WHEN dc.CompanyName IN ('Skipton Building Society') AND fma.ContractualArrearsInMonths > 0 THEN fma.ContractualArrearsInMonths
               ELSE 0 END AS MIASS
        , CASE WHEN dc.CompanyName IN ('Amber Homeloans Ltd', 'North Yorkshire Mortgages') AND fma.StandardArrearsAmount > 0 THEN fma.StandardArrearsAmount
               WHEN dc.CompanyName IN ('Skipton Building Society') AND fma.ContractualArrearsAmount > 0 THEN fma.ContractualArrearsAmount
               ELSE 0 END AS ArrBalSS
        , CASE WHEN da.AccountStartDate IS NOT NULL AND dc.CompanyName IN ('Amber Homeloans Ltd', 'North Yorkshire Mortgages') AND fma.StandardArrearsInMonths > 0 AND fma.StandardArrearsInMonths < 1 THEN 1
               WHEN da.AccountStartDate IS NOT NULL AND dc.CompanyName IN ('Skipton Building Society') AND fma.ContractualArrearsInMonths > 0 AND fma.ContractualArrearsInMonths < 1 THEN 1
               ELSE 0 END AS ArrBelow1MIA
        , CASE WHEN bs.S3Score IS NULL THEN ? ELSE FLOOR(bs.S3Score) END AS BH_Score
        , CASE WHEN rs.LatestRS IS NULL THEN 0 ELSE 1 END AS RS_Ind
FROM    P_DW.dbo.factMortgageAccountMonthSS fma
INNER JOIN  P_DW.dbo.dimAccount da ON fma.FK_Account = da.PK_Account
INNER JOIN  P_DW.dbo.dimCompany dc ON fma.FK_Company = dc.PK_Company
LEFT JOIN   P_ODS.reference.AHLNYMMigratedAccounts lu
    ON  da.AccountPool = lu.AHLNYM_PoolID AND da.AccountNumber = lu.AHLNYM_AccountNumber
INNER JOIN  P_DW.dbo.dimProduct dp ON fma.FK_CurrentMortgageProduct = dp.PK_Product
INNER JOIN  P_DW.dbo.dimMonth dm ON fma.FK_SnapshotDate = dm.PK_Month
INNER JOIN  P_DW.dbo.dimSubPopulation dsp ON fma.FK_SubPopulation = dsp.PK_SubPopulation
INNER JOIN  P_DW.dbo.dimAccountStates das ON fma.FK_AccountStates = das.PK_AccountStates
INNER JOIN  P_DW.dbo.dimArrangement dar ON fma.FK_ArrangementType = dar.PK_ArrangementType
LEFT JOIN   P_Modelling.COREMODEL.vwBehaviouralScorecard bs
    ON  dm.CalendarMonthYear = CONVERT(VARCHAR, bs.Period)
    AND (CASE WHEN dc.CompanyName = 'Amber Homeloans Ltd' THEN 'AHL'
              WHEN dc.CompanyName = 'North Yorkshire Mortgages' THEN 'NYM'
              ELSE 'SBS' END) = bs.CompanyCode
    AND da.AccountNumber = bs.PrimaryAccount
LEFT JOIN   (
                SELECT * FROM (
                    SELECT  a98.pool_id_o, a98.accnbr_o, a98.lettcode_o, a98.dc_creat_o_dt
                            , RANK() OVER (PARTITION BY a98.accnbr_o ORDER BY a98.dc_creat_o_dt DESC, a98.time_o DESC, a98.oid DESC) AS LatestRS
                    FROM    P_ODS.SKIPCORE.ADM98 a98
                    WHERE   a98.pool_id_o = '{cfg.lpa_check_pool}'
                    AND     a98.lettcode_o IN {rs_codes_in}
                    AND     a98.dc_creat_o BETWEEN ? AND ?
                ) a
                WHERE a.LatestRS = 1
            ) rs
    ON  da.AccountPool = rs.pool_id_o AND da.AccountNumber = rs.accnbr_o
WHERE   dc.CompanyName IN {company_in}
AND     da.AccountPool <> ?
AND     dm.CalendarMonthYear = CONVERT(VARCHAR, ?)
AND     da.AccountSubType = 'Primary'
AND     da.CurrentAccountClassification = 'Residential'
AND     das.AccountStatus IN ('Live')
AND     ((dc.CompanyName IN ('Skipton Building Society') AND fma.ContractualArrearsInMonths < 1)
         OR (dc.CompanyName IN ('Amber Homeloans Ltd', 'North Yorkshire Mortgages') AND fma.StandardArrearsInMonths < 1))
AND     dp.ProductCode NOT IN {product_ex}
AND     fma.CapitalBalanceLive > ?
AND     dar.ArrangementTypeCode < 0
AND     DATEDIFF(MONTH, da.AccountStartDate, DATEADD(MONTH, 0, dm.ReportingMonth)) > ?
AND     fma.RemainingTerm > ?
AND     da.AccountNumber NOT IN (
            SELECT b.AccountNumber
            FROM (
                SELECT a.AccountNumber, SUM(a.DeathFlag) AS DeathFlag
                FROM (
                    SELECT  da.AccountNumber, dc.CustomerNumber
                            , CASE WHEN dc.CustomerDateofDeath IS NOT NULL THEN 1 ELSE 0 END AS DeathFlag
                    FROM    P_DW.dbo.factMortgageAccount fma
                    INNER JOIN  P_DW.dbo.dimAccount da ON fma.FK_Account = da.PK_Account
                    INNER JOIN  P_DW.dbo.bridgeAccountCustomer bac
                        ON  da.PK_Account = bac.FK_Account AND bac.relationshipTypeCode = ?
                    INNER JOIN  P_DW.dbo.dimCustomer dc ON bac.FK_Customer = dc.PK_Customer
                    WHERE   da.AccountPool = ?
                    AND     da.AccountSubType = 'Primary'
                    AND     da.CurrentAccountStatus IN ('Live')
                    AND     da.CurrentAccountClassification = 'Residential'
                ) a
                GROUP BY a.AccountNumber
            ) b
            WHERE b.DeathFlag > 0
        )
AND     da.AccountNumber NOT IN (SELECT PrimaryAccount FROM CTE_DRS)
AND     da.AccountNumber NOT IN (SELECT PrimaryAccount FROM CTE_LPA)
AND     da.AccountNumber NOT IN {manual_excl_in}
ORDER BY (CASE WHEN lu.Allocated_AccountNumber IS NOT NULL THEN lu.Allocated_AccountNumber ELSE da.AccountNumber END)
"""

    # Parameter order must exactly match the '?' placeholders left-to-right
    # as they appear in the SQL text above. NOTE: CTE_LPA is defined (and
    # therefore textually appears) BEFORE the main SELECT, so its
    # placeholder(s) must be ordered first, ahead of BH_Score etc.
    return query, _build_ordered_params(cfg, rs_from, rs_to, yyyymm)


def _build_ordered_params(cfg: PreArrearsQualifierConfig, rs_from: int, rs_to: int, yyyymm: int) -> list:
    """Authoritative parameter list, in the exact left-to-right order the '?'
    placeholders appear in `build_qualifiers_query`'s SQL text:
    CTE_LPA classifications -> BH_Score default -> RS letter codes ->
    RS date window -> company scope -> excluded pool -> reporting period ->
    excluded products -> balance/term thresholds -> deceased-check params ->
    manual exclusions."""
    return (
        list(cfg.lpa_classifications)
        + [cfg.bh_score_missing_default]
        + list(cfg.rs_letter_codes)
        + [rs_from, rs_to]
        + list(cfg.company_scope)
        + [cfg.excluded_account_pool, yyyymm]
        + list(cfg.excluded_product_codes)
        + [cfg.capital_balance_threshold, cfg.min_months_since_completion, cfg.min_remaining_term_months]
        + [cfg.deceased_relationship_type_code, cfg.deceased_check_pool]
        + list(cfg.manual_excluded_accounts)
    )


def retry(times: int, backoff_seconds: float):
    """Simple retry decorator for transient DB connectivity failures."""
    import functools
    import time

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, times + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001 - deliberately broad, re-raised below
                    last_exc = exc
                    logger.warning(
                        "Attempt %s/%s failed for %s: %s", attempt, times, fn.__name__, exc
                    )
                    if attempt < times:
                        time.sleep(backoff_seconds * attempt)
            raise QualifierExtractionError(
                f"{fn.__name__} failed after {times} attempts"
            ) from last_exc
        return wrapper
    return decorator


class DataWarehouseConnector:
    """
    Thin wrapper around pyodbc reproducing the SAS libname's trusted
    (Windows-integrated) authentication. No credentials are stored or
    passed in code, matching `trusted connection=yes` in the SAS source.
    """

    def __init__(self, cfg: PreArrearsQualifierConfig):
        self.cfg = cfg
        self._conn = None

    def _connection_string(self) -> str:
        return (
            f"DRIVER={{{self.cfg.db_driver}}};"
            f"SERVER={self.cfg.db_server};"
            f"DATABASE={self.cfg.db_database};"
            f"Trusted_Connection=yes;"
            f"Connect Timeout={self.cfg.connect_timeout_seconds};"
        )

    @retry(times=3, backoff_seconds=5.0)
    def connect(self):
        import pyodbc  # imported lazily so the notebook can be reviewed without the driver installed

        logger.info("Connecting to %s/%s", self.cfg.db_server, self.cfg.db_database)
        self._conn = pyodbc.connect(self._connection_string(), timeout=self.cfg.connect_timeout_seconds)
        return self._conn

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is not None:
            self._conn.close()
            logger.info("Connection closed")


REQUIRED_COLUMNS = [
    "AccountNumber", "Lender", "Snapshot", "Segment", "BalSS",
    "MIASS", "ArrBalSS", "ArrBelow1MIA", "BH_Score", "RS_Ind",
]


NON_NULLABLE_COLUMNS = [
    "AccountNumber", "Lender", "Snapshot", "Segment",
    "BalSS", "MIASS", "ArrBalSS", "ArrBelow1MIA", "BH_Score", "RS_Ind",
]


def validate_qualifiers(df: pd.DataFrame, cfg: PreArrearsQualifierConfig) -> None:
    """
    Data-quality gate for the extracted qualifier population.
    Raises QualifierValidationError on any check failure; never silently
    mutates the population (no rows dropped, no values altered here).
    """
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise QualifierValidationError(f"Missing expected columns: {missing_cols}")

    if df.empty:
        logger.warning(
            "Qualifier extraction returned 0 rows for period %s. "
            "This can be legitimate (e.g. no eligible accounts) but should "
            "be confirmed before the strategy proceeds.",
            get_reporting_period(cfg.rundate),
        )
        return

    for col in NON_NULLABLE_COLUMNS:
        null_count = int(df[col].isna().sum())
        if null_count > 0:
            raise QualifierValidationError(
                f"Column '{col}' has {null_count} unexpected null(s); "
                f"source SQL defaults this field and should never be null."
            )

    dup_count = int(df["AccountNumber"].duplicated().sum())
    if dup_count > 0:
        # The SAS source has no explicit dedup on AccountNumber in this step;
        # a duplicate here most likely indicates a RANK() tie in the RS_Ind
        # lookup fanning out the join (see risk #6). Surface it, do not
        # silently collapse it, since silently deduping would be a
        # behaviour change.
        logger.error(
            "%s duplicate AccountNumber value(s) detected — likely a RANK() "
            "tie-break fan-out in the RS_Ind lookup. TODO: VERIFY against "
            "source data before this population is used downstream.",
            dup_count,
        )
        raise QualifierValidationError(
            f"{dup_count} duplicate AccountNumber rows detected; see log for detail."
        )

    below_floor = int((df["BalSS"] <= cfg.capital_balance_threshold).sum())
    if below_floor > 0:
        raise QualifierValidationError(
            f"{below_floor} row(s) at/below the capital balance threshold "
            f"({cfg.capital_balance_threshold}) leaked through the SQL filter."
        )

    logger.info(
        "Validation passed: %s rows, %s unique accounts, %s Resi / %s BTL",
        len(df), df["AccountNumber"].nunique(),
        int((df["Segment"] == "Resi").sum()), int((df["Segment"] == "BTL").sum()),
    )


def extract_pre_arrears_qualifiers(cfg: PreArrearsQualifierConfig) -> pd.DataFrame:
    """
    Main entry point. Equivalent to SAS `%ss;` — builds and executes the
    Step 1 qualifier query and returns the population as a DataFrame
    equivalent to SAS work table `SS<yyyymm>`.
    """
    yyyymm = get_reporting_period(cfg.rundate)
    logger.info("Starting Pre-Arrears Qualifiers extraction for period %s", yyyymm)

    query, params = build_qualifiers_query(cfg)

    with DataWarehouseConnector(cfg) as conn:
        conn.timeout = cfg.query_timeout_seconds
        try:
            df = pd.read_sql(query, conn, params=params)
        except Exception as exc:  # noqa: BLE001
            raise QualifierExtractionError(
                f"Qualifier query execution failed for period {yyyymm}"
            ) from exc

    # Preserve the SAS ORDER BY explicitly rather than trusting fetch order
    # (see risk #5). Sort key matches the SQL ORDER BY expression exactly.
    df = df.sort_values("AccountNumber", kind="stable").reset_index(drop=True)

    validate_qualifiers(df, cfg)

    logger.info("Completed Pre-Arrears Qualifiers extraction: %s rows", len(df))
    return df


####################################################################################################
# STEP 2: PREVIOUS PERFORMANCE (12M)
####################################################################################################
"""
previous_performance.py
Production module: Pre-Arrears Strategy — Step 2: Previous Performance (12m).

Converted 1:1 from Pre-Arrears.txt lines 218-296 (SAS macro %prep).
Reuses the DataWarehouseConnector / retry pattern introduced in the
Step 1 (Pre-Arrears Qualifiers) module.
"""


if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class PreviousPerformanceError(Exception):
    """Raised when Step 2 extraction/aggregation cannot be trusted to proceed."""


@dataclass(frozen=True)
class PreviousPerformanceConfig:
    """Defaults are identical to the SAS literals; see risk log above."""

    rundate: date  # must be the 1st day of a calendar month
    lookback_months: int = 12  # SAS: %do i=0 %to 12

    db_server: str = "SBSPSQLVS101\\MISPSQL01"
    db_database: str = "P_DW"
    db_driver: str = "ODBC Driver 17 for SQL Server"
    connect_timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: float = 5.0

    company_scope: List[str] = field(default_factory=lambda: [
        "Skipton Building Society", "Amber Homeloans Ltd", "North Yorkshire Mortgages",
    ])
    excluded_account_pool: str = "DRC00001"

    def __post_init__(self):
        if self.rundate.day != 1:
            raise ValueError(f"rundate must be the first day of a month; got {self.rundate}")
        if self.lookback_months < 1:
            raise ValueError("lookback_months must be >= 1")


def get_lookback_period(rundate: date, months_back: int) -> int:
    """Equivalent of SAS: intnx(month,&rundate,&i*-1) formatted yymmn6. -> int YYYYMM."""
    year, month = rundate.year, rundate.month
    total_months = year * 12 + (month - 1) - months_back
    return (total_months // 12) * 100 + (total_months % 12 + 1)


def build_monthly_indicator_query(cfg: PreviousPerformanceConfig) -> str:
    """
    Query template for a single historical month's arrears indicators.
    Identical logic to Pre-Arrears.txt lines 232-261, with the month
    literal left as a bind parameter (filled in per call).
    """
    company_in = _in_clause(len(cfg.company_scope))
    return f"""
SELECT  CASE WHEN lu.Allocated_AccountNumber IS NOT NULL THEN lu.Allocated_AccountNumber ELSE da.AccountNumber END AS AccountNumber
        , CASE WHEN dc.CompanyName IN ('Amber Homeloans Ltd', 'North Yorkshire Mortgages') AND fma.StandardArrearsInMonths >= 1 THEN 1
               WHEN dc.CompanyName IN ('Skipton Building Society') AND fma.ContractualArrearsInMonths >= 1 THEN 1
               ELSE 0 END AS MIA_1P
        , CASE WHEN dc.CompanyName IN ('Amber Homeloans Ltd', 'North Yorkshire Mortgages') AND fma.StandardArrearsAmount > 0 THEN 1
               WHEN dc.CompanyName IN ('Skipton Building Society') AND fma.ContractualArrearsAmount > 0 THEN 1
               ELSE 0 END AS Arr_Ind
FROM    P_DW.dbo.factMortgageAccountMonthSS fma
INNER JOIN  P_DW.dbo.dimAccount da ON fma.FK_Account = da.PK_Account
INNER JOIN  P_DW.dbo.dimCompany dc ON fma.FK_Company = dc.PK_Company
LEFT JOIN   P_ODS.reference.AHLNYMMigratedAccounts lu
    ON  da.AccountPool = lu.AHLNYM_PoolID AND da.AccountNumber = lu.AHLNYM_AccountNumber
INNER JOIN  P_DW.dbo.dimMonth dm ON fma.FK_SnapshotDate = dm.PK_Month
INNER JOIN  P_DW.dbo.dimAccountStates das ON fma.FK_AccountStates = das.PK_AccountStates
WHERE   dc.CompanyName IN {company_in}
AND     da.AccountPool <> ?
AND     dm.CalendarMonthYear = CONVERT(VARCHAR, ?)
AND     da.AccountSubType = 'Primary'
AND     das.AccountStatus IN ('Live')
AND     da.AccountStartDate <= DATEADD(MONTH, DATEDIFF(MONTH, -1, dm.BatchDate) - 1, -1)
ORDER BY (CASE WHEN lu.Allocated_AccountNumber IS NOT NULL THEN lu.Allocated_AccountNumber ELSE da.AccountNumber END)
"""


def build_monthly_indicator_params(cfg: PreviousPerformanceConfig, yyyymm: int) -> list:
    return list(cfg.company_scope) + [cfg.excluded_account_pool, yyyymm]


def build_history_string(monthly_flags: "list[int]") -> str:
    """
    Faithful reproduction of SAS's MIAH/ArrH bug (risk #1 above): the
    variable's length is fixed at 12 chars by its initial assignment
    before `cat()` overwrites it, so only the RIGHT-JUSTIFIED, 12-wide
    default-format representation of the FIRST monthly flag survives;
    everything else concatenated by `cat()` is truncated away.

    This function is deliberately named `_broken` in spirit: it exists
    to reproduce the legacy output byte-for-byte, not to compute a
    correct 12-month history string. Do not "fix" this without an
    explicit, approved business-rule change (see TODO: VERIFY above).
    """
    if not monthly_flags:
        return " " * 12
    first_value = monthly_flags[0]
    # SAS default BEST12. representation of a small integer: right-justified
    # in a 12-character field.
    return f"{first_value:>12d}"


def compute_previous_performance_flags(mia_flags: "list[int]", arr_flags: "list[int]") -> dict:
    """Equivalent of the final DATA step's summary-flag logic (lines 288-290)."""
    mia_sum = sum(mia_flags)
    arr_sum = sum(arr_flags)
    return {
        "MIAH": build_history_string(mia_flags),
        "ArrH": build_history_string(arr_flags),
        "MIA1X_pre12m": 1 if mia_sum >= 1 else 0,
        "AB1X_pre12m": 1 if arr_sum == 1 else 0,
        "AB2X_pre12m": 1 if arr_sum > 1 else 0,
    }


def extract_monthly_indicators(conn, cfg: PreviousPerformanceConfig, yyyymm: int) -> pd.DataFrame:
    """Equivalent of one loop iteration's `CREATE TABLE M<i>` (lines 229-262)."""
    query = build_monthly_indicator_query(cfg)
    params = build_monthly_indicator_params(cfg, yyyymm)
    df = pd.read_sql(query, conn, params=params)

    dup_count = int(df["AccountNumber"].duplicated().sum())
    if dup_count > 0:
        raise PreviousPerformanceError(
            f"{dup_count} duplicate AccountNumber row(s) for period {yyyymm}; "
            f"expected exactly one row per account (see risk #4)."
        )
    return df.sort_values("AccountNumber", kind="stable").reset_index(drop=True)


def extract_previous_performance(cfg: PreviousPerformanceConfig) -> pd.DataFrame:
    """
    Main entry point. Equivalent to SAS `%prep;` — builds the trailing
    lookback population and returns one row per account with the
    12-month summary indicators. Equivalent to SAS table `Prep<yyyymm>`.
    """
    yyyymm_current = get_lookback_period(cfg.rundate, 0)
    logger.info("Starting Previous Performance extraction for period %s (lookback=%s months)",
                yyyymm_current, cfg.lookback_months)

    with DataWarehouseConnector(cfg) as conn:
        # M0: fixes the account universe (equivalent to `data Prep<yyyymm>; set M0;`)
        base = extract_monthly_indicators(conn, cfg, yyyymm_current)
        base = base.rename(columns={"MIA_1P": "MIA_1P0", "Arr_Ind": "Arr_Ind0"})
        result = base[["AccountNumber"]].copy()

        mia_cols, arr_cols = [], []
        for i in range(1, cfg.lookback_months + 1):
            yyyymm_i = get_lookback_period(cfg.rundate, i)
            month_df = extract_monthly_indicators(conn, cfg, yyyymm_i)
            month_df = month_df.rename(columns={"MIA_1P": f"MIA_1P{i}", "Arr_Ind": f"Arr_Ind{i}"})

            # Equivalent of: merge Prep(in=a) M&i(in=b); by AccountNumber; if a;
            # -> a LEFT JOIN keeping only accounts in the base (M0) population.
            result = result.merge(month_df, on="AccountNumber", how="left")

            # Equivalent of: if MIA_1P&i=. then MIA_1P&i=0; (explicit coalesce, not accidental)
            result[f"MIA_1P{i}"] = result[f"MIA_1P{i}"].fillna(0).astype(int)
            result[f"Arr_Ind{i}"] = result[f"Arr_Ind{i}"].fillna(0).astype(int)
            mia_cols.append(f"MIA_1P{i}")
            arr_cols.append(f"Arr_Ind{i}")

    summary_columns = ["MIAH", "ArrH", "MIA1X_pre12m", "AB1X_pre12m", "AB2X_pre12m"]
    if len(result) > 0:
        summary_rows = []
        for _, row in result.iterrows():
            mia_flags = [int(row[c]) for c in mia_cols]
            arr_flags = [int(row[c]) for c in arr_cols]
            summary_rows.append(compute_previous_performance_flags(mia_flags, arr_flags))
        summary = pd.DataFrame(summary_rows, index=result.index)
    else:
        # No accounts in the base (M0) population is a legitimate business
        # outcome -- return an empty frame with the correct columns rather
        # than silently dropping MIAH/ArrH/MIA1X_pre12m/AB1X_pre12m/AB2X_pre12m
        # entirely (pd.DataFrame([]) has no columns of its own to concat).
        summary = pd.DataFrame(columns=summary_columns)

    final = pd.concat([result[["AccountNumber"]], summary], axis=1)

    logger.info("Completed Previous Performance extraction: %s rows", len(final))
    return final


####################################################################################################
# STEP 3: DDR STRATEGY TARGET IMPORT
####################################################################################################
"""
ddr_target_import.py
Production module: Pre-Arrears Strategy — Step 3: DDR Strategy Target Import.

Converted 1:1 from Pre-Arrears.txt lines 299-314 (SAS macro %ddr).

The extraction boundary (reading ddr.ddr<yymm>) is deliberately pluggable
via an injected `reader` callable, since the physical source of that
external dataset is not defined in the supplied excerpt (see risk #1).
"""


if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class DDRTargetImportError(Exception):
    """Raised when the Step 3 extraction/transform cannot be trusted to proceed."""


@dataclass(frozen=True)
class DDRTargetImportConfig:
    rundate: date  # must be the 1st day of a calendar month

    def __post_init__(self):
        if self.rundate.day != 1:
            raise ValueError(f"rundate must be the first day of a month; got {self.rundate}")


def get_ddr_source_month(rundate: date) -> str:
    """
    Equivalent of SAS: intnx(month,&rundate,1) formatted yymmn4.
    -> the month AFTER rundate, as a 4-character "YYMM" string
    (2-digit year, 2-digit month; no century, no separators).
    """
    total_months = rundate.year * 12 + (rundate.month - 1) + 1
    year, month = divmod(total_months, 12)
    month += 1  # divmod month index is 0-based
    return f"{year % 100:02d}{month:02d}"


def build_ddr_source_table_name(rundate: date) -> str:
    """Mirrors the SAS dataset reference `ddr.ddr&yymm` for documentation/logging."""
    return f"ddr.ddr{get_ddr_source_month(rundate)}"


def build_ddr_output_table_name(rundate: date) -> str:
    """Mirrors the SAS dataset name `DDR&yyyymm` for documentation/logging."""
    return f"DDR{get_reporting_period(rundate)}"


step3_REQUIRED_RAW_COLUMNS = ["PrimaryAccount", "Target"]


def transform_ddr_target_extract(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Faithful conversion of the SAS DATA step + PROC SORT (lines 304-311).
    Input: raw extract with (at least) PrimaryAccount, Target columns
    (the SAS `keep=` list on the `set` statement selects only these two
    — the equivalent of `SELECT PrimaryAccount, Target`, never `SELECT *`).
    Output: one row per AccountNumber with DDR=1, sorted ascending.
    """
    missing_cols = [c for c in step3_REQUIRED_RAW_COLUMNS if c not in raw.columns]
    if missing_cols:
        raise DDRTargetImportError(f"DDR source extract missing expected column(s): {missing_cols}")

    # SELECT PrimaryAccount, Target  (never SELECT *)
    working = raw[step3_REQUIRED_RAW_COLUMNS].copy()

    # AccountNumber = PrimaryAccount;  DDR = 1;
    working["AccountNumber"] = working["PrimaryAccount"]
    working["DDR"] = 1

    # if Target = 1;  (missing/NaN Target correctly evaluates to False here, matching SAS)
    working = working.loc[working["Target"] == 1]

    # keep AccountNumber DDR;
    working = working[["AccountNumber", "DDR"]]

    # proc sort ... nodup; by AccountNumber;
    # `nodup` drops a row only if it exactly matches the immediately preceding
    # row post-sort across ALL retained columns (not just the BY variable) --
    # replicated here as a full-row drop_duplicates after sorting, rather
    # than a subset=["AccountNumber"] shortcut (see risk #5).
    working = working.sort_values("AccountNumber", kind="stable").reset_index(drop=True)
    working = working.drop_duplicates(keep="first").reset_index(drop=True)

    return working


def validate_ddr_targets(df: pd.DataFrame) -> None:
    """Data-quality gate. Raises on any check failure; never silently mutates output."""
    required = ["AccountNumber", "DDR"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise DDRTargetImportError(f"Missing expected output column(s): {missing_cols}")

    if df.empty:
        logger.warning("DDR target import produced 0 rows. Confirm this is expected before proceeding.")
        return

    null_accounts = int(df["AccountNumber"].isna().sum())
    if null_accounts > 0:
        raise DDRTargetImportError(f"{null_accounts} null AccountNumber value(s) in DDR target output.")

    non_one_ddr = int((df["DDR"] != 1).sum())
    if non_one_ddr > 0:
        raise DDRTargetImportError(f"{non_one_ddr} row(s) with DDR != 1; DDR is expected to be a constant flag.")

    dup_accounts = int(df["AccountNumber"].duplicated().sum())
    if dup_accounts > 0:
        raise DDRTargetImportError(
            f"{dup_accounts} duplicate AccountNumber row(s) survived dedup — "
            f"check for an unexpected second distinct DDR value per account (see risk #5)."
        )

    is_sorted = df["AccountNumber"].is_monotonic_increasing
    if not is_sorted:
        raise DDRTargetImportError("Output is not sorted ascending by AccountNumber.")

    logger.info("Validation passed: %s DDR target account(s)", len(df))


def load_ddr_target_extract(
    cfg: DDRTargetImportConfig,
    reader: Callable[[str], pd.DataFrame],
) -> pd.DataFrame:
    """
    Main entry point. Equivalent to SAS `%ddr;`.

    `reader` is an injected callable: given the source table name (e.g.
    "ddr.ddr2608"), it must return a DataFrame containing at least
    PrimaryAccount and Target columns. This keeps the (currently
    unconfirmed — see risk #1) I/O mechanism swappable without touching
    the business logic in `transform_ddr_target_extract`.
    """
    source_table = build_ddr_source_table_name(cfg.rundate)
    output_table = build_ddr_output_table_name(cfg.rundate)
    logger.info("Starting DDR Strategy Target Import: source=%s -> output=%s", source_table, output_table)

    try:
        raw = reader(source_table)
    except Exception as exc:  # noqa: BLE001
        raise DDRTargetImportError(f"Failed to read DDR source extract '{source_table}'") from exc

    result = transform_ddr_target_extract(raw)
    validate_ddr_targets(result)

    logger.info("Completed DDR Strategy Target Import: %s rows", len(result))
    return result


####################################################################################################
# STEP 4: BUREAU RISKIER QUALIFIERS
####################################################################################################
"""
bureau_riskier_qualifiers.py
Production module: Pre-Arrears Strategy — Step 4: Bureau Riskier Qualifiers.

Converted from Pre-Arrears.txt lines 318-850 (unnamed proc sql pass-through).

Design: raw bureau attribute extraction stays as SQL Server pass-through
(it depends on warehouse tables not available locally); all 17 scoring
rules are re-expressed as small, independently-tested Python functions,
eliminating the ~20-fold duplication of the two Vertical Market CASE WHEN
patterns in the SAS source (see risk log / section 1 note on repeated
calculations). No threshold/selection logic is applied here -- see the
"disabled WHERE clause" note in the explanation above.
"""


if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class BureauQualifierError(Exception):
    """Raised when Step 4 extraction/scoring cannot be trusted to proceed."""


@dataclass(frozen=True)
class BureauQualifierConfig:
    rundate: date  # must be the 1st day of a calendar month

    db_server: str = "SBSPSQLVS101\\MISPSQL01"
    db_database: str = "P_DW"
    db_driver: str = "ODBC Driver 17 for SQL Server"
    connect_timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: float = 5.0

    company_scope: List[str] = field(default_factory=lambda: [
        "Skipton Building Society", "Amber Homeloans Ltd", "North Yorkshire Mortgages",
    ])
    excluded_account_pool: str = "DRC00001"
    excluded_product_codes: List[str] = field(default_factory=lambda: [
        "MIN01", "HOM10", "HM120", "HM121", "HM122", "HM123", "HM124", "HM125",
    ])
    capital_balance_threshold: float = 1000.0

    # See "threshold filter disabled" note: these exist only so a caller
    # can inspect what the SAS comment referenced. They are NEVER applied
    # by this module -- see `apply_disabled_threshold_filter` below, which
    # exists solely as a documented no-op guarded by an explicit flag.
    gbrs_general_bureau_risk_score: Optional[int] = None
    sbrs_secondary_bureau_risk_score: Optional[int] = None
    bgd_btl_geo_delphi_threshold: Optional[int] = None

    def __post_init__(self):
        if self.rundate.day != 1:
            raise ValueError(f"rundate must be the first day of a month; got {self.rundate}")


def normalise_postcode_pc1(raw_postcode: Optional[str]) -> Optional[str]:
    """
    Equivalent of the PC1 CASE expression in CTE_PC1 (lines 332-336):
    strips all spaces, then re-inserts a single space at a fixed position
    based on the *cleaned* (space-stripped) length. Lengths outside
    4-7 are returned unchanged (the SAS/T-SQL ELSE branch).
    """
    if raw_postcode is None:
        return None
    cleaned = raw_postcode.replace(" ", "")
    n = len(cleaned)
    if n == 7:
        return cleaned[:4] + " " + cleaned[4:]
    if n == 6:
        return cleaned[:3] + " " + cleaned[3:]
    if n == 5:
        return cleaned[:2] + " " + cleaned[2:]
    if n == 4:
        return cleaned[:1] + " " + cleaned[1:]
    return raw_postcode


def postcode_pc1_to_pc2(pc1: Optional[str]) -> Optional[str]:
    """
    Equivalent of the PC2 CASE expression in CTE_PC2 (lines 355-359):
    drops the PC1 value's last character for PC1 lengths 5-8 (i.e. the
    space-inserted forms of cleaned lengths 4-7); unchanged otherwise.
    """
    if pc1 is None:
        return None
    n = len(pc1)
    if n in (5, 6, 7, 8):
        return pc1[:-1]
    return pc1


def is_valid_pc1_candidate(pc1: Optional[str]) -> bool:
    """
    Equivalent of CTE_PC1's outer filter (lines 345-346):
    PC1 IS NOT NULL AND LEFT(PC1,1) NOT IN ('0'..'9')
    -- excludes postcode-shaped values that start with a digit (not a
    valid UK postcode outward-code first character).
    """
    if not pc1:
        return False
    return not pc1[0].isdigit()


def _in_range_inclusive(value, lo, hi) -> bool:
    """NULL-safe range check matching SQL's `value >= lo AND value <= hi` under 3-valued logic."""
    return value is not None and not pd.isna(value) and lo <= value <= hi


def _gt(value, threshold) -> bool:
    return value is not None and not pd.isna(value) and value > threshold


def _ge(value, threshold) -> bool:
    return value is not None and not pd.isna(value) and value >= threshold


def _eq(value, target) -> bool:
    return value is not None and not pd.isna(value) and value == target


def _in_set(value, allowed) -> bool:
    return value is not None and not pd.isna(value) and value in allowed


def score_clu(sp_f3_35, sp_f1_30, sp_f3_36) -> int:
    """Credit Limit Utilisation (lines 401-404)."""
    if _in_range_inclusive(sp_f3_35, 90, 9998) and _gt(sp_f1_30, 2) and _in_range_inclusive(sp_f3_36, 90, 9998):
        return 3
    if _in_range_inclusive(sp_f3_35, 90, 9998) and _gt(sp_f1_30, 2):
        return 2
    if _in_range_inclusive(sp_f3_35, 85, 9998) and _gt(sp_f1_30, 1):
        return 1
    return 0


def score_bds(clu_npr_l1m, ptbr_l6m_npr_l6m, ptbr_l3m_npr_l3m, no_ca_l3m, no_mlv_ca_l3m) -> int:
    """Behavioural Data Sharing (lines 412-415)."""
    clu_gate = _in_range_inclusive(clu_npr_l1m, 90, 9998)
    if clu_gate and _in_range_inclusive(ptbr_l6m_npr_l6m, 0, 5) and (_ge(no_ca_l3m, 3) or _gt(no_mlv_ca_l3m, 0)):
        return 3
    if clu_gate and _in_range_inclusive(ptbr_l6m_npr_l6m, 0, 5):
        return 2
    if clu_gate and _in_range_inclusive(ptbr_l3m_npr_l3m, 0, 5):
        return 1
    return 0


_WORST_STATUS_2_TO_3 = {"4", "5", "6", "7"}


_WORST_STATUS_0_TO_3 = {"0", "1", "2", "3"}


def score_vm_revolving(status_l6m, status_current, clu_pct, months_since_default) -> int:
    """
    Vertical Market score, "revolving" pattern -- used identically for
    Bank Cards, Retail Storecards, Current Accounts, and Home Shopping
    (lines 422-425, 441-444, 487-490, 506-509 -- verified byte-identical
    other than column names).
    """
    if _in_range_inclusive(months_since_default, 1, 12):
        return 3
    if (months_since_default is not None and not pd.isna(months_since_default) and 12 < months_since_default <= 24) \
            or _in_set(status_current, _WORST_STATUS_2_TO_3):
        return 2
    if _in_set(status_current, _WORST_STATUS_0_TO_3) and _in_set(status_l6m, _WORST_STATUS_2_TO_3) and _ge(clu_pct, 75):
        return 1
    return 0


def score_vm_instalment(status_l6m, status_current, months_since_default) -> int:
    """
    Vertical Market score, "instalment" pattern -- used identically for
    Credit Sales Agreements, Unsecured Loans, Vehicle Finance/HP, Secured
    Loans, Mortgages, and Utilities (lines 431-434, 450-453, 459-462,
    468-471, 477-480, 496-499 -- verified byte-identical other than
    column names).
    """
    if _in_range_inclusive(months_since_default, 1, 12):
        return 3
    if (months_since_default is not None and not pd.isna(months_since_default) and 12 < months_since_default <= 24) \
            or _in_set(status_current, _WORST_STATUS_2_TO_3):
        return 2
    if _in_set(status_current, _WORST_STATUS_0_TO_3) and _in_set(status_l6m, _WORST_STATUS_2_TO_3):
        return 1
    return 0


VM_REVOLVING_MARKETS = ["BankCards", "RetCards", "CA", "HS"]


VM_INSTALMENT_MARKETS = ["CSA", "UL", "VF_HP", "SL", "MG", "UT"]


def score_hc(hc_a_08, hc_c_05, hc_e_02) -> int:
    """Home Credit & PayDay Loans, HC family (lines 515-518)."""
    if _in_range_inclusive(hc_c_05, 1, 12):
        return 3
    if (hc_c_05 is not None and not pd.isna(hc_c_05) and 12 < hc_c_05 <= 24) or _gt(hc_a_08, 0):
        return 2
    if _eq(hc_a_08, 0) and _gt(hc_e_02, 0):
        return 1
    return 0


def score_pdl(pdl_e_01, pdl_c_01, pdl_f_01) -> int:
    """PayDay Loans, PDL family (lines 523-526)."""
    if _gt(pdl_e_01, 0):
        return 3
    if _eq(pdl_e_01, 0) and _gt(pdl_c_01, 0):
        return 2
    if _eq(pdl_e_01, 0) and _eq(pdl_c_01, 0) and _gt(pdl_f_01, 0):
        return 1
    return 0


def score_bank_iva_pir(main_bankrupt, main_iva, main_ccj, ea1_d_03) -> int:
    """Bankruptcy / IVA / CCJ / Public Information Record (lines 533-536)."""
    is_bankrupt_or_iva = (main_bankrupt == "B") or (main_iva == "V")
    if is_bankrupt_or_iva:
        return 3
    not_bankrupt_or_iva = (main_bankrupt != "B") and (main_iva != "V")
    if not_bankrupt_or_iva and _ge(main_ccj, 500) and _in_range_inclusive(ea1_d_03, 1, 12):
        return 2
    if not_bankrupt_or_iva and _ge(main_ccj, 500) and _gt(ea1_d_03, 12):
        return 1
    return 0


def score_npd_eb(npd_bal_npd_l12m, npd_bal_ebad_l12m, npd_bal_npd_l18m, npd_bal_ebad_l18m) -> int:
    """Never-Paid Defaults & Early Bads (lines 548-551)."""
    if _ge(npd_bal_npd_l12m, 500):
        return 3
    npd_l12m_below_500 = npd_bal_npd_l12m is not None and not pd.isna(npd_bal_npd_l12m) and npd_bal_npd_l12m < 500
    if npd_l12m_below_500 and _ge(npd_bal_ebad_l12m, 500):
        return 2
    if _eq(npd_bal_npd_l12m, 0) and _eq(npd_bal_ebad_l12m, 0) and (_ge(npd_bal_npd_l18m, 500) or _ge(npd_bal_ebad_l18m, 500)):
        return 1
    return 0


_CIFAS_CLEAN_CODES = {"N", "T", "00", "99"}


def score_cifas(sp_c_24) -> int:
    """CIFAS Fraud Category (line 554). NULL SP_C_24 is NOT IN the clean list -> scores 3, per SQL NOT IN semantics
    being NULL-safe here only because the comparison set doesn't include NULL; SQL `NULL NOT IN (...)` is actually
    UNKNOWN (not TRUE), so a NULL SP_C_24 does NOT score 3 in SQL Server -- see risk note below."""
    if sp_c_24 is None or pd.isna(sp_c_24):
        # SQL: `NULL NOT IN ('N','T','00','99')` evaluates to UNKNOWN, which the
        # CASE WHEN treats as not-satisfied -> falls to ELSE 0. Reproduced exactly.
        return 0
    return 3 if sp_c_24 not in _CIFAS_CLEAN_CODES else 0


SCORE_FIELD_NAMES = [
    "CLU_Score", "BDS_Score",
    "VM_BankCards_Score", "VM_CSA_Score", "VM_RetCards_Score", "VM_UL_Score",
    "VM_VF_HP_Score", "VM_SL_Score", "VM_MG_Score", "VM_CA_Score",
    "VM_UT_Score", "VM_HS_Score",
    "HC_Score", "PDL_Score", "BankIVAPIR_Score", "NPD_EB_Score", "CIFAS_Score",
]


def score_applicant(raw: dict) -> dict:
    """
    Computes all 17 criteria for one applicant's raw bureau attributes.
    `raw` keys match the Experian column names used in the SQL source
    (see `build_applicant_bureau_query` below for the exact column list).
    Missing keys are treated as NULL (consistent with a LEFT JOIN miss).
    """
    g = raw.get  # shorthand; g(key) returns None for missing keys, matching a bureau non-match
    return {
        "CLU_Score": score_clu(g("SP_F3_35"), g("SP_F1_30"), g("SP_F3_36")),
        "BDS_Score": score_bds(g("CLU_NPR_L1M"), g("PTBR_L6M_NPR_L6M"), g("PTBR_L3M_NPR_L3M"), g("NO_CA_L3M"), g("NO_MLV_CA_L3M")),
        "VM_BankCards_Score": score_vm_revolving(g("VM01_SP_VM2_04"), g("VM01_SP_VM2_05"), g("VM01_SP_VM2_23"), g("VM01_SP_VM2_29")),
        "VM_CSA_Score": score_vm_instalment(g("VM02_SP_VM1_04"), g("VM02_SP_VM1_05"), g("VM02_SP_VM1_21")),
        "VM_RetCards_Score": score_vm_revolving(g("VM03_SP_VM2_04"), g("VM03_SP_VM2_05"), g("VM03_SP_VM2_23"), g("VM03_SP_VM2_29")),
        "VM_UL_Score": score_vm_instalment(g("VM04_SP_VM1_04"), g("VM04_SP_VM1_05"), g("VM04_SP_VM1_21")),
        "VM_VF_HP_Score": score_vm_instalment(g("VM05_SP_VM1_04"), g("VM05_SP_VM1_05"), g("VM05_SP_VM1_21")),
        "VM_SL_Score": score_vm_instalment(g("VM06_SP_VM1_04"), g("VM06_SP_VM1_05"), g("VM06_SP_VM1_21")),
        "VM_MG_Score": score_vm_instalment(g("VM07_SP_VM1_04"), g("VM07_SP_VM1_05"), g("VM07_SP_VM1_21")),
        "VM_CA_Score": score_vm_revolving(g("VM08_SP_VM2_04"), g("VM08_SP_VM2_05"), g("VM08_SP_VM2_23"), g("VM08_SP_VM2_29")),
        "VM_UT_Score": score_vm_instalment(g("VM09_SP_VM1_04"), g("VM09_SP_VM1_05"), g("VM09_SP_VM1_21")),
        "VM_HS_Score": score_vm_revolving(g("VM10_SP_VM2_04"), g("VM10_SP_VM2_05"), g("VM10_SP_VM2_23"), g("VM10_SP_VM2_29")),
        "HC_Score": score_hc(g("HC_A_08"), g("HC_C_05"), g("HC_E_02")),
        "PDL_Score": score_pdl(g("PDL_E_01"), g("PDL_C_01"), g("PDL_F_01")),
        "BankIVAPIR_Score": score_bank_iva_pir(g("MAIN_BANKRUPT"), g("MAIN_IVA"), g("MAIN_CCJ"), g("EA1_D_03")),
        "NPD_EB_Score": score_npd_eb(g("NPD_BAL_NPD_SP_L12M"), g("NPD_BAL_EBAD_SP_L12M"), g("NPD_BAL_NPD_SP_L18M"), g("NPD_BAL_EBAD_SP_L18M")),
        "CIFAS_Score": score_cifas(g("SP_C_24")),
        "CII": g("SP_CONSUMER_INDEBT") or 0,
        "AAM": g("E5_S_05_1") or 0,
        "has_bureau_match": bool(raw.get("_has_match", False)),
    }


def build_customer_name(raw: dict) -> str:
    """Equivalent of: PORTFOLIO_TITLE + ' ' + PORTFOLIO_GIVEN_NM + ' ' + PORTFOLIO_FAM_NM."""
    parts = [raw.get("PORTFOLIO_TITLE") or "", raw.get("PORTFOLIO_GIVEN_NM") or "", raw.get("PORTFOLIO_FAM_NM") or ""]
    return " ".join(parts)


def combine_applicants(applicant1: dict, applicant2: dict) -> dict:
    """
    Equivalent of CTE_Merge -> CTE_App -> CTE_Final's per-criterion App-level
    MAX(C1, C2) and the final BureauRiskScore sum, for a single account.
    Both inputs are already coalesced to 0/'' for missing fields (see
    `score_applicant`, which defaults every numeric criterion to 0 when
    the bureau record is missing).
    """
    exp_data = 1 if applicant1.get("has_bureau_match") else 0
    cust2_data = 1 if applicant1.get("has_bureau_match") and applicant2.get("has_bureau_match") else 0

    app_scores = {name: max(applicant1[name], applicant2[name]) for name in SCORE_FIELD_NAMES}
    cii_app = max(applicant1["CII"], applicant2["CII"])
    aam_app = max(applicant1["AAM"], applicant2["AAM"])

    bureau_risk_score = sum(app_scores.values())

    return {
        "ExpData": exp_data,
        "Cust2Data": cust2_data,
        "CII_App": cii_app,
        "AAM_App": aam_app,
        **{f"{name}_App": value for name, value in app_scores.items()},
        "BureauRiskScore": bureau_risk_score,
    }


def resolve_geo_delphi(
    lending: str,
    own_bureau_gd_index: Optional[float],
    own_bureau_gd_score: Optional[float],
    pc1_lookup: dict,
    pc2_lookup: dict,
    property_postcode: Optional[str],
) -> dict:
    """
    Equivalent of CTE_C1's GD_Index/GD_Score/GD_BTLProxy2 CASE expressions
    (lines 380-391).

    - Resi: use the account's own bureau record's Geo-Delphi fields
      directly; 0 if absent.
    - BTL: try the property postcode against the PC1 reference table
      first; if absent, fall back to the PC2 (district-level) table;
      0 if neither matches. `GD_BTLProxy2=1` flags the PC2 fallback path
      specifically (matches the source; PC1-level BTL matches are NOT
      flagged as a "proxy" -- only the coarser PC2 fallback is).
    """
    if lending != "BTL":
        gd_index = own_bureau_gd_index if own_bureau_gd_index is not None else 0
        gd_score = own_bureau_gd_score if own_bureau_gd_score is not None else 0
        return {"GD_Index": gd_index, "GD_Score": gd_score, "GD_BTLProxy2": 0}

    pc1_match = pc1_lookup.get(property_postcode)
    if pc1_match is not None:
        return {"GD_Index": pc1_match["GD_Index1"], "GD_Score": pc1_match["GD_Score1"], "GD_BTLProxy2": 0}

    pc2_key = postcode_pc1_to_pc2(property_postcode)
    pc2_match = pc2_lookup.get(pc2_key)
    if pc2_match is not None:
        return {"GD_Index": pc2_match["GD_Index2"], "GD_Score": pc2_match["GD_Score2"], "GD_BTLProxy2": 1}

    return {"GD_Index": 0, "GD_Score": 0, "GD_BTLProxy2": 0}


def build_pc1_reference(experian_extract: pd.DataFrame) -> dict:
    """
    Equivalent of CTE_PC1: builds a {postcode: {GD_Index1, GD_Score1}}
    lookup from the whole Experian extract, keeping the best-ranked
    (lowest index, then lowest score) record per normalised postcode.
    `experian_extract` must already be reduced to one row per
    (Period, LenderCode, PrimaryAccount) -- i.e. the CustRank=1 filter
    from the SQL source -- before calling this.
    """
    candidates = []
    for _, row in experian_extract.iterrows():
        pc1 = normalise_postcode_pc1(row.get("PORTFOLIO_POSTCODE"))
        if is_valid_pc1_candidate(pc1):
            candidates.append({"PC1": pc1, "GD_Index1": row.get("ND_G_01"), "GD_Score1": row.get("ND_G_02")})

    if not candidates:
        return {}

    frame = pd.DataFrame(candidates)
    frame = frame.sort_values(["PC1", "GD_Index1", "GD_Score1"], ascending=[True, True, True], kind="stable")
    best_per_postcode = frame.drop_duplicates(subset="PC1", keep="first")
    return {
        row["PC1"]: {"GD_Index1": row["GD_Index1"], "GD_Score1": row["GD_Score1"]}
        for _, row in best_per_postcode.iterrows()
    }


def build_pc2_reference(pc1_lookup: dict) -> dict:
    """Equivalent of CTE_PC2: derives a district-level lookup from the PC1 lookup, same best-per-key rule."""
    candidates = [
        {"PC2": postcode_pc1_to_pc2(pc1), "GD_Index2": v["GD_Index1"], "GD_Score2": v["GD_Score1"]}
        for pc1, v in pc1_lookup.items()
    ]
    if not candidates:
        return {}
    frame = pd.DataFrame(candidates)
    frame = frame.sort_values(["PC2", "GD_Index2", "GD_Score2"], ascending=[True, True, True], kind="stable")
    best_per_district = frame.drop_duplicates(subset="PC2", keep="first")
    return {
        row["PC2"]: {"GD_Index2": row["GD_Index2"], "GD_Score2": row["GD_Score2"]}
        for _, row in best_per_district.iterrows()
    }


BUREAU_RAW_COLUMNS = [
    "SP_F3_35", "SP_F1_30", "SP_F3_36",
    "CLU_NPR_L1M", "PTBR_L6M_NPR_L6M", "PTBR_L3M_NPR_L3M", "NO_CA_L3M", "NO_MLV_CA_L3M",
    "VM01_SP_VM2_04", "VM01_SP_VM2_05", "VM01_SP_VM2_23", "VM01_SP_VM2_29",
    "VM02_SP_VM1_04", "VM02_SP_VM1_05", "VM02_SP_VM1_21",
    "VM03_SP_VM2_04", "VM03_SP_VM2_05", "VM03_SP_VM2_23", "VM03_SP_VM2_29",
    "VM04_SP_VM1_04", "VM04_SP_VM1_05", "VM04_SP_VM1_21",
    "VM05_SP_VM1_04", "VM05_SP_VM1_05", "VM05_SP_VM1_21",
    "VM06_SP_VM1_04", "VM06_SP_VM1_05", "VM06_SP_VM1_21",
    "VM07_SP_VM1_04", "VM07_SP_VM1_05", "VM07_SP_VM1_21",
    "VM08_SP_VM2_04", "VM08_SP_VM2_05", "VM08_SP_VM2_23", "VM08_SP_VM2_29",
    "VM09_SP_VM1_04", "VM09_SP_VM1_05", "VM09_SP_VM1_21",
    "VM10_SP_VM2_04", "VM10_SP_VM2_05", "VM10_SP_VM2_23", "VM10_SP_VM2_29",
    "HC_A_08", "HC_C_05", "HC_E_02",
    "PDL_C_01", "PDL_E_01", "PDL_F_01",
    "MAIN_IVA", "MAIN_BANKRUPT", "MAIN_CCJ", "EA1_D_03",
    "NPD_BAL_NPD_SP_L12M", "NPD_BAL_EBAD_SP_L12M", "NPD_BAL_NPD_SP_L18M", "NPD_BAL_EBAD_SP_L18M",
    "SP_C_24", "SP_CONSUMER_INDEBT", "E5_S_05_1",
    "PORTFOLIO_TITLE", "PORTFOLIO_GIVEN_NM", "PORTFOLIO_FAM_NM",
]


def build_applicant_bureau_query(cfg: BureauQualifierConfig, cust_rank: int, include_geo_and_lending: bool) -> str:
    """
    ONE parameterised template replacing the SAS source's two ~150-line
    near-duplicate CTE_C1 / CTE_C2 blocks (lines 369-745). `cust_rank`
    selects applicant 1 or 2 (matches the source's CustRank filter);
    `include_geo_and_lending` includes Lending/PostCode (only fetched
    once, for applicant 1, matching the original -- Geo-Delphi and
    Lending are account-level, not applicant-level attributes).
    Population predicate is byte-for-byte identical to Steps 1/2/4's
    CTE_C1/CTE_C2 WHERE clause.
    """
    company_in = _in_clause(len(cfg.company_scope))
    product_ex = _in_clause(len(cfg.excluded_product_codes))
    bureau_cols = ",\n        ex.".join(BUREAU_RAW_COLUMNS)
    geo_lending_select = (
        ", CASE WHEN dsp.SubPopulationCode = 'BTL' THEN 'BTL' ELSE 'Resi' END AS Lending"
        ", dms.PostCode"
        ", ex.ND_G_01, ex.ND_G_02"
        if include_geo_and_lending else ""
    )
    geo_lending_join = "INNER JOIN P_DW.dbo.dimSubPopulation dsp ON fma.FK_SubPopulation = dsp.PK_SubPopulation\n" \
                        "INNER JOIN P_DW.dbo.dimMortgageSecurity dms ON fma.FK_MortgageSecurity = dms.PK_MortgageSecurity\n" \
        if include_geo_and_lending else ""

    return f"""
SELECT  CASE WHEN lu.Allocated_AccountNumber IS NOT NULL THEN lu.Allocated_AccountNumber ELSE da.AccountNumber END AS AccountNumber
        {geo_lending_select}
        , ex.{bureau_cols}
FROM    P_DW.dbo.factMortgageAccountMonthSS fma
INNER JOIN  P_DW.dbo.dimAccount da ON fma.FK_Account = da.PK_Account
INNER JOIN  P_DW.dbo.dimMonth dm ON fma.FK_SnapshotDate = dm.PK_Month
INNER JOIN  P_DW.dbo.dimAccountStates das ON fma.FK_AccountStates = das.PK_AccountStates
INNER JOIN  P_DW.dbo.dimProduct dp ON fma.FK_CurrentMortgageProduct = dp.PK_Product
INNER JOIN  P_DW.dbo.dimCompany dc ON fma.FK_Company = dc.PK_Company
{geo_lending_join}LEFT JOIN   P_ODS.reference.AHLNYMMigratedAccounts lu
    ON  da.AccountPool = lu.AHLNYM_PoolID AND da.AccountNumber = lu.AHLNYM_AccountNumber
LEFT JOIN   (
                SELECT b.* FROM (
                    SELECT a.*, RANK() OVER (PARTITION BY a.Period, a.LenderCode, a.PrimaryAccount ORDER BY a.CustomerID ASC) AS CustRank
                    FROM P_ODS.EXPERIAN.DCMGen10 a
                    WHERE a.Period = ?
                ) b WHERE b.CustRank = ?
            ) ex
    ON  (CASE WHEN dc.CompanyName = 'Amber Homeloans Ltd' THEN 'AHL'
              WHEN dc.CompanyName = 'North Yorkshire Mortgages' THEN 'NYM'
              ELSE 'SBS' END) = ex.LenderCode
    AND da.AccountNumber = ex.PrimaryAccount
WHERE   dc.CompanyName IN {company_in}
AND     da.AccountPool <> ?
AND     dm.CalendarMonthYear = CONVERT(VARCHAR, ?)
AND     da.AccountSubType = 'Primary'
AND     da.CurrentAccountClassification = 'Residential'
AND     das.AccountStatus = 'Live'
AND     dp.ProductCode NOT IN {product_ex}
AND     fma.CapitalBalanceLive > ?
AND     ((dc.CompanyName IN ('Skipton Building Society') AND fma.ContractualArrearsInMonths < 1)
         OR (dc.CompanyName IN ('Amber Homeloans Ltd', 'North Yorkshire Mortgages') AND fma.StandardArrearsInMonths < 1))
AND     da.AccountStartDate <= DATEADD(MONTH, DATEDIFF(MONTH, -1, dm.BatchDate) - 1, -1)
"""


def build_applicant_bureau_params(cfg: BureauQualifierConfig, yyyymm: int, cust_rank: int) -> list:
    return (
        [yyyymm, cust_rank]
        + list(cfg.company_scope)
        + [cfg.excluded_account_pool, yyyymm]
        + list(cfg.excluded_product_codes)
        + [cfg.capital_balance_threshold]
    )


EXPERIAN_REFERENCE_QUERY = """
SELECT b.PORTFOLIO_POSTCODE, b.ND_G_01, b.ND_G_02
FROM (
    SELECT a.*, RANK() OVER (PARTITION BY a.Period, a.LenderCode, a.PrimaryAccount ORDER BY a.CustomerID ASC) AS CustRank
    FROM P_ODS.EXPERIAN.DCMGen10 a
    WHERE a.Period = ?
) b
WHERE b.CustRank = 1
"""


def apply_disabled_threshold_filter(df: pd.DataFrame, cfg: BureauQualifierConfig) -> pd.DataFrame:
    """
    Documented no-op. The SAS source's threshold WHERE clause is commented
    out (see explanation §1); this function exists purely so that fact is
    visible and enforced in code, not to apply any filtering. If GBRS/SBRS/
    BGD are ever confirmed to be required here, implement the filter
    explicitly and remove this guard -- do not silently start filtering.
    """
    if any(v is not None for v in (
        cfg.gbrs_general_bureau_risk_score, cfg.sbrs_secondary_bureau_risk_score, cfg.bgd_btl_geo_delphi_threshold,
    )):
        raise BureauQualifierError(
            "GBRS/SBRS/BGD were supplied but this component does not apply the disabled "
            "threshold filter (see risk log). Confirm intent before proceeding -- do not "
            "silently start filtering a strategy population."
        )
    return df


def validate_bureau_qualifiers(df: pd.DataFrame) -> None:
    required = ["AccountNumber", "Lending", "BureauRiskScore", "ExpData", "Cust2Data", "GD_Index", "GD_BTLProxy2"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise BureauQualifierError(f"Missing expected column(s): {missing_cols}")

    if df.empty:
        logger.warning("Bureau Riskier Qualifiers produced 0 rows. Confirm this is expected.")
        return

    dup_count = int(df["AccountNumber"].duplicated().sum())
    if dup_count > 0:
        raise BureauQualifierError(
            f"{dup_count} duplicate AccountNumber row(s) -- likely a RANK() tie fan-out "
            f"in the Experian CustRank lookup (see risk #4)."
        )

    bad_lending = df.loc[~df["Lending"].isin(["Resi", "BTL"])]
    if not bad_lending.empty:
        raise BureauQualifierError(f"{len(bad_lending)} row(s) with unexpected Lending value(s).")

    negative_scores = int((df["BureauRiskScore"] < 0).sum())
    if negative_scores > 0:
        raise BureauQualifierError(f"{negative_scores} row(s) with a negative BureauRiskScore -- should not be possible.")

    logger.info(
        "Validation passed: %s rows (%s Resi / %s BTL), BureauRiskScore range [%s, %s]",
        len(df), int((df["Lending"] == "Resi").sum()), int((df["Lending"] == "BTL").sum()),
        df["BureauRiskScore"].min(), df["BureauRiskScore"].max(),
    )


def extract_bureau_riskier_qualifiers(cfg: BureauQualifierConfig) -> pd.DataFrame:
    """
    Main entry point. Equivalent to Step 4's unnamed proc sql block.
    Returns one row per account with all App-level scores and
    BureauRiskScore -- UNFILTERED by risk score (see §1).
    """
    yyyymm = get_reporting_period(cfg.rundate)
    logger.info("Starting Bureau Riskier Qualifiers extraction for period %s", yyyymm)

    with DataWarehouseConnector(cfg) as conn:
        experian_ref = pd.read_sql(EXPERIAN_REFERENCE_QUERY, conn, params=[yyyymm])
        pc1_lookup = build_pc1_reference(experian_ref)
        pc2_lookup = build_pc2_reference(pc1_lookup)

        q1 = build_applicant_bureau_query(cfg, cust_rank=1, include_geo_and_lending=True)
        p1 = build_applicant_bureau_params(cfg, yyyymm, cust_rank=1)
        raw1 = pd.read_sql(q1, conn, params=p1)

        q2 = build_applicant_bureau_query(cfg, cust_rank=2, include_geo_and_lending=False)
        p2 = build_applicant_bureau_params(cfg, yyyymm, cust_rank=2)
        raw2 = pd.read_sql(q2, conn, params=p2)

    raw1_by_account = {row["AccountNumber"]: dict(row) for _, row in raw1.iterrows()}
    raw2_by_account = {row["AccountNumber"]: dict(row) for _, row in raw2.iterrows()}

    records = []
    for account_number, r1 in raw1_by_account.items():
        r1 = dict(r1)
        r1["_has_match"] = not pd.isna(r1.get("PORTFOLIO_TITLE")) if "PORTFOLIO_TITLE" in r1 else False
        r2 = dict(raw2_by_account.get(account_number, {}))
        r2["_has_match"] = bool(r2)

        applicant1_scores = score_applicant(r1)
        applicant2_scores = score_applicant(r2)
        combined = combine_applicants(applicant1_scores, applicant2_scores)

        geo = resolve_geo_delphi(
            lending=r1.get("Lending"),
            own_bureau_gd_index=r1.get("ND_G_01"),
            own_bureau_gd_score=r1.get("ND_G_02"),
            pc1_lookup=pc1_lookup,
            pc2_lookup=pc2_lookup,
            property_postcode=r1.get("PostCode"),
        )

        records.append({
            "AccountNumber": account_number,
            "Lending": r1.get("Lending"),
            **geo,
            **combined,
        })

    result_columns = ["AccountNumber", "Lending", "GD_Index", "GD_Score", "GD_BTLProxy2", "ExpData", "Cust2Data", "CII_App", "AAM_App"] + [f"{name}_App" for name in SCORE_FIELD_NAMES] + ["BureauRiskScore"]
    if records:
        result = pd.DataFrame(records).sort_values("AccountNumber", kind="stable").reset_index(drop=True)
    else:
        # No qualifying accounts this period is a legitimate business outcome
        # (e.g. an empty upstream population) -- return an empty frame with
        # the correct columns AND dtypes rather than crashing on the sort
        # below (a DataFrame with no columns at all) or, just as importantly,
        # leaving every column as generic 'object' dtype -- which would
        # silently break a later merge against Step 1's real (int64
        # AccountNumber) output with a dtype-mismatch error, in a genuine
        # production run, not just under test.
        dtype_map = {"AccountNumber": "int64", "Lending": "object"}
        result = pd.DataFrame({
            col: pd.Series(dtype=dtype_map.get(col, "int64")) for col in result_columns
        })
    result = apply_disabled_threshold_filter(result, cfg)  # documented no-op -- see function docstring
    validate_bureau_qualifiers(result)

    logger.info("Completed Bureau Riskier Qualifiers extraction: %s rows", len(result))
    return result


####################################################################################################
# STEP 5: PREVIOUS CONTACTS BARRED/REQUALIFIED
####################################################################################################
"""
previous_contacts.py
Production module: Pre-Arrears Strategy — Step 5: Previous Contacts
Barred/Requalified.

Converted 1:1 from Pre-Arrears.txt lines 854-873.

The extraction boundary (reading out.Resi_hist / out.BTL_hist) is
deliberately pluggable via injected `reader` callables, since the
physical source of these external datasets is not defined in the
supplied excerpt (see risk #5). `lagp` has no source-code default and
must be supplied explicitly (see risk #6).
"""


if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class PreviousContactsError(Exception):
    """Raised when Step 5 extraction/classification cannot be trusted to proceed."""


@dataclass(frozen=True)
class PreviousContactsConfig:
    # No default -- see risk #6. Must be supplied explicitly by the caller,
    # sourced from wherever the wider production job defines &lagp.
    lagp: float

    def __post_init__(self):
        if self.lagp is None:
            raise ValueError("lagp must be supplied explicitly; the SAS source has no default for &lagp (see risk #6).")


step5_REQUIRED_RAW_COLUMNS = ["AccountNumber", "SSContact"]


def _sas_missing_safe(series: pd.Series) -> pd.Series:
    """
    Reproduces SAS's rule that a missing numeric value compares as
    negative infinity (always "less than" any real number), rather than
    pandas' default where NaN compares False against everything. Used
    ONLY for comparison purposes -- the original (possibly-NaN) values
    are preserved in the output.
    """
    return series.fillna(-np.inf)


def dedup_to_latest_contact_per_account(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Equivalent of:
        proc sort by AccountNumber descending SSContact;
        data ...; set ...; by AccountNumber; if first.AccountNumber; run;

    Keeps one row per AccountNumber: the row with the highest SSContact
    value. Ties broken by original input order (stable sort), matching
    SAS's default stable proc sort (see risk #3).
    """
    missing_cols = [c for c in step5_REQUIRED_RAW_COLUMNS if c not in raw.columns]
    if missing_cols:
        raise PreviousContactsError(f"History extract missing expected column(s): {missing_cols}")

    working = raw[step5_REQUIRED_RAW_COLUMNS].copy()
    working["_sort_key"] = _sas_missing_safe(working["SSContact"])

    working = working.sort_values(
        ["AccountNumber", "_sort_key"], ascending=[True, False], kind="stable"
    )
    deduped = working.groupby("AccountNumber", as_index=False, sort=False).first()
    deduped = deduped.drop(columns="_sort_key").sort_values("AccountNumber", kind="stable").reset_index(drop=True)
    return deduped


def classify_barred_and_requalified(deduped: pd.DataFrame, lagp: float) -> "tuple[pd.DataFrame, pd.DataFrame]":
    """
    Equivalent of:
        data *_Barred; set *_hist_dd; if SSContact>&lagp;  run;
        data *_Requal; set *_hist_dd; if SSContact<=&lagp; run;

    Uses SAS missing-value comparison semantics (see risk #2): a missing
    SSContact is treated as negative infinity, so it always lands in
    Requal, never in Barred, and is never silently dropped.
    """
    effective = _sas_missing_safe(deduped["SSContact"])
    barred = deduped.loc[effective > lagp].copy()
    requalified = deduped.loc[effective <= lagp].copy()
    return barred, requalified


def _finalise_segment_stack(resi_df: pd.DataFrame, btl_df: pd.DataFrame, flag_column: str) -> pd.DataFrame:
    """
    Equivalent of:
        data Barred; set Resi_Barred BTL_Barred; Barred=1; drop SSContact; run;
        proc sort data=Barred; by AccountNumber; run;
    (and the analogous Requal block)
    """
    stacked = pd.concat([resi_df[["AccountNumber"]], btl_df[["AccountNumber"]]], ignore_index=True)
    stacked[flag_column] = 1
    stacked = stacked.sort_values("AccountNumber", kind="stable").reset_index(drop=True)
    return stacked


def validate_no_duplicate_accounts(df: pd.DataFrame, name: str) -> None:
    """
    Not an explicit check in the SAS source, but required here: Step 6's
    downstream merge assumes at most one row per AccountNumber in each
    merged table. A duplicate (e.g. from an account appearing in both
    Resi and BTL history) would silently fan out that merge (see risk #4).
    """
    dup_count = int(df["AccountNumber"].duplicated().sum())
    if dup_count > 0:
        raise PreviousContactsError(
            f"{dup_count} duplicate AccountNumber row(s) in '{name}' -- likely the same "
            f"account present in both Resi and BTL history. This would silently fan out "
            f"the Step 6 merge; resolve before proceeding (see risk #4)."
        )


def build_barred_and_requalified(
    cfg: PreviousContactsConfig,
    resi_reader: Callable[[], pd.DataFrame],
    btl_reader: Callable[[], pd.DataFrame],
) -> "tuple[pd.DataFrame, pd.DataFrame]":
    """
    Main entry point. Equivalent to Step 5 end-to-end. Returns
    (Barred, Requal) as separate DataFrames, matching the two SAS output
    tables of the same name.
    """
    logger.info("Starting Previous Contacts Barred/Requalified extraction (lagp=%s)", cfg.lagp)

    try:
        resi_raw = resi_reader()
    except Exception as exc:  # noqa: BLE001
        raise PreviousContactsError("Failed to read Resi history extract") from exc
    try:
        btl_raw = btl_reader()
    except Exception as exc:  # noqa: BLE001
        raise PreviousContactsError("Failed to read BTL history extract") from exc

    resi_dd = dedup_to_latest_contact_per_account(resi_raw)
    btl_dd = dedup_to_latest_contact_per_account(btl_raw)

    resi_barred, resi_requal = classify_barred_and_requalified(resi_dd, cfg.lagp)
    btl_barred, btl_requal = classify_barred_and_requalified(btl_dd, cfg.lagp)

    barred = _finalise_segment_stack(resi_barred, btl_barred, "Barred")
    requalified = _finalise_segment_stack(resi_requal, btl_requal, "Requal")

    validate_no_duplicate_accounts(barred, "Barred")
    validate_no_duplicate_accounts(requalified, "Requal")

    logger.info("Completed: %s barred account(s), %s requalified account(s)", len(barred), len(requalified))
    return barred, requalified


####################################################################################################
# STEP 6: MERGE, EXCLUSIONS, CUT-OFFS AND TARGET
####################################################################################################
"""
merge_exclusions_cutoffs.py
Production module: Pre-Arrears Strategy — Step 6: Data Merge, Exclusions,
Cut-offs and Target.

Converted 1:1 from Pre-Arrears.txt lines 877-911.

Takes the outputs of Steps 1-5 (as DataFrames) and produces the final
Qual_Resi / Qual_B qualifying tables. The seven threshold macro
variables (&RG1BS, &RG2BS, &GBRS, &BG1BS, &BG2BS, &SBRS, &BGD) have no
defaults in the supplied source and must be supplied explicitly (see
risk log above and Step 5's identical treatment of &lagp).
"""


if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class MergeExclusionsCutoffsError(Exception):
    """Raised when Step 6 merge/exclusion/cut-off logic cannot be trusted to proceed."""


@dataclass(frozen=True)
class MergeExclusionsCutoffsConfig:
    # None of these have a source-code default -- see risk log. All seven
    # must be supplied explicitly, sourced from the wider production job.
    resi_g1_behavioural_score_cutoff: float       # &RG1BS
    resi_g2_behavioural_score_cutoff: float       # &RG2BS
    general_bureau_risk_score_cutoff: float       # &GBRS
    btl_g1_behavioural_score_cutoff: float        # &BG1BS
    btl_g2_behavioural_score_cutoff: float        # &BG2BS
    secondary_bureau_risk_score_cutoff: float     # &SBRS
    btl_geo_delphi_cutoff: float                  # &BGD

    def __post_init__(self):
        required = [
            self.resi_g1_behavioural_score_cutoff, self.resi_g2_behavioural_score_cutoff,
            self.general_bureau_risk_score_cutoff, self.btl_g1_behavioural_score_cutoff,
            self.btl_g2_behavioural_score_cutoff, self.secondary_bureau_risk_score_cutoff,
            self.btl_geo_delphi_cutoff,
        ]
        if any(v is None for v in required):
            raise ValueError(
                "All seven threshold parameters must be supplied explicitly; the SAS "
                "source has no defaults for &RG1BS/&RG2BS/&GBRS/&BG1BS/&BG2BS/&SBRS/&BGD."
            )


def step6_validate_unique_account_number(df: pd.DataFrame, name: str) -> None:
    """Merge-safety check: a true SAS match-merge with a duplicate key produces
    a many-to-many fan-out, not a clean left join (see risk #2)."""
    dup_count = int(df["AccountNumber"].duplicated().sum())
    if dup_count > 0:
        raise MergeExclusionsCutoffsError(
            f"{dup_count} duplicate AccountNumber row(s) in '{name}' -- a SAS "
            f"match-merge on a duplicated key would silently fan out; refusing "
            f"to proceed (see risk #2)."
        )


REQUIRED_SS_COLUMNS = ["AccountNumber", "Segment", "BH_Score", "RS_Ind", "ArrBelow1MIA"]


REQUIRED_PREP_COLUMNS = ["AccountNumber", "MIA1X_pre12m", "AB1X_pre12m", "AB2X_pre12m"]


REQUIRED_DDR_COLUMNS = ["AccountNumber", "DDR"]


REQUIRED_BR_COLUMNS = ["AccountNumber", "Lending", "ExpData", "BureauRiskScore", "GD_Index"]


REQUIRED_BARRED_COLUMNS = ["AccountNumber", "Barred"]


REQUIRED_REQUAL_COLUMNS = ["AccountNumber", "Requal"]


def build_account_level_merge(
    ss: pd.DataFrame, prep: pd.DataFrame, ddr: pd.DataFrame, br: pd.DataFrame,
    barred: pd.DataFrame, requal: pd.DataFrame,
) -> pd.DataFrame:
    """
    Equivalent of the `Acc&ss` DATA step (merge + 3 exclusions + Requal
    coalesce + G1/G2/G3 grouping + NA filter).
    """
    for df, cols, name in [
        (ss, REQUIRED_SS_COLUMNS, "SS (Step 1)"), (prep, REQUIRED_PREP_COLUMNS, "Prep (Step 2)"),
        (ddr, REQUIRED_DDR_COLUMNS, "DDR (Step 3)"), (br, REQUIRED_BR_COLUMNS, "BR (Step 4)"),
        (barred, REQUIRED_BARRED_COLUMNS, "Barred (Step 5)"), (requal, REQUIRED_REQUAL_COLUMNS, "Requal (Step 5)"),
    ]:
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise MergeExclusionsCutoffsError(f"'{name}' input missing expected column(s): {missing}")
        step6_validate_unique_account_number(df, name)

    # merge SS&ss(in=a) Prep&ss(in=b) DDR&ss(in=c) BR&ss(in=d) Barred(in=e) Requal(in=f);
    # by AccountNumber; if a;
    # -> SS is required (driving population); everything else is an optional
    #    left-join enrichment.
    merged = ss.merge(prep, on="AccountNumber", how="left")
    merged = merged.merge(ddr, on="AccountNumber", how="left")
    merged = merged.merge(br, on="AccountNumber", how="left", suffixes=("", "_br"))
    merged = merged.merge(barred, on="AccountNumber", how="left")
    merged = merged.merge(requal, on="AccountNumber", how="left")

    # Data-quality cross-check (not present in the SAS source, does not
    # alter output): Segment (Step 1) and Lending (Step 4) should always
    # agree for a matched account. Warn, don't raise -- the SAS logic only
    # ever reads Segment here (see risk #3).
    both_present = merged["Lending"].notna()
    disagreement = both_present & (merged["Segment"] != merged["Lending"])
    disagreement_count = int(disagreement.sum())
    if disagreement_count > 0:
        logger.warning(
            "%s account(s) have Segment (Step 1) != Lending (Step 4). "
            "Qualifyer logic uses Segment only, matching the SAS source, but "
            "this divergence should be investigated (see risk #3).",
            disagreement_count,
        )

    # if AB2X_pre12m=0 and ArrBelow1MIA=0 and RS_Ind=1 then delete;
    redemption_clean_exclusion = (
        (merged["AB2X_pre12m"] == 0) & (merged["ArrBelow1MIA"] == 0) & (merged["RS_Ind"] == 1)
    )
    # if DDR=1 then delete;
    ddr_exclusion = merged["DDR"] == 1
    # if Barred=1 then delete;
    barred_exclusion = merged["Barred"] == 1

    excluded = redemption_clean_exclusion | ddr_exclusion | barred_exclusion
    logger.info(
        "Exclusions: %s redemption-clean, %s DDR-targeted, %s barred (may overlap), %s total rows removed",
        int(redemption_clean_exclusion.sum()), int(ddr_exclusion.sum()), int(barred_exclusion.sum()), int(excluded.sum()),
    )
    merged = merged.loc[~excluded].copy()

    # if Requal=1 then Requal=1; else Requal=0;
    merged["Requal"] = (merged["Requal"] == 1).astype(int)

    # if MIA1X_pre12m=1 then G1=1; else G1=0;
    merged["G1"] = (merged["MIA1X_pre12m"] == 1).astype(int)

    # if MIA1X_pre12m=0 and (AB2X_pre12m=1 or (AB1X_pre12m=1 and ArrBelow1MIA=1)) then G2=1; else G2=0;
    merged["G2"] = (
        (merged["MIA1X_pre12m"] == 0)
        & ((merged["AB2X_pre12m"] == 1) | ((merged["AB1X_pre12m"] == 1) & (merged["ArrBelow1MIA"] == 1)))
    ).astype(int)

    # if G1=0 and G2=0 and ExpData=1 then G3=1; else G3=0;
    merged["G3"] = ((merged["G1"] == 0) & (merged["G2"] == 0) & (merged["ExpData"] == 1)).astype(int)

    # Group='  '; if G1=1 then Group='G1'; else if G2=1 then Group='G2'; else if G3=1 then Group='G3'; else Group='NA';
    conditions = [merged["G1"] == 1, merged["G2"] == 1, merged["G3"] == 1]
    choices = ["G1", "G2", "G3"]
    merged["Group"] = np.select(conditions, choices, default="NA")

    # if Group in ('G1' 'G2' 'G3');
    merged = merged.loc[merged["Group"].isin(["G1", "G2", "G3"])].copy()

    merged = merged.sort_values("AccountNumber", kind="stable").reset_index(drop=True)
    return merged


def apply_qualifier_cutoffs(acc: pd.DataFrame, cfg: MergeExclusionsCutoffsConfig) -> pd.DataFrame:
    """
    Equivalent of the `Qual&ss` DATA step: segment/group-specific score
    cut-offs -> Qualifyer flag -> filter to Qualifyer=1.

    All `<=`/`>=` comparisons use SAS-safe missing-value semantics (see
    risk #1), even though BH_Score/BureauRiskScore/GD_Index should never
    actually be missing at this point given upstream guarantees.
    """
    bh_score = _sas_missing_safe(acc["BH_Score"])
    bureau_risk_score = _sas_missing_safe(acc["BureauRiskScore"])
    gd_index = _sas_missing_safe(acc["GD_Index"])

    resi_g1 = (acc["Segment"] == "Resi") & (acc["Group"] == "G1") & (bh_score <= cfg.resi_g1_behavioural_score_cutoff)
    resi_g2 = (acc["Segment"] == "Resi") & (acc["Group"] == "G2") & (bh_score <= cfg.resi_g2_behavioural_score_cutoff)
    resi_g3 = (acc["Segment"] == "Resi") & (acc["Group"] == "G3") & (bureau_risk_score >= cfg.general_bureau_risk_score_cutoff)
    btl_g1 = (acc["Segment"] == "BTL") & (acc["Group"] == "G1") & (bh_score <= cfg.btl_g1_behavioural_score_cutoff)
    btl_g2 = (acc["Segment"] == "BTL") & (acc["Group"] == "G2") & (bh_score <= cfg.btl_g2_behavioural_score_cutoff)
    btl_g3 = (acc["Segment"] == "BTL") & (acc["Group"] == "G3") & (
        (bureau_risk_score >= cfg.general_bureau_risk_score_cutoff)
        | ((bureau_risk_score >= cfg.secondary_bureau_risk_score_cutoff) & (gd_index >= cfg.btl_geo_delphi_cutoff))
    )

    qualifies = resi_g1 | resi_g2 | resi_g3 | btl_g1 | btl_g2 | btl_g3
    result = acc.copy()
    result["Qualifyer"] = qualifies.astype(int)

    # if Qualifyer=1;
    result = result.loc[result["Qualifyer"] == 1].copy().reset_index(drop=True)
    return result


def split_by_segment(qualified: pd.DataFrame) -> "tuple[pd.DataFrame, pd.DataFrame]":
    """Equivalent of: data Qual_Resi&ss; ... if Segment='Resi'; / data Qual_B&ss; ... if Segment='BTL';"""
    qual_resi = qualified.loc[qualified["Segment"] == "Resi"].copy().reset_index(drop=True)
    qual_btl = qualified.loc[qualified["Segment"] == "BTL"].copy().reset_index(drop=True)
    return qual_resi, qual_btl


def run_merge_exclusions_cutoffs(
    cfg: MergeExclusionsCutoffsConfig,
    ss: pd.DataFrame, prep: pd.DataFrame, ddr: pd.DataFrame, br: pd.DataFrame,
    barred: pd.DataFrame, requal: pd.DataFrame,
) -> "tuple[pd.DataFrame, pd.DataFrame]":
    """
    Main entry point. Equivalent to Step 6 end-to-end. Returns
    (Qual_Resi, Qual_B), matching the two final SAS output tables.
    """
    logger.info("Starting Data Merge, Exclusions, Cut-offs and Target")

    acc = build_account_level_merge(ss, prep, ddr, br, barred, requal)
    logger.info("After merge/exclusions/grouping: %s accounts in G1/G2/G3", len(acc))

    qualified = apply_qualifier_cutoffs(acc, cfg)
    logger.info("After cut-offs: %s accounts qualify", len(qualified))

    qual_resi, qual_btl = split_by_segment(qualified)
    logger.info("Completed: %s Resi qualifiers, %s BTL qualifiers", len(qual_resi), len(qual_btl))
    return qual_resi, qual_btl


####################################################################################################
# STEP 7: PORTFOLIO LANDLORD & HIGHEST CUSTOMER EXPOSURE
####################################################################################################
"""
btl_exposure_dedup.py
Production module: Pre-Arrears Strategy — Step 7: Portfolio Landlord &
Highest Customer Exposure.

Converted from Pre-Arrears.txt lines 914-1230.

Design: the four SQL Server pass-through queries (raw, set-based ranking/
aggregation over warehouse tables) stay as SQL -- reimplementing RANK()-
based dedup and multi-level aggregation in pandas would be higher-risk
than keeping it server-side and verified. Only the final merge/coalesce/
flag-derivation DATA step is converted to Python, consistent with the
scope of Steps 1-3.
"""


if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class BtlExposureDedupError(Exception):
    """Raised when Step 7 extraction/merge cannot be trusted to proceed."""


@dataclass(frozen=True)
class BtlExposureDedupConfig:
    rundate: date  # must be the 1st day of a calendar month

    db_server: str = "SBSPSQLVS101\\MISPSQL01"
    db_database: str = "P_DW"
    db_driver: str = "ODBC Driver 17 for SQL Server"
    connect_timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: float = 5.0

    # NOTE: SBS-only, deliberately NOT the multi-company scope used in
    # Steps 1/2/4 -- see risk #1. Named distinctly from other steps'
    # `excluded_account_pool` since this is a required-match, not an
    # exclusion.
    account_pool_scope: str = "SBS00001"

    capital_balance_threshold: float = 1000.0
    excluded_product_codes: List[str] = field(default_factory=lambda: [
        "MIN01", "HOM10", "HM120", "HM121", "HM122", "HM123", "HM124", "HM125",
    ])
    portfolio_landlord_btl_count_threshold: int = 4  # see risk #8 -- applied consistently, 4 usages in source

    deceased_relationship_type_code: str = "19"

    newl_application_types: List[str] = field(default_factory=lambda: [
        "D1", "B1", "C1", "D2", "PA", "FD", "FA", "FC",
    ])
    newl_application_window_start: date = date(2018, 2, 1)
    newl_not_cancelled_sentinel: str = "1900-01-01 00:00:00.000"

    def __post_init__(self):
        if self.rundate.day != 1:
            raise ValueError(f"rundate must be the first day of a month; got {self.rundate}")


def _core_population_predicate_sql(status_values: "list[str]", table_aliases: dict) -> str:
    """
    Shared WHERE-clause fragment repeated (with only the status list
    varying) across every query and nested subquery in this component
    (see risk #7). `table_aliases` maps logical names to the aliases
    used in the calling query (they differ slightly query to query).
    """
    da, dm, das, dp, fma = (
        table_aliases["da"], table_aliases["dm"], table_aliases["das"],
        table_aliases["dp"], table_aliases["fma"],
    )
    status_in = _in_clause(len(status_values))
    product_ex = _in_clause(8)  # excluded_product_codes is always 8 entries in this source
    return f"""
{da}.AccountPool = ?
AND     {dm}.CalendarMonthYear = CONVERT(VARCHAR, ?)
AND     {da}.AccountSubType = 'Primary'
AND     {da}.CurrentAccountClassification = 'Residential'
AND     {das}.AccountStatus IN {status_in}
AND     {fma}.CapitalBalanceLive > ?
AND     {dp}.ProductCode NOT IN {product_ex}
"""


def _core_population_params(cfg: BtlExposureDedupConfig, yyyymm: int, status_values: "list[str]") -> list:
    return (
        [cfg.account_pool_scope, yyyymm] + list(status_values)
        + [cfg.capital_balance_threshold] + list(cfg.excluded_product_codes)
    )


def build_exbtl_query(cfg: BtlExposureDedupConfig) -> str:
    aliases = {"da": "da", "dm": "dm", "das": "das", "dp": "dp", "fma": "fma"}
    predicate = _core_population_predicate_sql(["Live"], aliases)
    return f"""
SELECT  a.AccountNumber, a.TotalBTLs
FROM    (
            SELECT  da.AccountNumber
                    , dma.NumberOfExistingBuyToLets AS TotalBTLs
                    , RANK() OVER (PARTITION BY da.AccountNumber ORDER BY da.AccountStartDate DESC, dma.BK_MOR98 DESC) AS Ranking
            FROM    P_DW.dbo.factMortgageAccountMonthSS fma
            INNER JOIN  P_DW.dbo.dimAccount da ON fma.FK_Account = da.PK_Account
            INNER JOIN  P_DW.dbo.dimProduct dp ON fma.FK_CurrentMortgageProduct = dp.PK_Product
            INNER JOIN  P_DW.dbo.dimAccountStates das ON fma.FK_AccountStates = das.PK_AccountStates
            INNER JOIN  P_DW.dbo.dimMonth dm ON fma.FK_SnapshotDate = dm.PK_Month
            LEFT JOIN   P_DW.dbo.bridgeMortgageApplicationAccount bmaa
                ON  da.PK_Account = bmaa.FK_Account AND bmaa.isProgressedApplication = 'Y'
            LEFT JOIN   P_DW.dbo.dimMortgageApplication dma
                ON  bmaa.FK_MortgageApplication = dma.PK_MortgageApplication AND dma.BK_MOR98 <> 'Unknown'
            WHERE   {predicate}
        ) a
WHERE   a.Ranking = 1
AND     a.TotalBTLs >= ?
ORDER BY a.AccountNumber
"""


def build_exbtl_params(cfg: BtlExposureDedupConfig, yyyymm: int) -> list:
    return _core_population_params(cfg, yyyymm, ["Live"]) + [cfg.portfolio_landlord_btl_count_threshold]


def build_sbspl_query(cfg: BtlExposureDedupConfig) -> str:
    aliases = {"da": "da", "dm": "dm", "das": "das", "dp": "dp", "fma": "fma"}
    outer_predicate = _core_population_predicate_sql(["Live", "Possession"], aliases)
    inner_predicate = _core_population_predicate_sql(["Live", "Possession"], aliases)
    return f"""
SELECT  DISTINCT da.AccountNumber, 1 AS SBS_PL
FROM    P_DW.dbo.factMortgageAccountMonthSS fma
INNER JOIN  P_DW.dbo.dimAccount da ON fma.FK_Account = da.PK_Account
INNER JOIN  P_DW.dbo.bridgeAccountCustomer bac ON da.PK_Account = bac.FK_Account AND bac.relationshipTypeCode = ?
INNER JOIN  P_DW.dbo.dimCustomer dc ON bac.FK_Customer = dc.PK_Customer
INNER JOIN  P_DW.dbo.dimProduct dp ON fma.FK_CurrentMortgageProduct = dp.PK_Product
INNER JOIN  P_DW.dbo.dimAccountStates das ON fma.FK_AccountStates = das.PK_AccountStates
INNER JOIN  P_DW.dbo.dimMonth dm ON fma.FK_SnapshotDate = dm.PK_Month
INNER JOIN  P_DW.dbo.dimSubPopulation dsp ON fma.FK_SubPopulation = dsp.PK_SubPopulation
WHERE   {outer_predicate}
AND     dsp.SubPopulationCode = 'BTL'
AND     dc.CustomerNumber IN (
            SELECT b.CustomerNumber
            FROM (
                SELECT a.CustomerNumber, CASE WHEN COUNT(a.AccountNumber) >= ? THEN 1 ELSE 0 END AS SBS_4PlusBTL
                FROM (
                    SELECT dc.CustomerNumber, da.AccountNumber, fma.CapitalBalanceLive
                    FROM P_DW.dbo.factMortgageAccountMonthSS fma
                    INNER JOIN P_DW.dbo.dimAccount da ON fma.FK_Account = da.PK_Account
                    INNER JOIN P_DW.dbo.bridgeAccountCustomer bac ON da.PK_Account = bac.FK_Account AND bac.relationshipTypeCode = ?
                    INNER JOIN P_DW.dbo.dimCustomer dc ON bac.FK_Customer = dc.PK_Customer
                    INNER JOIN P_DW.dbo.dimProduct dp ON fma.FK_CurrentMortgageProduct = dp.PK_Product
                    INNER JOIN P_DW.dbo.dimAccountStates das ON fma.FK_AccountStates = das.PK_AccountStates
                    INNER JOIN P_DW.dbo.dimMonth dm ON fma.FK_SnapshotDate = dm.PK_Month
                    INNER JOIN P_DW.dbo.dimSubPopulation dsp ON fma.FK_SubPopulation = dsp.PK_SubPopulation
                    WHERE {inner_predicate}
                    AND dsp.SubPopulationCode = 'BTL'
                ) a
                GROUP BY a.CustomerNumber
            ) b
            WHERE b.SBS_4PlusBTL = 1
        )
ORDER BY da.AccountNumber
"""


def build_sbspl_params(cfg: BtlExposureDedupConfig, yyyymm: int) -> list:
    status = ["Live", "Possession"]
    return (
        [cfg.deceased_relationship_type_code]
        + _core_population_params(cfg, yyyymm, status)
        + [cfg.portfolio_landlord_btl_count_threshold]
        + [cfg.deceased_relationship_type_code]
        + _core_population_params(cfg, yyyymm, status)
    )


NEWL_APPLICATION_TYPE_PRIORITY = """
CASE WHEN s.Application_Type IN ('D1','B1','C1') THEN 1
     WHEN s.Application_Type = 'D2' THEN 2
     WHEN s.Application_Type = 'PA' THEN 3
     WHEN s.Application_Type = 'FC' THEN 4
     WHEN s.Application_Type = 'FD' THEN 5
     WHEN s.Application_Type = 'FA' THEN 6 END
"""


def build_newl_pl_query(cfg: BtlExposureDedupConfig) -> str:
    app_type_in = _in_clause(len(cfg.newl_application_types))
    return f"""
SELECT  AccountNumber, 1 AS NewL_PL
FROM    (
            SELECT  dma.AccountNumberPrimary AS AccountNumber
                    , CONVERT(VARCHAR, dma.ApplicationReference) + '/' + CONVERT(VARCHAR, dma.ApplicationNumber) AS ApplicationNumber
                    , s.BK_MortgageApplication, dma.PK_MortgageApplication, s.Application_Type
                    , CONVERT(DATE, dacd.CalendarDate) AS DIP_Date
                    , CONVERT(DATE, dad.CalendarDate) AS App_Date
                    , CONVERT(DATE, dcad.CalendarDate) AS Cancellation_Date
                    , CONVERT(DATE, dcod.CalendarDate) AS Completion_Date
                    , RANK() OVER (
                        PARTITION BY s.PrimaryAccountNumber
                        ORDER BY dcad.CalendarDate DESC, dcod.CalendarDate DESC, ({NEWL_APPLICATION_TYPE_PRIORITY}) DESC
                      ) AS LatestApp
                    , s.Request_Time
                    , CASE WHEN s.appSpareNumeric43 <= 0 THEN 0 ELSE CONVERT(INT, s.appSpareNumeric43) END AS NoMortProp
            FROM    P_DW.dbo.factMortgageApplication fma
            INNER JOIN  P_DW.dbo.dimMortgageApplication dma ON fma.FK_MortgageApplication = dma.PK_MortgageApplication
            INNER JOIN  P_DW.dbo.dimDate dacd ON fma.FK_ApplicationCreateDate = dacd.PK_Date
            INNER JOIN  P_DW.dbo.dimDate dad ON fma.FK_ApplicationDate = dad.PK_Date
            INNER JOIN  P_DW.dbo.dimDate dfod ON fma.FK_Date_OfferFirst = dfod.PK_Date
            INNER JOIN  P_DW.dbo.dimDate dlod ON fma.FK_Date_OfferLatest = dlod.PK_Date
            INNER JOIN  P_DW.dbo.dimDate dcad ON fma.FK_DateCancelled = dcad.PK_Date
            INNER JOIN  P_DW.dbo.dimDate dcod ON fma.FK_DateCompleted = dcod.PK_Date
            INNER JOIN  P_ODS.CREDIT_RISK.vwMortgageAppCreditScore_Latest s
                ON  CONVERT(VARCHAR, dma.ApplicationReference) + '/' + CONVERT(VARCHAR, dma.ApplicationNumber) = s.ApplicationNumber
            WHERE   s.Application_Type IN {app_type_in}
            AND     dacd.CalendarDate >= ?
            AND     dcod.CalendarDate >= ?
            AND     dcad.CalendarDate = ?
            AND     s.appSpareNumeric43 >= ?
        ) a
WHERE   a.LatestApp = 1
ORDER BY AccountNumber
"""


def build_newl_pl_params(cfg: BtlExposureDedupConfig) -> list:
    return (
        list(cfg.newl_application_types)
        + [cfg.newl_application_window_start, cfg.newl_application_window_start, cfg.newl_not_cancelled_sentinel]
        + [cfg.portfolio_landlord_btl_count_threshold]
    )


def build_highest_exposure_query(cfg: BtlExposureDedupConfig) -> str:
    aliases = {"da": "da", "dm": "dm", "das": "das", "dp": "dp", "fma": "fma"}
    status = ["Live", "Possession"]
    cte_predicate = _core_population_predicate_sql(status, aliases)
    dedup_predicate = _core_population_predicate_sql(status, aliases)
    exposure_predicate = _core_population_predicate_sql(status, aliases)
    return f"""
WITH CTE_BTLCus AS (
    SELECT  da.AccountNumber, dc.CustomerNumber
            , CASE WHEN bac.isPrimaryCustomer = 'Y' THEN 1 ELSE 0 END AS PrimCust
    FROM    P_DW.dbo.factMortgageAccountMonthSS fma
    INNER JOIN  P_DW.dbo.dimAccount da ON fma.FK_Account = da.PK_Account
    INNER JOIN  P_DW.dbo.bridgeAccountCustomer bac ON da.PK_Account = bac.FK_Account AND bac.relationshipTypeCode = ?
    INNER JOIN  P_DW.dbo.dimCustomer dc ON bac.FK_Customer = dc.PK_Customer
    INNER JOIN  P_DW.dbo.dimProduct dp ON fma.FK_CurrentMortgageProduct = dp.PK_Product
    INNER JOIN  P_DW.dbo.dimAccountStates das ON fma.FK_AccountStates = das.PK_AccountStates
    INNER JOIN  P_DW.dbo.dimMonth dm ON fma.FK_SnapshotDate = dm.PK_Month
    INNER JOIN  P_DW.dbo.dimSubPopulation dsp ON fma.FK_SubPopulation = dsp.PK_SubPopulation
    WHERE   {cte_predicate}
    AND     dsp.SubPopulationCode = 'BTL'
)
SELECT  he.AccountNumber, he.CustomerNumber, he.PrimCust, he.CustDecL12m, he.CustDec,
        he.BTL_Acc, he.Resi_Acc, he.BTL_Bal, he.Resi_Bal, he.CombBal
FROM    (
            SELECT  c.AccountNumber, c.CustomerNumber, c.PrimCust,
                    e.CustDecL12m, e.CustDec, e.BTL_Acc, e.Resi_Acc, e.BTL_Bal, e.Resi_Bal, e.CombBal
                    , RANK() OVER (
                        PARTITION BY c.AccountNumber
                        ORDER BY e.CustDecL12m ASC, e.CustDec ASC, e.CombBal DESC, c.PrimCust DESC, c.CustomerNumber ASC
                      ) AS HighestExp
            FROM    (
                        SELECT  a.CustomerNumber
                                , SUM(a.CustDecL12m) AS CustDecL12m, SUM(a.CustDec) AS CustDec
                                , SUM(a.BTL) AS BTL_Acc, SUM(a.Resi) AS Resi_Acc
                                , SUM(a.BTL_Bal) AS BTL_Bal, SUM(a.Resi_Bal) AS Resi_Bal
                                , SUM(a.CapitalBalanceLive) AS CombBal
                        FROM    (
                                    SELECT  dc.CustomerNumber
                                            , CASE WHEN dc.CustomerDateofDeath IS NOT NULL AND DATEDIFF(MONTH, dc.CustomerDateofDeath, GETDATE()) <= 12 THEN 1 ELSE 0 END AS CustDecL12m
                                            , CASE WHEN dc.CustomerDateofDeath IS NOT NULL THEN 1 ELSE 0 END AS CustDec
                                            , da.AccountNumber
                                            , CASE WHEN dsp.SubPopulationCode = 'BTL' THEN 1 ELSE 0 END AS BTL
                                            , CASE WHEN dsp.SubPopulationCode <> 'BTL' THEN 1 ELSE 0 END AS Resi
                                            , CASE WHEN dsp.SubPopulationCode = 'BTL' THEN fma.CapitalBalanceLive ELSE 0 END AS BTL_Bal
                                            , CASE WHEN dsp.SubPopulationCode <> 'BTL' THEN fma.CapitalBalanceLive ELSE 0 END AS Resi_Bal
                                            , fma.CapitalBalanceLive
                                    FROM    P_DW.dbo.factMortgageAccountMonthSS fma
                                    INNER JOIN  P_DW.dbo.dimAccount da ON fma.FK_Account = da.PK_Account
                                    INNER JOIN  P_DW.dbo.bridgeAccountCustomer bac ON da.PK_Account = bac.FK_Account AND bac.relationshipTypeCode = ?
                                    INNER JOIN  P_DW.dbo.dimCustomer dc ON bac.FK_Customer = dc.PK_Customer
                                    INNER JOIN  P_DW.dbo.dimProduct dp ON fma.FK_CurrentMortgageProduct = dp.PK_Product
                                    INNER JOIN  P_DW.dbo.dimAccountStates das ON fma.FK_AccountStates = das.PK_AccountStates
                                    INNER JOIN  P_DW.dbo.dimMonth dm ON fma.FK_SnapshotDate = dm.PK_Month
                                    WHERE   {dedup_predicate}
                                    AND     dc.CustomerNumber IN (
                                                SELECT DISTINCT dc.CustomerNumber
                                                FROM P_DW.dbo.factMortgageAccountMonthSS fma
                                                INNER JOIN P_DW.dbo.dimAccount da ON fma.FK_Account = da.PK_Account
                                                INNER JOIN P_DW.dbo.bridgeAccountCustomer bac ON da.PK_Account = bac.FK_Account AND bac.relationshipTypeCode = ?
                                                INNER JOIN P_DW.dbo.dimCustomer dc ON bac.FK_Customer = dc.PK_Customer
                                                INNER JOIN P_DW.dbo.dimProduct dp ON fma.FK_CurrentMortgageProduct = dp.PK_Product
                                                INNER JOIN P_DW.dbo.dimAccountStates das ON fma.FK_AccountStates = das.PK_AccountStates
                                                INNER JOIN P_DW.dbo.dimMonth dm ON fma.FK_SnapshotDate = dm.PK_Month
                                                INNER JOIN P_DW.dbo.dimSubPopulation dsp ON fma.FK_SubPopulation = dsp.PK_SubPopulation
                                                WHERE {exposure_predicate}
                                                AND dsp.SubPopulationCode = 'BTL'
                                            )
                                ) a
                        GROUP BY a.CustomerNumber
                    ) e
            INNER JOIN CTE_BTLCus c ON e.CustomerNumber = c.CustomerNumber
        ) he
WHERE   he.HighestExp = 1
ORDER BY he.AccountNumber
"""


def build_highest_exposure_params(cfg: BtlExposureDedupConfig, yyyymm: int) -> list:
    status = ["Live", "Possession"]
    return (
        [cfg.deceased_relationship_type_code]
        + _core_population_params(cfg, yyyymm, status)
        + [cfg.deceased_relationship_type_code]
        + _core_population_params(cfg, yyyymm, status)
        + [cfg.deceased_relationship_type_code]
        + _core_population_params(cfg, yyyymm, status)
    )


def step7_validate_unique_account_number(df: pd.DataFrame, name: str) -> None:
    dup_count = int(df["AccountNumber"].duplicated().sum())
    if dup_count > 0:
        raise BtlExposureDedupError(
            f"{dup_count} duplicate AccountNumber row(s) in '{name}' -- likely a RANK() "
            f"tie fan-out (see risk #3). Refusing to proceed with an ambiguous merge."
        )


def finalize_btl_portfolio_landlord_flags(
    qual_btl: pd.DataFrame,
    highest_exposure: pd.DataFrame,
    sbspl: pd.DataFrame,
    newl_pl: pd.DataFrame,
    exbtl: pd.DataFrame,
    cfg: BtlExposureDedupConfig,
) -> pd.DataFrame:
    """
    Equivalent of the final `data Qual_B&ss; merge ...; ...; run;` DATA
    step (lines 1219-1229). `qual_btl` is Step 6's BTL output (the `a`
    dataset / driving population).
    """
    for df, name in [
        (highest_exposure, "HighestExp"), (sbspl, "SBSPL"), (newl_pl, "NewL_PL"), (exbtl, "EXBTL"),
    ]:
        step7_validate_unique_account_number(df, name)
    step7_validate_unique_account_number(qual_btl, "Qual_B (Step 6)")

    merged = qual_btl.merge(highest_exposure, on="AccountNumber", how="left")
    merged = merged.merge(sbspl[["AccountNumber", "SBS_PL"]], on="AccountNumber", how="left")
    merged = merged.merge(newl_pl[["AccountNumber", "NewL_PL"]], on="AccountNumber", how="left")
    merged = merged.merge(exbtl[["AccountNumber", "TotalBTLs"]], on="AccountNumber", how="left")

    # if SBS_PL=. then SBS_PL=0; else SBS_PL=SBS_PL;
    merged["SBS_PL"] = merged["SBS_PL"].fillna(0).astype(int)
    # if NewL_PL=. then NewL_PL=0; else NewL_PL=NewL_PL;
    merged["NewL_PL"] = merged["NewL_PL"].fillna(0).astype(int)
    # if TotalBTLs=. then TotalBTLs=0; else TotalBTLs=TotalBTLs;
    merged["TotalBTLs"] = merged["TotalBTLs"].fillna(0).astype(int)

    # if SBS_PL=1 OR NewL_PL=1 then PL_True=1; else PL_True=0;
    merged["PL_True"] = ((merged["SBS_PL"] == 1) | (merged["NewL_PL"] == 1)).astype(int)

    # if PL_True=0 AND TotalBTLs>=4 then PL_Proxy=1; else PL_Proxy=0;
    merged["PL_Proxy"] = (
        (merged["PL_True"] == 0) & (merged["TotalBTLs"] >= cfg.portfolio_landlord_btl_count_threshold)
    ).astype(int)

    # if a;  -- already guaranteed: qual_btl is the left/driving frame in every merge above.

    # if CustDecL12m>0 or CustDec>0 then delete;
    # Missing CustDecL12m/CustDec (no HighestExp match) evaluates False for
    # a plain >0 test in BOTH SAS (missing = -inf) and pandas (NaN>0 = False)
    # -- no special missing-safe handling required here (see risk #4).
    deceased_exclusion = (merged["CustDecL12m"] > 0) | (merged["CustDec"] > 0)
    logger.info("Deceased-customer exclusion removes %s account(s)", int(deceased_exclusion.sum()))
    merged = merged.loc[~deceased_exclusion].copy()

    merged = merged.sort_values("AccountNumber", kind="stable").reset_index(drop=True)
    return merged


def extract_btl_exposure_dedup(cfg: BtlExposureDedupConfig, qual_btl: pd.DataFrame) -> pd.DataFrame:
    """
    Main entry point. Equivalent to Step 7 end-to-end. `qual_btl` is
    Step 6's BTL output. Returns the enriched, deceased-filtered result
    (the SAS source's overwritten `Qual_B&ss`).
    """
    yyyymm = get_reporting_period(cfg.rundate)
    logger.info("Starting Portfolio Landlord & Highest Customer Exposure extraction for period %s", yyyymm)

    with DataWarehouseConnector(cfg) as conn:
        exbtl = pd.read_sql(build_exbtl_query(cfg), conn, params=build_exbtl_params(cfg, yyyymm))
        sbspl = pd.read_sql(build_sbspl_query(cfg), conn, params=build_sbspl_params(cfg, yyyymm))
        newl_pl = pd.read_sql(build_newl_pl_query(cfg), conn, params=build_newl_pl_params(cfg))
        highest_exposure = pd.read_sql(
            build_highest_exposure_query(cfg), conn, params=build_highest_exposure_params(cfg, yyyymm)
        )

    result = finalize_btl_portfolio_landlord_flags(qual_btl, highest_exposure, sbspl, newl_pl, exbtl, cfg)
    logger.info("Completed: %s BTL account(s) after Step 7 enrichment/exclusion", len(result))
    return result


####################################################################################################
# TARGET RANKING AND VOLUME CAPPING
####################################################################################################
"""
target_ranking_and_capping.py
Production module: Pre-Arrears Strategy — Target Ranking and Volume
Capping (sits between Step 7 and Step 8 in the source).

Converted 1:1 from Pre-Arrears.txt lines 1233-1301.
"""


if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class TargetRankingError(Exception):
    """Raised when target ranking/capping cannot be trusted to proceed."""


@dataclass(frozen=True)
class TargetRankingConfig:
    # None of these have a source-code default -- all six must be
    # supplied explicitly, sourced from the wider production job.
    rg1_max: int  # &RG1Max
    rg2_max: int  # &RG2Max
    rg3_max: int  # &RG3Max
    bg1_max: int  # &BG1Max
    bg2_max: int  # &BG2Max
    bg3_max: int  # &BG3Max

    def __post_init__(self):
        required = [self.rg1_max, self.rg2_max, self.rg3_max, self.bg1_max, self.bg2_max, self.bg3_max]
        if any(v is None for v in required):
            raise ValueError(
                "All six volume-cap parameters must be supplied explicitly; the SAS "
                "source has no defaults for &RG1Max/&RG2Max/&RG3Max/&BG1Max/&BG2Max/&BG3Max."
            )


G1_G2_SORT_COLUMNS = ["Requal", "BH_Score", "MIASS", "BureauRiskScore", "AAM_App"]


G1_G2_SORT_ASCENDING = [True, True, False, False, True]


G3_RESI_SORT_COLUMNS = ["Requal", "BureauRiskScore", "BH_Score", "AAM_App", "BalSS", "AccountNumber"]


G3_RESI_SORT_ASCENDING = [True, False, True, True, False, True]


G3_BTL_SORT_COLUMNS = ["Requal", "BureauRiskScore", "GD_BTLProxy2", "GD_Index", "BH_Score", "AAM_App", "BalSS", "AccountNumber"]


G3_BTL_SORT_ASCENDING = [True, False, True, False, True, True, False, True]


def rank_and_cap(df: pd.DataFrame, sort_columns: "list[str]", ascending: "list[bool]", max_count: int) -> pd.DataFrame:
    """
    Generic replacement for the repeated SAS pattern:
        proc sort; by <sort_columns with directions>; run;
        data ...; set ...; Order+1; if Order<=&Max; run;

    Used identically 12 times in the source (6 Resi sub-groups, 6 BTL
    sub-groups) -- consolidated here since the underlying logic is
    identical each time, only the sort spec and max differ (see risk
    log's note on eliminating repeated calculations).
    """
    if max_count is None:
        raise TargetRankingError("max_count must be supplied explicitly (no SAS default exists).")

    sorted_df = df.sort_values(by=sort_columns, ascending=ascending, kind="stable").reset_index(drop=True)
    sorted_df["Order"] = range(1, len(sorted_df) + 1)
    return sorted_df.loc[sorted_df["Order"] <= max_count].copy().reset_index(drop=True)


def split_and_cap_resi_groups(qual_resi: pd.DataFrame, cfg: TargetRankingConfig) -> "dict[str, pd.DataFrame]":
    """Equivalent of lines 1234-1254 (RG1S..RG3A)."""
    groups = {}
    for group_label, sort_cols, sort_asc, max_count in [
        ("G1", G1_G2_SORT_COLUMNS, G1_G2_SORT_ASCENDING, cfg.rg1_max),
        ("G2", G1_G2_SORT_COLUMNS, G1_G2_SORT_ASCENDING, cfg.rg2_max),
        ("G3", G3_RESI_SORT_COLUMNS, G3_RESI_SORT_ASCENDING, cfg.rg3_max),
    ]:
        sbs_subset = qual_resi.loc[(qual_resi["Group"] == group_label) & (qual_resi["Lender"] == "SBS")]
        other_subset = qual_resi.loc[(qual_resi["Group"] == group_label) & (qual_resi["Lender"] != "SBS")]
        groups[f"R{group_label}S"] = rank_and_cap(sbs_subset, sort_cols, sort_asc, max_count)
        groups[f"R{group_label}A"] = rank_and_cap(other_subset, sort_cols, sort_asc, max_count)
    return groups


def split_and_cap_btl_groups(qual_btl: pd.DataFrame, cfg: TargetRankingConfig) -> "dict[str, pd.DataFrame]":
    """Equivalent of lines 1261-1280 (BG1S..BG3A)."""
    groups = {}
    for group_label, sort_cols, sort_asc, max_count in [
        ("G1", G1_G2_SORT_COLUMNS, G1_G2_SORT_ASCENDING, cfg.bg1_max),
        ("G2", G1_G2_SORT_COLUMNS, G1_G2_SORT_ASCENDING, cfg.bg2_max),
        ("G3", G3_BTL_SORT_COLUMNS, G3_BTL_SORT_ASCENDING, cfg.bg3_max),
    ]:
        sbs_subset = qual_btl.loc[(qual_btl["Group"] == group_label) & (qual_btl["Lender"] == "SBS")]
        other_subset = qual_btl.loc[(qual_btl["Group"] == group_label) & (qual_btl["Lender"] != "SBS")]
        groups[f"B{group_label}S"] = rank_and_cap(sbs_subset, sort_cols, sort_asc, max_count)
        groups[f"B{group_label}A"] = rank_and_cap(other_subset, sort_cols, sort_asc, max_count)
    return groups


def build_contact_history_target(capped_groups: "dict[str, pd.DataFrame]", group_labels: "list[str]", snapshot_period: int) -> pd.DataFrame:
    """
    Equivalent of R_Target / B_Target (lines 1257 / 1283):
    stacks the capped S+A sub-lists, sets SSContact to the reporting
    period, keeps only AccountNumber/Segment/SSContact.
    """
    frames = [capped_groups[label][["AccountNumber", "Segment"]] for label in group_labels]
    stacked = pd.concat(frames, ignore_index=True)
    stacked["SSContact"] = snapshot_period
    return stacked[["AccountNumber", "Segment", "SSContact"]]


def combine_lender_scope_groups(
    s_df: pd.DataFrame, a_df: pd.DataFrame, sort_columns: "list[str]", ascending: "list[bool]"
) -> pd.DataFrame:
    """
    Equivalent of: data RG1; set RG1S RG1A; run; proc sort ...; run;
    (and the five analogous RG2/RG3/BG1/BG2/BG3 blocks) -- stacks the
    capped S+A lists and re-sorts by the same group priority order,
    with NO further capping.
    """
    stacked = pd.concat([s_df, a_df], ignore_index=True)
    return stacked.sort_values(by=sort_columns, ascending=ascending, kind="stable").reset_index(drop=True)


def validate_target_consistency(
    contact_history_target: pd.DataFrame, combined_groups: "dict[str, pd.DataFrame]", name: str
) -> None:
    """
    Not an explicit SAS check, but added here: R_Target/B_Target and the
    combined RG*/BG* tables should represent the exact same set of
    accounts per group (same source rows, different projections). A
    divergence would indicate a bug in one of the two projections.
    """
    history_accounts = set(contact_history_target["AccountNumber"])
    combined_accounts = set()
    for df in combined_groups.values():
        combined_accounts |= set(df["AccountNumber"])
    if history_accounts != combined_accounts:
        only_in_history = history_accounts - combined_accounts
        only_in_combined = combined_accounts - history_accounts
        raise TargetRankingError(
            f"{name}: account set mismatch between the contact-history target and the "
            f"combined group tables. Only in history: {sorted(only_in_history)[:10]}... "
            f"Only in combined: {sorted(only_in_combined)[:10]}..."
        )


def run_target_ranking_and_capping(
    cfg: TargetRankingConfig, qual_resi: pd.DataFrame, qual_btl: pd.DataFrame, snapshot_period: int,
) -> dict:
    """
    Main entry point. Equivalent to the full lines 1233-1301 block.
    Returns a dict with the combined per-group tables (RG1/RG2/RG3/
    BG1/BG2/BG3) and the two contact-history feeds (R_Target/B_Target).
    """
    logger.info("Starting Target Ranking and Volume Capping")

    resi_groups = split_and_cap_resi_groups(qual_resi, cfg)
    btl_groups = split_and_cap_btl_groups(qual_btl, cfg)

    r_target = build_contact_history_target(resi_groups, ["RG1S", "RG1A", "RG2S", "RG2A", "RG3S", "RG3A"], snapshot_period)
    b_target = build_contact_history_target(btl_groups, ["BG1S", "BG1A", "BG2S", "BG2A", "BG3S", "BG3A"], snapshot_period)

    rg1 = combine_lender_scope_groups(resi_groups["RG1S"], resi_groups["RG1A"], G1_G2_SORT_COLUMNS, G1_G2_SORT_ASCENDING)
    rg2 = combine_lender_scope_groups(resi_groups["RG2S"], resi_groups["RG2A"], G1_G2_SORT_COLUMNS, G1_G2_SORT_ASCENDING)
    rg3 = combine_lender_scope_groups(resi_groups["RG3S"], resi_groups["RG3A"], G3_RESI_SORT_COLUMNS, G3_RESI_SORT_ASCENDING)
    bg1 = combine_lender_scope_groups(btl_groups["BG1S"], btl_groups["BG1A"], G1_G2_SORT_COLUMNS, G1_G2_SORT_ASCENDING)
    bg2 = combine_lender_scope_groups(btl_groups["BG2S"], btl_groups["BG2A"], G1_G2_SORT_COLUMNS, G1_G2_SORT_ASCENDING)
    bg3 = combine_lender_scope_groups(btl_groups["BG3S"], btl_groups["BG3A"], G3_BTL_SORT_COLUMNS, G3_BTL_SORT_ASCENDING)

    validate_target_consistency(r_target, {"RG1": rg1, "RG2": rg2, "RG3": rg3}, "Resi")
    validate_target_consistency(b_target, {"BG1": bg1, "BG2": bg2, "BG3": bg3}, "BTL")

    logger.info(
        "Completed: Resi RG1=%s RG2=%s RG3=%s | BTL BG1=%s BG2=%s BG3=%s",
        len(rg1), len(rg2), len(rg3), len(bg1), len(bg2), len(bg3),
    )

    return {
        "RG1": rg1, "RG2": rg2, "RG3": rg3, "BG1": bg1, "BG2": bg2, "BG3": bg3,
        "R_Target": r_target, "B_Target": b_target,
    }


####################################################################################################
# STEP 8: DIALLER SPECS
####################################################################################################
"""
dialler_specs.py
Production module: Pre-Arrears Strategy — Step 8: Dialler Specs.

Converted from Pre-Arrears.txt lines 1304-1581.
"""


if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class DiallerSpecsError(Exception):
    """Raised when Step 8 extraction/refinement/consolidation cannot be trusted to proceed."""


@dataclass(frozen=True)
class DiallerSpecsConfig:
    rundate: date  # must be the 1st day of a calendar month

    db_server: str = "SBSPSQLVS101\\MISPSQL01"
    db_database: str = "P_DW"
    db_driver: str = "ODBC Driver 17 for SQL Server"
    connect_timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: float = 5.0

    # NOTE: SBS-only, matching Step 7's scoping -- see risk #1.
    account_pool_scope: str = "SBS00001"
    capital_balance_threshold: float = 1000.0
    excluded_product_codes: List[str] = field(default_factory=lambda: [
        "MIN01", "HOM10", "HM120", "HM121", "HM122", "HM123", "HM124", "HM125",
    ])
    min_months_since_completion: int = 3
    min_remaining_term_months: int = 12
    deceased_relationship_type_code: str = "19"

    # The 6-account-per-customer cap (A1 primary + Acc2..Acc6) -- see risk #3.
    max_additional_accounts_per_customer: int = 5

    def __post_init__(self):
        if self.rundate.day != 1:
            raise ValueError(f"rundate must be the first day of a month; got {self.rundate}")


def build_customer_contact_query(cfg: DiallerSpecsConfig) -> str:
    product_ex = _in_clause(len(cfg.excluded_product_codes))
    return f"""
SELECT  da.AccountNumber, dc.CustomerNumber, fma.CapitalBalanceLive AS Balance
        , CASE WHEN dsp.SubPopulationCode = 'BTL' THEN 'BTL' ELSE 'Resi' END AS LendingType
        , da.AccountNumber AS IACCNBRO, dc.CustomerNumber AS Custnbro
        , dc.CustomerTitle AS Mretcu, dc.CustomerForename AS Firnameu, dc.CustomerSurname AS Surnameu
        , CASE WHEN bac.isPrimaryCustomer = 'Y' THEN 1 ELSE 0 END AS PrimCustFlag
        , dc.CustomerTelephoneDay, dc.CustomerTelephoneEvening, dc.CustomerTelephoneMobile
FROM    P_DW.dbo.factMortgageAccountMonthSS fma
INNER JOIN  P_DW.dbo.dimAccount da ON fma.FK_Account = da.PK_Account
INNER JOIN  P_DW.dbo.bridgeAccountCustomer bac
    ON  da.PK_Account = bac.FK_Account AND bac.relationshipTypeCode = ? AND bac.DeLinkDate IS NULL
INNER JOIN  P_DW.dbo.dimCustomer dc ON bac.FK_Customer = dc.PK_Customer
INNER JOIN  P_DW.dbo.dimMonth dm ON fma.FK_SnapshotDate = dm.PK_Month
INNER JOIN  P_DW.dbo.dimAccountStates das ON fma.FK_AccountStates = das.PK_AccountStates
INNER JOIN  P_DW.dbo.dimProduct dp ON fma.FK_CurrentMortgageProduct = dp.PK_Product
INNER JOIN  P_DW.dbo.dimArrangement dar ON fma.FK_ArrangementType = dar.PK_ArrangementType
INNER JOIN  P_DW.dbo.dimSubPopulation dsp ON fma.FK_SubPopulation = dsp.PK_SubPopulation
WHERE   dm.CalendarMonthYear = CONVERT(VARCHAR, ?)
AND     da.AccountPool = ?
AND     da.AccountSubType = 'Primary'
AND     das.AccountStatus IN ('Live')
AND     da.CurrentAccountClassification = 'Residential'
AND     dp.ProductCode NOT IN {product_ex}
AND     fma.ContractualArrearsInMonths < 1
AND     fma.CapitalBalanceLive > ?
AND     dar.ArrangementTypeCode < 0
AND     DATEDIFF(MONTH, da.AccountStartDate, DATEADD(MONTH, 0, dm.ReportingMonth)) > ?
AND     fma.RemainingTerm > ?
AND     da.AccountNumber NOT IN (
            SELECT b.AccountNumber
            FROM (
                SELECT a.AccountNumber, SUM(a.DeathFlag) AS DeathFlag
                FROM (
                    SELECT  da.AccountNumber, dc.CustomerNumber
                            , CASE WHEN dc.CustomerDateofDeath IS NOT NULL THEN 1 ELSE 0 END AS DeathFlag
                    FROM    P_DW.dbo.factMortgageAccount fma
                    INNER JOIN  P_DW.dbo.dimAccount da ON fma.FK_Account = da.PK_Account
                    INNER JOIN  P_DW.dbo.bridgeAccountCustomer bac
                        ON  da.PK_Account = bac.FK_Account AND bac.relationshipTypeCode = ?
                    INNER JOIN  P_DW.dbo.dimCustomer dc ON bac.FK_Customer = dc.PK_Customer
                    INNER JOIN  P_DW.dbo.dimProduct dp ON fma.FK_CurrentMortgageProduct = dp.PK_Product
                    WHERE   da.AccountPool = ?
                    AND     da.AccountSubType = 'Primary'
                    AND     da.CurrentAccountStatus IN ('Live')
                    AND     da.CurrentAccountClassification = 'Residential'
                ) a
                GROUP BY a.AccountNumber
            ) b
            WHERE b.DeathFlag > 0
        )
ORDER BY da.AccountNumber
"""


def build_customer_contact_params(cfg: DiallerSpecsConfig, yyyymm: int) -> list:
    return (
        [cfg.deceased_relationship_type_code, yyyymm, cfg.account_pool_scope]
        + list(cfg.excluded_product_codes)
        + [cfg.capital_balance_threshold, cfg.min_months_since_completion, cfg.min_remaining_term_months]
        + [cfg.deceased_relationship_type_code, cfg.account_pool_scope]
    )


def strip_non_digits(raw: Optional[str]) -> str:
    """Equivalent of: COMPRESS(field,'0123456789','k') -- keeps only digit characters."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    return "".join(ch for ch in str(raw) if ch.isdigit())


def normalize_uk_phone(digits: str) -> str:
    """
    Equivalent of the D3 CASE chain (lines 1409-1412, and the identical
    Evening/Mobile blocks): normalises a digit-only string to standard
    11-digit UK format, or '' if it doesn't match any recognised shape.
    """
    n = len(digits)
    if n == 11 and digits[0] == "0":
        return digits
    if n == 10 and digits[0] != "0":
        return "0" + digits
    if n == 12 and digits[:2] == "00":
        return digits[1:12]  # drop the first '0', keep the second + the 10-digit number
    return ""


def to_international_format(normalized_uk_number: str) -> str:
    """Equivalent of D31 (lines 1429-1431): '' stays '', else '+44' + the number minus its leading 0."""
    if normalized_uk_number == "":
        return ""
    return "+44" + normalized_uk_number[1:11]


def dedupe_phone_priority(tel_day: str, tel_eve: str, tel_mob: str) -> "tuple[str, str, str]":
    """
    Equivalent of D4's TelDay/TelEve/TelMob CASE chain (lines 1437-1439):
    Day is always kept if present; Evening kept only if it differs from
    Day; Mobile kept only if it differs from BOTH Day and Evening.
    """
    final_day = tel_day if tel_day != "" else ""
    if tel_eve == "" or tel_eve == final_day:
        final_eve = ""
    else:
        final_eve = tel_eve
    if tel_mob == "" or tel_mob == final_eve or tel_mob == final_day:
        final_mob = ""
    else:
        final_mob = tel_mob
    return final_day, final_eve, final_mob


def compute_contactability(tel_day: str, tel_eve: str, tel_mob: str) -> dict:
    """Equivalent of lines 1441-1445: Contactable flag and NoTel count."""
    contactable = 1 if (tel_day != "" or tel_eve != "" or tel_mob != "") else 0
    if contactable == 0:
        no_tel = 0
    else:
        no_tel = int(tel_day != "") + int(tel_eve != "") + int(tel_mob != "")
    return {"Contactable": contactable, "NoTel": no_tel, "TelDay": tel_day, "TelEve": tel_eve, "TelMob": tel_mob}


def refine_customer_phone_data(d1: pd.DataFrame) -> pd.DataFrame:
    """
    Equivalent of the full D2 -> D3 -> D31 -> D4 pipeline (lines
    1398-1449), applied row-wise since the branching logic is easier to
    verify correct this way than a vectorised equivalent.
    """
    required = ["CustomerTelephoneDay", "CustomerTelephoneEvening", "CustomerTelephoneMobile"]
    missing = [c for c in required if c not in d1.columns]
    if missing:
        raise DiallerSpecsError(f"D1 input missing expected column(s): {missing}")

    result_rows = []
    for _, row in d1.iterrows():
        day_digits = strip_non_digits(row["CustomerTelephoneDay"])
        eve_digits = strip_non_digits(row["CustomerTelephoneEvening"])
        mob_digits = strip_non_digits(row["CustomerTelephoneMobile"])

        day_norm = normalize_uk_phone(day_digits)
        eve_norm = normalize_uk_phone(eve_digits)
        mob_norm = normalize_uk_phone(mob_digits)

        day_intl = to_international_format(day_norm)
        eve_intl = to_international_format(eve_norm)
        mob_intl = to_international_format(mob_norm)

        final_day, final_eve, final_mob = dedupe_phone_priority(day_intl, eve_intl, mob_intl)
        contactability = compute_contactability(final_day, final_eve, final_mob)

        new_row = row.drop(labels=required).to_dict()
        new_row.update(contactability)
        result_rows.append(new_row)

    if result_rows:
        return pd.DataFrame(result_rows)

    # No customer contact records this period is a legitimate business
    # outcome (e.g. an empty upstream target population). Return an empty
    # frame with the correct columns rather than a columnless DataFrame,
    # which would otherwise KeyError on 'LendingType' in
    # split_by_lending_type() immediately downstream. Slicing d1 (rather
    # than rebuilding columns from scratch) preserves its real dtypes --
    # e.g. AccountNumber stays numeric -- so a downstream merge against a
    # non-empty frame doesn't fail on a dtype mismatch that's an artifact
    # of this fallback path, not a real business difference.
    remaining_columns = [c for c in d1.columns if c not in required]
    result = d1[remaining_columns].copy()
    result["Contactable"] = pd.Series(dtype="int64")
    result["NoTel"] = pd.Series(dtype="int64")
    result["TelDay"] = pd.Series(dtype="object")
    result["TelEve"] = pd.Series(dtype="object")
    result["TelMob"] = pd.Series(dtype="object")
    return result


def split_by_lending_type(d4: pd.DataFrame) -> "tuple[pd.DataFrame, pd.DataFrame]":
    """Equivalent of D_Resi / D_BTL (lines 1453-1457)."""
    d_resi = d4.loc[d4["LendingType"] == "Resi"].sort_values("AccountNumber", kind="stable").reset_index(drop=True)
    d_btl = d4.loc[d4["LendingType"] == "BTL"].sort_values("AccountNumber", kind="stable").reset_index(drop=True)
    return d_resi, d_btl


def select_best_contact_per_account(d_resi: pd.DataFrame) -> pd.DataFrame:
    """
    Equivalent of lines 1465-1473: Resi-only dedup to one contact row
    per account. Sort: AccountNumber asc, NoTel desc, PrimcustFlag desc,
    CustomerNumber asc; keep the first row per account (Rank_C BY-group
    reset, not a continuous counter -- see risk #5).
    """
    sorted_df = d_resi.sort_values(
        by=["AccountNumber", "NoTel", "PrimCustFlag", "CustomerNumber"],
        ascending=[True, False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    return sorted_df.groupby("AccountNumber", as_index=False, sort=False).first()


def merge_target_accounts_with_contacts_resi(target_accounts: pd.DataFrame, d_resi_best_contact: pd.DataFrame) -> pd.DataFrame:
    """
    Equivalent of T_Resi + the Resi merge (lines 1476-1485): merges by
    AccountNumber alone. Since d_resi_best_contact has one row per
    account, this cannot fan out.
    """
    return target_accounts.merge(d_resi_best_contact, on="AccountNumber", how="left", suffixes=("", "_contact"))


def merge_target_accounts_with_contacts_btl(target_accounts: pd.DataFrame, d_btl: pd.DataFrame) -> pd.DataFrame:
    """
    Equivalent of T_BTL + the BTL merge (lines 1528-1538): merges by
    (CustomerNumber, AccountNumber) jointly -- a DIFFERENT key from
    Resi's. d_btl was never deduplicated to one row per account, so a
    BTL account with multiple joint holders can appear more than once
    here (see risk #2) -- this is preserved deliberately, not corrected.
    """
    if "CustomerNumber" not in target_accounts.columns:
        raise DiallerSpecsError(
            "BTL target_accounts must include CustomerNumber to merge on (CustomerNumber, AccountNumber); "
            "if the upstream target list doesn't carry CustomerNumber yet, it must be joined in first."
        )
    return target_accounts.merge(
        d_btl, on=["CustomerNumber", "AccountNumber"], how="left", suffixes=("", "_contact")
    )


def consolidate_accounts_per_customer(
    merged: pd.DataFrame, cfg: DiallerSpecsConfig, extra_slot_columns: "list[str]" = None,
) -> pd.DataFrame:
    """
    Equivalent of the Rank_A dedup + A1..A6 wide-format join (lines
    1488-1518 for Resi, 1541-1573 for BTL). `extra_slot_columns`
    controls which columns ride along in the Acc2..Acc6 slots (BTL
    additionally carries ArrBalSS/Group per slot; Resi carries none --
    see risk #3's note on this being a deliberate output-shape
    difference).
    """
    extra_slot_columns = extra_slot_columns or []

    sorted_df = merged.sort_values(
        by=["CustomerNumber", "ArrBalSS", "Group", "BalSS", "AccountNumber"],
        ascending=[True, False, True, False, False],
        kind="stable",
    ).reset_index(drop=True)
    sorted_df["Rank_A"] = sorted_df.groupby("CustomerNumber").cumcount() + 1

    primary = sorted_df.loc[sorted_df["Rank_A"] == 1].copy().reset_index(drop=True)

    result = primary.copy()
    for slot in range(2, cfg.max_additional_accounts_per_customer + 2):  # Acc2..Acc6 by default
        slot_cols = ["CustomerNumber", "AccountNumber"] + extra_slot_columns
        slot_df = sorted_df.loc[sorted_df["Rank_A"] == slot, slot_cols].copy()
        rename_map = {"AccountNumber": f"Acc{slot}"}
        rename_map.update({col: f"Acc{slot}_{col.replace('ArrBalSS', 'Arr').replace('Group', 'G')}" for col in extra_slot_columns})
        slot_df = slot_df.rename(columns=rename_map)
        result = result.merge(slot_df, on="CustomerNumber", how="left")

    max_rank_seen = int(sorted_df["Rank_A"].max()) if not sorted_df.empty else 0
    if max_rank_seen > cfg.max_additional_accounts_per_customer + 1:
        overflow_customers = sorted_df.loc[
            sorted_df["Rank_A"] > cfg.max_additional_accounts_per_customer + 1, "CustomerNumber"
        ].unique().tolist()
        logger.warning(
            "%s customer(s) have more than %s target accounts; accounts beyond the "
            "cap are present upstream but NOT represented in this consolidated output "
            "(see risk #3). Affected customers: %s",
            len(overflow_customers), cfg.max_additional_accounts_per_customer + 1, overflow_customers[:10],
        )

    return result


def split_contactable_groups(consolidated: pd.DataFrame) -> "dict[str, pd.DataFrame]":
    """
    Equivalent of lines 1520-1523 (Resi) / 1578-1581 (BTL): splits the
    consolidated per-customer table into G1/G2/G3 (contactable) and
    NC (not contactable), each sorted by AccountNumber.

    Note: BTLFinal's intermediate `by Group` sort (line 1574) is
    intentionally not reproduced -- every branch below re-sorts by
    AccountNumber immediately after anyway, making it a verified no-op
    (see risk #7).
    """
    groups = {}
    for label in ["G1", "G2", "G3"]:
        subset = consolidated.loc[(consolidated["Group"] == label) & (consolidated["Contactable"] == 1)]
        groups[label] = subset.sort_values("AccountNumber", kind="stable").reset_index(drop=True)
    not_contactable = consolidated.loc[consolidated["Contactable"] == 0]
    groups["NC"] = not_contactable.sort_values("AccountNumber", kind="stable").reset_index(drop=True)
    return groups


def run_dialler_specs(
    cfg: DiallerSpecsConfig, rg1: pd.DataFrame, rg2: pd.DataFrame, rg3: pd.DataFrame,
    bg1: pd.DataFrame, bg2: pd.DataFrame, bg3: pd.DataFrame,
) -> dict:
    """
    Main entry point. Equivalent to Step 8 end-to-end (lines 1304-1581).
    `rg1`/`rg2`/`rg3`/`bg1`/`bg2`/`bg3` are the combined, capped group
    tables from the ranking/capping component. Returns a dict with
    ResiFinal/BTLFinal-derived group splits.
    """
    yyyymm = get_reporting_period(cfg.rundate)
    logger.info("Starting Dialler Specs extraction for period %s", yyyymm)

    with DataWarehouseConnector(cfg) as conn:
        d1 = pd.read_sql(
            build_customer_contact_query(cfg), conn, params=build_customer_contact_params(cfg, yyyymm)
        )

    d4 = refine_customer_phone_data(d1)
    d_resi, d_btl = split_by_lending_type(d4)
    d_resi_best_contact = select_best_contact_per_account(d_resi)

    t_resi = pd.concat([rg1, rg2, rg3], ignore_index=True).sort_values("AccountNumber", kind="stable")
    resi_merged = merge_target_accounts_with_contacts_resi(t_resi, d_resi_best_contact)
    resi_final = consolidate_accounts_per_customer(resi_merged, cfg, extra_slot_columns=[])
    resi_groups = split_contactable_groups(resi_final)

    t_btl = pd.concat([bg1, bg2, bg3], ignore_index=True).sort_values(["CustomerNumber", "AccountNumber"], kind="stable")
    btl_merged = merge_target_accounts_with_contacts_btl(t_btl, d_btl)
    btl_final = consolidate_accounts_per_customer(btl_merged, cfg, extra_slot_columns=["ArrBalSS", "Group"])
    btl_groups = split_contactable_groups(btl_final)

    logger.info(
        "Completed: Resi G1=%s G2=%s G3=%s NC=%s | BTL G1=%s G2=%s G3=%s NC=%s",
        len(resi_groups["G1"]), len(resi_groups["G2"]), len(resi_groups["G3"]), len(resi_groups["NC"]),
        len(btl_groups["G1"]), len(btl_groups["G2"]), len(btl_groups["G3"]), len(btl_groups["NC"]),
    )

    return {"ResiFinal": resi_final, "BTLFinal": btl_final, "Resi": resi_groups, "BTL": btl_groups}


####################################################################################################
# STEP 9: UPDATING HISTORY TABLES
####################################################################################################
"""
history_table_update.py
Production module: Pre-Arrears Strategy — Step 9: Updating History Tables.

Converted 1:1 from Pre-Arrears.txt lines 1585-1597.

The write-back destination (out.Resi_Hist / out.BTL_Hist) is left
pluggable via injected `writer` callables, since the physical location
of the `out` library is not defined in the supplied excerpt (see risk
#1, and Step 5/3's identical treatment of this library).
"""


if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class HistoryTableUpdateError(Exception):
    """Raised when the history/snapshot update cannot be trusted to proceed."""


@dataclass(frozen=True)
class HistoryTableUpdateConfig:
    rundate: date  # must be the 1st day of a calendar month

    def __post_init__(self):
        if self.rundate.day != 1:
            raise ValueError(f"rundate must be the first day of a month; got {self.rundate}")


def build_snapshot_table_name(prefix: str, rundate: date) -> str:
    """Mirrors the SAS dataset name `out.<prefix>&ss` for documentation/logging."""
    return f"out.{prefix}{get_reporting_period(rundate)}"


HISTORY_SCHEMA = ["AccountNumber", "Segment", "SSContact"]


def append_to_history(existing_history: pd.DataFrame, new_target: pd.DataFrame, name: str) -> pd.DataFrame:
    """
    Equivalent of: data out.Resi_Hist; set out.Resi_Hist R_Target; run;
                   proc sort data=out.Resi_Hist; by AccountNumber descending SSContact; run;
    (and the analogous BTL block). Validates schema match before
    appending -- not an explicit SAS check, but appending mismatched
    columns would silently corrupt the accumulated history (see risk #3).
    """
    for df, label in [(existing_history, f"existing {name}"), (new_target, f"new {name} target")]:
        missing = [c for c in HISTORY_SCHEMA if c not in df.columns]
        if missing:
            raise HistoryTableUpdateError(f"{label} is missing expected column(s): {missing}")

    combined = pd.concat(
        [existing_history[HISTORY_SCHEMA], new_target[HISTORY_SCHEMA]], ignore_index=True
    )
    combined = combined.sort_values(
        by=["AccountNumber", "SSContact"], ascending=[True, False], kind="stable"
    ).reset_index(drop=True)

    logger.info(
        "%s history: %s existing + %s new = %s total row(s)",
        name, len(existing_history), len(new_target), len(combined),
    )
    return combined


def build_monthly_snapshot(group_g1: pd.DataFrame, group_g2: pd.DataFrame, group_g3: pd.DataFrame) -> pd.DataFrame:
    """
    Equivalent of: data out.Resi&ss; set RG1 RG2 RG3; run;
                   proc sort data=out.Resi&ss; by AccountNumber; run;
    (and the analogous BTL block). Consumes Step 8's final, contactable-
    only RG1/RG2/RG3 (or BG1/BG2/BG3) -- see the naming note in the
    explanation above.
    """
    stacked = pd.concat([group_g1, group_g2, group_g3], ignore_index=True)
    return stacked.sort_values("AccountNumber", kind="stable").reset_index(drop=True)


def run_history_table_update(
    cfg: HistoryTableUpdateConfig,
    r_target: pd.DataFrame,
    b_target: pd.DataFrame,
    resi_contactable_groups: "dict[str, pd.DataFrame]",
    btl_contactable_groups: "dict[str, pd.DataFrame]",
    resi_history_reader: Callable[[], pd.DataFrame],
    btl_history_reader: Callable[[], pd.DataFrame],
    resi_history_writer: Callable[[pd.DataFrame], None],
    btl_history_writer: Callable[[pd.DataFrame], None],
    resi_snapshot_writer: Callable[[str, pd.DataFrame], None],
    btl_snapshot_writer: Callable[[str, pd.DataFrame], None],
) -> dict:
    """
    Main entry point. Equivalent to Step 9 end-to-end.

    `resi_contactable_groups`/`btl_contactable_groups` are Step 8's
    final output dicts (keys "G1"/"G2"/"G3"/"NC" -- only G1/G2/G3 are
    used here, matching the SAS source's `set RG1 RG2 RG3;`).

    Reader/writer callables keep the unconfirmed `out` library boundary
    pluggable (see risk #1), consistent with the Step 3/5 pattern.
    """
    logger.info("Starting Updating History Tables")

    try:
        existing_resi_history = resi_history_reader()
    except Exception as exc:  # noqa: BLE001
        raise HistoryTableUpdateError("Failed to read existing Resi history") from exc
    try:
        existing_btl_history = btl_history_reader()
    except Exception as exc:  # noqa: BLE001
        raise HistoryTableUpdateError("Failed to read existing BTL history") from exc

    updated_resi_history = append_to_history(existing_resi_history, r_target, "Resi")
    updated_btl_history = append_to_history(existing_btl_history, b_target, "BTL")

    resi_snapshot = build_monthly_snapshot(
        resi_contactable_groups["G1"], resi_contactable_groups["G2"], resi_contactable_groups["G3"]
    )
    btl_snapshot = build_monthly_snapshot(
        btl_contactable_groups["G1"], btl_contactable_groups["G2"], btl_contactable_groups["G3"]
    )

    resi_history_writer(updated_resi_history)
    btl_history_writer(updated_btl_history)
    resi_snapshot_writer(build_snapshot_table_name("Resi", cfg.rundate), resi_snapshot)
    btl_snapshot_writer(build_snapshot_table_name("BTL", cfg.rundate), btl_snapshot)

    logger.info(
        "Completed: Resi history=%s rows, BTL history=%s rows, Resi snapshot=%s rows, BTL snapshot=%s rows",
        len(updated_resi_history), len(updated_btl_history), len(resi_snapshot), len(btl_snapshot),
    )

    return {
        "Resi_Hist": updated_resi_history, "BTL_Hist": updated_btl_history,
        "Resi_Snapshot": resi_snapshot, "BTL_Snapshot": btl_snapshot,
    }


####################################################################################################
# STEP 10: DATA EXPORT
####################################################################################################
"""
data_export.py
Production module: Pre-Arrears Strategy — Step 10: Data Export.

Converted 1:1 from Pre-Arrears.txt lines 1602-1742.
"""


if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class DataExportError(Exception):
    """Raised when Step 10 export construction cannot be trusted to proceed."""


@dataclass(frozen=True)
class DataExportConfig:
    rundate: date  # must be the 1st day of a calendar month

    # Output directories -- defaults mirror the SAS source's literal paths
    # (folder/file naming structure preserved), but are almost certainly
    # environment-specific and should be overridden for the real target
    # deployment (see risk #3).
    cm_output_dir: str = r"H:\Portfolio Management\Credit Management\Collections Strategies 2019\Pre Arrears\2019 Strategy - Deployment\Outputs CM"
    dialler_output_dir: str = r"H:\Portfolio Management\Credit Management\Collections Strategies 2019\Pre Arrears\2019 Strategy - Deployment\Outputs Dialler"
    mailing_output_dir: str = r"H:\Portfolio Management\Credit Management\Collections Strategies 2019\Pre Arrears\2019 Strategy - Deployment\Mailing"
    monitoring_output_dir: str = r"H:\Portfolio Management\Credit Management\Collections Strategies 2019\Pre Arrears\2019 Strategy - Deployment\Monthly monitoring"
    # NOTE: deliberately a different year folder from every other export
    # in this component -- preserved exactly, see risk #4.
    residual_arrears_output_dir: str = r"H:\Portfolio Management\Credit Management\Collections Strategies 2023\Residual Arrears"

    def __post_init__(self):
        if self.rundate.day != 1:
            raise ValueError(f"rundate must be the first day of a month; got {self.rundate}")


def get_contact_period(rundate: date) -> int:
    """Equivalent of SAS: intnx(month,&rundate,1) formatted yymmn6. -> int YYYYMM (the &cm macro var, NEXT month)."""
    total_months = rundate.year * 12 + (rundate.month - 1) + 1
    year, month0 = divmod(total_months, 12)
    return year * 100 + (month0 + 1)


def get_contact_date(rundate: date) -> date:
    """Equivalent of SAS: intnx(month,&rundate,1) -> date value (the &cd macro var, 1st of NEXT month)."""
    total_months = rundate.year * 12 + (rundate.month - 1) + 1
    year, month0 = divmod(total_months, 12)
    return date(year, month0 + 1, 1)


def export_cm_workbook(sheets: "dict[str, pd.DataFrame]", output_path: Path) -> None:
    """
    Equivalent of the repeated `proc export ... dbms=xlsx replace; sheet="...";`
    block (8 occurrences in the source, 4 per segment) -- consolidated
    into one function writing all sheets of one workbook in a single call,
    eliminating the literal repetition without changing output shape.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    logger.info("Wrote CM workbook %s with sheets: %s", output_path, list(sheets.keys()))


DIALLER_COLUMNS = [
    "IACCNBRO", "Custnbro", "Mretcu", "Firnameu", "Surnameu",
    "TelDay", "TelEve", "TelMob", "Groups", "EXCLUDERECORD", "TimeZone",
]


def build_dialler_export(final_df: pd.DataFrame) -> pd.DataFrame:
    """
    Equivalent of R_Dialler / B_Dialler (lines 1643-1650, 1659-1666).
    Filters to Contactable=1 ONLY -- contrast with build_mailing_export
    and build_monitoring_export below, which deliberately do not filter.
    """
    required = ["Group", "Contactable"] + [c for c in DIALLER_COLUMNS if c not in ("Groups", "EXCLUDERECORD", "TimeZone")]
    missing = [c for c in required if c not in final_df.columns]
    if missing:
        raise DataExportError(f"Dialler export input missing expected column(s): {missing}")

    working = final_df.loc[final_df["Contactable"] == 1].copy()
    working["Groups"] = working["Group"]
    working["EXCLUDERECORD"] = ""
    working["TimeZone"] = "Europe/London"

    working = working[DIALLER_COLUMNS]
    working = working.sort_values(by=["Groups", "IACCNBRO"], kind="stable").reset_index(drop=True)
    return working


def build_mailing_export(final_df: pd.DataFrame) -> pd.DataFrame:
    """
    Equivalent of mailing_resi / mailing_btl (lines 1676-1681).
    Deliberately includes EVERY row of final_df -- no Contactable
    filter (mail doesn't need a phone number). Contrast with
    build_dialler_export above.
    """
    if "AccountNumber" not in final_df.columns:
        raise DataExportError("Mailing export input missing AccountNumber column.")
    return final_df[["AccountNumber"]].copy()


MONITORING_COLUMNS = ["ContactPeriod", "ContactDate", "Strategy", "PrimaryAccount", "Segment", "RiskGroup"]


def build_monitoring_export(final_df: pd.DataFrame, rundate: date) -> pd.DataFrame:
    """
    Equivalent of monitoring_resi / monitoring_btl (lines 1698-1720).
    Deliberately includes EVERY row of final_df -- no Contactable
    filter, same as mailing.
    """
    required = ["AccountNumber", "Lending", "Group"]
    missing = [c for c in required if c not in final_df.columns]
    if missing:
        raise DataExportError(f"Monitoring export input missing expected column(s): {missing}")

    working = final_df[required].copy()
    working["ContactPeriod"] = get_contact_period(rundate)
    working["ContactDate"] = get_contact_date(rundate)
    working["Strategy"] = "Pre Arrears"
    working["PrimaryAccount"] = working["AccountNumber"]
    working["Segment"] = working["Lending"]
    working["RiskGroup"] = working["Group"]

    working = working[MONITORING_COLUMNS]
    working = working.sort_values(by=["RiskGroup", "PrimaryAccount"], kind="stable").reset_index(drop=True)
    return working


def build_combined_monitoring_export(resi_final: pd.DataFrame, btl_final: pd.DataFrame, rundate: date) -> pd.DataFrame:
    """Equivalent of: data monitoring; set monitoring_resi monitoring_btl; run;"""
    resi_monitoring = build_monitoring_export(resi_final, rundate)
    btl_monitoring = build_monitoring_export(btl_final, rundate)
    return pd.concat([resi_monitoring, btl_monitoring], ignore_index=True)


def build_residual_arrears_exclusions(
    ddr_df: pd.DataFrame, resi_final: pd.DataFrame, btl_final: pd.DataFrame
) -> pd.DataFrame:
    """
    Equivalent of `rae` (lines 1734-1736): stacks AccountNumber-only from
    all three sources. Deliberately NOT deduplicated -- no `proc sort
    ... nodup` in the source (see risk #2). Duplicates, if any occur,
    are preserved.
    """
    for df, name in [(ddr_df, "DDR"), (resi_final, "ResiFinal"), (btl_final, "BTLFinal")]:
        if "AccountNumber" not in df.columns:
            raise DataExportError(f"{name} input missing AccountNumber column.")

    return pd.concat(
        [ddr_df[["AccountNumber"]], resi_final[["AccountNumber"]], btl_final[["AccountNumber"]]],
        ignore_index=True,
    )


def run_data_export(
    cfg: DataExportConfig,
    resi_groups: "dict[str, pd.DataFrame]",
    btl_groups: "dict[str, pd.DataFrame]",
    resi_final: pd.DataFrame,
    btl_final: pd.DataFrame,
    ddr_df: pd.DataFrame,
    write_to_disk: bool = True,
) -> dict:
    """
    Main entry point. Equivalent to Step 10 end-to-end. Returns every
    constructed DataFrame; optionally also writes them to the
    configured output directories (set write_to_disk=False to build the
    exports in memory only, e.g. for testing).

    `resi_groups`/`btl_groups` are Step 8's final output dicts (keys
    "G1"/"G2"/"G3"/"NC").
    """
    yyyymm = get_reporting_period(cfg.rundate)
    cm = get_contact_period(cfg.rundate)
    logger.info("Starting Data Export for period %s (contact period %s)", yyyymm, cm)

    resi_cm_sheets = {"RG1": resi_groups["G1"], "RG2": resi_groups["G2"], "RG3": resi_groups["G3"], "RNC": resi_groups["NC"]}
    btl_cm_sheets = {"BG1": btl_groups["G1"], "BG2": btl_groups["G2"], "BG3": btl_groups["G3"], "BNC": btl_groups["NC"]}

    r_dialler = build_dialler_export(resi_final)
    b_dialler = build_dialler_export(btl_final)

    mailing_resi = build_mailing_export(resi_final)
    mailing_btl = build_mailing_export(btl_final)

    monitoring = build_combined_monitoring_export(resi_final, btl_final, cfg.rundate)

    rae = build_residual_arrears_exclusions(ddr_df, resi_final, btl_final)

    outputs = {
        "resi_cm_sheets": resi_cm_sheets, "btl_cm_sheets": btl_cm_sheets,
        "R_Dialler": r_dialler, "B_Dialler": b_dialler,
        "mailing_resi": mailing_resi, "mailing_btl": mailing_btl,
        "monitoring": monitoring, "rae": rae,
    }

    if write_to_disk:
        export_cm_workbook(resi_cm_sheets, Path(cfg.cm_output_dir) / f"Resi{yyyymm}.xlsx")
        export_cm_workbook(btl_cm_sheets, Path(cfg.cm_output_dir) / f"BTL{yyyymm}.xlsx")

        export_cm_workbook(
            {f"PreArrears{yyyymm}.Resi": r_dialler}, Path(cfg.dialler_output_dir) / f"PreArrears{yyyymm}.Resi.xlsx"
        )
        export_cm_workbook(
            {f"PreArrears{yyyymm}.BTL": b_dialler}, Path(cfg.dialler_output_dir) / f"PreArrears{yyyymm}.BTL.xlsx"
        )

        mailing_path = Path(cfg.mailing_output_dir) / "MailingAccounts.xlsx"
        export_cm_workbook({"Resi": mailing_resi, "BTL": mailing_btl}, mailing_path)

        monitoring_path = Path(cfg.monitoring_output_dir) / "Monitoring.xlsx"
        export_cm_workbook({str(cm): monitoring}, monitoring_path)

        rae_path = Path(cfg.residual_arrears_output_dir) / "DDR_PA_Exclusions.xlsx"
        export_cm_workbook({"Sheet1": rae}, rae_path)

    logger.info("Completed Data Export")
    return outputs


def reconcile_workbook_sheets(legacy_sheets: "dict[str, pd.DataFrame]", python_sheets: "dict[str, pd.DataFrame]", key_column: str) -> dict:
    results = {}
    for sheet_name in set(legacy_sheets) | set(python_sheets):
        legacy_df = legacy_sheets.get(sheet_name, pd.DataFrame(columns=[key_column]))
        python_df = python_sheets.get(sheet_name, pd.DataFrame(columns=[key_column]))
        merged = legacy_df.merge(python_df, on=key_column, how="outer", indicator=True)
        results[sheet_name] = {
            "legacy_count": len(legacy_df),
            "python_count": len(python_df),
            "only_in_legacy": merged.loc[merged["_merge"] == "left_only", [key_column]],
            "only_in_python": merged.loc[merged["_merge"] == "right_only", [key_column]],
        }
    return results


################################################################################
# PLUGGABLE EXTERNAL DATA SOURCES -- see "WHAT YOU MUST CHANGE" item 4 above.
#
# These correspond to SAS libraries ("ddr", "out") that are never libname-
# defined in the supplied source excerpt. Wire each of these up to the real
# source/destination before running this script for real.
################################################################################

def read_ddr_target_extract(table_name: str) -> pd.DataFrame:
    """
    # >>> IMPLEMENT: read the DDR strategy target extract (SAS: ddr.ddr<yymm>).
    `table_name` is pre-built for you, e.g. "ddr.ddr2608" -- log it to confirm
    the naming convention matches your real source once wired up.
    Must return a DataFrame with at least columns: PrimaryAccount, Target.
    """
    raise NotImplementedError(
        f"read_ddr_target_extract('{table_name}') is not implemented -- "
        f"see 'WHAT YOU MUST CHANGE' item 4 in this file's header docstring."
    )


def read_resi_contact_history() -> pd.DataFrame:
    """
    # >>> IMPLEMENT: read the Resi contact-history table (SAS: out.Resi_hist).
    Must return a DataFrame with at least columns: AccountNumber, SSContact,
    Segment (Segment is read then dropped, matching the SAS source).
    """
    raise NotImplementedError(
        "read_resi_contact_history() is not implemented -- "
        "see 'WHAT YOU MUST CHANGE' item 4 in this file's header docstring."
    )


def read_btl_contact_history() -> pd.DataFrame:
    """# >>> IMPLEMENT: read the BTL contact-history table (SAS: out.BTL_hist). Same shape as read_resi_contact_history()."""
    raise NotImplementedError(
        "read_btl_contact_history() is not implemented -- "
        "see 'WHAT YOU MUST CHANGE' item 4 in this file's header docstring."
    )


def write_resi_contact_history(df: pd.DataFrame) -> None:
    """# >>> IMPLEMENT: persist the updated Resi contact-history table (SAS: out.Resi_Hist)."""
    raise NotImplementedError(
        "write_resi_contact_history() is not implemented -- "
        "see 'WHAT YOU MUST CHANGE' item 4 in this file's header docstring."
    )


def write_btl_contact_history(df: pd.DataFrame) -> None:
    """# >>> IMPLEMENT: persist the updated BTL contact-history table (SAS: out.BTL_Hist)."""
    raise NotImplementedError(
        "write_btl_contact_history() is not implemented -- "
        "see 'WHAT YOU MUST CHANGE' item 4 in this file's header docstring."
    )


def write_monthly_snapshot(table_name: str, df: pd.DataFrame) -> None:
    """
    # >>> IMPLEMENT: persist a monthly archival snapshot (SAS: out.Resi<yyyymm> / out.BTL<yyyymm>).
    `table_name` is pre-built for you, e.g. "out.Resi202607".
    """
    raise NotImplementedError(
        f"write_monthly_snapshot('{table_name}', ...) is not implemented -- "
        f"see 'WHAT YOU MUST CHANGE' item 4 in this file's header docstring."
    )


################################################################################
# USER CONFIGURATION -- edit the values below before running.
# Every value marked "# >>> EDIT" MUST be reviewed/changed before a real run.
################################################################################

# --- 1. Run date --------------------------------------------------------------
RUNDATE = date(2026, 7, 1)  # >>> EDIT: first day of the PRIOR month

# --- 2. Database connection ----------------------------------------------------
DB_SERVER = "SBSPSQLVS101\\MISPSQL01"   # >>> EDIT if different in your environment
DB_DATABASE = "P_DW"                      # >>> EDIT if different
DB_DRIVER = "ODBC Driver 17 for SQL Server"  # >>> EDIT: confirm installed driver name

# --- 3. Threshold values with NO default in the supplied SAS source -----------
# All quantities below were referenced as undefined macro variables in the SAS
# source (e.g. &lagp, &RG1BS). Real values MUST be sourced from the wider
# production job before this script can run. Leaving any of these as None will
# cause a clear, immediate error rather than a silent wrong answer.
LAGP = None                                   # >>> EDIT: Step 5 contact cooldown cutoff (SAS &lagp)
RESI_G1_BEHAVIOURAL_SCORE_CUTOFF = None       # >>> EDIT: Step 6 (SAS &RG1BS)
RESI_G2_BEHAVIOURAL_SCORE_CUTOFF = None       # >>> EDIT: Step 6 (SAS &RG2BS)
GENERAL_BUREAU_RISK_SCORE_CUTOFF = None       # >>> EDIT: Step 6 (SAS &GBRS)
BTL_G1_BEHAVIOURAL_SCORE_CUTOFF = None        # >>> EDIT: Step 6 (SAS &BG1BS)
BTL_G2_BEHAVIOURAL_SCORE_CUTOFF = None        # >>> EDIT: Step 6 (SAS &BG2BS)
SECONDARY_BUREAU_RISK_SCORE_CUTOFF = None     # >>> EDIT: Step 6 (SAS &SBRS)
BTL_GEO_DELPHI_CUTOFF = None                  # >>> EDIT: Step 6 (SAS &BGD)
RG1_MAX = None                                # >>> EDIT: Resi Group 1 volume cap (SAS &RG1Max)
RG2_MAX = None                                # >>> EDIT: Resi Group 2 volume cap (SAS &RG2Max)
RG3_MAX = None                                # >>> EDIT: Resi Group 3 volume cap (SAS &RG3Max)
BG1_MAX = None                                # >>> EDIT: BTL Group 1 volume cap (SAS &BG1Max)
BG2_MAX = None                                # >>> EDIT: BTL Group 2 volume cap (SAS &BG2Max)
BG3_MAX = None                                # >>> EDIT: BTL Group 3 volume cap (SAS &BG3Max)

# --- 4. Output file paths (Step 10) --------------------------------------------
# Defaults mirror the SAS source's literal Windows UNC paths. Update to wherever
# this script actually has write access.
CM_OUTPUT_DIR = r"H:\Portfolio Management\Credit Management\Collections Strategies 2019\Pre Arrears\2019 Strategy - Deployment\Outputs CM"              # >>> EDIT
DIALLER_OUTPUT_DIR = r"H:\Portfolio Management\Credit Management\Collections Strategies 2019\Pre Arrears\2019 Strategy - Deployment\Outputs Dialler"    # >>> EDIT
MAILING_OUTPUT_DIR = r"H:\Portfolio Management\Credit Management\Collections Strategies 2019\Pre Arrears\2019 Strategy - Deployment\Mailing"            # >>> EDIT
MONITORING_OUTPUT_DIR = r"H:\Portfolio Management\Credit Management\Collections Strategies 2019\Pre Arrears\2019 Strategy - Deployment\Monthly monitoring"  # >>> EDIT
RESIDUAL_ARREARS_OUTPUT_DIR = r"H:\Portfolio Management\Credit Management\Collections Strategies 2023\Residual Arrears"  # >>> EDIT (note: different year folder, preserved from source -- see header item 6)


def _require_thresholds_are_set():
    required = {
        "LAGP": LAGP,
        "RESI_G1_BEHAVIOURAL_SCORE_CUTOFF": RESI_G1_BEHAVIOURAL_SCORE_CUTOFF,
        "RESI_G2_BEHAVIOURAL_SCORE_CUTOFF": RESI_G2_BEHAVIOURAL_SCORE_CUTOFF,
        "GENERAL_BUREAU_RISK_SCORE_CUTOFF": GENERAL_BUREAU_RISK_SCORE_CUTOFF,
        "BTL_G1_BEHAVIOURAL_SCORE_CUTOFF": BTL_G1_BEHAVIOURAL_SCORE_CUTOFF,
        "BTL_G2_BEHAVIOURAL_SCORE_CUTOFF": BTL_G2_BEHAVIOURAL_SCORE_CUTOFF,
        "SECONDARY_BUREAU_RISK_SCORE_CUTOFF": SECONDARY_BUREAU_RISK_SCORE_CUTOFF,
        "BTL_GEO_DELPHI_CUTOFF": BTL_GEO_DELPHI_CUTOFF,
        "RG1_MAX": RG1_MAX, "RG2_MAX": RG2_MAX, "RG3_MAX": RG3_MAX,
        "BG1_MAX": BG1_MAX, "BG2_MAX": BG2_MAX, "BG3_MAX": BG3_MAX,
    }
    unset = [name for name, value in required.items() if value is None]
    if unset:
        raise RuntimeError(
            "The following required threshold(s) are not set in the USER "
            "CONFIGURATION section at the top of this file -- fill in real "
            "values sourced from the production job before running:\n  "
            + "\n  ".join(unset)
        )


################################################################################
# ORCHESTRATION -- runs every component in the same order as the SAS source.
################################################################################

def main():
    _require_thresholds_are_set()
    logger.info("=" * 80)
    logger.info("Starting Pre-Arrears Strategy pipeline for rundate=%s", RUNDATE)
    logger.info("=" * 80)

    # ---- Step 1: Pre-Arrears Qualifiers ---------------------------------------
    step1_cfg = PreArrearsQualifierConfig(
        rundate=RUNDATE, db_server=DB_SERVER, db_database=DB_DATABASE, db_driver=DB_DRIVER,
    )
    ss_df = extract_pre_arrears_qualifiers(step1_cfg)

    # ---- Step 2: Previous Performance (12m) -----------------------------------
    step2_cfg = PreviousPerformanceConfig(
        rundate=RUNDATE, db_server=DB_SERVER, db_database=DB_DATABASE, db_driver=DB_DRIVER,
    )
    prep_df = extract_previous_performance(step2_cfg)

    # ---- Step 3: DDR Strategy Target Import -----------------------------------
    step3_cfg = DDRTargetImportConfig(rundate=RUNDATE)
    ddr_df = load_ddr_target_extract(step3_cfg, reader=read_ddr_target_extract)

    # ---- Step 4: Bureau Riskier Qualifiers ------------------------------------
    step4_cfg = BureauQualifierConfig(
        rundate=RUNDATE, db_server=DB_SERVER, db_database=DB_DATABASE, db_driver=DB_DRIVER,
    )
    br_df = extract_bureau_riskier_qualifiers(step4_cfg)

    # ---- Step 5: Previous Contacts Barred/Requalified -------------------------
    step5_cfg = PreviousContactsConfig(lagp=LAGP)
    barred_df, requal_df = build_barred_and_requalified(
        step5_cfg, resi_reader=read_resi_contact_history, btl_reader=read_btl_contact_history,
    )

    # ---- Step 6: Merge, Exclusions, Cut-offs and Target ------------------------
    step6_cfg = MergeExclusionsCutoffsConfig(
        resi_g1_behavioural_score_cutoff=RESI_G1_BEHAVIOURAL_SCORE_CUTOFF,
        resi_g2_behavioural_score_cutoff=RESI_G2_BEHAVIOURAL_SCORE_CUTOFF,
        general_bureau_risk_score_cutoff=GENERAL_BUREAU_RISK_SCORE_CUTOFF,
        btl_g1_behavioural_score_cutoff=BTL_G1_BEHAVIOURAL_SCORE_CUTOFF,
        btl_g2_behavioural_score_cutoff=BTL_G2_BEHAVIOURAL_SCORE_CUTOFF,
        secondary_bureau_risk_score_cutoff=SECONDARY_BUREAU_RISK_SCORE_CUTOFF,
        btl_geo_delphi_cutoff=BTL_GEO_DELPHI_CUTOFF,
    )
    qual_resi, qual_btl = run_merge_exclusions_cutoffs(
        step6_cfg, ss_df, prep_df, ddr_df, br_df, barred_df, requal_df,
    )

    # ---- Step 7: Portfolio Landlord & Highest Customer Exposure ---------------
    step7_cfg = BtlExposureDedupConfig(
        rundate=RUNDATE, db_server=DB_SERVER, db_database=DB_DATABASE, db_driver=DB_DRIVER,
    )
    qual_btl = extract_btl_exposure_dedup(step7_cfg, qual_btl)  # enriches/filters qual_btl in place

    # ---- Target Ranking and Volume Capping -------------------------------------
    ranking_cfg = TargetRankingConfig(
        rg1_max=RG1_MAX, rg2_max=RG2_MAX, rg3_max=RG3_MAX,
        bg1_max=BG1_MAX, bg2_max=BG2_MAX, bg3_max=BG3_MAX,
    )
    snapshot_period = get_reporting_period(RUNDATE)
    ranking_result = run_target_ranking_and_capping(ranking_cfg, qual_resi, qual_btl, snapshot_period)
    rg1, rg2, rg3 = ranking_result["RG1"], ranking_result["RG2"], ranking_result["RG3"]
    bg1, bg2, bg3 = ranking_result["BG1"], ranking_result["BG2"], ranking_result["BG3"]
    r_target, b_target = ranking_result["R_Target"], ranking_result["B_Target"]

    # ---- Step 8: Dialler Specs ---------------------------------------------------
    step8_cfg = DiallerSpecsConfig(
        rundate=RUNDATE, db_server=DB_SERVER, db_database=DB_DATABASE, db_driver=DB_DRIVER,
    )
    dialler_result = run_dialler_specs(step8_cfg, rg1, rg2, rg3, bg1, bg2, bg3)
    resi_final, btl_final = dialler_result["ResiFinal"], dialler_result["BTLFinal"]
    resi_groups, btl_groups = dialler_result["Resi"], dialler_result["BTL"]

    # ---- Step 9: Updating History Tables -------------------------------------
    step9_cfg = HistoryTableUpdateConfig(rundate=RUNDATE)
    run_history_table_update(
        step9_cfg, r_target, b_target, resi_groups, btl_groups,
        resi_history_reader=read_resi_contact_history, btl_history_reader=read_btl_contact_history,
        resi_history_writer=write_resi_contact_history, btl_history_writer=write_btl_contact_history,
        resi_snapshot_writer=write_monthly_snapshot, btl_snapshot_writer=write_monthly_snapshot,
    )

    # ---- Step 10: Data Export --------------------------------------------------
    step10_cfg = DataExportConfig(
        rundate=RUNDATE,
        cm_output_dir=CM_OUTPUT_DIR, dialler_output_dir=DIALLER_OUTPUT_DIR,
        mailing_output_dir=MAILING_OUTPUT_DIR, monitoring_output_dir=MONITORING_OUTPUT_DIR,
        residual_arrears_output_dir=RESIDUAL_ARREARS_OUTPUT_DIR,
    )
    run_data_export(step10_cfg, resi_groups, btl_groups, resi_final, btl_final, ddr_df, write_to_disk=True)

    logger.info("=" * 80)
    logger.info("Pre-Arrears Strategy pipeline completed successfully for rundate=%s", RUNDATE)
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
