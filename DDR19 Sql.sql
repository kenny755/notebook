/* =====================================================================
   DDR19 post Geese - Direct Debit rejections strategy (T-SQL port of the SAS script)
   Run in SSMS against SBSPSQLVS101\MISPSQL01, database P_DW.
   Run the WHOLE file in one go (no GO statements: variables and #temp tables must survive).
   ===================================================================== */
SET NOCOUNT ON;
DECLARE @MonthRun    DATETIME     = DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0);  -- SAS monthrun = first day of current month. To re-run another month type it as 'YYYYMMDD', e.g. '20250801'
DECLARE @RunDate     DATETIME     = GETDATE();   -- today; only used to exclude accounts currently in the Debt Respite Scheme
DECLARE @YYYYMM      INT          = CONVERT(INT, CONVERT(VARCHAR(6), DATEADD(MONTH,-1,@MonthRun),112));  -- SAS &yyyymm (month-end data / Experian period)
DECLARE @Cutoff1     INT          = 632;         -- SAS %let cutoff1
DECLARE @Cutoff2     INT          = 670;         -- SAS %let cutoff2
DECLARE @GroupNoMIA  VARCHAR(20)  = '0MIA';      -- zero + MIA


/*** Step 1: DD rejections for the 1st raise (current vs previous month, minus Debt Respite Scheme) ***/
IF OBJECT_ID('tempdb..#dd') IS NOT NULL DROP TABLE #dd;

WITH CTE_RTPCurrMonth AS
(
SELECT  d99.accnbr_o AS AccountNumber
        , CONVERT(VARCHAR(4),@MonthRun,12) AS 'DDRej'
        , d06.rej_rsn_u AS 'Rejection_Reason'
        , CASE WHEN d06.rej_rsn_u LIKE ('%Refer To Pay%') THEN 1 ELSE 0 END AS 'RTP_Flag'

FROM    P_ODS.SKIPCORE.DDS99 d99

INNER JOIN  P_ODS.SKIPCORE.DDS06 d06
ON  d99.rej_cde_o = d06.rej_cde_u
AND d06.rej_cnt_u=0
AND d06.dc_cease_o_dt IS NULL

WHERE   d99.pool_id_o='SBS00001'
AND     d99._deleted=0
AND     d99.rej_cde_o>0
AND     d99.dc_reqst_o_dt=@MonthRun /*First day of current month*/
),

CTE_RTPPrevMonth AS
(
SELECT  d99.accnbr_o AS AccountNumber
        , 1 AS 'Flag'

FROM    P_ODS.SKIPCORE.DDS99 d99

INNER JOIN  P_ODS.SKIPCORE.DDS06 d06
ON  d99.rej_cde_o = d06.rej_cde_u
AND d06.rej_cnt_u=0
AND d06.dc_cease_o_dt IS NULL

WHERE   d99.pool_id_o='SBS00001'
AND     d99._deleted=0
AND     d99.rej_cde_o>0
AND     d99.dc_reqst_o_dt=DATEADD(MONTH,-1,@MonthRun) /*First day of previous month*/
),

CTE_DRS AS
(
SELECT  m99.primacno_o AS 'PrimaryAccount'
        , m98.accnbr_o AS 'AccountNumber'
        , da.AccountOrigin
        , CONVERT(DATE,drs._timestamp) AS 'DRS_DateRecorded'
        , CONVERT(DATE,drs.startDate) AS 'DRS_StartDate'
        , CONVERT(DATE,drs.endDate) AS 'DRS_EndDate'
        , drs.arrearsAmount AS 'DRS_Arrears'

FROM    P_ODS.SKIPCORE.DatDebtRespiteSchemeDetails drs

INNER JOIN  P_ODS.SKIPCORE.MOR98 m98
ON  drs.myAccount = m98.oid
AND m98.pool_id_o='SBS00001'
INNER JOIN  P_ODS.SKIPCORE.MOR99 m99
ON  m98.myMOR99 = m99.oid
AND m98.pool_id_o = m99.pool_id_o
AND m98.primacno_o = m99.primacno_o
INNER JOIN  P_DW.dbo.dimAccount da
ON  m98.oid = da.BK_Account
AND m98.pool_id_o = da.AccountPool
AND m98.accnbr_o = da.AccountNumber
AND da.AccountType=10

WHERE   CONVERT(DATE,drs.endDate)>=CONVERT(DATE,@RunDate) /*@RunDate = today: the only place the clock is used*/
AND     drs.ceased=0
)

SELECT  a.*
        , CASE WHEN b.Flag=1 THEN 1 ELSE 0 END AS 'RLM_Flag'
        , CASE WHEN a.RTP_Flag=1 AND ISNULL(b.Flag,0)=0 THEN 1 ELSE 0 END AS 'RTP_NoRLM'
INTO    #dd
FROM    CTE_RTPCurrMonth a
LEFT JOIN   CTE_RTPPrevMonth b
ON  a.AccountNumber = b.AccountNumber

WHERE   a.AccountNumber NOT IN (SELECT AccountNumber FROM CTE_DRS) /*Removes accounts currently under the Debt Respite Scheme*/

ORDER BY a.AccountNumber;


/*** Step 2: Accounts with a current or future payment holiday (PH) ***/
IF OBJECT_ID('tempdb..#ph') IS NOT NULL DROP TABLE #ph;

SELECT  a.AccountNumber
        , a.PH
        , a.PHStartDate
        , a.PHEndDate
INTO    #ph
FROM    (
        SELECT  a96.accnbr_o AS 'AccountNumber'
                , 1 AS 'PH'
                , CONVERT(DATE,a96.dc_start_o_dt) AS 'PHStartDate'
                , CONVERT(DATE,a96.dc_end_o_dt) AS 'PHEndDate'
                , RANK() OVER(PARTITION BY a96.accnbr_o ORDER BY a96.dc_start_o_dt ASC, a96.oid ASC) AS 'PHOrder'

        FROM    P_ODS.SKIPCORE.ARR96 a96

        INNER JOIN  P_DW.dbo.dimAccount da
        ON  a96.accnbr_o = da.AccountNumber
        AND a96.pool_id_o = da.AccountPool
        AND da.CurrentAccountStatus='Live'
        AND da.AccountSubType='Primary'
        AND da.CurrentAccountClassification IN ('Residential','BTL Commercial','Pure Commercial')
        INNER JOIN  P_ODS.SKIPCORE.MOR98 m98
        ON  da.AccountPool = m98.pool_id_o
        AND da.BK_Account = m98.oid

        WHERE   ((da.AccountOrigin NOT IN ('Amber Homeloans Ltd','North Yorkshire Mortgages Ltd') AND a96.dc_cease_o_dt IS NULL)
                OR  da.AccountOrigin IN ('Amber Homeloans Ltd','North Yorkshire Mortgages Ltd'))
        AND     a96.arr_type_o=33
        AND     a96.dc_start_o_dt<=DATEADD(DAY,-1,DATEADD(MONTH,1,@MonthRun)) /*Last day of current month*/
        AND     a96.dc_end_o_dt>=@MonthRun /*First day of current month*/
        )a

WHERE   a.PHOrder=1
ORDER BY a.AccountNumber;


/*** Step 3: Month-end data, flags, behavioural score, red-status letters ***/
IF OBJECT_ID('tempdb..#me') IS NOT NULL DROP TABLE #me;

SELECT  dpa.AccountNumber AS 'PrimaryAccount'
        , da.AccountNumber
        , CASE  WHEN da.AccountOrigin='Amber Homeloans Ltd' THEN 'AHL' WHEN da.AccountOrigin='North Yorkshire Mortgages Ltd' THEN 'NYM' ELSE 'SBS' END AS 'Lender'
        , CASE  WHEN da.CurrentAccountClassification='Residential' THEN 'Resi' WHEN da.CurrentAccountClassification='BTL Commercial' THEN 'BTLC' ELSE 'Comm' END AS 'AccType'
        , CASE  WHEN dsp.SubPopulationCode='BTL' THEN 'BTL' ELSE 'Resi' END AS 'AccClass'
        , das.AccountStatus AS 'StatusMonthEnd'
        , fma.CapitalBalanceLive AS 'CombBal'
        , CASE  WHEN da.AccountStartDate IS NULL THEN '' ELSE CONVERT(DATE,da.AccountStartDate) END AS 'CompDate'
        , CASE  WHEN das.AccountStatus IN ('Redeemed','Redeemed With Balance','Possession') OR fma.CapitalBalanceLive<=0 THEN 1 ELSE 0 END AS 'RedLastM_Flag'
        , CASE  WHEN da.AccountEndDate>=dm.ReportingMonth THEN 1 ELSE 0 END AS 'RedCurrentM_Flag'
        , CASE  WHEN da.AccountEndDate IS NULL THEN '' ELSE CONVERT(DATE,da.AccountEndDate) END AS 'RedDate'
        , CASE  WHEN da.AccountSubType='Primary' THEN 1 ELSE 0 END AS 'PrimAcc_Flag'
        , CASE  WHEN dp.ProductCode IN ('MIN01','HOM10','HM120','HM121','HM122','HM123','HM124','HM125') THEN 1 ELSE 0 END AS 'Deed_Flag'
        , CASE  WHEN da.CurrentAccountClassification='Pure Commercial' THEN 1 ELSE 0 END AS 'Comm_Flag'
        , CASE  WHEN (DATEDIFF(MONTH,da.AccountStartDate,DATEADD(MONTH,0,DATEADD(MONTH,DATEDIFF(MONTH,0,dm.BatchDate),0)))<=2)
                OR (da.AccountStartDate IS NULL AND fma.CapitalBalanceLive>0) THEN 1 ELSE 0 END AS 'RecentComp_Flag'
        , CASE  WHEN DATEDIFF(MONTH,da.AccountStartDate,@MonthRun)<=1 THEN 1 ELSE 0 END AS 'CompLM_Flag'
        , CASE  WHEN da.AccountStartDate IS NULL OR fma.ContractualArrearsAmount<=0 THEN 0 ELSE fma.ContractualArrearsAmount END AS 'Arrears'
        , CASE  WHEN da.AccountStartDate IS NULL OR fma.ContractualArrearsInMonths<=0 THEN 0 ELSE ROUND(fma.ContractualArrearsInMonths,2) END AS 'MIA'
        , CASE  WHEN da.AccountStartDate IS NOT NULL AND fma.ContractualArrearsInMonths>=1 THEN 1 ELSE 0 END AS 'MIA1P_Flag'
        , CASE  WHEN rs.LetterCode IS NULL THEN '' ELSE rs.LetterCode END AS 'RSLetter'
        , CASE  WHEN rs.RSDate IS NULL THEN '1900-01-01' ELSE rs.RSDate END AS 'RSLetterSent'
        , CASE  WHEN rs.RSPeriod IS NULL THEN 0 ELSE 1 END AS 'RedStat_Flag'
        , CASE  WHEN bs.S3Score IS NULL THEN 999 ELSE FLOOR(bs.S3Score) END AS 'BS'
INTO    #me
FROM    P_DW.dbo.factMortgageAccountMonthSS fma

INNER JOIN  P_DW.dbo.dimAccount da
ON  fma.FK_Account = da.PK_Account
INNER JOIN  P_DW.dbo.dimAccount dpa
ON  fma.FK_PrimaryAccount = dpa.PK_Account
INNER JOIN  P_DW.dbo.dimMonth dm
ON  fma.FK_SnapshotDate = dm.PK_Month
INNER JOIN  P_DW.dbo.dimAccountStates das
ON  fma.FK_AccountStates = das.PK_AccountStates
INNER JOIN  P_DW.dbo.dimSubPopulation dsp
ON  fma.FK_SubPopulation = dsp.PK_SubPopulation
INNER JOIN  P_DW.dbo.dimProduct dp
ON  fma.FK_CurrentMortgageProduct = dp.PK_Product
LEFT JOIN   (
            SELECT  a.AccountNumber, a.RSPeriod, a.LetterCode, a.RSDate
            FROM    (
                    SELECT  a98.accnbr_o AS 'AccountNumber', YEAR(a98.dc_creat_o_dt)*100+MONTH(a98.dc_creat_o_dt) AS 'RSPeriod'
                            , a98.lettcode_o AS 'LetterCode', CONVERT(DATE,a98.dc_creat_o_dt) AS 'RSDate', a98.time_o AS 'LetterTime', a98.oid
                            , RANK() OVER(PARTITION BY a98.accnbr_o ORDER BY a98.dc_creat_o_dt DESC, a98.time_o DESC, a98.oid DESC) AS 'RSLatest'
                    FROM    P_ODS.SKIPCORE.ADM98 a98
                    WHERE   a98.pool_id_o='SBS00001'
                    AND     a98.lettcode_o IN ('RED001','RED01E','RED01F')
                    AND     CONVERT(VARCHAR(6),a98.dc_creat_o_dt,112)=CONVERT(VARCHAR,@YYYYMM)
                    )a
            WHERE   a.RSLatest=1) rs
ON  da.AccountNumber = rs.AccountNumber

LEFT JOIN   P_Modelling.COREMODEL.vwBehaviouralScorecard bs
ON  dm.CalendarMonthYear = CONVERT(VARCHAR,bs.Period)
AND da.AccountNumber = bs.PrimaryAccount

WHERE   dm.CalendarMonthYear=CONVERT(VARCHAR,@YYYYMM)
AND     da.AccountPool='SBS00001'
AND     das.AccountStatus IN ('Live','Possession','Redeemed','Redeemed With Balance')

ORDER BY da.AccountNumber;


/*** Step 4: Merge on AccountNumber + de-dup to one row per PrimaryAccount ***/
IF OBJECT_ID('tempdb..#dd_merged') IS NOT NULL DROP TABLE #dd_merged;

/* SAS: merge dd(in=a) me(in=b) ph(in=c); by AccountNumber; if a;  ==> LEFT JOINs from #dd */
/* SAS: proc sort by PrimaryAccount descending RTP_Flag descending PrimAcc_Flag; then if first.PrimaryAccount;
   SAS sort is stable and the merge output was ordered by AccountNumber, so AccountNumber ASC is the tie-break. */
SELECT  AccountNumber,
       DDRej,
       Rejection_Reason,
       RTP_Flag,
       RLM_Flag,
       RTP_NoRLM,
       PrimaryAccount,
       Lender,
       AccType,
       AccClass,
       StatusMonthEnd,
       CombBal,
       CompDate,
       RedLastM_Flag,
       RedCurrentM_Flag,
       RedDate,
       PrimAcc_Flag,
       Deed_Flag,
       Comm_Flag,
       RecentComp_Flag,
       CompLM_Flag,
       Arrears,
       MIA,
       MIA1P_Flag,
       RSLetter,
       RSLetterSent,
       RedStat_Flag,
       BS,
       PH,
       PHStartDate,
       PHEndDate
INTO    #dd_merged
FROM    (
        SELECT  dd.AccountNumber,
        dd.DDRej,
        dd.Rejection_Reason,
        dd.RTP_Flag,
        dd.RLM_Flag,
        dd.RTP_NoRLM,
        me.PrimaryAccount,
        me.Lender,
        me.AccType,
        me.AccClass,
        me.StatusMonthEnd,
        me.CombBal,
        me.CompDate,
        me.RedLastM_Flag,
        me.RedCurrentM_Flag,
        me.RedDate,
        me.PrimAcc_Flag,
        me.Deed_Flag,
        me.Comm_Flag,
        me.RecentComp_Flag,
        me.CompLM_Flag,
        me.Arrears,
        me.MIA,
        me.MIA1P_Flag,
        me.RSLetter,
        me.RSLetterSent,
        me.RedStat_Flag,
        me.BS,
        ph.PH,
        ph.PHStartDate,
        ph.PHEndDate,
        ROW_NUMBER() OVER (PARTITION BY me.PrimaryAccount
                           ORDER BY dd.RTP_Flag DESC, me.PrimAcc_Flag DESC, dd.AccountNumber ASC) AS rn
        FROM    #dd dd
        LEFT JOIN   #me me ON me.AccountNumber = dd.AccountNumber
        LEFT JOIN   #ph ph ON ph.AccountNumber = dd.AccountNumber
        ) x
WHERE   x.rn = 1;


/*** Step 5: Experian data (customer 1 and 2) -> CLUhe90pct, PDLRecent ***/
IF OBJECT_ID('tempdb..#ah1') IS NOT NULL DROP TABLE #ah1;
IF OBJECT_ID('tempdb..#ah2') IS NOT NULL DROP TABLE #ah2;
IF OBJECT_ID('tempdb..#exp') IS NOT NULL DROP TABLE #exp;

/* Customer 1 of the primary account */
SELECT  ex.*
INTO    #ah1
FROM    (SELECT b.Period
                , b.PrimaryAccount
                , CASE  WHEN b.SP_F3_36<=0 THEN 0 WHEN b.SP_F3_36>=9000 THEN 0 ELSE CONVERT(FLOAT,b.SP_F3_36)/100  END AS 'CLU_AH1'
                , CASE  WHEN b.PDL_A_05 BETWEEN 1 AND 12 THEN 1 ELSE 0 END AS 'PDL_AH1'
        FROM    (
                SELECT  a.*, RANK() OVER(PARTITION BY a.Period, a.LenderCode, a.PrimaryAccount ORDER BY a.CustomerID ASC) AS 'CustRank'
                FROM    P_ODS.EXPERIAN.DCMGen10 a
                WHERE   a.Period=@YYYYMM
                AND     a.LenderCode='SBS'
                )b
        WHERE b.CustRank=1
        )ex
ORDER BY    ex.PrimaryAccount;

/* Customer 2 of the primary account */
SELECT  ex.*
INTO    #ah2
FROM    (SELECT b.Period
                , b.PrimaryAccount
                , CASE  WHEN b.SP_F3_36<=0 THEN 0 WHEN b.SP_F3_36>=9000 THEN 0 ELSE CONVERT(FLOAT,b.SP_F3_36)/100  END AS 'CLU_AH2'
                , CASE  WHEN b.PDL_A_05 BETWEEN 1 AND 12 THEN 1 ELSE 0 END AS 'PDL_AH2'
        FROM    (
                SELECT  a.*, RANK() OVER(PARTITION BY a.Period, a.LenderCode, a.PrimaryAccount ORDER BY a.CustomerID ASC) AS 'CustRank'
                FROM    P_ODS.EXPERIAN.DCMGen10 a
                WHERE   a.Period=@YYYYMM
                AND     a.LenderCode='SBS'
                )b
        WHERE b.CustRank=2
        )ex
ORDER BY    ex.PrimaryAccount;

/* SAS: merge ah1(in=a) ah2(in=b); if a; missing CLU_AH2/PDL_AH2 -> 0;
        MaxCLU=max(of CLU_AH1-CLU_AH2) (SAS max ignores missing);  CLUhe90pct = MaxCLU>=0.9;  PDLRecent = PDL_AH1=1 or PDL_AH2=1 */
SELECT  a.PrimaryAccount
        , CASE WHEN m.MaxCLU>=0.9 THEN 1 ELSE 0 END AS CLUhe90pct
        , CASE WHEN a.PDL_AH1=1 OR ISNULL(b.PDL_AH2,0)=1 THEN 1 ELSE 0 END AS PDLRecent
INTO    #exp
FROM    #ah1 a
LEFT JOIN   #ah2 b ON b.PrimaryAccount = a.PrimaryAccount
CROSS APPLY (SELECT CASE WHEN a.CLU_AH1 IS NULL THEN ISNULL(b.CLU_AH2,0)
                         WHEN a.CLU_AH1 >= ISNULL(b.CLU_AH2,0) THEN a.CLU_AH1
                         ELSE ISNULL(b.CLU_AH2,0) END AS MaxCLU) m;


/*** Step 6-7: Second merge on PrimaryAccount, Groups, Target, CompLM, remove PH accounts ***/
IF OBJECT_ID('tempdb..#ddr') IS NOT NULL DROP TABLE #ddr;

/* Step 6 (SAS second merge on PrimaryAccount) = LEFT JOIN #exp, missing -> 0.
   Step 7 (groups + Target): each flag only fires if the earlier ones are 0, exactly as the SAS if/else order.
   SAS treats a missing BS as the lowest number (so BS<=cutoff is TRUE); ISNULL(BS,-1) reproduces that. */
SELECT  d.AccountNumber
        , d.DDRej
        , d.Rejection_Reason
        , d.RTP_Flag
        , d.RLM_Flag
        , d.RTP_NoRLM
        , d.PrimaryAccount
        , d.Lender
        , d.AccType
        , d.AccClass
        , d.StatusMonthEnd
        , d.CombBal
        , d.CompDate
        , d.RedLastM_Flag
        , d.RedCurrentM_Flag
        , d.RedDate
        , d.PrimAcc_Flag
        , d.Deed_Flag
        , d.Comm_Flag
        , d.RecentComp_Flag
        , d.CompLM_Flag
        , d.Arrears
        , d.MIA
        , d.MIA1P_Flag
        , d.RSLetter
        , d.RSLetterSent
        , d.RedStat_Flag
        , d.BS
        , ISNULL(e.CLUhe90pct,0) AS CLUhe90pct
        , ISNULL(e.PDLRecent,0) AS PDLRecent
        , g.[Group] AS [Group]
        , CASE WHEN d.RTP_NoRLM=1 AND g.[Group]=@GroupNoMIA AND ISNULL(d.BS,-1)<=@Cutoff1 THEN 1
             WHEN d.RTP_NoRLM=1 AND g.[Group]=@GroupNoMIA AND ISNULL(e.PDLRecent,0)=1 THEN 1
             WHEN d.RTP_NoRLM=1 AND g.[Group]=@GroupNoMIA AND ISNULL(d.BS,-1)<=@Cutoff2 AND ISNULL(e.CLUhe90pct,0)=1 THEN 1
             ELSE 0 END AS Target
        , CASE WHEN d.RTP_Flag=1 AND d.CompLM_Flag=1 THEN 1 ELSE 0 END AS CompLM
INTO    #ddr
FROM    #dd_merged d
LEFT JOIN   #exp e ON e.PrimaryAccount = d.PrimaryAccount
CROSS APPLY (SELECT CASE WHEN d.Comm_Flag=1 THEN 1 ELSE 0 END AS PC) s1
CROSS APPLY (SELECT CASE WHEN s1.PC=0 AND (d.RedLastM_Flag=1 OR d.RedCurrentM_Flag=1 OR d.Deed_Flag=1) THEN 1 ELSE 0 END AS RD) s2
CROSS APPLY (SELECT CASE WHEN s1.PC=0 AND s2.RD=0 AND d.MIA1P_Flag=1 THEN 1 ELSE 0 END AS ARR) s3
CROSS APPLY (SELECT CASE WHEN s1.PC=0 AND s2.RD=0 AND s3.ARR=0 AND d.RecentComp_Flag=1 THEN 1 ELSE 0 END AS RC) s4
CROSS APPLY (SELECT CASE WHEN s1.PC=0 AND s2.RD=0 AND s3.ARR=0 AND s4.RC=0 AND d.RedStat_Flag=1 THEN 1 ELSE 0 END AS RSR) s5
CROSS APPLY (SELECT CASE WHEN s1.PC=0 AND s2.RD=0 AND s3.ARR=0 AND s4.RC=0 AND s5.RSR=0 THEN 1 ELSE 0 END AS NoMIA) s6
CROSS APPLY (SELECT CASE WHEN s1.PC=1 THEN 'Commercial'
                         WHEN s2.RD=1 THEN 'Red/Rep/Deed'
                         WHEN s3.ARR=1 THEN '1+MIA'
                         WHEN s4.RC=1 THEN 'RecentComp'
                         WHEN s5.RSR=1 THEN 'RSRequested'
                         WHEN s6.NoMIA=1 THEN @GroupNoMIA
                         ELSE 'Unknown' END AS [Group]) g
WHERE   ISNULL(d.PH,0)<>1   /* SAS: if PH=1 then delete */
ORDER BY d.PrimaryAccount;


/*** Step 8: Extracts (one per Excel sheet) ***/

-- Sheet: MIA0
SELECT AccountNumber, DDRej, Rejection_Reason, PrimaryAccount, Lender, AccType, AccClass, Arrears, BS, CLUhe90pct, PDLRecent, [Group], Target
FROM #ddr
WHERE Target=1
ORDER BY PrimaryAccount;

-- Sheet: MIA1Plus
SELECT AccountNumber, DDRej, Rejection_Reason, PrimaryAccount, Lender, AccType, AccClass, Arrears, MIA, RSLetter, RSLetterSent, BS, CLUhe90pct, PDLRecent, [Group]
FROM #ddr
WHERE [Group]='1+MIA' AND RTP_NoRLM=1
ORDER BY PrimaryAccount;

-- Sheet: CompLM
SELECT AccountNumber, DDRej, Rejection_Reason, PrimaryAccount, Lender, AccType, CompDate, Arrears, MIA, [Group]
FROM #ddr
WHERE CompLM=1
ORDER BY PrimaryAccount;

-- Sheet: Commercial
SELECT AccountNumber, DDRej, Rejection_Reason, PrimaryAccount, Lender, Arrears, MIA, BS, CLUhe90pct, PDLRecent, [Group]
FROM #ddr
WHERE Comm_Flag=1
ORDER BY PrimaryAccount;
