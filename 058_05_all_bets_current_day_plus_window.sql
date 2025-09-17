-- Rolling daily report: Aviator bets with odd >= threshold for users whose FTD is within last N days.
DECLARE brand_key STRING DEFAULT 'BrandN1';
DECLARE ftd_lookback_days INT64 DEFAULT 3;
DECLARE window_days INT64 DEFAULT 5;
DECLARE odd_threshold FLOAT64 DEFAULT 1.5;
DECLARE aviator_id INT64 DEFAULT 188000000;

WITH fd AS (
  SELECT
    Brand,
    CAST(ID AS INT64) AS UserId,
    DATE(FirstDepositDate) AS FD_Date,
    FirstDepositAmount AS FTD_DepositLC
  FROM `warehouse.UserLatest`
  WHERE Brand = brand_key
    AND FirstDepositDate IS NOT NULL
    AND DATE(FirstDepositDate) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ftd_lookback_days DAY) AND CURRENT_DATE()
),
bounds AS (
  SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL ftd_lookback_days DAY) AS d_min,
    CURRENT_DATE() AS d_max
),
all_bets_window AS (
  SELECT
    o.Brand,
    CAST(o.EmployeeId AS INT64) AS UserId,
    o.Amount AS StakeLC
  FROM `warehouse.Orders` o
  JOIN fd
    ON fd.Brand = o.Brand AND fd.UserId = CAST(o.EmployeeId AS INT64)
  WHERE o.Brand = brand_key
    AND o.ProdType IN (9, 11)
    AND o.GameId = aviator_id
    AND o.FreeBetBonusId IS NULL
    AND o.Status != 6
    AND o.WinStatusId NOT IN (0, 3, 6)
    AND o.Amount > 0
    AND o.CreateTime >= TIMESTAMP((SELECT d_min FROM bounds))
    AND o.CreateTime < TIMESTAMP(DATE_ADD((SELECT d_max FROM bounds), INTERVAL 1 DAY))
    AND DATE(o.CreateTime) BETWEEN fd.FD_Date AND LEAST(DATE_ADD(fd.FD_Date, INTERVAL window_days DAY), CURRENT_DATE())
),
totals AS (
  SELECT Brand, UserId, SUM(StakeLC) AS TotalStakeLC_Window
  FROM all_bets_window
  GROUP BY Brand, UserId
),
hits AS (
  SELECT
    o.Brand,
    CAST(o.EmployeeId AS INT64) AS UserId,
    fd.FD_Date AS FD_Date,
    fd.FTD_DepositLC AS FTD_DepositLC,
    o.ID AS OrderId,
    DATE(o.CreateTime) AS BetDate,
    o.Amount AS StakeLC,
    o.Payout AS WinLC,
    SAFE_DIVIDE(o.Payout, NULLIF(o.Amount, 0)) AS odd
  FROM `warehouse.Orders` o
  JOIN fd
    ON fd.Brand = o.Brand AND fd.UserId = CAST(o.EmployeeId AS INT64)
  WHERE o.Brand = brand_key
    AND o.ProdType IN (9, 11)
    AND o.GameId = aviator_id
    AND o.FreeBetBonusId IS NULL
    AND o.Status != 6
    AND o.WinStatusId NOT IN (0, 3, 6)
    AND o.Amount > 0
    AND o.Payout > 0
    AND o.CreateTime >= TIMESTAMP((SELECT d_min FROM bounds))
    AND o.CreateTime < TIMESTAMP(DATE_ADD((SELECT d_max FROM bounds), INTERVAL 1 DAY))
    AND DATE(o.CreateTime) BETWEEN fd.FD_Date AND LEAST(DATE_ADD(fd.FD_Date, INTERVAL window_days DAY), CURRENT_DATE())
    AND SAFE_DIVIDE(o.Payout, NULLIF(o.Amount, 0)) >= odd_threshold
)
SELECT
  h.Brand,
  h.UserId,
  h.FD_Date,
  h.FTD_DepositLC,
  h.OrderId,
  h.BetDate,
  h.StakeLC,
  h.WinLC,
  h.odd,
  COALESCE(t.TotalStakeLC_Window, 0) AS TotalStakeLC_Window
FROM hits h
LEFT JOIN totals t
  ON t.Brand = h.Brand AND t.UserId = h.UserId
ORDER BY h.UserId, h.OrderId;
