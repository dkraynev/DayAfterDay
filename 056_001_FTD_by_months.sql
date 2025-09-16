-- time window
WITH params AS (
  SELECT
    DATE_TRUNC(CURRENT_DATE(), YEAR) AS start_date,
    DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) AS end_date_excl
),

-- Registration channels
registration_channels AS (
  SELECT Id, Brand
  FROM `warehouse.Channels`
),

-- Brands
brands AS (
  SELECT Brand
  FROM `warehouse.BrandsView`
  WHERE GroupName = 'BRANDNAME'
    AND Type = 'BRANDNAME_B2C'
),

-- 1) FTD cohort
ftd_users AS (
  SELECT
    u.Brand,
    u.ID AS UserId,
    DATE(u.FirstDepositDate) AS ftd_date,
    DATE_TRUNC(DATE(u.FirstDepositDate), MONTH) AS ftd_month,
    u.UserName
  FROM `warehouse.UserLatest` AS u
  JOIN brands b
    ON u.Brand = b.Brand
  LEFT JOIN registration_channels rc
    ON rc.Id = u.RegistrationChannel AND rc.Brand = u.Brand
  LEFT JOIN `warehouse.UserPrePaid` pp
    ON pp.UserId = u.ID AND pp.Brand = u.Brand
  CROSS JOIN params p
  WHERE
    u.UserType = 2
    AND u.FirstDepositDate IS NOT NULL
    AND DATE(u.FirstDepositDate) >= p.start_date
    AND DATE(u.FirstDepositDate) <  p.end_date_excl
    AND (rc.Id != 9 OR rc.Id IS NULL)
    AND (pp.Id IS NULL OR CAST(pp.IsConverted AS BOOL))
),

-- 2) Post-FTD activity
played AS (
  SELECT
    d.Brand,
    d.UserId,
    SUM(IFNULL(d.betCnt, 0)) AS bet_cnt,
    SUM(IFNULL(d.BetAmountUSD, 0)) AS bet_amt_usd
  FROM `warehouse.dailystats` AS d
  JOIN ftd_users f
    ON f.Brand = d.Brand
   AND f.UserId = d.UserId
  CROSS JOIN params p
  WHERE
    DATE(d.statshour) >= f.ftd_date
    AND DATE(d.statshour) <  p.end_date_excl
    AND (d.ChannelId != 9 OR d.ChannelId IS NULL)
  GROUP BY 1, 2
),

-- 3) cohort 'no activity'
cohort AS (
  SELECT
    f.Brand,
    f.UserId,
    f.ftd_month,
    (IFNULL(p.bet_cnt, 0) > 0 OR IFNULL(p.bet_amt_usd, 0) > 0) AS has_bet
  FROM ftd_users f
  LEFT JOIN played p
    ON p.Brand = f.Brand AND p.UserId = f.UserId
)

-- 4) Monthly, per-brand
SELECT
  FORMAT_DATE('%Y-%m', ftd_month) AS month,
  Brand,
  COUNT(DISTINCT UserId) AS ftd_users,
  COUNT(DISTINCT IF(has_bet = FALSE, UserId, NULL)) AS no_bet_users,
  ROUND(
    SAFE_DIVIDE(
      COUNT(DISTINCT IF(has_bet = FALSE, UserId, NULL)),
      COUNT(DISTINCT UserId)
    ) * 100
  , 2) AS no_bet_pct
FROM cohort
GROUP BY month, Brand
ORDER BY month ASC, Brand ASC;
