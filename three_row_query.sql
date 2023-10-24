---query #1 (main)

WITH active(eventdate, dau) AS (
  SELECT eventtime::date,
         COUNT(DISTINCT devtodevid)
    FROM p102968.sessions 
   WHERE eventtime BETWEEN @start_date AND @end_date
  GROUP BY 1  
),
paying(eventdate, pay_users, revenue, new_payment, transactions, avg_check) AS (
  SELECT eventtime::date,
         COUNT(DISTINCT devtodevid),
         SUM(revenueusd),
         SUM(revenueusd) FILTER (WHERE paymentcount = 1),
         COUNT(devtodevid),
         AVG(revenueusd)
    FROM p102968.payments
   WHERE eventtime BETWEEN @start_date AND @end_date
  GROUP BY 1
)

SELECT a.eventdate, 
       dau,
       (pay_users * 100.0 / dau) AS paying_share,
       revenue,
       new_payment,
       (revenue - new_payment) AS repeat_payment,
       transactions,
       avg_check
  FROM 
       active a JOIN paying p
       USING(eventdate)
ORDER BY 1;

---query #2 (ads)

WITH new_list(eventdate, publisher, new_users) AS (
  SELECT created::date,
         COALESCE(publisher, 'Organic'),
         COUNT(devtodevid) AS new
    FROM p102968.users
   WHERE created BETWEEN @start_date AND @end_date
  GROUP BY 1, 2 
),
dau_list(eventdate, publisher, dau) AS (
  SELECT eventtime::date,
         COALESCE(publisher, 'Organic'),
         COUNT(DISTINCT s.devtodevid)
    FROM 
         p102968.sessions s LEFT JOIN p102968.users u
         USING(devtodevid)
   WHERE eventtime BETWEEN @start_date AND @end_date
   GROUP BY 1, 2
),
rev_list(eventdate, publisher, revenue) AS (
  SELECT eventtime::date,
         COALESCE(publisher, 'Organic'),
         SUM(revenueusd)
    FROM 
         p102968.payments p LEFT JOIN p102968.users u
         USING(devtodevid)
   WHERE eventtime BETWEEN @start_date AND @end_date
   GROUP BY 1, 2
)

SELECT eventdate,
       COALESCE(publisher, 'Total') AS publisher,
       new_users,
       arpu
  FROM (
	SELECT nl.eventdate,
               nl.publisher,
               SUM(new_users) AS new_users,
               SUM(revenue) / SUM(dau) AS arpu
          FROM 
               new_list nl JOIN dau_list dl
               USING(eventdate, publisher)
                           JOIN rev_list rl
               USING(eventdate, publisher)
        GROUP BY GROUPING SETS (1, (1, 2))
       ) tab
ORDER BY 1, 3;

---query #3 (country)

WITH income(eventdate, country, revenue) AS (
  SELECT TO_CHAR(eventtime, 'YYYY-MM'),
         country,
         SUM(revenueusd)
    FROM p102968.payments
   WHERE eventtime BETWEEN @start_date AND @end_date
  GROUP BY 1, 2
),
retention(devtodevid) AS (
    SELECT DISTINCT devtodevid 
      FROM p102968.sessions
     WHERE eventtime BETWEEN created + INTERVAL '24 hours' 
                         AND created + INTERVAL '48 hours'
),
common(eventdate, country, new_users, day_retention) AS (
    SELECT TO_CHAR(u.created, 'YYYY-MM'), 
           u.country, 
           COUNT(u.devtodevid),
           COUNT(r.devtodevid) * 100.0 / COUNT(u.devtodevid)
      FROM 
           p102968.users u LEFT JOIN retention r
           USING(devtodevid)
     WHERE u.created BETWEEN @start_date AND @end_date
    GROUP BY 1, 2
)

SELECT c.eventdate, 
       name AS country, 
       new_users, 
       day_retention, 
       revenue,
       (revenue / SUM(revenue) OVER (PARTITION BY c.eventdate) * 100) AS revenue_share
  FROM 
       common c JOIN income i
       USING(eventdate, country)
                LEFT JOIN public.countries pc
       ON c.country = pc.code
ORDER BY 1, 3 DESC;
