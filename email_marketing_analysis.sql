--Calculate account metrics
WITH account_data AS (


SELECT
s.date,
sp.country,
acc.send_interval,
acc.is_verified,
acc.is_unsubscribed,
COUNT (acc.id) AS account_cnt


FROM `DA.session_params` sp
JOIN `DA.session` s
ON sp.ga_session_id = s.ga_session_id
JOIN `DA.account_session` acs
ON s.ga_session_id = acs.ga_session_id
JOIN `DA.account` acc
ON acs.account_id = acc.id


GROUP BY 1,2,3,4,5),




--Calculate email metrics
email_data AS (


SELECT
DATE_ADD (date, INTERVAL es.sent_date DAY) AS sent_date,
sp.country,
acc.send_interval,
acc.is_verified,
acc.is_unsubscribed,
COUNT (es.id_message) AS sent_msg,
COUNT (eo.id_message) AS open_msg,
COUNT (ev.id_message) AS visit_msg


FROM `DA.session_params` sp
JOIN `DA.session` s
ON sp.ga_session_id = s.ga_session_id
JOIN `DA.account_session` acs
ON s.ga_session_id = acs.ga_session_id
JOIN `DA.account` acc
ON acs.account_id = acc.id
JOIN `DA.email_sent` es
ON acc.id = es.id_account
LEFT JOIN `DA.email_open` eo
ON es.id_message = eo.id_message
LEFT JOIN `DA.email_visit` ev
ON es.id_message = ev.id_message


GROUP BY 1,2,3,4,5),




--Combine account and email metrics using UNION ALL
union_data AS (


SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
account_cnt,
0 AS sent_msg,
0 AS open_msg,
0 AS visit_msg
FROM account_data


UNION ALL


SELECT
sent_date,
country,
send_interval,
is_verified,
is_unsubscribed,
0 AS account_cnt,
sent_msg,
open_msg,
visit_msg
FROM email_data),




--Aggregate account and email metrics using SUM
total_data AS (


SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
SUM (account_cnt) AS account_cnt,
SUM (sent_msg) AS sent_msg,
SUM (open_msg) AS open_msg,
SUM (visit_msg) AS visit_msg


FROM union_data


GROUP BY 1,2,3,4,5),




--Calculate total accounts and total sent emails by country using a window function
total_data1 AS (


SELECT *,
SUM (account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
SUM (sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt


FROM total_data),




--Display country rankings by total subscribers and total sent emails
--Rank in descending order by message and account count!!!
total_data2 AS (


SELECT *,
DENSE_RANK () OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
DENSE_RANK () OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt


FROM total_data1)




--Display the full dataset with filtering
SELECT *
FROM total_data2
WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10
ORDER BY country

