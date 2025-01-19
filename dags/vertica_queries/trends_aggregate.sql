SELECT
    tn.trend_id,
    tn.name AS trend_name,
    COUNT(rq.request_id) AS request_count,
    MIN(datediff(year, ub.birthdate, now())) AS min_age,
    ROUND(AVG(datediff(year, ub.birthdate, now())), 0) AS avg_age,
    MAX(datediff(year, ub.birthdate, now())) AS max_age,
    l.region_id,
    rn.name AS region
FROM
    anchor_model.Trend_Name tn
JOIN
    anchor_model.Contains c ON  tn.trend_id = c.trend_id
JOIN
    anchor_model.Requested rq ON c.request_id = rq.request_id
JOIN
    anchor_model.User_Birthdate ub ON rq.user_id = ub.user_id
JOIN
    anchor_model.User_Gender ug ON rq.user_id = ug.user_id
JOIN
    anchor_model.Lives l ON rq.user_id = l.user_id
JOIN
    anchor_model.Region_Name rn ON l.region_id = rn.region_id
GROUP BY
    tn.name, rn.name, tn.trend_id, l.region_id
ORDER BY tn.trend_id;