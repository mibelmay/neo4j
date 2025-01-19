SELECT
    rt.text AS request_text,
    count(*) AS request_count,
    u.gender_id,
    l.region_id,
    c.trend_id
FROM anchor_model.Contains c
JOIN anchor_model.Request_Text rt ON c.request_id = rt.request_id
JOIN anchor_model.Requested r ON c.request_id = r.request_id
JOIN anchor_model.User_Gender u ON r.user_id = u.user_id
JOIN anchor_model.Lives l ON u.user_id = l.user_id
GROUP BY rt.text, u.gender_id, l.region_id, c.trend_id
ORDER BY gender_id, l.region_id;