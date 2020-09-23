SELECT P.type
FROM Pokemon P, Evolution E
WHERE P.id = E.before_id
GROUP BY P.type
HAVING COUNT(*) >= 3
ORDER BY P.type DESC;