SELECT P.type, COUNT(P.type)
FROM Pokemon P
GROUP BY P.type
ORDER BY COUNT(P.type), P.type;