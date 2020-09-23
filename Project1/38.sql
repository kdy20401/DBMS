SELECT P.name
FROM Pokemon P
WHERE P.id IN (
  SELECT E.after_id
  FROM Evolution E
  WHERE E.after_id NOT IN (
    SELECT E.before_id
    FROM Evolution E))
ORDER BY P.name;