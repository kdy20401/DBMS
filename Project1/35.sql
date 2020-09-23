SELECT P.name
FROM Pokemon P, Evolution E
WHERE P.id = E.before_id AND P.id > E.after_id
ORDER BY P.name ASC;