SELECT P.name
FROM Pokemon P
WHERE P.type = 'Grass' AND P.id IN (
  SELECT E.before_id
  FROM Evolution E
);