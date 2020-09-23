SELECT P.name
FROM Pokemon P
WHERE P.type IN (
  SELECT TYPE_COUNT.type
  FROM (
    SELECT P.type, COUNT(*) AS num
    FROM Pokemon P
    GROUP BY P.type ) AS TYPE_COUNT
  WHERE TYPE_COUNT.num >= (
    SELECT MAX(TYPE_COUNT.num)
    FROM (
      SELECT P.type, COUNT(*) AS num
      FROM Pokemon P
      GROUP BY P.type ) AS TYPE_COUNT
    WHERE TYPE_COUNT.num < (
      SELECT MAX(TYPE_COUNT.num)
      FROM (
        SELECT P.type, COUNT(*) AS num
        FROM Pokemon P
        GROUP BY P.type ) AS TYPE_COUNT)))
ORDER BY P.name;