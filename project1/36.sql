SELECT T.name
FROM Trainer T, CatchedPokemon CP
WHERE T.id = CP.owner_id AND CP.pid IN (
  SELECT P.id
  FROM Pokemon P
  WHERE P.id IN (
    SELECT E.after_id
    FROM Evolution E
    WHERE E.after_id NOT IN (
      SELECT E.before_id
      FROM Evolution E)))
ORDER BY T.name;
