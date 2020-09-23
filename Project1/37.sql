SELECT T.name, SUM(CP.level)
FROM CatchedPokemon CP, Trainer T
WHERE CP.owner_id = T.id
GROUP BY CP.owner_id
HAVING SUM(CP.level) = (
  SELECT MAX(LevelSum.sum)
  FROM (
    SELECT SUM(CP.level) as sum
    FROM CatchedPokemon CP, Trainer T
    WHERE CP.owner_id = T.id
    GROUP BY CP.owner_id) AS LevelSum )
ORDER BY T.name;


