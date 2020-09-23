SELECT CP.owner_id, COUNT(*) AS num
FROM CatchedPokemon CP
GROUP BY CP.owner_id
HAVING COUNT(*) = (
  SELECT MAX(COUNT.num)
  FROM (
    SELECT COUNT(*) AS num
    FROM CatchedPokemon CP
    GROUP BY CP.owner_id ) AS COUNT)
ORDER BY CP.owner_id;