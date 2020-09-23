SELECT T.name
FROM CatchedPokemon CP, Trainer T
WHERE CP.owner_id = T.id
GROUP BY T.name, CP.pid
HAVING COUNT(*) >= 2
ORDER BY T.name;