SELECT T.name, MAX(CP.level)
FROM Trainer T, CatchedPokemon CP
WHERE T.id = CP.owner_id
GROUP BY T.id
HAVING COUNT(CP.pid) >= 4
ORDER BY T.name;