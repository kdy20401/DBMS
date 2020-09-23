SELECT T.name
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id
GROUP BY T.name
HAVING COUNT(*) >= 3
ORDER BY COUNT(*) DESC;