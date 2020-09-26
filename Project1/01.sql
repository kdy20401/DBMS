SELECT T.name
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id
GROUP BY T.id
HAVING COUNT(*) >= 3
ORDER BY COUNT(*) DESC;