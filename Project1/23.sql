SELECT DISTINCT(T.name)
FROM Trainer T, CatchedPokemon CP
WHERE T.id = CP.owner_id AND CP.level <= 10
ORDER BY T.name;