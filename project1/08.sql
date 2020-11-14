SELECT AVG(CP.level)
FROM Trainer T, CatchedPokemon CP
WHERE T.id = CP.owner_id AND T.name = 'Red';