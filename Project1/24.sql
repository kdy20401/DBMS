SELECT C.name, AVG(CP.level)
FROM City C, Trainer T, CatchedPokemon CP
WHERE C.name = T.hometown AND T.id = CP.owner_id
GROUP BY C.name
ORDER BY AVG(CP.level);