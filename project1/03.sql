SELECT AVG(CP.level)
FROM City C, Trainer T, CatchedPokemon CP, Pokemon P
WHERE C.name = T.hometown AND T.id = CP.owner_id AND CP.pid = P.id AND C.name = 'Sangnok City'
AND P.type = 'Electric';