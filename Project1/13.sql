SELECT P.name, P.id
FROM Pokemon P, CatchedPokemon CP, Trainer T, City C
WHERE C.name = T.hometown AND T.id = CP.owner_id AND CP.pid = P.id AND C.name = 'Sangnok City'
ORDER BY P.id;