SELECT P.name, CP.level, CP.nickname
FROM Gym G, Trainer T, CatchedPokemon CP, Pokemon P
WHERE G.leader_id = T.id AND T.id = CP.owner_id AND CP.pid = P.id AND CP.nickname LIKE 'A%'
ORDER BY P.name DESC;