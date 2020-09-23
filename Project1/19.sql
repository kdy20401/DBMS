SELECT COUNT(DISTINCT(P.type))
FROM Trainer T, Gym G, CatchedPokemon CP, Pokemon P
WHERE T.id = G.leader_id AND T.id = CP.owner_id AND CP.pid = P.id AND
G.city = 'Sangnok City';