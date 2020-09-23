SELECT AVG(CP.level)
FROM Trainer T, Gym G, CatchedPokemon CP
WHERE T.id = G.leader_id AND T.id = CP.owner_id;