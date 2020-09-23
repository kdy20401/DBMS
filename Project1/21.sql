SELECT T.name, COUNT(CP.pid) as num
FROM Trainer T, Gym G, CatchedPokemon CP
WHERE T.id = G.leader_id AND T.id = CP.owner_id
GROUP BY T.name
ORDER BY T.name;