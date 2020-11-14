SELECT T.name, AVG(CP.level)
FROM Gym G, Trainer T, CatchedPokemon CP
WHERE G.leader_id = T.id AND T.id = CP.owner_id
GROUP BY T.id
ORDER BY T.name;