SELECT T.name, AVG(CP.level) as avg
FROM Trainer T, CatchedPokemon CP, Pokemon P
WHERE T.id = CP.owner_id AND CP.pid = P.id AND (P.type = 'Normal' OR P.type = 'Electric')
GROUP BY T.id
ORDER BY avg;