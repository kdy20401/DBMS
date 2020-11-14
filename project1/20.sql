SELECT T.name, COUNT(CP.pid) as num 
FROM Trainer T, CatchedPokemon CP
WHERE T.id = CP.owner_id AND T.hometown = 'Sangnok City'
GROUP BY T.id
ORDER BY num;