SELECT COUNT(*)
FROM CatchedPokemon CP, Pokemon P
WHERE CP.pid = P.id
GROUP BY P.type
ORDER BY P.type;