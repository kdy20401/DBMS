SELECT P.name
FROM CatchedPokemon CP, Pokemon P
WHERE CP.pid = P.id AND CP.nickname LIKE '% %'
ORDER BY P.name DESC;