SELECT DISTINCT P.name, P.type
FROM Pokemon P, CatchedPokemon CP
WHERE P.id = CP.pid AND CP.level >= 30
ORDER BY P.name;