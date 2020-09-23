SELECT P.name
FROM Pokemon P LEFT JOIN CatchedPokemon CP ON P.id = CP.pid
WHERE ISNULL(CP.owner_id)
ORDER BY P.name;
