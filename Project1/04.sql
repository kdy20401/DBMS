SELECT CP.nickname
FROM CatchedPokemon CP, Pokemon P
WHERE CP.pid = P.id AND CP.level >= 50
ORDER BY CP.nickname;