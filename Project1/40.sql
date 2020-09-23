SELECT T.hometown, CP.nickname
FROM Trainer T, CatchedPokemon CP, (
  SELECT T.hometown AS hometown, MAX(CP.level) AS maxLevel
  FROM Trainer T, CatchedPokemon CP
  WHERE T.id = CP.owner_id
  GROUP BY T.hometown ) AS HT_MAX
WHERE T.id = CP.owner_id AND T.hometown = HT_MAX.hometown AND CP.level = HT_MAX.maxLevel
ORDER BY T.hometown;