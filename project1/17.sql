SELECT COUNT(T.pid)
FROM (
  SELECT CP.pid
  FROM City C, Trainer T, CatchedPokemon CP
  WHERE C.name = T.hometown AND T.id = CP.owner_id AND C.name = 'Sangnok City'
  GROUP BY CP.pid
  ) AS T;