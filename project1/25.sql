SELECT P.name
FROM Pokemon P
WHERE P.name IN (
  SELECT DISTINCT(P.name)
  FROM Trainer T, CatchedPokemon CP, Pokemon P
  WHERE T.id = CP.owner_id AND CP.pid = P.id AND T.hometown = 'Sangnok City')
AND P.name IN (
  SELECT DISTINCT(P.name)
  FROM Trainer T, CatchedPokemon CP, Pokemon P
  WHERE T.id = CP.owner_id AND CP.pid = P.id AND T.hometown = 'Brown City');