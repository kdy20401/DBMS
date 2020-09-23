SELECT T.name
FROM Trainer T, (
  SELECT C.name
  FROM City C
  ORDER BY C.name
  ) AS C
WHERE T.hometown = C.name;