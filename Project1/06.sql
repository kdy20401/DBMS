SELECT T.name
FROM Trainer T, City C
WHERE T.hometown = C.name AND C.name = 'Blue City'
ORDER BY T.name;