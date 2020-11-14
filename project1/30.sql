SELECT P.id, P.name, ET.step2Name, ET.step3Name
FROM Pokemon P, (
  SELECT STEP.step1, P1.name AS step2Name, P2.name AS step3Name  
  FROM (
    SELECT E1.before_id AS step1, E1.after_id AS step2, E2.after_id AS step3
    FROM Evolution E1, Evolution E2
    WHERE E1.after_id = E2.before_id) AS STEP, Pokemon P1, Pokemon P2
  WHERE STEP.step2 = P1.id AND STEP.step3 = P2.id) AS ET
WHERE P.id = ET.step1
ORDER BY P.id;