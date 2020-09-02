/*** QUESTION 1 ***/

WITH rskCTE AS (
    SELECT r.PersonID, r.AttributedPayer, r.RiskLevel, r.RiskDateTime,
        RANK() OVER (PARTITION BY r.PersonID ORDER BY r.RiskDateTime DESC) AS rnk
    FROM dbo.Risk AS r
)
SELECT p.PersonName, rc.AttributedPayer, rc.RiskLevel, rc.RiskDateTime
FROM dbo.Person AS p
    LEFT JOIN rskCTE AS rc
        ON p.PersonID = rc.PersonID
        AND rc.rnk = 1;


/*** QUESTION 2 ***/

WITH pCTE AS (
    SELECT
        PARSENAME(REPLACE(p.PersonName, ' ', '.'), 3) AS n1,
        PARSENAME(REPLACE(p.PersonName, ' ', '.'), 2) AS n2,
        PARSENAME(REPLACE(p.PersonName, ' ', '.'), 1) AS n3
    FROM dbo.Person AS p
)
SELECT
    COALESCE(
        CASE WHEN pc.n1 NOT LIKE '%[^a-zA-Z]%' THEN pc.n1 ELSE NULL END,
        CASE WHEN pc.n2 NOT LIKE '%[^a-zA-Z]%' THEN pc.n2 ELSE NULL END,
        CASE WHEN pc.n3 NOT LIKE '%[^a-zA-Z]%' THEN pc.n3 ELSE NULL END
    ) AS FirstName,
    COALESCE(
        CASE WHEN pc.n3 NOT LIKE '%[^a-zA-Z]%' THEN pc.n3 ELSE NULL END,
        CASE WHEN pc.n2 NOT LIKE '%[^a-zA-Z]%' THEN pc.n2 ELSE NULL END,
        CASE WHEN pc.n1 NOT LIKE '%[^a-zA-Z]%' THEN pc.n1 ELSE NULL END
    ) AS LastName,
    COALESCE(
        CASE WHEN pc.n1 LIKE '%[()]%' THEN REPLACE(REPLACE(pc.n1, '(', ''), ')', '') ELSE NULL END,
        CASE WHEN pc.n2 LIKE '%[()]%' THEN REPLACE(REPLACE(pc.n2, '(', ''), ')', '') ELSE NULL END,
        CASE WHEN pc.n3 LIKE '%[()]%' THEN REPLACE(REPLACE(pc.n3, '(', ''), ')', '') ELSE NULL END,
        ''
    ) AS NickName
FROM pCTE AS pc;


/*** QUESTION 3 ***/

-- Moving average of last 3 risk scores
WITH pCTE AS (
    SELECT
        p.PersonID,
        PARSENAME(REPLACE(p.PersonName, ' ', '.'), 3) AS n1,
        PARSENAME(REPLACE(p.PersonName, ' ', '.'), 2) AS n2,
        PARSENAME(REPLACE(p.PersonName, ' ', '.'), 1) AS n3
    FROM dbo.Person AS p
), pNames AS (
    SELECT
        pc.PersonID,
        COALESCE(
            CASE WHEN pc.n1 NOT LIKE '%[^a-zA-Z]%' THEN pc.n1 ELSE NULL END,
            CASE WHEN pc.n2 NOT LIKE '%[^a-zA-Z]%' THEN pc.n2 ELSE NULL END,
            CASE WHEN pc.n3 NOT LIKE '%[^a-zA-Z]%' THEN pc.n3 ELSE NULL END
        ) AS FirstName,
        COALESCE(
            CASE WHEN pc.n3 NOT LIKE '%[^a-zA-Z]%' THEN pc.n3 ELSE NULL END,
            CASE WHEN pc.n2 NOT LIKE '%[^a-zA-Z]%' THEN pc.n2 ELSE NULL END,
            CASE WHEN pc.n1 NOT LIKE '%[^a-zA-Z]%' THEN pc.n1 ELSE NULL END
        ) AS LastName
    FROM pCTE AS pc
)
SELECT pn.FirstName, pn.LastName, r.AttributedPayer AS Payer, r.RiskScore, r.RiskDateTime,
    AVG(r.RiskScore) OVER (PARTITION BY r.PersonID, r.AttributedPayer 
        ORDER BY r.RiskDateTime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS Last3MovingAvg
FROM pNames AS pn
    LEFT JOIN dbo.Risk AS r
        ON pn.PersonID = r.PersonID
ORDER BY pn.PersonID, r.AttributedPayer, r.RiskDateTime DESC;