.mode csv
.headers on
pragma temp_store = MEMORY;

.import ../outData/a1_models.csv a1_models
.import ../outData/a2_models.csv a2_models
.import ../outData/a3_models.csv a3_models
.import ../outData/b1_models.csv b1_models
.import ../outData/b2_models.csv b2_models
.import ../outData/c1_models.csv c1_models
.import ../outData/c2_models.csv c2_models
.import ../outData/c3_models.csv c3_models
.import ../outData/e1_models.csv e1_models
.import ../outData/e2_models.csv e2_models
.import ../outData/d1_models.csv d1_models
.import ../outData/d2_models.csv d2_models
.import ../outData/f_models.csv f_models
.import full_frame.csv full_frame

ALTER TABLE a1_models ADD COLUMN f_model TEXT;
ALTER TABLE a2_models ADD COLUMN f_model TEXT;
ALTER TABLE a3_models ADD COLUMN f_model TEXT;
ALTER TABLE b1_models ADD COLUMN f_model TEXT;
ALTER TABLE b2_models ADD COLUMN f_model TEXT;
ALTER TABLE c1_models ADD COLUMN f_model TEXT;
ALTER TABLE c2_models ADD COLUMN f_model TEXT;
ALTER TABLE c3_models ADD COLUMN f_model TEXT;
ALTER TABLE d1_models ADD COLUMN f_model TEXT;
ALTER TABLE d2_models ADD COLUMN f_model TEXT;
ALTER TABLE e1_models ADD COLUMN f_model TEXT;
ALTER TABLE e2_models ADD COLUMN f_model TEXT;
ALTER TABLE f_models ADD COLUMN f_model TEXT;

.headers off
CREATE TABLE iops (
    prdn TEXT NOT NULL,
    assg_seq INTEGER NOT NULL,
    UNIQUE(prdn, assg_seq)
);
.import ../inData/iops_prdn_assg_seq.csv iops

CREATE TABLE patent_metadata (
    prdn TEXT NOT NULL,
    grant_yr INT NOT NULL,
    app_yr INT NOT NULL,
    num_assigs INT NOT NULL,
    us_inventor_flag INT NOT NULL,
    UNIQUE (prdn) -- for index on grant_yr
);
.import ../inData/prdn_metadata.csv patent_metadata
.headers on

CREATE INDEX
    a1_indx
ON
    a1_models(prdn, assg_seq, firmid);
CREATE INDEX
    a2_indx
ON
    a2_models(prdn, assg_seq, firmid);
CREATE INDEX
    a3_indx
ON
    a3_models(prdn, assg_seq, firmid);
CREATE INDEX
    b1_indx
ON
    b1_models(prdn, assg_seq, firmid);
CREATE INDEX
    b2_indx
ON
    b2_models(prdn, assg_seq, firmid);
CREATE INDEX
    c1_indx
ON
    c1_models(prdn, assg_seq, firmid);
CREATE INDEX
    c2_indx
ON
    c2_models(prdn, assg_seq, firmid);
CREATE INDEX
    c3_indx
ON
    c3_models(prdn, assg_seq, firmid);
CREATE INDEX
    e1_indx
ON
    e1_models(prdn, assg_seq, firmid);
CREATE INDEX
    e2_indx
ON
    e2_models(prdn, assg_seq, firmid);
CREATE INDEX
    d1_indx
ON
    d1_models(prdn, assg_seq, firmid);
CREATE INDEX
    d2_indx
ON
    d2_models(prdn, assg_seq, firmid);
CREATE INDEX
    f_indx
ON
    f_models(prdn, assg_seq, firmid);
CREATE INDEX
    frame_indx
ON
    full_frame(prdn, assg_seq);
    
    
-- The final crosswalk without the F models
CREATE TABLE crosswalk AS SELECT * FROM a1_models;
INSERT INTO crosswalk SELECT * FROM a2_models;
INSERT INTO crosswalk SELECT * FROM a3_models;
INSERT INTO crosswalk SELECT * FROM b1_models;
INSERT INTO crosswalk SELECT * FROM b2_models;
INSERT INTO crosswalk SELECT * FROM c1_models;
INSERT INTO crosswalk SELECT * FROM c2_models;
INSERT INTO crosswalk SELECT * FROM c3_models;
INSERT INTO crosswalk SELECT * FROM e1_models;
INSERT INTO crosswalk SELECT * FROM e2_models;
INSERT INTO crosswalk SELECT * FROM d1_models;
INSERT INTO crosswalk SELECT * FROM d2_models;
    
CREATE INDEX
    cw_indx
ON
    crosswalk(prdn, assg_seq);
-- Cleanup of the IOPs that escaped...
DELETE FROM crosswalk
WHERE EXISTS (
    SELECT *
    FROM iops
    WHERE
        iops.prdn = crosswalk.prdn AND
        iops.assg_seq = crosswalk.assg_seq
);

-- Now put in whatever didn't get a model assigned
DELETE FROM full_frame
WHERE EXISTS (
    SELECT *
    FROM iops
    WHERE
        iops.prdn = full_frame.prdn AND
        iops.assg_seq = full_frame.assg_seq
);
DELETE FROM full_frame
WHERE EXISTS (
    SELECT *
    FROM crosswalk
    WHERE
        crosswalk.prdn = full_frame.prdn AND
        crosswalk.assg_seq = full_frame.assg_seq
);
UPDATE full_frame
SET
    app_yr = 
        (
            SELECT patent_metadata.app_yr
            FROM patent_metadata
            WHERE full_frame.prdn = patent_metadata.prdn
        ),
    grant_yr = 
        (
            SELECT patent_metadata.grant_yr
            FROM patent_metadata
            WHERE full_frame.prdn = patent_metadata.prdn
        ),
    us_inventor_flag = 
        (
            SELECT patent_metadata.us_inventor_flag
            FROM patent_metadata
            WHERE full_frame.prdn = patent_metadata.prdn
        ),
    multiple_assignee_flag = 
        (
            SELECT patent_metadata.num_assigs
            FROM patent_metadata
            WHERE full_frame.prdn = patent_metadata.prdn
        )
WHERE EXISTS (
    SELECT 1
    FROM patent_metadata
    WHERE full_frame.prdn = patent_metadata.prdn
);
ALTER TABLE full_frame ADD COLUMN f_model TEXT;
INSERT INTO crosswalk SELECT * FROM full_frame;

.output crosswalk.csv
SELECT * 
FROM crosswalk
WHERE grant_yr > 1999;
.output stdout



-- This is for the F models below...    
DROP TABLE full_frame;
.import full_frame.csv full_frame
CREATE INDEX
    frame_indx
ON
    full_frame(prdn, assg_seq);

/*
-- Remove things that we found 'correctly' with other models
DELETE FROM a1_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        a1_models.prdn = f_models.prdn AND
        a1_models.assg_seq = f_models.assg_seq AND
        a1_models.firmid = f_models.firmid
);

DELETE FROM a2_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        a2_models.prdn = f_models.prdn AND
        a2_models.assg_seq = f_models.assg_seq AND
        a2_models.firmid = f_models.firmid
);

DELETE FROM a3_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        a3_models.prdn = f_models.prdn AND
        a3_models.assg_seq = f_models.assg_seq AND
        a3_models.firmid = f_models.firmid
);

DELETE FROM b1_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        b1_models.prdn = f_models.prdn AND
        b1_models.assg_seq = f_models.assg_seq AND
        b1_models.firmid = f_models.firmid
);

DELETE FROM b2_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        b2_models.prdn = f_models.prdn AND
        b2_models.assg_seq = f_models.assg_seq AND
        b2_models.firmid = f_models.firmid
);

DELETE FROM c1_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        c1_models.prdn = f_models.prdn AND
        c1_models.assg_seq = f_models.assg_seq AND
        c1_models.firmid = f_models.firmid
);

DELETE FROM c2_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        c2_models.prdn = f_models.prdn AND
        c2_models.assg_seq = f_models.assg_seq AND
        c2_models.firmid = f_models.firmid
);

DELETE FROM c3_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        c3_models.prdn = f_models.prdn AND
        c3_models.assg_seq = f_models.assg_seq AND
        c3_models.firmid = f_models.firmid
);

DELETE FROM d1_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        d1_models.prdn = f_models.prdn AND
        d1_models.assg_seq = f_models.assg_seq AND
        d1_models.firmid = f_models.firmid
);

DELETE FROM d2_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        d2_models.prdn = f_models.prdn AND
        d2_models.assg_seq = f_models.assg_seq AND
        d2_models.firmid = f_models.firmid
);

DELETE FROM e1_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        e1_models.prdn = f_models.prdn AND
        e1_models.assg_seq = f_models.assg_seq AND
        e1_models.firmid = f_models.firmid
);

DELETE FROM e2_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        e2_models.prdn = f_models.prdn AND
        e2_models.assg_seq = f_models.assg_seq AND
        e2_models.firmid = f_models.firmid
);
*/

-- Change things that we found 'correctly' with other models
UPDATE a1_models
SET f_model = (
    SELECT f_models.model_type
    FROM f_models
    WHERE
        a1_models.prdn = f_models.prdn AND
        a1_models.assg_seq = f_models.assg_seq AND
        a1_models.firmid = f_models.firmid
)
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        a1_models.prdn = f_models.prdn AND
        a1_models.assg_seq = f_models.assg_seq AND
        a1_models.firmid = f_models.firmid
);
DELETE FROM a1_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        a1_models.prdn = f_models.prdn AND
        a1_models.assg_seq = f_models.assg_seq AND
        a1_models.firmid != f_models.firmid
);
DELETE FROM f_models
WHERE EXISTS (
    SELECT 1
    FROM a1_models
    WHERE
        a1_models.prdn = f_models.prdn AND
        a1_models.assg_seq = f_models.assg_seq AND
        a1_models.firmid = f_models.firmid
);

UPDATE a2_models
SET f_model = (
    SELECT f_models.model_type
    FROM f_models
    WHERE
        a2_models.prdn = f_models.prdn AND
        a2_models.assg_seq = f_models.assg_seq AND
        a2_models.firmid = f_models.firmid
)
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        a2_models.prdn = f_models.prdn AND
        a2_models.assg_seq = f_models.assg_seq AND
        a2_models.firmid = f_models.firmid
);
DELETE FROM a2_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        a2_models.prdn = f_models.prdn AND
        a2_models.assg_seq = f_models.assg_seq AND
        a2_models.firmid != f_models.firmid
);
DELETE FROM f_models
WHERE EXISTS (
    SELECT 1
    FROM a2_models
    WHERE
        a2_models.prdn = f_models.prdn AND
        a2_models.assg_seq = f_models.assg_seq AND
        a2_models.firmid = f_models.firmid
);

UPDATE a3_models
SET f_model = (
    SELECT f_models.model_type
    FROM f_models
    WHERE
        a3_models.prdn = f_models.prdn AND
        a3_models.assg_seq = f_models.assg_seq AND
        a3_models.firmid = f_models.firmid
)
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        a3_models.prdn = f_models.prdn AND
        a3_models.assg_seq = f_models.assg_seq AND
        a3_models.firmid = f_models.firmid
);
DELETE FROM a3_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        a3_models.prdn = f_models.prdn AND
        a3_models.assg_seq = f_models.assg_seq AND
        a3_models.firmid != f_models.firmid
);
DELETE FROM f_models
WHERE EXISTS (
    SELECT 1
    FROM a3_models
    WHERE
        a3_models.prdn = f_models.prdn AND
        a3_models.assg_seq = f_models.assg_seq AND
        a3_models.firmid = f_models.firmid
);

UPDATE b1_models
SET f_model = (
    SELECT f_models.model_type
    FROM f_models
    WHERE
        b1_models.prdn = f_models.prdn AND
        b1_models.assg_seq = f_models.assg_seq AND
        b1_models.firmid = f_models.firmid
)
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        b1_models.prdn = f_models.prdn AND
        b1_models.assg_seq = f_models.assg_seq AND
        b1_models.firmid = f_models.firmid
);
DELETE FROM b1_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        b1_models.prdn = f_models.prdn AND
        b1_models.assg_seq = f_models.assg_seq AND
        b1_models.firmid != f_models.firmid
);
DELETE FROM f_models
WHERE EXISTS (
    SELECT 1
    FROM b1_models
    WHERE
        b1_models.prdn = f_models.prdn AND
        b1_models.assg_seq = f_models.assg_seq AND
        b1_models.firmid = f_models.firmid
);

UPDATE b2_models
SET f_model = (
    SELECT f_models.model_type
    FROM f_models
    WHERE
        b2_models.prdn = f_models.prdn AND
        b2_models.assg_seq = f_models.assg_seq AND
        b2_models.firmid = f_models.firmid
)
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        b2_models.prdn = f_models.prdn AND
        b2_models.assg_seq = f_models.assg_seq AND
        b2_models.firmid = f_models.firmid
);
DELETE FROM b2_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        b2_models.prdn = f_models.prdn AND
        b2_models.assg_seq = f_models.assg_seq AND
        b2_models.firmid != f_models.firmid
);
DELETE FROM f_models
WHERE EXISTS (
    SELECT 1
    FROM b2_models
    WHERE
        b2_models.prdn = f_models.prdn AND
        b2_models.assg_seq = f_models.assg_seq AND
        b2_models.firmid = f_models.firmid
);

UPDATE c1_models
SET f_model = (
    SELECT f_models.model_type
    FROM f_models
    WHERE
        c1_models.prdn = f_models.prdn AND
        c1_models.assg_seq = f_models.assg_seq AND
        c1_models.firmid = f_models.firmid
)
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        c1_models.prdn = f_models.prdn AND
        c1_models.assg_seq = f_models.assg_seq AND
        c1_models.firmid = f_models.firmid
);
DELETE FROM c1_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        c1_models.prdn = f_models.prdn AND
        c1_models.assg_seq = f_models.assg_seq AND
        c1_models.firmid != f_models.firmid
);
DELETE FROM f_models
WHERE EXISTS (
    SELECT 1
    FROM c1_models
    WHERE
        c1_models.prdn = f_models.prdn AND
        c1_models.assg_seq = f_models.assg_seq AND
        c1_models.firmid = f_models.firmid
);

UPDATE c2_models
SET f_model = (
    SELECT f_models.model_type
    FROM f_models
    WHERE
        c2_models.prdn = f_models.prdn AND
        c2_models.assg_seq = f_models.assg_seq AND
        c2_models.firmid = f_models.firmid
)
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        c2_models.prdn = f_models.prdn AND
        c2_models.assg_seq = f_models.assg_seq AND
        c2_models.firmid = f_models.firmid
);
DELETE FROM c2_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        c2_models.prdn = f_models.prdn AND
        c2_models.assg_seq = f_models.assg_seq AND
        c2_models.firmid != f_models.firmid
);
DELETE FROM f_models
WHERE EXISTS (
    SELECT 1
    FROM c2_models
    WHERE
        c2_models.prdn = f_models.prdn AND
        c2_models.assg_seq = f_models.assg_seq AND
        c2_models.firmid = f_models.firmid
);

UPDATE c3_models
SET f_model = (
    SELECT f_models.model_type
    FROM f_models
    WHERE
        c3_models.prdn = f_models.prdn AND
        c3_models.assg_seq = f_models.assg_seq AND
        c3_models.firmid = f_models.firmid
)
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        c3_models.prdn = f_models.prdn AND
        c3_models.assg_seq = f_models.assg_seq AND
        c3_models.firmid = f_models.firmid
);
DELETE FROM c3_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        c3_models.prdn = f_models.prdn AND
        c3_models.assg_seq = f_models.assg_seq AND
        c3_models.firmid != f_models.firmid
);
DELETE FROM f_models
WHERE EXISTS (
    SELECT 1
    FROM c3_models
    WHERE
        c3_models.prdn = f_models.prdn AND
        c3_models.assg_seq = f_models.assg_seq AND
        c3_models.firmid = f_models.firmid
);

UPDATE d1_models
SET f_model = (
    SELECT f_models.model_type
    FROM f_models
    WHERE
        d1_models.prdn = f_models.prdn AND
        d1_models.assg_seq = f_models.assg_seq AND
        d1_models.firmid = f_models.firmid
)
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        d1_models.prdn = f_models.prdn AND
        d1_models.assg_seq = f_models.assg_seq AND
        d1_models.firmid = f_models.firmid
);
DELETE FROM d1_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        d1_models.prdn = f_models.prdn AND
        d1_models.assg_seq = f_models.assg_seq AND
        d1_models.firmid != f_models.firmid
);
DELETE FROM f_models
WHERE EXISTS (
    SELECT 1
    FROM d1_models
    WHERE
        d1_models.prdn = f_models.prdn AND
        d1_models.assg_seq = f_models.assg_seq AND
        d1_models.firmid = f_models.firmid
);

UPDATE d2_models
SET f_model = (
    SELECT f_models.model_type
    FROM f_models
    WHERE
        d2_models.prdn = f_models.prdn AND
        d2_models.assg_seq = f_models.assg_seq AND
        d2_models.firmid = f_models.firmid
)
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        d2_models.prdn = f_models.prdn AND
        d2_models.assg_seq = f_models.assg_seq AND
        d2_models.firmid = f_models.firmid
);
DELETE FROM d2_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        d2_models.prdn = f_models.prdn AND
        d2_models.assg_seq = f_models.assg_seq AND
        d2_models.firmid != f_models.firmid
);
DELETE FROM f_models
WHERE EXISTS (
    SELECT 1
    FROM d2_models
    WHERE
        d2_models.prdn = f_models.prdn AND
        d2_models.assg_seq = f_models.assg_seq AND
        d2_models.firmid = f_models.firmid
);

UPDATE e1_models
SET f_model = (
    SELECT f_models.model_type
    FROM f_models
    WHERE
        e1_models.prdn = f_models.prdn AND
        e1_models.assg_seq = f_models.assg_seq AND
        e1_models.firmid = f_models.firmid
)
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        e1_models.prdn = f_models.prdn AND
        e1_models.assg_seq = f_models.assg_seq AND
        e1_models.firmid = f_models.firmid
);
DELETE FROM e1_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        e1_models.prdn = f_models.prdn AND
        e1_models.assg_seq = f_models.assg_seq AND
        e1_models.firmid != f_models.firmid
);
DELETE FROM f_models
WHERE EXISTS (
    SELECT 1
    FROM e1_models
    WHERE
        e1_models.prdn = f_models.prdn AND
        e1_models.assg_seq = f_models.assg_seq AND
        e1_models.firmid = f_models.firmid
);

UPDATE e2_models
SET f_model = (
    SELECT f_models.model_type
    FROM f_models
    WHERE
        e2_models.prdn = f_models.prdn AND
        e2_models.assg_seq = f_models.assg_seq AND
        e2_models.firmid = f_models.firmid
)
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        e2_models.prdn = f_models.prdn AND
        e2_models.assg_seq = f_models.assg_seq AND
        e2_models.firmid = f_models.firmid
);
DELETE FROM e2_models
WHERE EXISTS (
    SELECT 1
    FROM f_models
    WHERE
        e2_models.prdn = f_models.prdn AND
        e2_models.assg_seq = f_models.assg_seq AND
        e2_models.firmid != f_models.firmid
);
DELETE FROM f_models
WHERE EXISTS (
    SELECT 1
    FROM e2_models
    WHERE
        e2_models.prdn = f_models.prdn AND
        e2_models.assg_seq = f_models.assg_seq AND
        e2_models.firmid = f_models.firmid
);

UPDATE f_models SET f_model = f_models.model_type;

-- Replace things we found 'incorrectly' with other models
-- DELETE FROM a1_models
-- WHERE EXISTS (
--     SELECT 1
--     FROM f_models
--     WHERE
--         a1_models.prdn = f_models.prdn AND
--         a1_models.assg_seq = f_models.assg_seq AND
--         a1_models.firmid != f_models.firmid
-- );

-- DELETE FROM a2_models
-- WHERE EXISTS (
--     SELECT 1
--     FROM f_models
--     WHERE
--         a2_models.prdn = f_models.prdn AND
--         a2_models.assg_seq = f_models.assg_seq AND
--         a2_models.firmid != f_models.firmid
-- );

-- DELETE FROM a3_models
-- WHERE EXISTS (
--     SELECT 1
--     FROM f_models
--     WHERE
--         a3_models.prdn = f_models.prdn AND
--         a3_models.assg_seq = f_models.assg_seq AND
--         a3_models.firmid != f_models.firmid
-- );

-- DELETE FROM b1_models
-- WHERE EXISTS (
--     SELECT 1
--     FROM f_models
--     WHERE
--         b1_models.prdn = f_models.prdn AND
--         b1_models.assg_seq = f_models.assg_seq AND
--         b1_models.firmid != f_models.firmid
-- );

-- DELETE FROM b2_models
-- WHERE EXISTS (
--     SELECT 1
--     FROM f_models
--     WHERE
--         b2_models.prdn = f_models.prdn AND
--         b2_models.assg_seq = f_models.assg_seq AND
--         b2_models.firmid != f_models.firmid
-- );

-- DELETE FROM c1_models
-- WHERE EXISTS (
--     SELECT 1
--     FROM f_models
--     WHERE
--         c1_models.prdn = f_models.prdn AND
--         c1_models.assg_seq = f_models.assg_seq AND
--         c1_models.firmid != f_models.firmid
-- );

-- DELETE FROM c2_models
-- WHERE EXISTS (
--     SELECT 1
--     FROM f_models
--     WHERE
--         c2_models.prdn = f_models.prdn AND
--         c2_models.assg_seq = f_models.assg_seq AND
--         c2_models.firmid != f_models.firmid
-- );

-- DELETE FROM c3_models
-- WHERE EXISTS (
--     SELECT 1
--     FROM f_models
--     WHERE
--         c3_models.prdn = f_models.prdn AND
--         c3_models.assg_seq = f_models.assg_seq AND
--         c3_models.firmid != f_models.firmid
-- );

-- DELETE FROM d1_models
-- WHERE EXISTS (
--     SELECT 1
--     FROM f_models
--     WHERE
--         d1_models.prdn = f_models.prdn AND
--         d1_models.assg_seq = f_models.assg_seq AND
--         d1_models.firmid != f_models.firmid
-- );

-- DELETE FROM d2_models
-- WHERE EXISTS (
--     SELECT 1
--     FROM f_models
--     WHERE
--         d2_models.prdn = f_models.prdn AND
--         d2_models.assg_seq = f_models.assg_seq AND
--         d2_models.firmid != f_models.firmid
-- );

-- DELETE FROM e1_models
-- WHERE EXISTS (
--     SELECT 1
--     FROM f_models
--     WHERE
--         e1_models.prdn = f_models.prdn AND
--         e1_models.assg_seq = f_models.assg_seq AND
--         e1_models.firmid != f_models.firmid
-- );

-- DELETE FROM e2_models
-- WHERE EXISTS (
--     SELECT 1
--     FROM f_models
--     WHERE
--         e2_models.prdn = f_models.prdn AND
--         e2_models.assg_seq = f_models.assg_seq AND
--         e2_models.firmid != f_models.firmid
-- );


-- The final crosswalk with the F models
DROP TABLE crosswalk;
CREATE TABLE crosswalk AS SELECT * FROM f_models;
INSERT INTO crosswalk SELECT * FROM a1_models;
INSERT INTO crosswalk SELECT * FROM a2_models;
INSERT INTO crosswalk SELECT * FROM a3_models;
INSERT INTO crosswalk SELECT * FROM b1_models;
INSERT INTO crosswalk SELECT * FROM b2_models;
INSERT INTO crosswalk SELECT * FROM c1_models;
INSERT INTO crosswalk SELECT * FROM c2_models;
INSERT INTO crosswalk SELECT * FROM c3_models;
INSERT INTO crosswalk SELECT * FROM e1_models;
INSERT INTO crosswalk SELECT * FROM e2_models;
INSERT INTO crosswalk SELECT * FROM d1_models;
INSERT INTO crosswalk SELECT * FROM d2_models;
    
CREATE INDEX
    cw_indx
ON
    crosswalk(prdn, assg_seq);
-- Cleanup of the IOPs that escaped...
DELETE FROM crosswalk
WHERE EXISTS (
    SELECT *
    FROM iops
    WHERE
        iops.prdn = crosswalk.prdn AND
        iops.assg_seq = crosswalk.assg_seq
);

-- Now put in whatever didn't get a model assigned
DELETE FROM full_frame
WHERE EXISTS (
    SELECT *
    FROM iops
    WHERE
        iops.prdn = full_frame.prdn AND
        iops.assg_seq = full_frame.assg_seq
);
DELETE FROM full_frame
WHERE EXISTS (
    SELECT *
    FROM crosswalk
    WHERE
        crosswalk.prdn = full_frame.prdn AND
        crosswalk.assg_seq = full_frame.assg_seq
);
UPDATE full_frame
SET
    app_yr = 
        (
            SELECT patent_metadata.app_yr
            FROM patent_metadata
            WHERE full_frame.prdn = patent_metadata.prdn
        ),
    grant_yr = 
        (
            SELECT patent_metadata.grant_yr
            FROM patent_metadata
            WHERE full_frame.prdn = patent_metadata.prdn
        ),
    us_inventor_flag = 
        (
            SELECT patent_metadata.us_inventor_flag
            FROM patent_metadata
            WHERE full_frame.prdn = patent_metadata.prdn
        ),
    multiple_assignee_flag = 
        (
            SELECT patent_metadata.num_assigs
            FROM patent_metadata
            WHERE full_frame.prdn = patent_metadata.prdn
        )
WHERE EXISTS (
    SELECT 1
    FROM patent_metadata
    WHERE full_frame.prdn = patent_metadata.prdn
);
ALTER TABLE full_frame ADD COLUMN f_model TEXT;
INSERT INTO crosswalk SELECT * FROM full_frame;

.output crosswalk_F.csv
SELECT * 
FROM crosswalk
WHERE grant_yr > 1999;
.output stdout


