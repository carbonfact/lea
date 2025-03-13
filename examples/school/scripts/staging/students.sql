WITH raw_students AS (
    SELECT * FROM './seeds/raw_students.csv'
)

SELECT
    -- #UNIQUE
    -- #NO_NULLS
    id AS student_id,
    first_name,
    -- #UNIQUE_BY(first_name)
    last_name,
    -- #SET{'Stanford University', 'University of California Berkeley', 'Princeton University', 'Harvard University', 'Massachusetts Institute of Technology'}
    university,
FROM raw_students;
