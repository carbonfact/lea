WITH raw_grades AS (
    SELECT * FROM './seeds/raw_grades.csv'
)

SELECT
    -- #NO_NULLS
    student_id,
    -- #NO_NULLS
    class_name,
    -- #NO_NULLS
    grade,
    -- #NO_NULLS
    strptime(exam_date, '%m-%Y') AS exam_date,
FROM raw_grades;
