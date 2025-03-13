WITH grades_per_class_per_semester AS (
  SELECT
    student_id,
    class_name,
    grade,
    CASE
        WHEN datepart('month', exam_date) BETWEEN 1 AND 6 THEN 'Semester 1'
        ELSE 'Semester 2'
    END AS semester
    FROM staging.grades
),
avg_grades_per_class AS (
  SELECT
    student_id,
    class_name,
    semester,
    AVG(grade) AS average_grade
  FROM grades_per_class_per_semester
    GROUP BY class_name, semester, student_id
  )
SELECT
    students.student_id,
    CONCAT(students.first_name, ' ', students.last_name) AS student_name,
    grades_per_class.class_name,
    grades_per_class.semester,
    grades_per_class.average_grade,
    students.university
FROM avg_grades_per_class AS grades_per_class
LEFT JOIN staging.students AS students
ON grades_per_class.student_id = students.student_id
ORDER BY student_name, class_name;
