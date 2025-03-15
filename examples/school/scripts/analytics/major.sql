WITH ordered_yearly_results AS (
  SELECT
    student_name,
    AVG(average_grade) AS total_grade
  FROM
    core.yearly_results
  GROUP BY student_name
)
SELECT
  student_name,
  total_grade
  FROM ordered_yearly_results
  ORDER BY total_grade DESC
  LIMIT 1;
