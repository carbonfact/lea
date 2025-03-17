WITH ordered_results AS (
  SELECT
    student_name,
    class_name,
    -- semester,    --uncomment here
    -- row_number() OVER (PARTITION BY class_name, semester  --uncomment here
    row_number() OVER (PARTITION BY class_name   --comment here
      ORDER BY average_grade DESC
    ) AS ranking
  FROM
    core.yearly_results
)
SELECT
  student_name,
  -- semester,  --uncomment here
  class_name AS domain,
  CASE
    WHEN ranking = 1 THEN 1000
    WHEN ranking = 2 THEN 500
    ELSE 0
  END AS scholarship_amount
  FROM ordered_results
  WHERE ranking <= 2;
