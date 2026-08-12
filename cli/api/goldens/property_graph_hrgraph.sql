CREATE OR REPLACE PROPERTY GRAPH `project-id.dataset-id.HRGraph` NODE TABLES (
  `project-id.dataset-id.employee` AS Employee KEY (id) LABEL Employee PROPERTIES ARE ALL COLUMNS EXCEPT (ssn, salary),
  `project-id.dataset-id.manager` AS Manager KEY (id) LABEL Manager PROPERTIES ARE ALL COLUMNS EXCEPT (bonus)
)
EDGE TABLES (
  `project-id.dataset-id.reports` AS Reports SOURCE KEY (employee_id) REFERENCES Employee (id) DESTINATION KEY (manager_id) REFERENCES Manager (id) LABEL Reports PROPERTIES ARE ALL COLUMNS
)
