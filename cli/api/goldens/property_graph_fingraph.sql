CREATE OR REPLACE PROPERTY GRAPH `project-id.dataset-id.FinGraph` NODE TABLES (
  `project-id.dataset-id.account` AS Account KEY (id) LABEL Account PROPERTIES ARE ALL COLUMNS,
  `project-id.dataset-id.person` AS Person KEY (id) LABEL Person PROPERTIES ARE ALL COLUMNS
)
EDGE TABLES (
  `project-id.dataset-id.person_own_account` AS PersonOwnAccount SOURCE KEY (id) REFERENCES Person (id) DESTINATION KEY (account_id) REFERENCES Account (id) LABEL Owns PROPERTIES ARE ALL COLUMNS,
  `project-id.dataset-id.account_transfer_account` AS AccountTransferAccount SOURCE KEY (id) REFERENCES Account (id) DESTINATION KEY (to_id) REFERENCES Account (id) LABEL Transfers PROPERTIES (amount, create_time)
)
