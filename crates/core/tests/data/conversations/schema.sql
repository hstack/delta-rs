CREATE OR REPLACE TABLE conversations(
  _acp_system_metadata STRUCT( -- 1
    acp_sourceBatchId VARCHAR,  -- 2
    commitBatchId VARCHAR,  --3
    trackingId VARCHAR, 
    rowId VARCHAR, 
    rowVersion INT8, 
    primaryIdentity STRUCT(
      id VARCHAR, 
      namespace STRUCT(
        code VARCHAR
      )
    ), 
    ingestTime INT8, -- 4
    isDeleted BOOL  -- 5
  ),
  agenticExperience STRUCT(
    agents STRUCT(
      agentID VARCHAR
    )[], 
    version VARCHAR
  ), 
  conversation STRUCT(
    conversationID VARCHAR, 
    turnID VARCHAR, 
    text STRUCT(
      raw VARCHAR, 
      source VARCHAR
    )
  ), 
  identityMap MAP(
    VARCHAR, 
    STRUCT(
      id VARCHAR, 
      prim BOOLEAN
    )[]
  ), 
  _ACP_DATE DATE, 
  _ACP_BATCHID VARCHAR, 
);

