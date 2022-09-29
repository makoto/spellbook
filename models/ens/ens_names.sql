{{config(
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "ens",
                                    \'["makoto"]\') }}')}}


SELECT
  resolver,
  node as reverse_node,
  name,
  tx_hash,
  index
FROM (
  SELECT
    resolver,
    node,
    name,
    ROW_NUMBER() OVER (PARTITION BY resolver, node ORDER BY block_number DESC, index DESC) AS seqno,
    tx_hash,
    index
  FROM (
    SELECT
      contract_address AS resolver,
      topic2 AS node,
      decode(unhex(SUBSTR(DATA, 128)), 'UTF-8') as name,
      block_number,
      tx_hash,
      index
    FROM
    --   ethereum.logs
        {{source('ethereum', 'logs')}} logs
    WHERE
      -- NameChanged (index_topic_1 bytes32 _node, string _name) event
      topic1 = "0xb7d29e911041e8d9b843369e890bcb72c9388692ba48b65ac54e7214c4c348f7"
    UNION ALL
    SELECT 
        contract_address AS resolver,
        node,
        _name as name, 
        call_block_number as block_number,
        call_tx_hash as tx_hash,
        0 as index
    -- FROM ethereumnameservice_ethereum.DefaultReverseResolver_call_setName as trace_names
    FROM {{source('ethereumnameservice_ethereum', 'DefaultReverseResolver_call_setName')}} trace_names
    WHERE
      node IS NOT NULL
      AND _name IS NOT NULL 
    ))
WHERE
  seqno = 1