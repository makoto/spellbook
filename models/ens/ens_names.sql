{{config(
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "ens",
                                    \'["makoto"]\') }}')}}


SELECT
  resolver,
  node,
  name
FROM (
  SELECT
    resolver,
    node,
    name,
    ROW_NUMBER() OVER (PARTITION BY resolver, node ORDER BY block_number DESC, transaction_index DESC) AS seqno
  FROM (
    SELECT
      contract_address AS resolver,
      topic2 AS node,
      decode(unhex(SUBSTR(DATA, 128)), 'UTF-8') as name,
      block_number,
      index as transaction_index
    FROM
      ethereum.logs
    WHERE
      topic1 = "0xb7d29e911041e8d9b843369e890bcb72c9388692ba48b65ac54e7214c4c348f7"
    UNION ALL
    SELECT 
        contract_address AS resolver,
        node,
        _name as name, 
        call_block_number as block_number,
        0 as transaction_index
    FROM ethereumnameservice_ethereum.DefaultReverseResolver_call_setName AS trace_names
    WHERE
      node IS NOT NULL
      AND _name IS NOT NULL 
    ))
WHERE
  seqno = 1