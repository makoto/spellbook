{{config(
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "ens",
                                    \'["makoto"]\') }}')}}

SELECT * FROM (
SELECT
  node,
  resolver
FROM (
  SELECT
    node,
    resolver,
    ROW_NUMBER() OVER (PARTITION BY node ORDER BY deployment DESC, block_number DESC, log_index DESC) AS seqno
  FROM (
    SELECT
      node,
      resolver,
      evt_block_number as block_number,
      evt_index as log_index,
      0 AS deployment
    FROM
      ethereumnameservice_ethereum.ENSRegistry_evt_NewResolver as a
    UNION ALL
    SELECT
      node,
      resolver,
      evt_block_number as block_number,
      evt_index as log_index,
      1 AS deployment
    FROM
      ethereumnameservice_ethereum.ENSRegistryWithFallback_evt_NewResolver as b
  )
  ORDER BY node
) WHERE seqno = 1 
) as a 
INNER JOIN ethereum.contracts as c ON a.resolver = c.address
