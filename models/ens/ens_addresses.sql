{{config(
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "ens",
                                    \'["makoto"]\') }}')}}

SELECT
  resolver,
  node,
  addr as address
FROM (
  SELECT
    contract_address AS resolver,
    tx_hash,
    topic1,
    topic2 as node,
    CONCAT("0x", SUBSTR(data, 27)) AS addr,
    ROW_NUMBER() OVER (PARTITION BY contract_address, topic2
    ORDER BY
      block_number DESC,
      index DESC) AS seqno
  FROM
    ethereum.logs
  WHERE
    topic1 = "0x52d7d861f09ab3d26239d492e8968629f95e9e318cf0b73bfddc441522a15fd2"
) as addrs
WHERE
  seqno = 1
