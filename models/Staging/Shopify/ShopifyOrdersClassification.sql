with cte1 as (
{% set table_name_query %}
{{set_table_name('%tempdatonfix%customerjourney%')}}
{% endset %}

{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
{% set results_list = [] %}
{% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
    SELECT
    REGEXP_EXTRACT(a.id, r'(\d+)$') AS order_id,
    a.createdat,
    g.source,
    g.medium,
    g.campaign,
    g.content,
    g.term,
    f.occurredat,
    f.id AS touch_id,
    f.referrerUrl,
    f.source AS source_main,
    f.sourcetype,
    CASE
      When Lower(g.source) = 'reviewology' then "Non-Organic"
      WHEN LOWER(f.sourcetype) = "seo" THEN "Organic"
      WHEN LOWER(g.medium) = "organic" THEN "Organic"
      WHEN f.sourcetype IS NULL AND g.source IS NULL AND g.medium IS NULL THEN "Organic"
    ELSE
    "Non-Organic"
  END
    AS order_type,
  IF
    (c.channelName = "Facebook", "Non-Organic", "") medium_internal,
    DENSE_RANK() OVER(PARTITION BY a.id ORDER BY _daton_batch_runtime) AS rnk
  FROM
    {{i}} a
  LEFT JOIN
    UNNEST(channelInformation) b
  LEFT JOIN
    UNNEST(channelDefinition) c
  LEFT JOIN
    UNNEST(moments) d
  LEFT JOIN
    UNNEST(d.edges) e
  LEFT JOIN
    UNNEST(e.node) f
  LEFT JOIN
    UNNEST(f.utmparameters) g
  WHERE
    c.channelName IN ("Online Store",
      "Facebook") QUALIFY rnk = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
)

SELECT
    createdat,
    order_id,
  IF
    (STRING_AGG(COALESCE(order_type,medium_internal),",") LIKE "%Non-Organic%","Non-Organic","Organic") AS order_type
  FROM
    cte1
  GROUP BY
    1,
    2