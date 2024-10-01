{% macro create_udf_bulk_rest_api_v2() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(
        json OBJECT
    ) returns ARRAY api_integration = {% if target.name == "prod" %}
        aws_aleo_api AS 'https://gvmfebiq6g.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api' -- CHANGE TO PROD URL WHEN DEPLOYED
    {% else %}
        aws_aleo_api_dev AS 'https://gvmfebiq6g.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {%- endif %};
{% endmacro %}
