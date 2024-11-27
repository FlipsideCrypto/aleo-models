{% macro create_decode_128_udf() %}
    CREATE OR REPLACE FUNCTION {{ target.database }}.{{ target.schema }}.decode_u128_to_ascii(input STRING)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
    $$
        if (!INPUT) return null;
        
        // Remove any 'u128' suffix if present
        let cleanInput = INPUT.replace('u128', '').trim();
        
        // Convert the numeric string to a BigInt
        let num = BigInt(cleanInput);
        
        // Convert to ASCII
        let result = '';
        while (num > 0n) {
            result = String.fromCharCode(Number(num & 0xFFn)) + result;
            num = num >> 8n;
        }
        
        return result.trim();
    $$;
{% endmacro %}