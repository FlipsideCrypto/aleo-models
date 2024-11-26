CREATE OR REPLACE FUNCTION decode_u128_to_ascii(u128_value NUMBER)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('numpy')
HANDLER = 'decode_u128_to_ascii'
AS
$$
def decode_u128_to_ascii(u128_value):
    """
    Decode an unsigned 128-bit integer to an ASCII string with numpy.
    """
    binary_representation = bin(u128_value)[2:]
    padded_binary = binary_representation.zfill(128)
    chunks = [padded_binary[i:i+8] for i in range(0, len(padded_binary), 8)]
    ascii_characters = [chr(int(chunk, 2)) for chunk in chunks if int(chunk, 2) != 0]
    decoded_text = ''.join(ascii_characters)
    return decoded_text
$$;