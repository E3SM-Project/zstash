"""
String resources for Zstash
"""

main_usage = '''zstash <command> [<args>]

Available zstash commands:
  create     create new archive
  update     update existing archive
  extract    extract files from archive

For help with a specific command
  zstash command --help
'''

extract_md5_mismatch = """md5 mismatch for: {file}
md5 of extracted file: {extracted_md5}
md5 of original file:  {original_md5}"""
