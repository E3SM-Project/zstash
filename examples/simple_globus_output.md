Run: `REQUEST_SCOPES_EARLY: bool = True` => authenticate 1x on toy run, 0x on real run (i.e., it achieves our goal)

| Code block | Run |
| --- | --- |
| TOY: `check_state_files` | `INI_PATH: /home/ac.forsyth2/.zstash.ini does NOT exist.`, `GLOBUS_CFG: /home/ac.forsyth2/.globus-native-apps.cfg does NOT exist.`, `TOKEN_FILE: /home/ac.forsyth2/.zstash_globus_tokens.json does NOT exist.` |
| TOY: `get_local_endpoint_id` | `Added local_endpoint to ini file`, `Got local endpoint from NAME_TO_ENDPOINT_MAP` |
| TOY: `get_transfer_client_with_auth` | `No stored tokens found - starting authentication`, paste URL to web browser, Argonne prompt (no login), NERSC prompt (no login), Must add label (no default), "Allow", paste auth code to command line |
| TOY: `transfer_client.submit_transfer` try/except block | `Used fresh authentication - tokens now stored for next time` |
| `For toy_run, skipped_second_auth=` | `True` |
| REAL: `check_state_files` | `INI_PATH: /home/ac.forsyth2/.zstash.ini exists.`, `GLOBUS_CFG: /home/ac.forsyth2/.globus-native-apps.cfg does NOT exist.`, `TOKEN_FILE: /home/ac.forsyth2/.zstash_globus_tokens.json exists.` |
| REAL: `get_local_endpoint_id` | `Got local_endpoint from ini file`, `Got local endpoint from NAME_TO_ENDPOINT_MAP` (implies the value retreived was `None`...) |
| REAL: `get_transfer_client_with_auth` | `Found stored refresh token - using it` |
| REAL: `transfer_client.submit_transfer` try/except block | `Bypassed authentication entirely - used stored tokens!` |
| `For real_run, skipped_second_auth` | `True` |

After run:
```bash
cat /home/ac.forsyth2/.zstash.ini
# [local]
# globus_endpoint_uuid =

cat /home/ac.forsyth2/.globus-native-apps.cfg
# cat: /home/ac.forsyth2/.globus-native-apps.cfg: No such file or directory

cat /home/ac.forsyth2/.zstash_globus_tokens.json
# {
#   "transfer.api.globus.org": {
#     "access_token": "alphanumeric token here>",
#     "refresh_token": "<alphanumeric token here>",
#     "expires_at": <number here>
#   }
# }
```
