- Case 1: `REQUEST_SCOPES_EARLY: bool = False` => authenticate 2x on toy run, 0x on real run
- Case 2: `REQUEST_SCOPES_EARLY: bool = True` => authenticate 1x on toy run, 1x on real run

| Code block | Case 1 | Case 2 |
| --- | --- | --- |
| TOY: `check_state_files` `INI_PATH`  | `/home/ac.forsyth2/.zstash.ini does NOT exist.` | Same |
| TOY: `check_state_files` `GLOBUS_CFG` | `/home/ac.forsyth2/.globus-native-apps.cfg does NOT exist.` | Same |
| TOY: `get_local_endpoint_id` | `Added local_endpoint to ini file`, `Got local endpoint from NAME_TO_ENDPOINT_MAP` | Same |
| TOY: `native_client_login`| Pasting URL brings us to "Allow" screen immediately, paste auth code at command line | Prompt (login NOT required) for Argonne, prompt (login NOT required) for NERSC, "Allow" screen, paste auth code at command line |
| TOY: `transfer_client.submit_transfer` try/except | Prompt (login required) for Argonne, prompt (login required) for NERSC, "Allow" screen, paste auth code at command line, `Consents added, please re-run the previous command to start transfer` | `Bypassed 2nd authentication.` |
| `For toy_run, skipped_second_auth=` | `False` | `True` |
| REAL: `check_state_files` `INI_PATH`  | `/home/ac.forsyth2/.zstash.ini exists.` | Same |
| REAL: `check_state_files` `GLOBUS_CFG` | `/home/ac.forsyth2/.globus-native-apps.cfg exists.` | Same |
| REAL: `get_local_endpoint_id` | `Got local_endpoint from ini file`, `Got local endpoint from NAME_TO_ENDPOINT_MAP` (implies the value retreived was `None`...) | Same |
| REAL: `native_client_login`| No logins or prompts | Pasting URL brings us to "Allow" screen immediately, paste auth code at command line |
| REAL: `transfer_client.submit_transfer` try/except | `Bypassed 2nd authentication.` | Same |
| `For real_run, skipped_second_auth=` | `True` | Same |
