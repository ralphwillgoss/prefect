# Prefect Logging

[Prefect Reference Documentation](https://docs.prefect.io/core/concepts/logging.html)

## Environment Vars
These are required in order for the demo to run correctly:  
`set PREFECT__LOGGING__LEVEL=INFO`  
`set PREFECT__LOGGING__EXTRA_LOGGERS="['lib']"`  

## Notes
- I found logging behaviour differs between LocalExecutor and LocalDaskExecutor or DaskExecutor, test all
  - LocalExecutor can give the impression that the logging is working, when it might not
- Extra Loggers inherit prefects logging level, no way to have separate logging levels