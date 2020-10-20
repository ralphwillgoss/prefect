# Prefect Mapping

## Mapping within a map
We expect `individual_preprocessing` to be parallelised, however it appears to run synchronously.  
If you run `demo.py` you'll see its behaviour.  
The func `expensive_computation` works as expected.