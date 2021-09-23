# jsonl2parquet
C# based JSONL to PARQUET converter is a sample implementation.

KNOWN LIMITATIONS: Currently, the project contains the following limiations:
1. JSON Arrays are not being handled in he best manner, each element in the array will become own column. 
   * For example: 
     * ```{"person": { "fname": "John", "lname": "Smith", "phones":["212.555.1212", "800.555.1212"]}}```
     * produces "phones[0]" and "phones[1]" columns in output
2. Changes in inferred data types are not being handled. 
   * For example:
     * ```{"price": 355}```
     * ```{"price": 345.22}```
Data type inferred from the first row will be Integer, however, inferred data type for row 2 is Float. This causes the subsequenct incompatible data values to be thrown out and reported as NULL.
