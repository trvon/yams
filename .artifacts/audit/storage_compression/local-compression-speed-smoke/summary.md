# Local vs S3-Compatible Storage Benchmark

| Backend | Store (files/s) | Retrieve mean (ms) | Retrieve ops/s | Search mean (ms) | Search qps | Delete mean (ms) | Delete ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| local_compressed | 7.61 | 2171.09 | 0.46 | 1064.63 | 0.94 | 1066.49 | 0.94 |
| local_raw | 7.60 | 2197.76 | 0.46 | 1067.01 | 0.94 | 1071.99 | 0.93 |
