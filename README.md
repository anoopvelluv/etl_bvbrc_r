## Run Pipeline

Welcome to the ETL script to load BV-BRC genome files from the FTP path:  
🔗 https://www.bv-brc.org/docs/quick_references/ftp.html

To execute the pipeline manually, run the following command:

```bash
R etl_bvbrc_r/R/main.R


```
To automate the pipeline execution daily at 2:00 AM, add the following line to your crontab:
```cron
0 2 * * * Rscript /full/path/to/etl_bvbrc_r/R/main.R >> /full/path/to/logs/etl_bvbrc.log 2>&1

