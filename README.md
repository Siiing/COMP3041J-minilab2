# COMP3041J Mini-Project 2 — Cloud Service Log Analytics

This folder contains **MapReduce-style** Python jobs (Hadoop Streaming–compatible) and a **Ray** script for degraded-service detection, plus a small **local runner** to test outputs without a cluster.

## Dataset

Place `Comp3041J MiniProject 2 Dataset.csv` in this directory (as provided by the course).

## Task 1 — Object storage (S3 or OSS)

Upload the CSV to your bucket. This repo cannot do that for you: use the AWS or Alibaba console/CLI with your own credentials. In the **group report**, record the bucket URI and briefly justify object storage for append-heavy / large log files.

## Task 2 — MapReduce (three jobs)

Mappers and reducer use **tab-separated** `key\tvalue` lines, standard for Hadoop Streaming.

| Output | Mapper | Reducer |
|--------|--------|---------|
| Request count by `service_name` | `mapreduce/mapper_request_count_by_service.py` | `mapreduce/reducer_sum.py` |
| Count `status_code >= 500` by service | `mapreduce/mapper_server_error_by_service.py` | `mapreduce/reducer_sum.py` |
| Slow requests (`response_time_ms > 800`) by `service,endpoint` | `mapreduce/mapper_slow_endpoint_count.py` | `mapreduce/reducer_sum.py` |

### Hadoop Streaming (example pattern)

Adjust paths, JAR name, and I/O to match your Lab 4 environment (local Hadoop, EMR, etc.):

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files mapreduce/mapper_request_count_by_service.py,mapreduce/reducer_sum.py \
  -mapper "python3 mapper_request_count_by_service.py" \
  -reducer "python3 reducer_sum.py" \
  -input s3://YOUR_BUCKET/path/logs.csv \
  -output s3://YOUR_BUCKET/path/out_request_count/
```

Repeat with the other two mappers and **different** `-output` directories. Save the **part-r-00000** (or merged) outputs for the report.

### Top 10 slow endpoints

After job 3, take the reducer output and sort by count descending, then take the first 10 lines. With this repo’s local runner:

```bash
python tools/run_local_mapreduce.py "Comp3041J MiniProject 2 Dataset.csv" mapreduce/mapper_slow_endpoint_count.py mapreduce/reducer_sum.py --top10
```

### Local test (no Hadoop)

```bash
cd /path/to/this/repo
python tools/run_local_mapreduce.py "Comp3041J MiniProject 2 Dataset.csv" mapreduce/mapper_request_count_by_service.py mapreduce/reducer_sum.py
python tools/run_local_mapreduce.py "Comp3041J MiniProject 2 Dataset.csv" mapreduce/mapper_server_error_by_service.py mapreduce/reducer_sum.py
python tools/run_local_mapreduce.py "Comp3041J MiniProject 2 Dataset.csv" mapreduce/mapper_slow_endpoint_count.py mapreduce/reducer_sum.py --top10
```

## Task 3 — Ray degraded services

Install Ray: `pip install -r requirements.txt`

```bash
python ray_degraded_services.py "Comp3041J MiniProject 2 Dataset.csv"
```

Rules implemented: for each service, **slow rate** > 20% (`response_time_ms > 800`), or **server error rate** > 10% (`status_code >= 500`), or **≥ 5** rows with `error_type == Timeout`. Output: `service_name,reason` (one line per satisfied reason).

## Deliverables checklist (course)

- Source (this repo or zip): MapReduce scripts, Ray script, **saved outputs**, README.
- **Group report** (template, anonymized PDF/Word).
- **Individual report** + **Brightspace quiz** (each student).
- Document **execution environment** (e.g. local / EC2 / Ray local) for each run in the report.

## GenAI policy

Use generated code as a **starting point**; you must understand, test, and describe your own design and results in your own words for the reports and quiz.
