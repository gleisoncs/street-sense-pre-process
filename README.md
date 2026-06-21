# street-sense-pre-process

Geofence / map-matching pre-filter that runs **before** the main Street Sense
pipeline. It decides whether a given MP4 should be processed at all, based on
*where it was filmed*.

```
Kafka filter1  →  worker1 (this)  →  Kafka topic1  →  street-sense-workers
{media, region}    GPS + geofence     {media, model}
```

## What worker1 does

For each job on Kafka topic **`filter1`**:

```json
{"media": "/i/gopro/GX010284.MP4", "region": "curitiba"}
```

1. Extracts the GoPro **GPS track** from the MP4's GPMF stream (same approach as
   `street-sense-workers/worker1.py`).
2. **Map-matches** the track against the named region's boundary polygon: the
   file passes only if it **has GPS** and **all** of its points fall inside the
   region (configurable via `MIN_INSIDE_RATIO`).
3. On success, produces the job for the main pipeline on Kafka **`topic1`**:

```json
{"media": "/i/gopro/GX010284.MP4", "model": "train4/weights/best.pt"}
```

Files with no GPS, or whose track strays outside the region, are **dropped**
(logged, not forwarded).

The message may override the model with a `"model"` field; otherwise
`DEFAULT_MODEL` (env) is used.

## Regions

Boundaries are polygons in `REGIONS` (worker1.py), checked with ray-casting
point-in-polygon. Built in:

| region | area |
|--------|------|
| `curitiba` | Curitiba + immediate metro, Paraná, BR |
| `parana` / `paraná` | state of Paraná, BR |

They are rectangular bounding boxes (coarse geofence); replace a region's
polygon with an official boundary for tighter matching.

## Run

```bash
pip install -r requirements.txt   # needs ffmpeg/ffprobe on PATH
cp .env.example .env
python worker1.py
```

Or via Docker (ffmpeg baked in):

```bash
docker build -t street-sense-pre-process .
docker run --rm --env-file .env street-sense-pre-process
```

## Config (env)

| var | default | meaning |
|-----|---------|---------|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `INPUT_TOPIC` | `filter1` | jobs in: `{media, region}` |
| `OUTPUT_TOPIC` | `topic1` | jobs out: `{media, model}` |
| `DEFAULT_MODEL` | `train4/weights/best.pt` | model when the job omits one |
| `MIN_INSIDE_RATIO` | `1.0` | fraction of points required inside (1.0 = all) |

## Manual test

```bash
# produce a job (run on the Kafka box)
echo '{"media":"/i/gopro/GX010284.MP4","region":"curitiba"}' | \
  /opt/kafka/bin/kafka-console-producer.sh --topic filter1 --bootstrap-server localhost:9092
```
