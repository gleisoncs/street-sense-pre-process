"""street-sense-pre-process — worker1 (geofence / map-matching).

Pre-filter step that runs BEFORE the main pipeline. For each MP4 it:

  1. consumes a job from Kafka topic `filter1`:
         {"media": "/i/gopro/GX010284.MP4", "region": "curitiba"}
  2. extracts the GoPro GPS track from the MP4 (same GPMF approach as
     street-sense-workers/worker1.py),
  3. map-matches the track against the named region's boundary: the file passes
     only if it HAS GPS and ALL of its points fall inside the region polygon,
  4. on success, produces the job for the main pipeline on Kafka `topic1`:
         {"media": "/i/gopro/GX010284.MP4", "model": "train4/weights/best.pt"}

Files with no GPS, or that stray outside the region, are dropped (logged, not
forwarded).

Message may override the model: {"media": ..., "region": ..., "model": "..."}.
Otherwise DEFAULT_MODEL (env) is used.
"""
import os
import json
import logging
from pathlib import Path
from datetime import timezone

import psycopg2
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

LOG_FILE = os.path.join(os.path.dirname(__file__), 'worker1.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
log = logging.getLogger('pre-process-worker1')

KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS', 'localhost:9092')
INPUT_TOPIC = os.environ.get('INPUT_TOPIC', 'filter1')
OUTPUT_TOPIC = os.environ.get('OUTPUT_TOPIC', 'topic1')
#DEFAULT_MODEL = os.environ.get('DEFAULT_MODEL', 'train4/weights/best.pt')
DEFAULT_MODEL = os.environ.get('DEFAULT_MODEL', 'yolov8s_1280_b12_e200/weights/best.pt')

# Fraction of GPS points that must fall inside the region. 1.0 = every point
# (strict, the default per spec). Lower it to tolerate a few stray fixes.
MIN_INSIDE_RATIO = float(os.environ.get('MIN_INSIDE_RATIO', '1.0'))

# Postgres holding the `regioes` table (osmnx boundary polygons).
DB_CONFIG = {
    'host':     os.environ.get('DB_HOST', 'localhost'),
    'port':     int(os.environ.get('DB_PORT', 5432)),
    'user':     os.environ.get('DB_USER', 'admin'),
    'password': os.environ.get('DB_PASSWORD', 'senha123'),
    'dbname':   os.environ.get('DB_NAME', 'mybank'),
}


# ---------------------------------------------------------------------------
# GPS extraction — same GPMF path as street-sense-workers/worker1.py
# ---------------------------------------------------------------------------
def extract_gps_points(video_path: str):
    """Return a list of (lat, lon) from the MP4's GoPro GPMF track, or []."""
    from gopro_overlay.ffmpeg import FFMPEG
    from gopro_overlay.ffmpeg_gopro import FFMPEGGoPro
    from gopro_overlay.gpmf.gpmf import GPMD
    from gopro_overlay.gpmf.visitors.gps import GPS5Visitor

    try:
        ffmpeg_gopro = FFMPEGGoPro(FFMPEG())
        recording = ffmpeg_gopro.find_recording(Path(video_path))
        if not recording.data:
            log.warning(f'[GPS] No GPMF stream in {video_path}')
            return []

        gpmd = GPMD.parse(recording.load_data())
        packets = []

        class _C:
            def collect(self, counter, components):
                if components.basetime is not None and components.points and components.timestamp is not None:
                    packets.append(components)

        gpmd.accept(GPS5Visitor(converter=_C().collect))
        if not packets:
            log.warning(f'[GPS] No GPS5 packets in {video_path}')
            return []

        points = []
        for pkt in packets:
            for pt in pkt.points:
                if pt.lat == 0.0 and pt.lon == 0.0:
                    continue
                points.append((float(pt.lat), float(pt.lon)))
        return points

    except Exception as e:
        log.error(f'[GPS] Extração falhou para {video_path}: {e}')
        return []


def track_inside_region(points, region_name: str):
    """(passed, inside, total, label) — map-match the track against the named
    region using the `regioes` table (PostGIS ST_Contains, real OSM polygons).

    The region name is matched against the bairro, or the city when there is no
    bairro (so 'curitiba' -> city row, 'campina do siqueira' -> bairro row).
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        # Match the region name against each row's OWN identifying name (the
        # field named by its tipo), accent- and case-insensitive — so 'parana'
        # matches the estado row, not a bairro that merely sits in Paraná.
        cur.execute(
            """
            SELECT id, cidade, bairro, estado
            FROM regioes
            WHERE unaccent(lower(%(q)s)) = unaccent(lower(
                CASE tipo
                    WHEN 'bairro' THEN bairro
                    WHEN 'cidade' THEN cidade
                    WHEN 'estado' THEN estado
                END
            ))
            ORDER BY (tipo = 'bairro') DESC, (tipo = 'cidade') DESC
            LIMIT 1
            """,
            {'q': region_name.strip()},
        )
        row = cur.fetchone()
        if not row:
            log.error(f'[REGION] região desconhecida na tabela regioes: "{region_name}"')
            return False, 0, len(points), None

        region_id, cidade, bairro, estado = row
        label = bairro or cidade or estado

        lons = [lon for _lat, lon in points]
        lats = [lat for lat, _lon in points]
        cur.execute(
            """
            SELECT
                count(*) FILTER (
                    WHERE ST_Contains(r.geom, ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326))
                ) AS inside,
                count(*) AS total
            FROM regioes r,
                 unnest(%s::float8[], %s::float8[]) AS p(lon, lat)
            WHERE r.id = %s
            """,
            (lons, lats, region_id),
        )
        inside, total = cur.fetchone()
        ratio = inside / total if total else 0.0
        return ratio >= MIN_INSIDE_RATIO, inside, total, label
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def start_worker1():
    log.info('=' * 60)
    log.info('      PRE-PROCESS WORKER 1: GEOFENCE / MAP-MATCHING')
    log.info(f'  {INPUT_TOPIC} -> {OUTPUT_TOPIC} | regions from table "regioes" @ {DB_CONFIG["host"]}')
    log.info('=' * 60)

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKERS,
        'group.id': 'pre-process-group-v1',
        'auto.offset.reset': 'earliest',
    })
    producer = Producer({'bootstrap.servers': KAFKA_BROKERS})
    consumer.subscribe([INPUT_TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error(f'[KAFKA] {msg.error()}')
                continue

            raw = msg.value().decode('utf-8')
            try:
                job = json.loads(raw)
                media = job['media']
                region = job['region']
            except (json.JSONDecodeError, TypeError, KeyError) as e:
                log.error(f'[MSG] payload inválido ({e}): {raw}')
                consumer.commit(msg)
                continue

            model = job.get('model', DEFAULT_MODEL)
            log.info(f'[JOB] media={media} region={region} model={model}')

            if not os.path.exists(media):
                log.error(f'[SKIP] arquivo não encontrado: {media}')
                consumer.commit(msg)
                continue

            points = extract_gps_points(media)
            if not points:
                log.warning(f'[SKIP] {media} — sem GPS, não encaminhado')
                consumer.commit(msg)
                continue

            passed, inside, total, label = track_inside_region(points, region)
            if not passed:
                log.info(f'[DROP] {media} — fora de "{label or region}" ({inside}/{total} pontos dentro)')
                consumer.commit(msg)
                continue

            out = json.dumps({'media': media, 'model': model})
            producer.produce(OUTPUT_TOPIC, value=out.encode('utf-8'))
            producer.flush()
            log.info(f'[PASS] {media} dentro de "{label}" ({inside}/{total}) -> {OUTPUT_TOPIC}: {out}')
            consumer.commit(msg)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    start_worker1()
