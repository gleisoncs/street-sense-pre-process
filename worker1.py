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
DEFAULT_MODEL = os.environ.get('DEFAULT_MODEL', 'train4/weights/best.pt')

# Fraction of GPS points that must fall inside the region. 1.0 = every point
# (strict, the default per spec). Lower it to tolerate a few stray fixes.
MIN_INSIDE_RATIO = float(os.environ.get('MIN_INSIDE_RATIO', '1.0'))

# ---------------------------------------------------------------------------
# Regions — boundary polygons as [(lat, lon), ...] (ray-casting containment).
# Rectangular bounding boxes are good enough for a coarse city/state geofence;
# swap in a finer polygon (e.g. an official boundary) per region as needed.
# ---------------------------------------------------------------------------
def _bbox(lat_min, lat_max, lon_min, lon_max):
    return [(lat_min, lon_min), (lat_min, lon_max), (lat_max, lon_max), (lat_max, lon_min)]

# Curitiba municipal boundary — approximate polygon (lat, lon), tracing the
# elongated N–S shape of the city rather than a loose rectangle.
CURITIBA = [
    (-25.345, -49.355),  # NW
    (-25.345, -49.270),  # N
    (-25.360, -49.230),
    (-25.385, -49.195),  # NE
    (-25.430, -49.185),  # E
    (-25.490, -49.190),
    (-25.540, -49.205),  # SE
    (-25.585, -49.230),
    (-25.620, -49.270),  # S
    (-25.640, -49.300),
    (-25.635, -49.335),  # SW
    (-25.600, -49.360),
    (-25.545, -49.375),
    (-25.480, -49.385),  # W
    (-25.425, -49.380),
    (-25.385, -49.372),
]

REGIONS = {
    # city of Curitiba, Paraná, Brazil — approximate municipal boundary
    'curitiba': CURITIBA,
    # state of Paraná, Brazil (coarse bbox)
    'parana':   _bbox(-26.72, -22.52, -54.62, -48.02),
    'paraná':   _bbox(-26.72, -22.52, -54.62, -48.02),
}


def point_in_polygon(lat: float, lon: float, polygon) -> bool:
    """Ray-casting point-in-polygon. polygon: list of (lat, lon) vertices."""
    inside = False
    n = len(polygon)
    j = n - 1
    for i in range(n):
        lat_i, lon_i = polygon[i]
        lat_j, lon_j = polygon[j]
        if ((lon_i > lon) != (lon_j > lon)) and \
           (lat < (lat_j - lat_i) * (lon - lon_i) / (lon_j - lon_i) + lat_i):
            inside = not inside
        j = i
    return inside


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
    """(passed, inside, total) — does the track map-match the named region?"""
    polygon = REGIONS.get(region_name.lower().strip())
    if polygon is None:
        log.error(f'[REGION] região desconhecida: "{region_name}" '
                  f'(conhecidas: {", ".join(sorted(REGIONS))})')
        return False, 0, len(points)

    inside = sum(1 for lat, lon in points if point_in_polygon(lat, lon, polygon))
    total = len(points)
    ratio = inside / total if total else 0.0
    return ratio >= MIN_INSIDE_RATIO, inside, total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def start_worker1():
    log.info('=' * 60)
    log.info('      PRE-PROCESS WORKER 1: GEOFENCE / MAP-MATCHING')
    log.info(f'  {INPUT_TOPIC} -> {OUTPUT_TOPIC} | regions: {", ".join(sorted(REGIONS))}')
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

            passed, inside, total = track_inside_region(points, region)
            if not passed:
                log.info(f'[DROP] {media} — fora de "{region}" ({inside}/{total} pontos dentro)')
                consumer.commit(msg)
                continue

            out = json.dumps({'media': media, 'model': model})
            producer.produce(OUTPUT_TOPIC, value=out.encode('utf-8'))
            producer.flush()
            log.info(f'[PASS] {media} dentro de "{region}" ({inside}/{total}) -> {OUTPUT_TOPIC}: {out}')
            consumer.commit(msg)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    start_worker1()
