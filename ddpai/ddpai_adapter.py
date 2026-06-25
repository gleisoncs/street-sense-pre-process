"""DDPai N3 Pro -> Street Sense pipeline adapter.

DDPai stores GPS separately from the video (NMEA `.gpx`, bundled in a tar with a
`.git` extension), and uses underscore filenames that break the workers' name
parsing. This adapter makes a DDPai clip consumable by the EXISTING
street-sense-workers pipeline with no changes to worker1-4:

  1. locate the matching GPS track for the MP4 (loose file, tar/*.git, or tmp),
  2. parse the DDPai NMEA ($GPRMC/$GPGGA) into GPS points,
  3. write them to Redis `gps\<clean>` in the SAME shape worker1 produces,
  4. copy the MP4 to a worker1-mounted dir under an underscore-free name,
  5. publish {media, model} to topic1.

worker1 then extracts frames as usual; its GoPro GPMF step finds nothing and
no-ops (the GPS is already in Redis), so worker2/3/4 run unchanged and worker4
inserts occurrences with coordinates.

Example:
    python ddpai_adapter.py \
        --media /i/erialdo/DCIM/200video/front/20260621152153_0060.mp4 \
        --gps-dir /i/erialdo/DCIM/203gps
"""
import argparse
import glob
import json
import os
import re
import shutil
import tarfile
from datetime import datetime, timezone

import redis as redislib
from confluent_kafka import Producer

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS', 'localhost:9092')
OUTPUT_TOPIC = os.environ.get('OUTPUT_TOPIC', 'topic1')
OUT_DIR = os.environ.get('OUT_DIR', '/ctmp')
DEFAULT_MODEL = os.environ.get('DEFAULT_MODEL', 'yolov8s_1280_b12_e200/weights/best.pt')
GPS_REDIS_TTL = int(os.environ.get('GPS_REDIS_TTL', 3600))


def clean_name(media_path: str) -> str:
    """Pipeline-safe name: basename without extension and without underscores
    (worker1/2/4 split frame names on '_', so the base must have none)."""
    base = os.path.splitext(os.path.basename(media_path))[0]
    return re.sub(r'[^0-9A-Za-z]', '', base)


def find_gps_bytes(media_path: str, gps_dir: str):
    """Return the raw DDPai gpx bytes for this MP4, searching loose / tar / tmp."""
    name = os.path.splitext(os.path.basename(media_path))[0]
    member = f'{name}.gpx'

    loose = os.path.join(gps_dir, member)
    if os.path.exists(loose):
        with open(loose, 'rb') as f:
            return f.read()

    for tarpath in sorted(glob.glob(os.path.join(gps_dir, 'tar', '*.git'))
                          + glob.glob(os.path.join(gps_dir, 'tar', '*.tar'))):
        try:
            with tarfile.open(tarpath) as tf:
                if member in tf.getnames():
                    print(f'[GPS] {member} encontrado em {os.path.basename(tarpath)}', flush=True)
                    return tf.extractfile(member).read()
        except tarfile.TarError:
            continue

    tmp = os.path.join(gps_dir, 'tmp', f'{name}_T.gpx')
    if os.path.exists(tmp):
        with open(tmp, 'rb') as f:
            return f.read()

    return None


def _nmea_deg(value: str, hemi: str) -> float:
    """ddmm.mmmm + hemisphere -> signed decimal degrees."""
    v = float(value)
    deg = int(v // 100)
    minutes = v - deg * 100
    dec = deg + minutes / 60.0
    return -dec if hemi in ('S', 'W') else dec


def parse_ddpai_nmea(raw: bytes):
    """Parse DDPai NMEA text into (points, start_iso).

    points: [{lat, lon, ele, time}] with naive UTC ISO timestamps — the same
    shape street-sense-workers/worker1 writes to Redis.
    """
    text = raw.decode('latin-1', errors='ignore')

    # altitude by UTC time from $GPGGA
    alt_by_t = {}
    for m in re.finditer(r'\$GPGGA,([^,]*),([^,]*),([NS]),([^,]*),([EW]),(\d+),[^,]*,[^,]*,([^,]*),', text):
        t = m.group(1)
        alt_by_t[t] = float(m.group(7)) if m.group(7) else 0.0

    points = []
    for m in re.finditer(
        r'\$GPRMC,([^,]*),([AV]),([^,]*),([NS]),([^,]*),([EW]),[^,]*,[^,]*,(\d{6})', text
    ):
        t, status, lat_s, ns, lon_s, ew, date_s = m.groups()
        if status != 'A' or not lat_s or not lon_s:
            continue
        try:
            hh, mm, ss = int(t[0:2]), int(t[2:4]), int(float(t[4:]))
            dd, mo, yy = int(date_s[0:2]), int(date_s[2:4]), 2000 + int(date_s[4:6])
            dt = datetime(yy, mo, dd, hh, mm, ss, tzinfo=timezone.utc)
        except (ValueError, IndexError):
            continue
        points.append({
            'lat': _nmea_deg(lat_s, ns),
            'lon': _nmea_deg(lon_s, ew),
            'ele': alt_by_t.get(t, 0.0),
            'time': dt.replace(tzinfo=None).isoformat(),
        })

    if not points:
        return [], None
    return points, points[0]['time']


def main():
    ap = argparse.ArgumentParser(description='DDPai MP4+gpx -> Street Sense pipeline')
    ap.add_argument('--media', required=True, help='DDPai MP4 path (as seen here, e.g. /i/.../front/X.mp4)')
    ap.add_argument('--gps-dir', required=True, help='DDPai 203gps dir (holds loose/.gpx, tar/, tmp/)')
    ap.add_argument('--model', default=DEFAULT_MODEL)
    a = ap.parse_args()

    if not os.path.exists(a.media):
        raise SystemExit(f'[ERRO] MP4 não encontrado: {a.media}')

    raw = find_gps_bytes(a.media, a.gps_dir)
    if raw is None:
        raise SystemExit(f'[ERRO] GPS (.gpx) não encontrado para {os.path.basename(a.media)}')

    points, start = parse_ddpai_nmea(raw)
    if not points:
        raise SystemExit('[ERRO] nenhum ponto GPS válido no gpx')

    name = clean_name(a.media)

    # 1) GPS to Redis, exactly like worker1 does
    r = redislib.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    r.set(f'gps\\{name}', json.dumps({'points': points, 'start': start}), ex=GPS_REDIS_TTL)
    print(f'[GPS] {len(points)} pontos -> Redis "gps\\{name}" (start={start})', flush=True)

    # 2) copy MP4 under an underscore-free name into the worker1-mounted dir
    os.makedirs(OUT_DIR, exist_ok=True)
    out_media = os.path.join(OUT_DIR, f'{name}.mp4')
    if os.path.abspath(out_media) != os.path.abspath(a.media):
        shutil.copy(a.media, out_media)
    print(f'[COPY] {a.media} -> {out_media}', flush=True)

    # 3) publish to topic1 for worker1
    producer = Producer({'bootstrap.servers': KAFKA_BROKERS})
    payload = json.dumps({'media': out_media, 'model': a.model})
    producer.produce(OUTPUT_TOPIC, value=payload.encode('utf-8'))
    producer.flush()
    print(f'[OK] {OUTPUT_TOPIC}: {payload}', flush=True)


if __name__ == '__main__':
    main()
