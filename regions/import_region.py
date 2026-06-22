"""Research a region boundary with osmnx and load it into the `regioes` table.

Pipeline: osmnx geocode -> GeoJSON -> ogr2ogr -> PostGIS. Re-importing the same
region replaces it (delete-by-key, then append), so it is safe to re-run and to
add new cities/neighborhoods later.

Examples:
    python import_region.py --query "Curitiba, Paraná, Brazil" \
        --tipo cidade --cidade Curitiba --estado "Paraná" --pais Brazil

    python import_region.py --query "Campina do Siqueira, Curitiba, Paraná, Brazil" \
        --tipo bairro --bairro "Campina do Siqueira" --cidade Curitiba \
        --estado "Paraná" --pais Brazil
"""
import argparse
import os
import subprocess

import osmnx as ox

# osmnx moved geocode_to_gdf around across versions; resolve it either way.
try:
    geocode_to_gdf = ox.geocode_to_gdf
except AttributeError:  # osmnx >= 2.0
    from osmnx import geocoder
    geocode_to_gdf = geocoder.geocode_to_gdf

PG_DSN = os.environ.get('PG_DSN', 'host=postgres port=5432 user=admin password=senha123 dbname=mybank')
GEOJSON = '/tmp/region.geojson'


def sql_quote(v: str) -> str:
    return v.replace("'", "''")


def main():
    ap = argparse.ArgumentParser(description='Import an osmnx region boundary into regioes')
    ap.add_argument('--query', required=True, help='Nominatim query, e.g. "Curitiba, Paraná, Brazil"')
    ap.add_argument('--tipo', required=True, choices=['estado', 'cidade', 'bairro'])
    ap.add_argument('--cidade', required=True)
    ap.add_argument('--bairro', default=None)
    ap.add_argument('--estado', required=True)
    ap.add_argument('--pais', required=True)
    ap.add_argument('--nome', default=None, help='display name (defaults to --query)')
    a = ap.parse_args()

    print(f'[osmnx] geocoding: {a.query}', flush=True)
    gdf = geocode_to_gdf(a.query)

    osm_id = None
    for c in ('osm_id', 'osmid'):
        if c in gdf.columns:
            try:
                osm_id = int(gdf.iloc[0][c])
            except (TypeError, ValueError):
                osm_id = None
            break

    gdf = gdf[['geometry']].copy()
    gdf['nome'] = a.nome or a.query
    gdf['tipo'] = a.tipo
    gdf['bairro'] = a.bairro
    gdf['cidade'] = a.cidade
    gdf['estado'] = a.estado
    gdf['pais'] = a.pais
    gdf['osm_id'] = osm_id

    gdf.to_file(GEOJSON, driver='GeoJSON')
    print(f'[geojson] wrote {GEOJSON} ({len(gdf)} feature)', flush=True)

    pg = f'PG:{PG_DSN}'

    # delete-by-key so a re-import replaces the region instead of duplicating
    bairro_pred = f"bairro = '{sql_quote(a.bairro)}'" if a.bairro else 'bairro IS NULL'
    delete_sql = (
        f"DELETE FROM regioes WHERE tipo='{sql_quote(a.tipo)}' "
        f"AND pais='{sql_quote(a.pais)}' AND estado='{sql_quote(a.estado)}' "
        f"AND cidade='{sql_quote(a.cidade)}' AND {bairro_pred}"
    )
    subprocess.run(['ogrinfo', pg, '-sql', delete_sql], check=True)

    subprocess.run([
        'ogr2ogr', '-f', 'PostgreSQL', pg, GEOJSON,
        '-nln', 'regioes', '-append', '-update',
        '-nlt', 'PROMOTE_TO_MULTI',
        '-a_srs', 'EPSG:4326',
    ], check=True)

    print(f'[OK] {a.tipo} carregado: {a.cidade}'
          + (f' / {a.bairro}' if a.bairro else '')
          + f' ({a.estado}, {a.pais})', flush=True)


if __name__ == '__main__':
    main()
