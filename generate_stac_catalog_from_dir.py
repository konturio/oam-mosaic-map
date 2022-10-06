from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pystac
import rasterio
from shapely.geometry import box, GeometryCollection, shape, mapping


# create catalog
reor20_catalog = pystac.Catalog(id='reor20', description='test catalog')

# collection requires spatial extent and temporal extent
creation_date = datetime.strptime('2016-04-18', '%Y-%m-%d')

spatial_extent = pystac.SpatialExtent([None, None, None, None])
temporal_extent = pystac.TemporalExtent([(creation_date, None)])
extent = pystac.Extent(spatial_extent, temporal_extent)

huston_collection = pystac.Collection(id='huston', description='Collection for huston', extent=extent)

reor20_catalog.add_child(huston_collection)

# collect images
curr_path = Path.cwd().joinpath('input')
all_cogs = list(curr_path.glob('*.tif'))

for cog in all_cogs:
    uri = str((cog.name).replace('.tif',''))
    print(uri)
    params = {}
    params['id'] = cog.name
    with rasterio.open(cog) as src:
        params['bbox'] = list(src.bounds)
        params['geometry'] = mapping(box(*params['bbox']))
    part_time = timedelta(minutes=int(str(cog.name)[17:21]))
    params['datetime'] = creation_date + part_time
    params['properties'] = {}

    item = pystac.Item(**params)
    asset = pystac.Asset(href=uri, title='Geotif', media_type=pystac.MediaType.GEOTIFF)
    item.add_asset(key='image', asset=asset)
    
    huston_collection.add_item(item)

huston_collection.update_extent_from_items()

reor20_catalog.normalize_hrefs('what-is-this')
reor20_catalog.validate_all()
reor20_catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)