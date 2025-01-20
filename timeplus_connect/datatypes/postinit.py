from timeplus_connect.datatypes import registry, dynamic, geometric

dynamic.SHARED_DATA_TYPE = registry.get_from_name('array(string, string)')
dynamic.STRING_DATA_TYPE = registry.get_from_name('string')

# timeplusd doesn't support geometric type.
point = 'tuple(float64, float64)'
ring = f'array({point})'
polygon = f'array({ring})'
multi_polygon = f'array({polygon})'
geometric.POINT_DATA_TYPE = registry.get_from_name(point)
geometric.RING_DATA_TYPE = registry.get_from_name(ring)
geometric.POLYGON_DATA_TYPE = registry.get_from_name(polygon)
geometric.MULTI_POLYGON_DATA_TYPE = registry.get_from_name(multi_polygon)
