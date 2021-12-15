import pandas as pd
import numpy as np


def conversion_table(layout_file_PATH, output_PATH): ## generate conversion table
    layout = pd.read_json(f"{layout_file_PATH}", orient="index")
    from pyproj import Transformer
    transformer = Transformer.from_crs("epsg:3844", "WGS84")
    coords = layout[["x", "y"]]
    lat2, lon2 = transformer.transform(layout.y, layout.x)
    coords["x_WGS84"] = lat2
    coords["y_WGS84"] = lon2
    coords.to_json(f"{output_PATH}/conversion_table.json", orient="index")
    return


def raw_epsg3844_to_wgs84(x, y): ## conversion between coordinates
    from pyproj import Transformer
    transformer = Transformer.from_crs("epsg:3844", "WGS84")
    lat2, lon2 = transformer.transform(y, x)
    return [lat2, lon2]


def wgs84_to_3844(lat, lon): ## conversion between coordinates and node matching
    coords = pd.read_json("./storage/conversion_table.json", orient="index")
    idx = np.where((np.round(coords.x_WGS84, 5) == np.round(lat, 5)) & (np.round(coords.y_WGS84, 5) == np.round(lon, 5)) & ~(coords.index.str.contains("-A", regex=True)))
    node = coords.iloc[idx]
    return node.index[0], node.x, node.y


def epsg3844_to_wgs84(x, y): ## conversion between coordinates and node matching
    coords = pd.read_json("./storage/conversion_table.json", orient="index")
    idx = np.where((np.round(coords.x_WGS84, 4) == np.round(x, 4)) & (np.round(coords.y_WGS84, 4) == np.round(y, 4)) & ~(coords.index.str.contains("-A", regex=True)))
    node = coords.iloc[idx]
    return node.index[0], node.x_WGS84, node.y_WGS84

def get_geo_info(junction): ## conversion between coordinates and node matching
    coords = pd.read_json("./storage/conversion_table.json", orient="index")
    node = coords.loc[junction]
    return node.index[0], node.x_WGS84, node.y_WGS84, node.x, node.y

