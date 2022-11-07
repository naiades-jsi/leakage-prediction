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
    coords_84 = pd.read_csv("./storage/coords_elements_epanet.csv", index_col="id")
    idx = np.where(abs((np.round(coords_84.y, 5) - np.round(lat, 5))< 0.00005) & (abs(np.round(coords_84.x, 5) - np.round(lon, 5))< 0.00005) & ~(coords_84.index.str.contains("-A", regex=True)) & ~(coords_84.index.str.contains("-B", regex=True)))
    node = coords_84.iloc[idx].index[0]
    return node, coords.loc[node].x, coords.loc[node].y

def epsg3844_to_wgs84(x, y): ## conversion between coordinates and node matching
    coords_84 = pd.read_csv("./storage/coords_elements_epanet.csv", index_col="id")
    idx = np.where(abs((np.round(coords_84.y, 5) - np.round(y, 5))< 0.00005) & (abs(np.round(coords_84.x, 5) - np.round(x, 5))< 0.00005) & ~(coords_84.index.str.contains("-A", regex=True)) & ~(coords_84.index.str.contains("-B", regex=True)))
    node = coords_84.iloc[idx]
    return node.index[0], node.x.values[0], node.y.values[0]

def get_geo_info(junction): ## conversion between coordinates and node matching
    coords = pd.read_json("./storage/conversion_table.json", orient="index")
    node = coords.loc[junction]
    return node.index[0], node.x_WGS84, node.y_WGS84, node.x, node.y

