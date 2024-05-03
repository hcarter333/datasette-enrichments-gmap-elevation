#Follows model 6est in 
#https://github.com/datasette/datasette-enrichments-opencage/blob/main/datasette_enrichments_opencage/__init__.py
#original version is copied from the above link
from asyncio.windows_events import NULL
from tarfile import NUL
from datasette import hookimpl
from datasette_enrichments import Enrichment
from datasette.database import Database
from wtforms import (
    Form,
    StringField,
    TextAreaField,
    PasswordField,
)
from wtforms.validators import DataRequired
import httpx
import json
import secrets
import sqlite_utils
import matplotlib.pyplot as plt
import base64
import numpy as np
from scipy.stats import linregress
from plugins.datasette_gis_partial_path import (gis_partial_path_lat_sql, gis_partial_path_lng_sql)



@hookimpl
def register_enrichments(datasette):
    return [GmapElevationEnrichment()]


class GmapElevationEnrichment(Enrichment):
    name = "Google Maps API elevation"
    slug = "gm_api_elevation"
    description = "Return the elevation profile between two latitude/longitude points using Google Maps elevation API"
    batch_size = 1
    log_traceback = True

    async def get_config_form(self, datasette: "Datasette", db: Database, table: str):
        def get_text_columns(conn):
            db = sqlite_utils.Database(conn)
            return [
                key for key, value in db[table].columns_dict.items() if value == str
            ]

        text_columns = await db.execute_fn(get_text_columns)

        class ConfigForm(Form):
            input = TextAreaField(
                "Get Elevation along input path on Earth",
                description="A template to run against each row to generate elevation input. Use {{ COL }} for columns. Input should be lat,lng|lat,lng,pathlength_meters,samples",
                validators=[DataRequired(message="Prompt is required.")],
                default=" ".join(["{{ %s }}" % c for c in text_columns]),
            )

        def stash_api_key(form, field):
            if not hasattr(datasette, "_enrichments_gmap_elevation_stashed_keys"):
                datasette._enrichments_gmap_elevation_stashed_keys = {}
            key = secrets.token_urlsafe(16)
            datasette._enrichments_gmap_elevation_stashed_keys[key] = field.data
            field.data = key

        class ConfigFormWithKey(ConfigForm):
            api_key = PasswordField(
                "API key",
                description="Your Google Maps API key",
                validators=[
                    DataRequired(message="API key is required."),
                    stash_api_key,
                ],
            )

        plugin_config = datasette.plugin_config("datasette-enrichments-gmap-elevation") or {}
        api_key = plugin_config.get("api_key")

        return ConfigForm if api_key else ConfigFormWithKey

    async def enrich_batch(self, rows, datasette, db, table, pks, config):
        
        url = "https://maps.googleapis.com/maps/api/elevation/json"
        params = {
            "key": resolve_api_key(datasette, config),
            "limit": 1,
        }
        params["no_annotations"] = 1
        row = rows[0]
        input = config["input"]
        for key, value in row.items():
            input = input.replace("{{ %s }}" % key, str(value or "")).replace(
                "{{%s}}" % key, str(value or "")
            )
        eps = input.split("|")
        coords_st = eps[0] + "," + eps[1]
        coords = coords_st.split(",")
        print("Sending = " + str(coords[0]) + "," + str(coords[1]) + "|" + str(coords[2]) + "," + str(coords[3]) + " " + str(coords[4]) + " " + str(coords[5]))
        p_lat=gis_partial_path_lat_sql(str(coords[0]),str(coords[1]),str(coords[2]),str(coords[3]),str(coords[4]))
        p_lng=gis_partial_path_lng_sql(str(coords[0]),str(coords[1]),str(coords[2]),str(coords[3]),str(coords[4]))
        params["path"] = str(coords[0]) + "," + str(coords[1]) + "|" + str(p_lat) + "," + str(p_lng)
        params["samples"] = str(coords[5])
        print(params);
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()

        #Now! Place it in the column!
        update = {
            "json_elevation": json.dumps(data),
        }
        ids = [row[pk] for pk in pks]

        def do_update(conn):
            sqlite_utils.Database(conn)[table].update(ids, update, alter=True)

        await db.execute_write_fn(do_update)


class ApiKeyError(Exception):
    pass


def resolve_api_key(datasette, config):
    plugin_config = datasette.plugin_config("datasette-enrichments-gmap-elevation") or {}
    api_key = plugin_config.get("api_key")
    if api_key:
        return api_key
    # Look for it in config
    api_key_name = config.get("api_key")
    if not api_key_name:
        raise ApiKeyError("No API key reference found in config")
    # Look it up in the stash
    #                          datasette_enrichments_gmaps_api_stashed_keys
    if not hasattr(datasette, "_enrichments_gmap_elevation_stashed_keys"):
        raise ApiKeyError("No API key stash found")
    stashed_keys = datasette._enrichments_gmap_elevation_stashed_keys
    if api_key_name not in stashed_keys:
        raise ApiKeyError("No API key found in stash for {}".format(api_key_name))
    return stashed_keys[api_key_name]
