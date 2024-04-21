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
#from datasette_gis_partial_path import (gis_partial_path_lat_sql, gis_partial_path_lng_sql)

chart_num = 0


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
                description="A template to run against each row to generate elevation input. Use {{ COL }} for columns. Input should be lat,lng|lat,lng&samples=200",
                validators=[DataRequired(message="Prompt is required.")],
                default=" ".join(["{{ %s }}" % c for c in text_columns]),
            )
            json_column = StringField(
                "Store JSON in column",
                description="To store full JSON from Google Maps API, enter a column name here",
                render_kw={
                    "placeholder": "Leave this blank if you only want to store latitude/longitude"
                },
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
        #  https://maps.googleapis.com/maps/api/elevation/json?address=URI-ENCODED-PLACENAME&key=b591350c2f9c48a7b7176660bbfd802a
        global chart_num
        chart_num+=1
        url = "https://maps.googleapis.com/maps/api/elevation/json"
        params = {
            "key": resolve_api_key(datasette, config),
            "limit": 1,
        }
        json_column = config.get("json_column")
        if not json_column:
            params["no_annotations"] = 1
        row = rows[0]
        input = config["input"]
        for key, value in row.items():
            input = input.replace("{{ %s }}" % key, str(value or "")).replace(
                "{{%s}}" % key, str(value or "")
            )
        params["path"] = input
        params["samples"] = "200"
        print(params);
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        distance = []
        distance = range(200)
        elevation = []

        print(data)
        if not data["results"]:
            raise ValueError("No results found for {}".format(input))
        for result in data["results"]:
            elevation.append(float(result["elevation"]))
        plt.plot(distance,elevation)
        plt.xlabel('Distance')
        plt.ylabel('Elevation')
        plt.title('Elevation Profile')
        plt.grid(True)
        plt.savefig('elevation_chart'+str(chart_num)+'.png')
        png_data = NULL
        with open('elevation_chart'+str(chart_num)+'.png', 'rb') as file:
            png_data = file.read()
        
        e_p = ""
        e_p = base64.b64encode(png_data).decode('utf-8')
        #Now! Place it in the column!
        result = '{"img_src": "data:image/png;base64,' + e_p + '"}'
        print("The result is " + str(result))
        update = {
            "elevation": result,
        }
        plt.clf()
        plt.cla()
        if json_column:
            update[json_column] = json.dumps(data)

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
