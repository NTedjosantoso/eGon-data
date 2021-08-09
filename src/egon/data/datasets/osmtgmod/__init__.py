import sys
import configparser
import psycopg2
import os
import csv
import datetime
import logging
import codecs
import shutil
from pathlib import Path
import egon.data.config
from egon.data.config import settings
import egon.data.subprocess as subproc
from egon.data import db
from egon.data.datasets import Dataset
from airflow.operators.postgres_operator import PostgresOperator
import importlib_resources as resources


def run():

    # execute osmTGmod

    data_config = egon.data.config.datasets()
    osm_config = data_config["openstreetmap"]["original_data"]

    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        target_path = osm_config["target"]["path"]
    else:
        target_path = osm_config["target"]["path_testmode"]

    filtered_osm_pbf_path_to_file = os.path.join(
        egon.data.__path__[0], "datasets", "osm", target_path
    )
    docker_db_config = db.credentials()

    osmtgmod(
        config_database=docker_db_config["POSTGRES_DB"],
        config_basepath="osmTGmod/egon-data",
        config_continue_run=False,
        filtered_osm_pbf_path_to_file=filtered_osm_pbf_path_to_file,
        docker_db_config=docker_db_config,
    )


def import_osm_data():

    osmtgmod_repos = Path(".") / "osmTGmod"

    # Delete repository if it already exists
    if osmtgmod_repos.exists() and osmtgmod_repos.is_dir():
        shutil.rmtree(osmtgmod_repos)

    subproc.run(
        [
            "git",
            "clone",
            "--single-branch",
            "--branch",
            "features/egon",
            "https://github.com/openego/osmTGmod.git",
        ]
    )

    data_config = egon.data.config.datasets()
    osm_config = data_config["openstreetmap"]["original_data"]

    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        target_path = osm_config["target"]["path"]
    else:
        target_path = osm_config["target"]["path_testmode"]

    filtered_osm_pbf_path_to_file = os.path.join(
        egon.data.__path__[0], "datasets", "osm", target_path
    )

    docker_db_config=db.credentials()
    config_database=docker_db_config["POSTGRES_DB"]
    config_basepath="osmTGmod/egon-data"

    config = configparser.ConfigParser()
    config.read(config_basepath + ".cfg")
    config["postgres_server"]["host"] = docker_db_config["HOST"]
    config["postgres_server"]["port"] = docker_db_config["PORT"]
    config["postgres_server"]["user"] = docker_db_config[
            "POSTGRES_USER"]
    config["postgres_server"]["password"] = docker_db_config[
            "POSTGRES_PASSWORD"]

    logging.info("Creating status table ...")
    db.execute_sql(
        """
        DROP TABLE IF EXISTS _db_status;
        CREATE TABLE _db_status (module TEXT, status BOOLEAN);
        INSERT INTO _db_status (module, status) VALUES ('grid_model', FALSE);
        """
        )

    logging.info("Status table created.")

    # egon-specific, in order to not fill up the results schema,
    # it is dropped before creation
    logging.info("Dropping osmtgmod_results schema if exists")
    db.execute_sql("DROP SCHEMA IF EXISTS osmtgmod_results CASCADE;")

    logging.info("Loading functions and result schema ...")
    scripts = [
            "sql-scripts/extensions.sql",
            "sql-scripts/functions.sql",
            "sql-scripts/admin_boundaries.sql",
            "sql-scripts/electrical_properties.sql",
            "sql-scripts/build_up_db.sql",
        ]
    for script in scripts:
            logging.info("Running script {0} ...".format(script))
            with codecs.open(
                    os.path.join("osmTGmod",
                                 script), "r", "utf-8-sig") as fd:
                sqlfile = fd.read()
            db.execute_sql(sqlfile)
            logging.info("Done.")

    db.execute_sql(
            """UPDATE _db_status SET status = TRUE
            WHERE module = 'grid_model'; """
        )


    logging.info("osmTGmod-database successfully built up!")

    logging.info("Importing OSM-data to database.")

    logging.info("Using pdf file: {}".format(filtered_osm_pbf_path_to_file))
    logging.info(
        f"""Assuming osmosis is avaliable at
        {config['osm_data']['osmosis_path_to_binary']}"""
        )

    # create directory to store osmosis' temp files
    osmosis_temp_dir = Path('osmTGmod') / "osmosis_temp/"
    if not os.path.exists(osmosis_temp_dir):
        os.mkdir(osmosis_temp_dir)

    subproc.run(
            "JAVACMD_OPTIONS='%s' %s --read-pbf %s --write-pgsql \
                database=%s host=%s user=%s password=%s"
            % (
                f"-Djava.io.tmpdir={osmosis_temp_dir}",
                os.path.join("osmTGmod",
                             config["osm_data"]["osmosis_path_to_binary"]),
                filtered_osm_pbf_path_to_file,
                config_database,
                config["postgres_server"]["host"]
                + ":"
                + config["postgres_server"]["port"],
                config["postgres_server"]["user"],
                config["postgres_server"]["password"],
            ),
            shell=True,
        )
    logging.info("Importing OSM-Data...")

    # After updating OSM-Data, power_tables (for editing)
    # have to be updated as well
    logging.info("Creating power-tables...")
    db.execute_sql("SELECT otg_create_power_tables ();")

    # Update OSM Metadata
    logging.info("Updating OSM metadata")
    v_date = datetime.datetime.now().strftime("%Y-%m-%d")
    db.execute_sql(f"UPDATE osm_metadata SET imported = '{v_date}'")
    logging.info("OSM data imported to database successfully.")


def osmtgmod(
    config_database="egon-data",
    config_basepath="osmTGmod/egon-data",
    config_continue_run=False,
    filtered_osm_pbf_path_to_file=None,
    docker_db_config=None,
):

    if 'germany-21' in filtered_osm_pbf_path_to_file:
        """
        Manually add under construction substation expansion in Garenfeld
        to existing substation. (see:)
        """
        print('Manually updating geometry of substation in Garenfeld')
        db.execute_sql(
            """DROP TRIGGER IF EXISTS
            power_ways_update ON power_ways CASCADE """)

        db.execute_sql(
            """
            UPDATE power_ways
            SET way =  (SELECT ST_SetSRID(ST_AsText(
                '0102000000160000001612D5004A081E4020A8644A35B349407B0ACA'
                '7E27071E405F23EE563BB34940287CB60E0E061E4055A4C2D842B34940352FE29'
                '6EA051E4017940E7B46B34940C0D02346CF051E4042EBE1CB44B34940D67E219A'
                '2F051E40FECF06054AB349407F964A442F031E40C2F441F471B34940A8A544676'
                '1021E40AB9412CA8FB349409C4848881E021E40B7BA08C691B34940B22D4E1430'
                '001E40CE913856BDB34940E2810B122C001E40898CAEAFDBB349402CDAF043480'
                '11E40ED678C32F0B349402FE640E25C041E405A86F21AF1B3494061D525C46F04'
                '1E40ABEF60C892B34940DC2F9FAC18061E400D33D9E495B349401FD7868A71061'
                'E40D2D8A89894B3494083932353F4061E40077360DE88B34940624ED02687071E'
                '404F08782D7CB349405000C5C892091E403EFBDBAF4CB349403DDBFEF04E091E4'
                '0658D7A8846B349405AD5928E72081E405BE8EF4A37B349401612D5004A081E40'
                '20A8644A35B34940'), 4326))
            WHERE name = 'Garenfeld'
            AND id = 24667346
            """)

    # ==============================================================
    # Setup logging
    # ==============================================================
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    logformat = logging.Formatter(
        "%(asctime)s %(message)s", "%m/%d/%Y %H:%M:%S"
    )
    sh = logging.StreamHandler()
    sh.setFormatter(logformat)
    log.addHandler(sh)
    logging.info(
        "\n\n======================\nego_otg\n======================")
    logging.info("Logging to standard output...")
    # catch up some log messages from evaluation of command line arguments
    logging.info("Database: {}".format(config_database))
    logging.info(
        "Path for configuration file and results: {}"
        .format(config_basepath)
    )
    # ==============================================================
    # read configuration from file and create folder structure
    # ==============================================================
    logging.info(
            (
                "Taking db connection credentials from eGon-data "
                "with respect to the given docker_db_config variable"
            )
        )
    config = configparser.ConfigParser()
    config.read(config_basepath + ".cfg")
    config["postgres_server"]["host"] = docker_db_config["HOST"]
    config["postgres_server"]["port"] = docker_db_config["PORT"]
    config["postgres_server"]["user"] = docker_db_config[
            "POSTGRES_USER"]
    config["postgres_server"]["password"] = docker_db_config[
            "POSTGRES_PASSWORD"]

    # Setting osmTGmod folder structure:
    logging.info("Checking/Creating file directories")
    input_data_dir = os.path.join(config_basepath, "input_data")
    result_dir = os.path.join(config_basepath, "results")
    # Basic folders are created if not existent
    if not os.path.exists(input_data_dir):
        os.makedirs(input_data_dir)
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    # start logging to file
    logfile = os.path.join(config_basepath, config_database + ".log")
    fh = logging.FileHandler(logfile)
    fh.setFormatter(logformat)
    log.addHandler(fh)
    logging.info("Logging to file '{0}' is set up".format(logfile))
    logging.info(
        "Now logging both to standard output and to file '{0}'...".format(
            logfile
        )
    )
    logging.info(
        "\n\n======================\nego_otg\n======================")
    # copy config file
    logging.info(
        "Copying configuration file to '{0}'.".format(
            os.path.join(config_basepath, config_database + ".cfg")
        )
    )
    os.system(
        "cp {0} {1}".format(
            config_basepath + ".cfg",
            os.path.join(config_basepath, config_database + ".cfg"),
        )
    )

    # Connects to new Database
    logging.info("Connecting to database {} ..."
                     .format(config_database))
    conn = psycopg2.connect(
            host=config["postgres_server"]["host"],
            port=config["postgres_server"]["port"],
            database=config_database,
            user=config["postgres_server"]["user"],
            password=config["postgres_server"]["password"],
        )

    cur = conn.cursor()

    min_voltage = 110000

    if not config_continue_run:
        logging.info("Setting min_voltage...")
        cur.execute(
            """
            UPDATE abstr_values
                SET val_int = %s
                WHERE val_description = 'min_voltage'""",
            (min_voltage,),
        )
        conn.commit()

        logging.info("Setting main_station...")
        cur.execute(
            """
            UPDATE abstr_values
                SET val_int = %s
                WHERE val_description = 'main_station'""",
            (config.getint("abstraction", "main_station"),),
        )
        conn.commit()

        logging.info("Setting graph_dfs...")
        cur.execute(
            """
            UPDATE abstr_values
                SET val_bool = %s
                WHERE val_description = 'graph_dfs'""",
            (config.getboolean("abstraction", "graph_dfs"),),
        )
        conn.commit()

        logging.info("Setting conn_subgraphs...")
        cur.execute(
            """
            UPDATE abstr_values
                SET val_bool = %s
                WHERE val_description = 'conn_subgraphs'""",
            (config.getboolean("abstraction", "conn_subgraphs"),),
        )
        conn.commit()

        logging.info("Setting transfer_busses...")
        cur.execute(
            """
            UPDATE abstr_values
                SET val_bool = %s
                WHERE val_description = 'transfer_busses'""",
            (config.getboolean("abstraction", "transfer_busses"),),
        )
        conn.commit()

        # setting transfer busses
        path_for_transfer_busses = input_data_dir + "/transfer_busses.csv"
        logging.info(
            "Reading transfer busses from file {} ...".format(
                path_for_transfer_busses
            )
        )
        logging.info("Deleting all entries from transfer_busses table ...")
        cur.execute(
            """
        DELETE FROM transfer_busses;
        """
        )
        conn.commit()

        # using the base egon database for transfer bus creation.
        cur.execute(
            """
        DROP TABLE if exists transfer_busses_complete;
        CREATE TABLE transfer_busses_complete as
        SELECT DISTINCT ON (osm_id) * FROM
        (SELECT * FROM grid.egon_ehv_substation
        UNION SELECT bus_id, lon, lat, point, polygon, voltage,
        power_type, substation, osm_id, osm_www, frequency, subst_name,
        ref, operator, dbahn, status
        FROM grid.egon_hvmv_substation ORDER BY osm_id) as foo;
        """
        )
        conn.commit()

        with open(path_for_transfer_busses, "w") as this_file:
            cur.copy_expert(
                """COPY transfer_busses_complete to
                STDOUT WITH CSV HEADER""",
                this_file,
            )
            conn.commit()

        reader = csv.reader(open(path_for_transfer_busses, "r"))
        next(reader, None)  # Skips header
        logging.info("Copying transfer-busses from CSV to database...")
        for row in reader:
            osm_id = str(row[8])
            if osm_id[:1] == "w":
                object_type = "way"
            elif osm_id[:1] == "n":
                object_type = "node"
            else:
                object_type = None
            osm_id_int = int(osm_id[1:])
            center_geom = str(row[3])
            cur.execute(
                """
                INSERT INTO transfer_busses (osm_id, object_type,
                                             center_geom)
                VALUES (%s, %s, %s);
                """,
                (osm_id_int, object_type, center_geom),
            )
            conn.commit()
        logging.info("All transfer busses imported successfully")

    # Execute power_script
    logging.info(
        (
            "Preparing execution of abstraction script "
            "'sql-scripts/power_script.sql' ..."
        )
    )
    with codecs.open("osmTGmod/sql-scripts/power_script.sql", "r",
                     "utf-8-sig") as fd:
        sqlfile = fd.read()
    # remove lines starting with "--" (comments), tabulators and empty line
    # beware: comments in C-like style (such as /* comment */) arn't parsed!
    sqlfile_without_comments = "".join(
        [
            line.lstrip().split("--")[0] + "\n"
            if not line.lstrip().split("--")[0] == ""
            else ""
            for line in sqlfile.split("\n")
        ]
    )

    logging.info("Stating execution of  power script...")
    config_continue_run_at = -1

    if not config_continue_run:  # debugging - to be removed
        cur.execute(
            """drop table if exists debug;create table debug
            (step_before int,max_bus_id int, num_bus int,max_branch_id int,
            num_branch int, num_110_bus int, num_220_bus int,
            num_380_bus int)"""
        )
        conn.commit()

    # split sqlfile in commands seperated by ";", while not considering
    # symbols for splitting if "escaped" by single quoted strings.
    # Drop everything after last semicolon.
    for i, command in enumerate(
        "'".join(
            [
                segment.replace(";", "§") if i % 2 == 0 else segment
                for i, segment in enumerate(
                    sqlfile_without_comments.split("'")
                )
            ]
        ).split("§")[:-1]
    ):

        if i >= config_continue_run_at:
            logging.info(
                "Executing SQL statement {0}:{1}\n".format(i, command)
            )
            try:
                cur.execute(command)
                conn.commit()
            except:
                logging.exception(
                    (
                        "Exception raised with command {0}. "
                        "Check data and code "
                        "and restart with 'python ego_otg.py {1} {0}'."
                    ).format(i, config_database)
                )
                sys.exit()
            if i > 16:  # debugging - to be removed
                cur.execute(
                    """insert into debug values ({0},
                    (select max(id) from bus_data),(select count(*)
                    from bus_data),(select max(branch_id)
                    from branch_data),(select count(*)
                    from branch_data),(select count(*)
                    from bus_data where voltage = 110000),
                    (select count (*) from bus_data where voltage = 220000),
                    (select count (*)
                    from bus_data where voltage = 380000))""".format(
                        i
                    )
                )
                conn.commit()

    logging.info("Power-script executed successfully.")

    logging.info("Saving Results...")
    cur.execute("SELECT otg_save_results ();")
    conn.commit()

    logging.info("Abstraction process complete!")

    # ==============================================================
    # Write results
    # ==============================================================
    logging.info("Writing results")

    tables = ["bus_data", "branch_data", "dcline_data", "results_metadata"]
    for table in tables:
        logging.info("writing %s..." % table)
        filename = os.path.join(result_dir, table + ".csv")
        logging.info(
            "Writing contents of table {0} to {1}...".format(table,
                                                             filename)
        )
        query = "SELECT * FROM osmtgmod_results.%s " % (table,)
        outputquery = "COPY ({0}) TO STDOUT WITH DELIMITER \
            ',' CSV HEADER".format(
            query
        )
        with open(filename, encoding="utf-8", mode="w") as fh:
            cur.copy_expert(outputquery, fh)

    logging.info("All tables written!")

    logging.info("EXECUTION FINISHED SUCCESSFULLY!")


def to_pypsa(version="'0.0.0'"):
    db.execute_sql(
            f"""
            -- CLEAN UP OF TABLES
            DELETE FROM grid.egon_etrago_bus
            WHERE version = {version}
            AND carrier = 'AC';
            DELETE FROM grid.egon_etrago_line
            WHERE version = {version};
            DELETE FROM grid.egon_etrago_transformer
            WHERE version = {version};
            """
            )

    for scenario_name in ["'eGon2035'", "'eGon100RE'"]:

        db.execute_sql(
            f"""
            -- BUS DATA
            INSERT INTO grid.egon_etrago_bus (version, scn_name, bus_id, v_nom,
                                             geom, x, y, carrier)
            SELECT
              {version},
              {scenario_name},
              bus_i AS bus_id,
              base_kv AS v_nom,
              geom,
              ST_X(geom) as x,
              ST_Y(geom) as y,
              'AC' as carrier
              FROM osmtgmod_results.bus_data
              WHERE result_id = 1;


            -- BRANCH DATA
            INSERT INTO grid.egon_etrago_line (version, scn_name, line_id, bus0,
                                              bus1, x, r, b, s_nom, cables, v_nom,
                                              geom, topo)
            SELECT
              {version},
              {scenario_name},
              branch_id AS line_id,
              f_bus AS bus0,
              t_bus AS bus1,
              br_x AS x,
              br_r AS r,
              br_b as b,
              rate_a as s_nom,
              cables,
              branch_voltage/1000 as v_nom,
              geom,
              topo
              FROM osmtgmod_results.branch_data
              WHERE result_id = 1 and (link_type = 'line' or
                                       link_type = 'cable');


            -- TRANSFORMER DATA
            INSERT INTO grid.egon_etrago_transformer (version, scn_name,
                                                     trafo_id, bus0, bus1, x,
                                                     s_nom, tap_ratio,
                                                     phase_shift, geom, topo)
            SELECT
              {version},
              {scenario_name},
              branch_id AS trafo_id,
              f_bus AS bus0,
              t_bus AS bus1,
              br_x/100 AS x,
              rate_a as s_nom,
              tap AS tap_ratio,
              shift AS phase_shift,
              geom,
              topo
              FROM osmtgmod_results.branch_data
              WHERE result_id = 1 and link_type = 'transformer';


            -- per unit to absolute values

            UPDATE grid.egon_etrago_line a
            SET
                 r = r * (((SELECT v_nom
                            FROM grid.egon_etrago_bus b
                            WHERE bus_id=bus1
                            AND a.scn_name = b.scn_name
                            )*1000)^2 / (100 * 10^6)),
                 x = x * (((SELECT v_nom
                            FROM grid.egon_etrago_bus b
                            WHERE bus_id=bus1
                            AND a.scn_name = b.scn_name
                            )*1000)^2 / (100 * 10^6)),
                 b = b * (((SELECT v_nom
                            FROM grid.egon_etrago_bus b
                            WHERE bus_id=bus1
                            AND a.scn_name = b.scn_name
                            )*1000)^2 / (100 * 10^6));

            -- calculate line length (in km) from geoms

            UPDATE grid.egon_etrago_line a
            SET
                 length = result.length
                 FROM
                 (SELECT b.line_id, st_length(b.geom,false)/1000 as length
                  from grid.egon_etrago_line b)
                 as result
            WHERE a.line_id = result.line_id;


            -- delete buses without connection to AC grid and generation or
            -- load assigned

            DELETE FROM grid.egon_etrago_bus
            WHERE scn_name={scenario_name}
            AND carrier = 'AC'
            AND version = {version}
            AND bus_id NOT IN
            (SELECT bus0 FROM grid.egon_etrago_line WHERE
             scn_name={scenario_name})
            AND bus_id NOT IN
            (SELECT bus1 FROM grid.egon_etrago_line WHERE
             scn_name={scenario_name})
            AND bus_id NOT IN
            (SELECT bus0 FROM grid.egon_etrago_transformer
             WHERE scn_name={scenario_name})
            AND bus_id NOT IN
            (SELECT bus1 FROM grid.egon_etrago_transformer
             WHERE scn_name={scenario_name});
                """
        )

class Osmtgmod(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="Osmtgmod",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(
                import_osm_data,
                run,
               # {
                    PostgresOperator(
                        task_id="osmtgmod_substation",
                        sql=resources.read_text(__name__, "substation_otg.sql"),
                        postgres_conn_id="egon_data",
                        autocommit=True,
                    ),
                    to_pypsa
              #  }
            ),
        )
