# -------------------------------------------------------------------------------
# Name:         sql_attribution
# Purpose:      SQL algorithm for attributing conversions to marketing points
# Comments:     joao.natali@team.neustar
# -------------------------------------------------------------------------------
import argparse
import sys
from typing import Optional
import json
import logging
import py_path
from utils.hive.hive_query import hive_query


class Model:
    """Model representation"""

    def __init__(self, model_json_path: str):
        with open(model_json_path, "r") as f:
            self.model_dict = json.load(f)

        self.features, self.coefficients, self.marketing_flag = zip(
            *[
                (
                    f["ModelVarName"],
                    f["ModelVarCoeff"],
                    bool(int(f["ModelVarMktgFlag"])),
                )
                for f in self.model_dict["ModelVarList"]
                if f["ModelVarCoeff"] != ""   # and float(f["ModelVarCoeff"]) != 0
                and int(f["flagExclude"]) == 0
            ]
        )

        intercept_lookup = [
            float(f["ModelVarCoeff"])
            for f in self.model_dict["ModelVarList"]
            if f["ModelVarName"].lower() == "intercept"
        ]
        assert len(intercept_lookup) == 1, "Wrong number of intercepts in model JSON"
        self.intercept = intercept_lookup[0]

    @property
    def mkt_features(self):
        return (f for (f, m) in zip(self.features, self.marketing_flag) if m is True)

    @property
    def non_mkt_features(self):
        return (
            f for (f, m) in zip(self.features, self.marketing_flag) if m is not True
        )

    @property
    def all_terms(self):
        return " + ".join(
            ["{}*{}".format(c, f) for (c, f) in zip(self.coefficients, self.features)]
        )

    @property
    def non_mkt_terms(self):
        return " + ".join(
            [
                "{}*{}".format(c, f)
                for (c, f, m) in zip(
                    self.coefficients, self.features, self.marketing_flag
                )
                if m is not True
            ]
        )

    @property
    def base_prob(self):
        terms = self.non_mkt_terms
        if len(terms) == 0:
            argument = self.intercept
        else:
            argument = "{} + {}".format(self.intercept, terms)
        return "1/(1+exp(-({})))".format(argument)

    @property
    def total_prob(self):
        terms = self.all_terms
        if len(terms) == 0:
            argument = self.intercept
        else:
            argument = "{} + {}".format(self.intercept, terms)
        return "1/(1+exp(-({})))".format(argument)


def gen_subtractive_incr_prob(model: Model):
    """Generates subtractive incremental probability SQL code"""
    raw_prob_list = [
        "total_prob - {prob} AS raw_incr_prob_{feature}".format(
            prob="1/(1+exp(-({} + total_terms - {})))".format(
                model.intercept, "{}*{}".format(c, f)
            ),
            feature=f,
        )
        for (f, c, m) in zip(model.features, model.coefficients, model.marketing_flag)
        if m
    ]
    return raw_prob_list


def gen_additive_incr_prob(model: Model):
    """Generates additive incremental probability SQL code"""
    raw_prob_list = [
        "case when {nonzero_attr} then ({prob} - base_prob) else 0.0 end AS raw_incr_prob_{feature}".format(
            prob="1/(1+exp(-({} + base_terms + {})))".format(
                model.intercept, "{}*{}".format(c, f)
            ),
            feature=f,
            nonzero_attr="abs({}) > 0.0000000001".format("{}*{}".format(c, f)),
        )
        for (f, c, m) in zip(model.features, model.coefficients, model.marketing_flag)
        if m
    ]
    return raw_prob_list


def attribution(
    stack: str,
    dest_table: str,
    db: str,
    model_json: str,
    stack_filter: str,
    config: Optional[str] = None,
    subtractive: bool = True,
    dry_run: bool = False,
):
    """
    Attributes observed conversions to different model features

    Parameters:
    -----------
    stack :
        Name of modeling stack containing model features
    dest_table :
        Table in which to store attribution results
    db :
        Destination DB for results
    model_json :
        Path to model.JSON file
    stack_filter:
        Stack SQL filter to be added to WHERE clause
    config :
        Client platform_config.xml path
    subtractive :
        Flag defining if attribution is done with subtractive approach

    Returns
    -------
    """

    if subtractive:
        gen_incr_prob = gen_subtractive_incr_prob
    else:
        gen_incr_prob = gen_additive_incr_prob

    model = Model(model_json)

    # Supporting terms
    query_terms = """
        DROP TABLE IF EXISTS {db}.analytics_attr_prob_terms;
        DROP VIEW IF EXISTS {db}.analytics_attr_prob_terms;
        CREATE TABLE {db}.analytics_attr_prob_terms AS
        SELECT
            userid,
            ref_timestamp,
            actuuid,
            {mkt_features},
            {base_terms} AS base_terms,
            {total_terms} AS total_terms,
            {base_prob} AS base_prob,
            {total_prob} AS total_prob
        FROM
            {stack}
        WHERE
            response = 1 AND
            {stack_filter}
        ;""".format(
        db=db,
        mkt_features=", ".join(["{}".format(f) for f in model.mkt_features]),
        base_terms=model.non_mkt_terms,
        total_terms=model.all_terms,
        base_prob=model.base_prob,
        total_prob=model.total_prob,
        stack=stack,
        stack_filter=stack_filter,
    )
    logging.info("Query precursor terms:\n{}".format(query_terms))
    if not dry_run:
        hive_query(query_terms, config=config, raw=True)

    # Raw incremental probabilities
    query_raw_prob = """
    DROP TABLE IF EXISTS {db}.analytics_attr_raw_incr_prob;
    DROP VIEW IF EXISTS {db}.analytics_attr_raw_incr_prob;
    CREATE TABLE {db}.analytics_attr_raw_incr_prob AS
    SELECT
        userid,
        ref_timestamp,
        actuuid,
        {mkt_features},
        base_prob,
        total_prob,
        (base_prob)/(total_prob) AS base_value,
        1 AS total_value,
        {raw_incr_prob},
        {raw_prob_sum} AS raw_prob_sum
    FROM
        {db}.analytics_attr_prob_terms
    ;""".format(
        db=db,
        mkt_features=", ".join(["{}".format(f) for f in model.mkt_features]),
        raw_incr_prob=",\n    ".join(gen_incr_prob(model)),
        raw_prob_sum=" + ".join([t.split(" AS ")[0] for t in gen_incr_prob(model)]),
    )
    logging.info("Query raw probability:\n{}".format(query_raw_prob))
    if not dry_run:
        hive_query(query_raw_prob, config=config, raw=True)

    # Attribution
    rel_attr_pct = ",\n".join(
        [
            "{1}/((CASE WHEN raw_prob_sum > 0 OR raw_prob_sum < 0 THEN raw_prob_sum ELSE 1 END)) AS relattr_{0}".format(f, "raw_incr_prob_{}".format(f))
            for f in model.mkt_features
        ]
    )

    mar_attr = ",\n".join(
        [
            "(total_value - base_value) * {1}/(CASE WHEN raw_prob_sum > 0 OR raw_prob_sum < 0 THEN raw_prob_sum ELSE 1 END) AS marattr_{0}".format(
                f, "raw_incr_prob_{}".format(f)
            )
            for f in model.mkt_features
        ]
    )
    query_attribution = """
    DROP TABLE IF EXISTS {dest_table};
    DROP VIEW IF EXISTS {dest_table};
    CREATE TABLE {dest_table} AS
    SELECT
        userid,
        ref_timestamp,
        actuuid,
        {mkt_features},
        base_prob,
        total_prob,
        base_value,
        total_value,
        raw_prob_sum,
        {rel_attr_pct},
        {mar_attr}
    FROM {db}.analytics_attr_raw_incr_prob
    ;""".format(
        db=db,
        dest_table=dest_table,
        mkt_features=", ".join(["{}".format(f) for f in model.mkt_features]),
        raw_prob_sum=" + ".join(
            ["raw_incr_prob_{}".format(f) for f in model.mkt_features]
        ),
        rel_attr_pct=rel_attr_pct,
        mar_attr=mar_attr,
    )
    logging.info("Query attribution:\n{}".format(query_attribution))
    if not dry_run:
        hive_query(query_attribution, config=config, raw=True)


def create_parser():
    parser = argparse.ArgumentParser(
        description="Generates attribution to features using SQL queries"
    )
    parser.add_argument(
        "--config",
        dest="config",
        action="store",
        required=True,
        type=str,
        help="Path to the platform_config.xml file for the client",
    )
    parser.add_argument(
        "--stack",
        dest="stack",
        metavar="db_name.table_name",
        action="store",
        type=str,
        required=True,
        help="Name of the modeling stack in Hive",
    )
    parser.add_argument(
        "--dest-table",
        dest="dest_table",
        metavar="db_name.table_name",
        action="store",
        type=str,
        required=True,
        help="Name of the attribution table to be created in Hive",
    )
    parser.add_argument(
        "--db",
        dest="db",
        metavar="db_name",
        action="store",
        type=str,
        required=True,
        help="Database to store intermediate tables",
    )
    parser.add_argument(
        "--model-json",
        dest="model_json",
        metavar="/path/to/model.json",
        action="store",
        required=True,
        type=str,
        help="Path to the JSON file containing the model specification",
    )
    parser.add_argument(
        "--stack-filter",
        dest="stack_filter",
        metavar="var='value'",
        action="store",
        required=True,
        type=str,
        help="SQL WHERE clause to filter stack",
    )
    parser.add_argument(
        "--subtractive",
        dest="subtractive",
        action="store_true",
        help="If passed, subtractive attribution will be used instead of additive",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="If passed, no Hive calls are made.",
    )
    parser.add_argument(
        "--log",
        dest="loglevel",
        action="store",
        choices=["WARNING", "INFO", "DEBUG"],
        required=False,
        default="INFO",
        type=str,
        help="logging level",
    )
    return parser


if __name__ == "__main__":

    parser = create_parser()
    args = parser.parse_args()

    # TODO: Validating inputs

    # Setting up logging
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % args.loglevel)

    stdout_handler = logging.StreamHandler(sys.stdout)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s %(levelname)s %(thread)d: %(message)s",
        handlers=[stdout_handler],
    )

    logging.info("Starting... args:\n{}".format(args))
    logging.info("Package lookup path:\n{}".format(sys.path))

    attribution(
        stack=args.stack,
        dest_table=args.dest_table,
        db=args.db,
        model_json=args.model_json,
        stack_filter=args.stack_filter,
        config=args.config,
        subtractive=args.subtractive,
        dry_run=args.dry_run,
    )
